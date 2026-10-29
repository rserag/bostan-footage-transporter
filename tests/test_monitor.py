import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("monitor", Path(__file__).parents[1] / "src/bostan_monitor.py")
monitor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(monitor)
NOW = 1800000000


def pi_sample():
    return dict(version=1, timestamp=NOW, record_up=1, publish_up=1,
                active_token="opaque", active_bytes=1000, active_mtime=NOW,
                oldest_active_start=NOW - 1800, latest_closed=NOW - 1800,
                segment_seconds=3600, spool_bytes=512 * 1024**2,
                free_bytes=10 * 1024**3, reserve_bytes=2 * 1024**3,
                max_spool_bytes=8 * 1024**3, pending_count=0, pending_bytes=0,
                oldest_pending=NOW, recent_bytes_per_second=93000, invalid_entries=0)


def server_sample():
    return dict(version=1, timestamp=NOW, expected=[dict(path_token="indexed", published=NOW - 3600)],
                missing_assets=0, intentional_deletions=16)


class MonitorTests(unittest.TestCase):
    def evaluate(self, pi=None, server=None, indexed=None, state=None, current=NOW):
        return monitor.evaluate({}, state if state is not None else {},
                                pi if pi is not None else pi_sample(),
                                server if server is not None else server_sample(),
                                indexed if indexed is not None else {"indexed"}, current)

    def test_healthy_pipeline_and_conservative_capacity(self):
        conditions, metrics = self.evaluate()
        self.assertFalse(any(conditions.values()))
        self.assertEqual(metrics["archive_intentional_deletions"], 16)
        self.assertAlmostEqual(metrics["pi_buffer_seconds"], 7.5 * 1024**3 / (93000 * 1.2))

    def test_growth_requires_bytes_to_advance(self):
        state = {}
        pi = pi_sample()
        self.evaluate(pi=pi, state=state)
        for elapsed in (60, 120, 181):
            pi["timestamp"] = NOW + elapsed
            # Touching a file without growing it must not fake capture health.
            pi["active_mtime"] = NOW + elapsed
            conditions, metrics = self.evaluate(pi=pi, state=state, current=NOW + elapsed)
        self.assertTrue(conditions["capture_stalled"])
        self.assertEqual(metrics["pi_growth_age_seconds"], 181)
        pi["active_bytes"] += 1
        conditions, _ = self.evaluate(pi=pi, state=state, current=NOW + 181)
        self.assertFalse(conditions["capture_stalled"])

    def test_hour_rotation_and_first_stale_file(self):
        state = {"growth": dict(token="previous-hour", size=9000, last=NOW - 60)}
        conditions, _ = self.evaluate(state=state)
        self.assertFalse(conditions["capture_stalled"])
        pi = pi_sample()
        pi["active_mtime"] = NOW - 300
        conditions, _ = self.evaluate(pi=pi)
        self.assertTrue(conditions["capture_stalled"])

    def test_services_late_segments_queue_and_storage_guards(self):
        pi = pi_sample()
        pi.update(record_up=0, publish_up=0, oldest_active_start=NOW - 4201,
                  pending_count=2, pending_bytes=123, oldest_pending=NOW - 901,
                  invalid_entries=1, free_bytes=2 * 1024**3)
        conditions, metrics = self.evaluate(pi=pi)
        for name in ("recorder_down", "publisher_down", "late_segment", "upload_backlog",
                     "queue_invalid", "buffer_low", "buffer_critical"):
            self.assertTrue(conditions[name], name)
        self.assertEqual(metrics["pi_buffer_seconds"], 0)
        pi.update(free_bytes=10 * 1024**3, spool_bytes=8 * 1024**3)
        self.assertTrue(self.evaluate(pi=pi)[0]["buffer_critical"])

    def test_index_grace_and_missing_archive(self):
        server = server_sample()
        server["expected"].append(dict(path_token="new", published=NOW - 599))
        self.assertFalse(self.evaluate(server=server)[0]["jellyfin_missing"])
        server["expected"][-1]["published"] = NOW - 601
        server["missing_assets"] = 1
        conditions, metrics = self.evaluate(server=server)
        self.assertTrue(conditions["jellyfin_missing"])
        self.assertTrue(conditions["archive_missing"])
        self.assertEqual(metrics["jellyfin_missing_count"], 1)

    def test_unreachable_gates_children_and_clock_skew(self):
        conditions, metrics = monitor.evaluate({}, {}, None, None, None, NOW)
        self.assertTrue(conditions["pi_unreachable"])
        self.assertTrue(conditions["server_unreachable"])
        self.assertIsNone(conditions["capture_stalled"])
        self.assertIsNone(conditions["archive_missing"])
        self.assertIsNone(conditions["jellyfin_missing"])
        pi = pi_sample()
        pi["timestamp"] -= 121
        self.assertTrue(self.evaluate(pi=pi)[0]["clock_skew"])

    def test_failure_grace_recovery_dedup_and_restart(self):
        state, sent = {}, []
        send = lambda config, text: sent.append(text)
        monitor.advance(state, {"pi_unreachable": True}, NOW)
        monitor.advance(state, {"pi_unreachable": True}, NOW + 179)
        monitor.notify({"telegram": {}}, state, NOW + 179, send)
        self.assertEqual(sent, [])
        monitor.advance(state, {"pi_unreachable": True}, NOW + 180)
        monitor.notify({"telegram": {}}, state, NOW + 180, send)
        state = json.loads(json.dumps(state))  # survives process/service restart
        monitor.notify({"telegram": {}}, state, NOW + 9999, send)
        self.assertEqual(len(sent), 1)
        monitor.advance(state, {"pi_unreachable": False}, NOW + 10000)
        monitor.advance(state, {"pi_unreachable": False}, NOW + 10119)
        monitor.notify({"telegram": {}}, state, NOW + 10119, send)
        self.assertEqual(len(sent), 1)
        monitor.advance(state, {"pi_unreachable": False}, NOW + 10120)
        monitor.notify({"telegram": {}}, state, NOW + 10120, send)
        self.assertEqual(len(sent), 2)
        self.assertIn("FAILURE", sent[0])
        self.assertIn("RECOVERED", sent[1])

    def test_unknown_does_not_resolve_failure_or_keep_pending_grace(self):
        state = {}
        monitor.advance(state, {"recorder_down": True}, NOW)
        monitor.advance(state, {"recorder_down": True}, NOW + 120)
        monitor.advance(state, {"recorder_down": None}, NOW + 500)
        self.assertTrue(state["alarms"]["recorder_down"]["active"])
        state = {}
        monitor.advance(state, {"recorder_down": True}, NOW)
        monitor.advance(state, {"recorder_down": None}, NOW + 200)
        monitor.advance(state, {"recorder_down": True}, NOW + 500)
        self.assertFalse(state["alarms"]["recorder_down"]["active"])

    def test_notification_failure_backoff_and_retry(self):
        state, sent = {}, []
        monitor.advance(state, {"capture_stalled": True}, NOW)
        def failed(config, text):
            sent.append(text)
            raise RuntimeError("secret credential URL must not escape")
        self.assertFalse(monitor.notify({"telegram": {}}, state, NOW, failed))
        self.assertFalse(monitor.notify({"telegram": {}}, state, NOW + 30, failed))
        self.assertEqual(len(sent), 1)
        self.assertFalse(state["alarms"]["capture_stalled"]["notified"])
        self.assertTrue(monitor.notify({"telegram": {}}, state, NOW + 60,
                                       lambda c, t: sent.append(t)))
        self.assertTrue(state["alarms"]["capture_stalled"]["notified"])

    def test_unsent_incident_recovery_does_not_send_stale_failure(self):
        state = {}
        monitor.advance(state, {"capture_stalled": True}, NOW)
        monitor.advance(state, {"capture_stalled": False}, NOW + 1)
        monitor.advance(state, {"capture_stalled": False}, NOW + 121)
        sent = []
        monitor.notify({"telegram": {}}, state, NOW + 121, lambda c, t: sent.append(t))
        self.assertEqual(sent, [])

    def test_atomic_state_and_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            monitor.atomic_write(path, '{"ok":true}')
            self.assertTrue(monitor.load(path)["ok"])
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(list(Path(tmp).iterdir()), [path])
        state = {}
        conditions, metrics = self.evaluate(state=state)
        monitor.advance(state, conditions, NOW)
        text = monitor.prometheus(metrics, state, conditions, NOW)
        self.assertIn("bostan_monitor_timestamp_seconds", text)
        self.assertNotIn("opaque", text)
        self.assertNotIn("indexed", text)
        with self.assertRaises(ValueError):
            monitor.prometheus({"bad": float("nan")}, state, conditions, NOW)

    def test_complete_collector_cycle_persists_sanitized_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = dict(state_file=tmp + "/state.json", metrics_file=tmp + "/bostan.prom",
                          pi={}, server={}, jellyfin={}, telegram={})
            with patch.object(monitor, "ssh_probe", side_effect=[pi_sample(), server_sample()]), \
                    patch.object(monitor, "jellyfin_tokens", return_value={"indexed"}), \
                    patch.object(monitor.time, "time", return_value=NOW), \
                    patch("sys.stdout", new_callable=__import__("io").StringIO) as out:
                monitor.collect(config)
            data = json.loads(out.getvalue())
            self.assertEqual(data["notifications_ok"], 1)
            self.assertEqual(data["active_checks"], [])
            self.assertIn("bostan_monitor_timestamp_seconds", Path(config["metrics_file"]).read_text())
            self.assertEqual(len(monitor.load(config["state_file"])["alarms"]), len(monitor.RULES))

    def test_pi_probe_excludes_acknowledged_grace_copies_and_bad_queue(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for directory in ("active/run", "ready", "receipts", ".ssh"):
                (root / directory).mkdir(parents=True)
            # Neither camera env nor SSH key is read or exposed by the probe.
            (root / ".ssh/id_ed25519").write_text("PRIVATE_SENTINEL")
            active = root / "active/run/20270115T070000Z.mkv"
            active.write_bytes(b"a" * 20)
            os.utime(active, (NOW, NOW))
            ident = "a" * 32 + "-20270115T065820Z"
            manifest = dict(id=ident, size=10, sha256="a" * 64, duration_seconds=100,
                            start_utc="2027-01-15T06:58:20+00:00", end_utc="2027-01-15T07:00:00+00:00",
                            closed_utc="2027-01-15T07:00:00+00:00")
            (root / "ready" / (ident + ".json")).write_text(json.dumps(manifest))
            (root / "ready" / (ident + ".mkv")).write_bytes(b"b" * 10)
            (root / "receipts" / (ident + ".json")).write_text(json.dumps(dict(
                receipt=dict(status="committed", id=ident, size=10, sha256="a" * 64), verified_at=NOW - 100)))
            (root / "ready/bad.json").write_text('{"camera":"rtsp://PRIVATE_SENTINEL"}')
            data = monitor.probe_pi(dict(spool=tmp, segment_seconds=3600, reserve_bytes=1,
                                         max_spool_bytes=1000000), clock=lambda: NOW, services=lambda u: 1)
            self.assertEqual(data["pending_count"], 0)
            self.assertEqual(data["invalid_entries"], 1)
            self.assertNotIn("PRIVATE_SENTINEL", json.dumps(data))
            self.assertNotIn(tmp, json.dumps(data))
            self.assertEqual(data["active_bytes"], 20)

    def test_deferred_items_remain_visible_and_reduce_available_buffer(self):
        core_spec = importlib.util.spec_from_file_location("queue_core", Path(monitor.__file__).with_name("bostan.py"))
        core = importlib.util.module_from_spec(core_spec)
        core_spec.loader.exec_module(core)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ready").mkdir()
            ident = "a" * 32 + "-20270115T065820Z"
            manifest = dict(id=ident, size=1024, sha256="a" * 64, duration_seconds=100,
                            start_utc="2027-01-15T06:58:20+00:00", end_utc="2027-01-15T07:00:00+00:00",
                            closed_utc="2027-01-15T07:00:00+00:00", timestamp_basis="test", close_reason="test")
            path = root / "ready" / (ident + ".json")
            path.write_text(json.dumps(manifest))
            path.with_suffix(".mkv").write_bytes(b"x" * 1024)
            config = dict(spool=tmp, segment_seconds=3600, reserve_bytes=0, max_spool_bytes=1000000,
                          grace_seconds=7200)
            with patch.object(core.time, "time", return_value=NOW), patch.object(core, "remote") as remote:
                core.publish_once(config)
            remote.assert_not_called()
            before = monitor.probe_pi(config, clock=lambda: NOW, services=lambda u: 1)
            self.assertEqual(before["pending_count"], 1)
            self.assertEqual(before["invalid_entries"], 1)
            self.assertGreaterEqual(before["spool_bytes"], 1024 + path.stat().st_size)
            conditions, metrics = self.evaluate(pi=before)
            self.assertTrue(conditions["queue_invalid"])
            self.assertEqual(metrics["pi_buffer_bytes"], 1000000 - before["spool_bytes"])
            # Shared network outages are not mislabeled as corrupt recordings.
            core.clear_retry(core.retry_path(root, path))
            core.defer(root / "retry/service.json", None, "service", "service_unavailable", NOW, maximum=300)
            self.assertEqual(monitor.probe_pi(config, clock=lambda: NOW, services=lambda u: 1)["invalid_entries"], 0)
            # Missing local footage with no receipt, bad receipt times and bad
            # start times are visible even before the publisher sees them.
            path.with_suffix(".mkv").unlink()
            self.assertGreater(monitor.probe_pi(config, clock=lambda: NOW, services=lambda u: 1)["invalid_entries"], 0)
            (root / "receipts" / path.name).write_text(json.dumps(dict(receipt=dict(manifest, status="committed"),
                                                                     verified_at=float("nan"))))
            self.assertGreater(monitor.probe_pi(config, clock=lambda: NOW, services=lambda u: 1)["invalid_entries"], 0)
            manifest["start_utc"] = "bad"
            path.write_text(json.dumps(manifest))
            data = monitor.probe_pi(config, clock=lambda: NOW, services=lambda u: 1)
            self.assertGreater(data["invalid_entries"], 0)
            self.assertNotIn(tmp, json.dumps(data))

    def test_queue_probe_bounds_metadata_and_rejects_fifo_receipt_without_blocking(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ready").mkdir()
            (root / "receipts").mkdir()
            oversized = root / "ready/oversized.json"
            oversized.write_text(" " * 65536 + "{}")
            with self.assertRaises(ValueError):
                monitor.load_queue_json(oversized)
            ident = "a" * 32 + "-20270115T065820Z"
            manifest = dict(id=ident, size=10, sha256="a" * 64, duration_seconds=100,
                            start_utc="2027-01-15T06:58:20+00:00", end_utc="2027-01-15T07:00:00+00:00",
                            closed_utc="2027-01-15T07:00:00+00:00")
            (root / "ready" / (ident + ".json")).write_text(json.dumps(manifest))
            (root / "ready" / (ident + ".mkv")).write_bytes(b"b" * 10)
            os.mkfifo(root / "receipts" / (ident + ".json"))
            config = root / "config.json"
            config.write_text(json.dumps(dict(spool=tmp, segment_seconds=3600, reserve_bytes=0,
                                             max_spool_bytes=1000000)))
            code = ("import json,runpy,sys; m=runpy.run_path(sys.argv[1]); "
                    "print(json.dumps(m['probe_pi'](m['load'](sys.argv[2]),services=lambda unit:1)))")
            result = subprocess.run([sys.executable, "-c", code, str(Path(monitor.__file__)), str(config)],
                                    capture_output=True, text=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)
            data = json.loads(result.stdout)
            self.assertGreaterEqual(data["invalid_entries"], 2)
            self.assertEqual(data["active_bytes"], 0)
            self.assertEqual(data["pending_count"], 1)

    def test_server_probe_is_read_only_and_honors_only_deleted_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base, state = root / "library", root / "state"
            base.mkdir()
            state.mkdir()
            db_path = state / "receipts.sqlite3"
            with sqlite3.connect(db_path) as db:
                db.execute("CREATE TABLE recordings(id TEXT, size INTEGER, path TEXT, committed INTEGER)")
                for ident in ("keep", "deleted", "planned", "missing"):
                    db.execute("INSERT INTO recordings VALUES(?,?,?,1)", (ident, 3, str(base / (ident + ".mkv"))))
            for suffix in (".mkv", ".json", ".nfo"):
                (base / ("keep" + suffix)).write_text("abc")
            (state / "intentional-deletions.json").write_text(json.dumps(dict(
                deleted=dict(status="deleted"), planned=dict(status="planned"))))
            before = db_path.read_bytes()
            data = monitor.probe_server(dict(library_root=str(base), state=str(state)), clock=lambda: NOW)
            self.assertEqual(db_path.read_bytes(), before)
            self.assertEqual(data["intentional_deletions"], 1)
            self.assertEqual(data["missing_assets"], 2)
            self.assertEqual(data["expected"][0]["path_token"], monitor.token("keep.mkv"))
            self.assertNotIn(tmp, json.dumps(data))
            self.assertEqual(len(list(state.iterdir())), 2)

    def test_cli_failure_does_not_expose_exception_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "PRIVATE_SENTINEL.json"
            path.write_text("invalid JSON PRIVATE_SENTINEL")
            result = subprocess.run([sys.executable, str(Path(monitor.__file__)), "probe-pi",
                                     "--config", str(path)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertNotIn("PRIVATE_SENTINEL", result.stdout + result.stderr)
            self.assertNotIn("Traceback", result.stderr)

    def test_ssh_is_pinned_noninteractive_and_restricted(self):
        with patch.object(monitor.subprocess, "run") as run:
            run.return_value.stdout = b'{"version":1}'
            monitor.ssh_probe(dict(destination="observer@host", ssh_key="key", known_hosts="pinned"))
            args = run.call_args.args[0]
            self.assertIn("StrictHostKeyChecking=yes", args)
            self.assertIn("IdentitiesOnly=yes", args)
            self.assertEqual(args[-1], "status")
            self.assertEqual(run.call_args.kwargs["stdin"], subprocess.DEVNULL)
        with self.assertRaises(ValueError):
            monitor.ssh_probe(dict(destination="-oProxyCommand=bad"))


if __name__ == "__main__":
    unittest.main()
