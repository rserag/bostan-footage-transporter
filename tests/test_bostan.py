import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import os
import shutil
from pathlib import Path
import subprocess
import sqlite3
import tempfile
import sys
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "bostan", Path(__file__).parents[1] / "src/bostan.py"
)
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)


class SafetyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory()
        cls.video = Path(cls.fixture.name) / "test.mkv"
        subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=160x120:rate=10",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:sample_rate=8000",
                "-t",
                "2",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-c:a",
                "pcm_alaw",
                str(cls.video),
            ],
            check=True,
        )
        cls.content = cls.video.read_bytes()

    @classmethod
    def tearDownClass(cls):
        cls.fixture.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.library = self.root / "show"
        self.library.mkdir()
        self.c = dict(
            server_reserve_bytes=0,
            library_root=str(self.library),
            state=str(self.root / "state"),
            timezone="Asia/Yerevan",
            season_epoch="2026-10",
            show_title="Camera & garden",
        )
        self.m = dict(
            id="a" * 32 + "-20261029T120000Z",
            sha256=hashlib.sha256(self.content).hexdigest(),
            size=len(self.content),
            start_utc="2026-10-29T12:00:00+00:00",
            end_utc="2026-10-29T12:00:02+00:00",
            closed_utc="2026-10-29T12:00:03+00:00",
            duration_seconds=2,
            partial=True,
            timestamp_basis="recorder clock",
            close_reason="test",
        )

    def request(self, action="upload", m=None, body=None):
        m = m or self.m
        stream = io.BytesIO(
            (json.dumps(dict(action=action, manifest=m)) + "\n").encode()
            + (self.content if body is None and action == "upload" else body or b"")
        )
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            b.receive(self.c, stream)
        return json.loads(out.getvalue())

    def test_roundtrip_metadata_and_idempotence(self):
        a = self.request()
        self.assertTrue(b.receipt_matches(a, self.m))
        self.assertEqual(a, self.request())
        self.assertEqual(a, self.request("verify"))
        self.assertEqual(len(list(self.library.rglob("*.mkv"))), 1)
        self.assertIn("Camera &amp; garden", (self.library / "tvshow.nfo").read_text())

    def test_interrupted_upload_never_published(self):
        with self.assertRaises(ValueError):
            self.request(body=self.content[:100])
        self.assertEqual(list(self.library.rglob("*.mkv")), [])
        self.assertTrue(b.receipt_matches(self.request(), self.m))

    def test_lost_commit_receipt_retry(self):
        ack = self.request()
        with contextlib.closing(
            sqlite3.connect(Path(self.c["state"]) / "receipts.sqlite3")
        ) as db:
            db.execute("UPDATE recordings SET committed=0")
            db.commit()
        self.assertEqual(self.request(), ack)
        self.assertEqual(len(list(self.library.rglob("*.mkv"))), 1)

    def test_hash_mismatch_retains_no_published_video(self):
        m = copy.deepcopy(self.m)
        m["sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            self.request(m=m)
        self.assertEqual(list(self.library.rglob("*.mkv")), [])

    def test_traversal_rejected(self):
        m = copy.deepcopy(self.m)
        m["id"] = "../../escape"
        with self.assertRaises(ValueError):
            self.request(m=m)

    def test_corrupted_remote_not_acknowledged(self):
        a = self.request()
        Path(a["path"]).write_bytes(b"broken")
        with self.assertRaises(ValueError):
            self.request("verify")

    def test_missing_committed_video_is_not_resurrected_by_upload_retry(self):
        ack = self.request()
        Path(ack["path"]).unlink()
        with self.assertRaises(b.ItemFailure):
            self.request("upload")
        self.assertFalse(Path(ack["path"]).exists())
        with contextlib.closing(sqlite3.connect(Path(self.c["state"]) / "receipts.sqlite3")) as db:
            self.assertEqual(db.execute("SELECT committed,episode FROM recordings").fetchall(), [(1, 1)])

    def test_missing_metadata_not_acknowledged(self):
        a = self.request()
        Path(a["path"]).with_suffix(".json").unlink()
        with self.assertRaises(ValueError):
            self.request("verify")

    def test_changed_manifest_not_acknowledged(self):
        self.request()
        m = copy.deepcopy(self.m)
        m["close_reason"] = "modified"
        with self.assertRaises(ValueError):
            self.request("verify", m=m)

    def queue(self):
        c = dict(
            spool=str(self.root / "spool"),
            reserve_bytes=0,
            max_spool_bytes=10**9,
            grace_seconds=0,
        )
        ready = Path(c["spool"]) / "ready"
        ready.mkdir(parents=True)
        p = ready / (self.m["id"] + ".mkv")
        p.write_bytes(self.content)
        b.atomic_json(p.with_suffix(".json"), self.m)
        return c, p

    def test_no_receipt_no_deletion(self):
        c, p = self.queue()
        with patch.object(b, "remote", side_effect=b.ServiceUnavailable("offline")):
            b.publish_once(c)
        self.assertTrue(p.exists())

    def test_remote_recheck_required_before_delete(self):
        c, p = self.queue()
        ack = dict(self.m, status="committed")
        with patch.object(b, "remote", return_value=ack):
            b.publish_once(c)
        self.assertTrue(p.exists())
        with patch.object(b, "remote", side_effect=b.ItemFailure("remote_rejected")):
            b.publish_once(c)
        self.assertTrue(p.exists())
        with patch.object(b.time, "time", return_value=b.time.time() + 61), \
                patch.object(b, "remote", return_value=ack):
            b.publish_once(c)
        self.assertFalse(p.exists())

    def later(self, c, number=1):
        m = dict(self.m, id=f"{number:032x}-20261029T130000Z",
                 start_utc="2026-10-29T13:00:00+00:00", end_utc="2026-10-29T13:00:02+00:00")
        p = Path(c["spool"]) / "ready" / (m["id"] + ".mkv")
        p.write_bytes(self.content)
        b.atomic_json(p.with_suffix(".json"), m)
        return m, p

    def test_bad_manifests_and_names_do_not_block_later_uploads(self):
        for change in ("json", "timestamp", "time_type", "shape", "filename", "id_date", "duration"):
            with self.subTest(change=change):
                c, p = self.queue()
                later, healthy = self.later(c)
                manifest = p.with_suffix(".json")
                m = dict(self.m)
                if change == "json":
                    manifest.write_text('{"url":"rtsp://PRIVATE_SENTINEL"')
                elif change == "shape":
                    manifest.write_text("[]")
                elif change == "filename":
                    manifest.rename(manifest.with_name("unsafe-name.json"))
                else:
                    key, value = dict(timestamp=("start_utc", "invalid"), time_type=("end_utc", []),
                                      id_date=("id", "a" * 32 + "-20261399T120000Z"),
                                      duration=("duration_seconds", float("nan")))[change]
                    m[key] = value
                    b.atomic_json(manifest, m)
                with patch.object(b, "remote", return_value=dict(later, status="committed")) as remote, \
                        self.assertLogs(b.LOG, level="WARNING") as logs:
                    b.publish_once(c)
                self.assertEqual([call.args[1]["id"] for call in remote.call_args_list], [later["id"]])
                self.assertTrue(p.exists())
                self.assertTrue(healthy.exists())
                self.assertNotIn("PRIVATE_SENTINEL", " ".join(logs.output))
                retry_files = list((Path(c["spool"]) / "retry").glob("*.json"))
                self.assertEqual(len(retry_files), 1)
                self.assertNotIn("PRIVATE_SENTINEL", retry_files[0].read_text())
                shutil.rmtree(Path(c["spool"]))

    def test_changed_and_missing_videos_are_retained_and_isolated(self):
        c, p = self.queue()
        later, _ = self.later(c)
        missing, missing_p = self.later(c, 2)
        missing_p.unlink()
        # Same length so the hash check, rather than size alone, must detect this.
        p.write_bytes(b"x" * len(self.content))
        with patch.object(b, "remote", return_value=dict(later, status="committed")) as remote:
            b.publish_once(c)
        self.assertEqual(remote.call_count, 1)
        self.assertEqual(remote.call_args.args[1]["id"], later["id"])
        self.assertTrue(p.exists())
        self.assertTrue(missing_p.with_suffix(".json").exists())
        reasons = {json.loads(f.read_text())["reason"] for f in (Path(c["spool"]) / "retry").glob("*.json")}
        self.assertEqual(reasons, {"video_changed", "video_missing"})

    def test_corrupt_saved_receipt_never_overwritten_or_used_for_cleanup(self):
        for saved in ("{", "[]", json.dumps(dict(receipt=dict(self.m, status="committed"), verified_at=float("nan"))),
                      json.dumps(dict(receipt=dict(self.m, status="committed", sha256="0" * 64), verified_at=1))):
            with self.subTest(saved=saved[:10]):
                c, p = self.queue()
                later, _ = self.later(c)
                rec = Path(c["spool"]) / "receipts" / (self.m["id"] + ".json")
                rec.parent.mkdir()
                rec.write_text(saved)
                with patch.object(b, "remote", return_value=dict(later, status="committed")) as remote:
                    b.publish_once(c)
                self.assertEqual(remote.call_count, 1)
                self.assertTrue(p.exists())
                self.assertEqual(rec.read_text(), saved)
                shutil.rmtree(Path(c["spool"]))

    def test_arbitrary_filename_and_symlink_are_not_read_or_logged(self):
        c, p = self.queue()
        ready = p.parent
        # Invalid filesystem bytes and newline characters must not poison sorting
        # or be inserted into diagnostics. The retry key uses filesystem bytes.
        raw_name = b"\xff-PRIVATE_SENTINEL\n.json" if sys.platform == "linux" else b"bad-PRIVATE_SENTINEL\n.json"
        bad = ready / os.fsdecode(raw_name)
        bad.write_text("broken")
        bad.with_suffix(".mkv").write_bytes(b"retained")
        external = self.root / "private.json"
        external.write_text(json.dumps(self.m))
        link = ready / "untrusted.json"
        link.symlink_to(external)
        with patch.object(b, "remote", return_value=dict(self.m, status="committed")) as remote, \
                self.assertLogs(b.LOG, level="WARNING") as logs:
            b.publish_once(c)
        remote.assert_called_once()
        self.assertNotIn("PRIVATE_SENTINEL", " ".join(logs.output))
        self.assertTrue(bad.with_suffix(".mkv").exists())
        self.assertTrue(external.exists())
        self.assertTrue(link.is_symlink())

    def test_storage_pressure_cleans_only_verified_later_copy(self):
        c, p = self.queue()
        p.write_bytes(b"broken")
        c.update(grace_seconds=7200, reserve_bytes=10**18)
        later, healthy = self.later(c)
        rec = Path(c["spool"]) / "receipts" / (later["id"] + ".json")
        rec.parent.mkdir()
        ack = dict(later, status="committed")
        b.atomic_json(rec, dict(receipt=ack, verified_at=b.time.time()))
        with patch.object(b, "remote", return_value=ack) as remote:
            b.publish_once(c)
        remote.assert_called_once()
        self.assertEqual(remote.call_args.args[2], "verify")
        self.assertTrue(p.exists())
        self.assertFalse(healthy.exists())
        self.assertFalse(b.space_ok(c))

    def test_real_remote_corruption_isolated_later_delivery_and_grace_cleanup(self):
        c, p = self.queue()
        c["grace_seconds"] = 120
        ack = self.request()
        rec = Path(c["spool"]) / "receipts" / (self.m["id"] + ".json")
        rec.parent.mkdir()
        b.atomic_json(rec, dict(receipt=ack, verified_at=1))
        remote_video = Path(ack["path"])
        remote_video.write_bytes(b"x" * len(self.content))
        later, healthy = self.later(c)

        def transport(config, manifest, action, video=None):
            try:
                return self.request(action, manifest)
            except ValueError:
                raise b.ItemFailure("remote_rejected") from None

        with patch.object(b, "remote", side_effect=transport), patch.object(b.time, "time", return_value=1000):
            b.publish_once(c)
        self.assertTrue(p.exists())
        self.assertTrue(healthy.exists())
        self.assertEqual(remote_video.read_bytes(), b"x" * len(self.content))
        later_rec = Path(c["spool"]) / "receipts" / (later["id"] + ".json")
        self.assertTrue(later_rec.exists())
        with patch.object(b, "remote", side_effect=transport), patch.object(b.time, "time", return_value=1121):
            b.publish_once(c)
        self.assertTrue(p.exists())
        self.assertFalse(healthy.exists())
        self.assertFalse(healthy.with_suffix(".json").exists())
        self.assertTrue(later_rec.exists())

    def test_item_backoff_survives_restart_and_repairs_retry_promptly(self):
        c, p = self.queue()
        p.write_bytes(b"broken")
        root = Path(c["spool"])
        manifest = p.with_suffix(".json")
        with patch.object(b.time, "time", return_value=1000):
            b.publish_once(c)
        state = b.load_retry(b.retry_path(root, manifest))
        self.assertEqual(state["next_attempt"], 1060)
        # Reloading the module simulates loss of all process-local retry state.
        fresh = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fresh)
        with patch.object(fresh.time, "time", return_value=1059), patch.object(fresh, "publish_item") as work:
            fresh.publish_once(c)
        work.assert_not_called()
        with patch.object(fresh.time, "time", return_value=1060):
            fresh.publish_once(c)
        state = fresh.load_retry(fresh.retry_path(root, manifest))
        self.assertEqual((state["failures"], state["next_attempt"]), (2, 1180))
        p.write_bytes(self.content)
        with patch.object(fresh.time, "time", return_value=1061), \
                patch.object(fresh, "remote", return_value=dict(self.m, status="committed")) as remote:
            fresh.publish_once(c)
        remote.assert_called_once()
        self.assertFalse(fresh.retry_path(root, manifest).exists())

    def test_global_outage_one_attempt_per_due_pass_no_item_penalties(self):
        c, p = self.queue()
        for i in range(1, 8):
            self.later(c, i)
        current = 1000
        for count, delay in enumerate((60, 120, 240, 300, 300), start=1):
            with patch.object(b.time, "time", return_value=current), \
                    patch.object(b, "remote", side_effect=b.ServiceUnavailable("PRIVATE_SENTINEL")) as remote, \
                    patch.object(b, "digest", wraps=b.digest) as hashed:
                b.publish_once(c)
            self.assertEqual(remote.call_count, 1)
            self.assertEqual(hashed.call_count, 1)
            state = b.load_retry(Path(c["spool"]) / "retry/service.json")
            self.assertEqual((state["failures"], state["next_attempt"]), (count, current + delay))
            with patch.object(b.time, "time", return_value=current + delay - 1), \
                    patch.object(b, "remote") as remote, patch.object(b, "digest") as hashed:
                b.publish_once(c)
            remote.assert_not_called()
            hashed.assert_not_called()
            current += delay
        self.assertEqual(len(list((Path(c["spool"]) / "retry").glob("*.json"))), 1)
        def committed(config, m, action, video=None):
            return dict(m, status="committed")
        with patch.object(b.time, "time", return_value=current), patch.object(b, "remote", side_effect=committed) as remote:
            b.publish_once(c)
        self.assertEqual(remote.call_count, 8)
        self.assertEqual(list((Path(c["spool"]) / "retry").glob("*.json")), [])
        self.assertTrue(p.exists())

    def test_capped_item_retry_and_damaged_retry_metadata_isolation(self):
        c, p = self.queue()
        root = Path(c["spool"])
        retry = b.retry_path(root, p.with_suffix(".json"))
        retry.parent.mkdir()
        b.defer(retry, dict(failures=100, signature="same"), "same", "video_changed", 1000)
        self.assertEqual(b.load_retry(retry)["next_attempt"], 4600)
        retry.write_text("damaged scheduler metadata")
        later, _ = self.later(c)
        (root / "retry/service.json").write_text("damaged shared metadata")
        with patch.object(b, "remote", return_value=dict(later, status="committed")) as remote:
            b.publish_once(c)
        self.assertEqual(remote.call_count, 1)
        self.assertTrue(p.exists())
        self.assertEqual(retry.read_text(), "damaged scheduler metadata")

    def test_missing_local_video_needs_valid_receipt_before_manifest_cleanup(self):
        c, p = self.queue()
        p.unlink()
        rec = Path(c["spool"]) / "receipts" / (self.m["id"] + ".json")
        rec.parent.mkdir()
        b.atomic_json(rec, dict(receipt=dict(self.m, status="committed"), verified_at=float("inf")))
        b.publish_once(c)
        self.assertTrue(p.with_suffix(".json").exists())
        b.atomic_json(rec, dict(receipt=dict(self.m, status="committed"), verified_at=1))
        b.publish_once(c)
        self.assertFalse(p.with_suffix(".json").exists())

    def test_untrusted_remote_receipt_does_not_delete_or_block_later(self):
        c, p = self.queue()
        later, _ = self.later(c)
        def transport(config, m, action, video=None):
            return dict(m, status="committed", sha256="wrong") if m == self.m else dict(m, status="committed")
        with patch.object(b, "remote", side_effect=transport) as remote:
            b.publish_once(c)
        self.assertEqual(remote.call_count, 2)
        self.assertTrue(p.exists())
        self.assertFalse((Path(c["spool"]) / "receipts" / (self.m["id"] + ".json")).exists())

    def test_unexpected_item_exception_is_isolated_and_sanitized(self):
        c, p = self.queue()
        later, healthy = self.later(c)
        digest = b.digest
        def hash_file(path):
            if path == p:
                raise RuntimeError("rtsp://PRIVATE_SENTINEL")
            return digest(path)
        with patch.object(b, "digest", side_effect=hash_file), \
                patch.object(b, "remote", return_value=dict(later, status="committed")) as remote, \
                self.assertLogs(b.LOG, level="WARNING") as logs:
            b.publish_once(c)
        remote.assert_called_once()
        self.assertTrue(p.exists())
        self.assertTrue(healthy.exists())
        self.assertNotIn("PRIVATE_SENTINEL", " ".join(logs.output))

    def test_receiver_cli_reports_sanitized_failure_scope(self):
        config = self.root / "server.json"
        for manifest, reserve, scope in ((dict(self.m, start_utc="rtsp://PRIVATE_SENTINEL"), 0, "item"),
                                         (self.m, 10**18, "service")):
            config.write_text(json.dumps(dict(self.c, server_reserve_bytes=reserve)))
            result = subprocess.run([sys.executable, str(Path(b.__file__)), "receive", "--config", str(config)],
                input=json.dumps(dict(action="upload", manifest=manifest)).encode() + b"\n", capture_output=True)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads(result.stdout), dict(status="rejected", scope=scope))
            self.assertNotIn(b"PRIVATE_SENTINEL", result.stdout + result.stderr)

    def test_invalid_media_is_item_rejection_and_later_media_still_commits(self):
        config = self.root / "server.json"
        config.write_text(json.dumps(self.c))
        bad = dict(self.m, sha256=hashlib.sha256(b"bad").hexdigest(), size=3)
        result = subprocess.run([sys.executable, str(Path(b.__file__)), "receive", "--config", str(config)],
            input=json.dumps(dict(action="upload", manifest=bad)).encode() + b"\nbad", capture_output=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout), dict(status="rejected", scope="item"))
        later = dict(self.m, id="b" * 32 + "-20261029T130000Z")
        self.assertTrue(b.receipt_matches(self.request(m=later), later))
        self.assertEqual(len(list(self.library.rglob("*.mkv"))), 1)

    def test_publisher_through_receiver_process_isolates_failure_and_cleans_verified_copy(self):
        c, p = self.queue()
        p.with_suffix(".json").write_text("{broken JSON")
        later, healthy = self.later(c)
        config = self.root / "server.json"
        config.write_text(json.dumps(self.c))
        executable = self.root / "ssh"
        # Substitute only the SSH hop; the real pipe writer, receiver process,
        # FFmpeg validation, SQLite receipt and remote rehash all run normally.
        argv = [sys.executable, str(Path(b.__file__)), "receive", "--config", str(config)]
        executable.write_text(f"#!{sys.executable}\nimport os\nos.execv({sys.executable!r}, {argv!r})\n")
        executable.chmod(0o700)
        c.update(known_hosts="unused", ssh_key="unused", destination="unused")
        with patch.dict(os.environ, PATH=str(self.root) + os.pathsep + os.environ["PATH"]):
            b.publish_once(c)
            self.assertTrue(healthy.exists())
            b.publish_once(c)
        self.assertTrue(p.exists())
        self.assertFalse(healthy.exists())
        self.assertFalse(healthy.with_suffix(".json").exists())
        published = list(self.library.rglob("*.mkv"))
        self.assertEqual(len(published), 1)
        self.assertEqual(b.digest(published[0]), later["sha256"])
        self.assertTrue((Path(c["spool"]) / "receipts" / (later["id"] + ".json")).exists())

    def test_remote_understands_receiver_scope_and_legacy_failure(self):
        executable = self.root / "ssh"
        config = dict(known_hosts="unused", ssh_key="unused", destination="unused")
        for code, payload, error in ((1, dict(status="rejected", scope="item"), b.ItemFailure),
                                     (1, dict(status="rejected", scope="service"), b.ServiceUnavailable),
                                     (255, dict(status="rejected", scope="item"), b.ServiceUnavailable),
                                     (1, None, b.ServiceUnavailable), (0, [], b.ItemFailure)):
            executable.write_text(f"#!{sys.executable}\nimport sys\nsys.stdin.readline()\nprint({json.dumps(payload)!r})\nsys.exit({code})\n")
            executable.chmod(0o700)
            with patch.dict(os.environ, PATH=str(self.root) + os.pathsep + os.environ["PATH"]):
                with self.assertRaises(error):
                    b.remote(config, self.m, "verify")

    def test_server_full_does_not_publish(self):
        self.c["server_reserve_bytes"] = 10**18
        with self.assertRaises(ValueError):
            self.request()
        self.assertEqual(list(self.library.rglob("*.mkv")), [])

    def test_concurrent_verified_cleanup_during_space_scan(self):
        c, victim = self.queue()
        original = Path.is_file

        def cleanup(path):
            if path == victim:
                victim.unlink(missing_ok=True)
                return True
            return original(path)

        with patch.object(Path, "is_file", cleanup):
            self.assertGreaterEqual(b.spool_bytes(Path(c["spool"])), 0)

    def test_storage_guard(self):
        c, p = self.queue()
        c["reserve_bytes"] = 10**18
        self.assertFalse(b.space_ok(c))

    def test_receipt_binding(self):
        for k in ["id", "sha256", "size"]:
            a = dict(self.m, status="committed")
            a[k] = "wrong"
            self.assertFalse(b.receipt_matches(a, self.m))

    def test_orphan_recovery(self):
        c = dict(
            spool=str(self.root / "recover"),
            camera_name="Test",
            timezone="UTC",
            segment_seconds=3600,
        )
        root = Path(c["spool"])
        (root / "ready").mkdir(parents=True)
        run = root / "active" / ("b" * 32)
        run.mkdir(parents=True)
        (run / "20261029T120000Z.mkv").write_bytes(self.content)
        b.recover(c)
        self.assertEqual(len(list((root / "ready").glob("*.json"))), 1)
        # A salvage does not prove the remux preserved every recoverable packet.
        self.assertEqual((run / "20261029T120000Z.mkv").read_bytes(), self.content)


if __name__ == "__main__":
    unittest.main()
