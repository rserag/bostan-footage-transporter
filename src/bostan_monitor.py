#!/usr/bin/env python3
"""Read-only probes and an off-recorder monitor. Python standard library only.

Never read camera credentials, process arguments, or journal messages. Probe
output deliberately contains numbers and opaque file tokens, not media paths.
"""

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request


def timestamp(value):
    result = dt.datetime.fromisoformat(value).timestamp()
    if not math.isfinite(result):
        raise ValueError("invalid timestamp")
    return result


def token(value):
    return hashlib.sha256(str(value).encode()).hexdigest()


def load(path):
    return json.loads(Path(path).read_text())


def atomic_write(path, text, mode=0o600):
    path = Path(path)
    fd, name = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w") as out:
            os.fchmod(out.fileno(), mode)
            out.write(text)
            out.flush()
            os.fsync(out.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def regular_files(root, pattern):
    """Ignore normal publisher/finalizer races; never traverse symbolic links."""
    for path in root.glob(pattern):
        try:
            if not path.is_symlink() and path.is_file():
                yield path, path.stat()
        except FileNotFoundError:
            continue


def service_status(unit):
    result = subprocess.run(
        ["systemctl", "show", unit, "-p", "ActiveState", "-p", "SubState"],
        capture_output=True, timeout=5, text=True,
    )
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    return int(values.get("ActiveState") == "active" and values.get("SubState") == "running")


def probe_pi(c, clock=time.time, services=service_status):
    current = clock()
    root = Path(c["spool"])
    active = list(regular_files(root, "active/*/*.mkv"))
    ready = list(regular_files(root, "ready/*.mkv"))
    total = 0
    for directory, children, files in os.walk(root, followlinks=False):
        children[:] = [name for name in children if not name.startswith(".")
                       and not (Path(directory) / name).is_symlink()]
        for name in files:
            try:
                path = Path(directory) / name
                if not path.is_symlink():
                    total += path.stat().st_size
            except FileNotFoundError:
                continue
    latest = max(active, key=lambda entry: entry[1].st_mtime, default=None)
    pending, rates, closes = [], [], []
    invalid = 0
    for path, stat in regular_files(root, "ready/*.json"):
        try:
            m = load(path)
            closed = timestamp(m["closed_utc"])
            closes.append(closed)
            if m["duration_seconds"] > 10:
                rates.append((closed, m["size"] / m["duration_seconds"]))
            receipt = root / "receipts" / path.name
            acknowledged = False
            if receipt.exists():
                saved = load(receipt)["receipt"]
                acknowledged = (saved.get("status") == "committed" and all(
                    saved.get(k) == m[k] for k in ("id", "size", "sha256")))
                if not acknowledged:
                    invalid += 1
            if not acknowledged:
                pending.append((closed, m["size"]))
        except FileNotFoundError:
            # Atomic cleanup may remove the manifest between listing and read.
            continue
        except (ValueError, KeyError, TypeError):
            invalid += 1
    for path, stat in regular_files(root, "receipts/*.json"):
        try:
            closes.append(float(load(path)["verified_at"]))
        except FileNotFoundError:
            continue
        except (ValueError, KeyError, TypeError):
            invalid += 1
    orphan = sum(1 for p, s in ready
                 if current - s.st_mtime > 300 and not p.with_suffix(".json").exists())
    starts = []
    for path, stat in active:
        try:
            starts.append(dt.datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ")
                          .replace(tzinfo=dt.timezone.utc).timestamp())
        except ValueError:
            invalid += 1
    return dict(
        version=1, timestamp=current,
        record_up=services("bostan-record.service"),
        publish_up=services("bostan-publish.service"),
        active_token=token(latest[0].relative_to(root)) if latest else "",
        active_bytes=latest[1].st_size if latest else 0,
        active_mtime=latest[1].st_mtime if latest else 0,
        oldest_active_start=min(starts, default=current),
        latest_closed=max(closes, default=0),
        segment_seconds=c["segment_seconds"],
        spool_bytes=total, free_bytes=shutil.disk_usage(root).free,
        reserve_bytes=c["reserve_bytes"], max_spool_bytes=c["max_spool_bytes"],
        pending_count=len(pending), pending_bytes=sum(size for _, size in pending),
        oldest_pending=min((closed for closed, _ in pending), default=current),
        recent_bytes_per_second=statistics.median(
            rate for _, rate in sorted(rates)[-6:]) if rates else 0,
        invalid_entries=invalid + orphan,
    )


def probe_server(c, clock=time.time):
    base, state = Path(c["library_root"]).resolve(), Path(c["state"])
    deletion_file = state / "intentional-deletions.json"
    deleted = load(deletion_file) if deletion_file.exists() else {}
    if not isinstance(deleted, dict):
        raise ValueError("invalid deletion ledger")
    # mode=ro: this probe must never create a DB or change receipt/episode state.
    with contextlib.closing(sqlite3.connect(
            (state / "receipts.sqlite3").resolve().as_uri() + "?mode=ro", timeout=2)) as db:
        rows = db.execute("SELECT id,size,path FROM recordings WHERE committed=1").fetchall()
    expected, missing, excluded = [], 0, 0
    for ident, size, filename in rows:
        entry = deleted.get(ident, {})
        if isinstance(entry, dict) and entry.get("status") == "deleted":
            excluded += 1
            continue
        path = Path(filename)
        try:
            relative = path.resolve().relative_to(base)
            if path.is_symlink() or path.stat().st_size != size:
                raise ValueError("missing or changed media")
            manifest = path.with_suffix(".json")
            nfo = path.with_suffix(".nfo")
            if not manifest.is_file() or not nfo.is_file():
                raise ValueError("missing metadata")
            expected.append(dict(path_token=token(relative.as_posix()),
                                 published=path.stat().st_mtime))
        except (FileNotFoundError, ValueError):
            missing += 1
    return dict(version=1, timestamp=clock(), expected=expected,
                missing_assets=missing, intentional_deletions=excluded)


def ssh_probe(c):
    host = c["destination"]
    if not re.fullmatch(r"[A-Za-z0-9_.@:-]+", host) or host.startswith("-"):
        raise ValueError("invalid monitor destination")
    result = subprocess.run([
        "ssh", "-F", "/dev/null", "-T", "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
        "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=8",
        "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=1",
        "-i", c["ssh_key"], "-o", "UserKnownHostsFile=" + c["known_hosts"],
        "-p", str(c.get("port", 22)), host, "status",
    ], stdin=subprocess.DEVNULL, capture_output=True, timeout=20, check=True)
    if len(result.stdout) > 16 * 1024 * 1024:
        raise ValueError("oversize probe")
    data = json.loads(result.stdout)
    if data.get("version") != 1 or "error" in data:
        raise ValueError("invalid probe")
    return data


def jellyfin_tokens(c):
    key = Path(c["key_file"]).read_text().strip()
    prefix = c["library_root"].rstrip("/") + "/"
    found, start = set(), 0
    while True:
        query = urllib.parse.urlencode(dict(
            ParentId=c["series_id"], IncludeItemTypes="Episode", Recursive="true",
            Fields="Path", StartIndex=start, Limit=500))
        request = urllib.request.Request(c["url"].rstrip("/") + "/Items?" + query,
                                         headers={"X-Emby-Token": key})
        with urllib.request.urlopen(request, timeout=10) as response:
            data = json.load(response)
        items = data["Items"]
        for item in items:
            path = item.get("Path", "")
            if path.startswith(prefix):
                found.add(token(path[len(prefix):]))
        start += len(items)
        if start >= data["TotalRecordCount"]:
            return found
        if not items:
            raise ValueError("incomplete Jellyfin response")


# name: (failure grace seconds, severity, readable problem, operator action)
RULES = {
    "pi_unreachable": (180, "critical", "Pi/VPN is unreachable",
        "Check Pi power, router and VPN; recording state is unknown. Avoid rebooting blindly."),
    "server_unreachable": (180, "critical", "Archive server is unreachable",
        "Check server and LAN/SSH; local footage remains queued on the Pi."),
    "recorder_down": (120, "critical", "Recording service is down",
        "Inspect bostan-record service and sanitized logs; check camera and storage."),
    "publisher_down": (120, "warning", "Upload service is down",
        "Inspect bostan-publish service; protect queued footage and check receiver access."),
    "capture_stalled": (0, "critical", "No footage growth for at least 3 minutes",
        "Check camera power/connectivity and recorder/storage guard; new footage may be lost."),
    "late_segment": (120, "warning", "A recording is more than 10 minutes late closing",
        "Inspect recorder finalization/recovery and orphan files; retain all unverified footage."),
    "upload_backlog": (0, "warning", "Closed footage has waited over 15 minutes for upload",
        "Check VPN, receiver/storage and oldest queue entry; do not delete unverified files."),
    "queue_invalid": (120, "warning", "A queue/receipt entry is invalid or an orphan is retained",
        "Inspect retained entries privately; preserve files for repair or manual quarantine."),
    "buffer_low": (300, "warning", "Estimated Pi recording buffer is below 6 hours",
        "Restore uploads or add safe storage; the estimate assumes no further cleanup."),
    "buffer_critical": (120, "critical", "Estimated Pi buffer is below 2 hours or storage guard reached",
        "Urgently restore uploads/free verified storage safely; recording will pause at the guard."),
    "jellyfin_unavailable": (180, "warning", "Jellyfin indexing check cannot run",
        "Check Jellyfin availability and monitor API credential permissions; uploads may still work."),
    "jellyfin_missing": (120, "warning", "Committed footage is absent from Jellyfin after 10 minutes",
        "Check Jellyfin library mount, permissions and scan; compare committed files before refreshing."),
    "archive_missing": (120, "critical", "Committed archive footage or metadata is missing/size changed",
        "Inspect archive storage and receipt ledger; exclude only documented intentional deletions."),
    "clock_skew": (120, "warning", "A probe clock differs from the monitor by over 2 minutes",
        "Check NTP on Pi and server; timing and buffer diagnostics may be inaccurate."),
}


def evaluate(c, state, pi, server, indexed, current):
    conditions = {name: None for name in RULES}
    metrics = {}
    conditions["pi_unreachable"] = pi is None
    conditions["server_unreachable"] = server is None
    conditions["jellyfin_unavailable"] = indexed is None
    offsets = []
    if pi is not None:
        offset = current - pi["timestamp"]
        offsets.append(abs(offset))
        conditions["recorder_down"] = pi["record_up"] != 1
        conditions["publisher_down"] = pi["publish_up"] != 1
        previous = state.get("growth", {})
        last = previous.get("last", current - max(0, pi["timestamp"] - pi["active_mtime"])
                            if pi["active_mtime"] else current)
        if (pi["active_bytes"] > 0 and (pi["active_token"] != previous.get("token")
                or pi["active_bytes"] > previous.get("size", 0))):
            # A newly observed stale file is not evidence of current growth.
            last = max(last, current - max(0, pi["timestamp"] - pi["active_mtime"]))
        state["growth"] = dict(token=pi["active_token"], size=pi["active_bytes"], last=last)
        growth_age = max(0, current - last)
        conditions["capture_stalled"] = growth_age > 180
        conditions["late_segment"] = (pi["timestamp"] - pi["oldest_active_start"]
                                        > pi["segment_seconds"] + 600)
        backlog_age = max(0, pi["timestamp"] - pi["oldest_pending"]) if pi["pending_count"] else 0
        conditions["upload_backlog"] = backlog_age > 900
        conditions["queue_invalid"] = pi["invalid_entries"] > 0
        rate = max(c.get("fallback_bytes_per_second", 100000),
                   pi["recent_bytes_per_second"] * 1.2)
        available = max(0, min(pi["max_spool_bytes"] - pi["spool_bytes"],
                               pi["free_bytes"] - pi["reserve_bytes"]))
        buffer_seconds = available / rate
        conditions["buffer_low"] = buffer_seconds < 21600
        conditions["buffer_critical"] = buffer_seconds < 7200
        metrics.update(pi_free_bytes=pi["free_bytes"], pi_spool_bytes=pi["spool_bytes"],
                       pi_buffer_seconds=buffer_seconds, pi_buffer_bytes=available,
                       pi_estimated_bytes_per_second=rate, pi_growth_age_seconds=growth_age,
                       upload_pending_count=pi["pending_count"], upload_pending_bytes=pi["pending_bytes"],
                       upload_oldest_age_seconds=backlog_age,
                       pi_oldest_active_age_seconds=max(0, pi["timestamp"] - pi["oldest_active_start"]),
                       pi_latest_closed_age_seconds=max(0, pi["timestamp"] - pi["latest_closed"]),
                       pi_invalid_entries=pi["invalid_entries"],
                       pi_record_up=pi["record_up"], pi_publish_up=pi["publish_up"])
    if server is not None:
        offsets.append(abs(current - server["timestamp"]))
        conditions["archive_missing"] = server["missing_assets"] > 0
        metrics.update(archive_missing_count=server["missing_assets"],
                       archive_expected_count=len(server["expected"]),
                       archive_intentional_deletions=server["intentional_deletions"])
        if indexed is not None:
            missing = sum(1 for entry in server["expected"]
                          if entry["path_token"] not in indexed
                          and server["timestamp"] - entry["published"] > 600)
            conditions["jellyfin_missing"] = missing > 0
            metrics["jellyfin_missing_count"] = missing
    conditions["clock_skew"] = max(offsets) > 120 if offsets else None
    return conditions, metrics


def advance(state, conditions, current, recovery_seconds=120):
    """Durable transitions: upstream unknown never resolves a known failure."""
    alarms = state.setdefault("alarms", {})
    for name, problem in conditions.items():
        alarm = alarms.setdefault(name, dict(active=False, notified=False))
        if problem is None:
            alarm.pop("pending", None)
            alarm.pop("healthy", None)
        elif problem:
            alarm.pop("healthy", None)
            alarm.setdefault("pending", current)
            if current - alarm["pending"] >= RULES[name][0]:
                alarm["active"] = True
        else:
            alarm.pop("pending", None)
            alarm.setdefault("healthy", current)
            if current - alarm["healthy"] >= recovery_seconds:
                alarm["active"] = False
    return alarms


def telegram(c, message):
    key = Path(c["token_file"]).read_text().strip()
    chat = Path(c["chat_id_file"]).read_text().strip()
    payload = urllib.parse.urlencode(dict(chat_id=chat, text=message)).encode()
    request = urllib.request.Request("https://api.telegram.org/bot" + key + "/sendMessage",
                                     data=payload, method="POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        if not json.load(response).get("ok"):
            raise ValueError("notification rejected")


def notify(c, state, current, sender=telegram, save=lambda: None):
    ok = True
    deadline = time.monotonic() + 25
    for name, alarm in state["alarms"].items():
        desired = alarm["active"]
        if desired == alarm["notified"]:
            alarm.pop("retry_at", None)
            alarm.pop("retries", None)
            continue
        if current < alarm.get("retry_at", 0) or time.monotonic() >= deadline:
            ok = False
            continue
        grace, severity, problem, action = RULES[name]
        message = ("Bostan " + ("FAILURE" if desired else "RECOVERED") + " [" + severity + "]\n"
                   + problem + ("\n" + action if desired else "\nHealthy for at least 2 minutes.")
                   + "\nCheck: " + name)
        try:
            sender(c["telegram"], message)
        except Exception:
            # URL exceptions can contain the bot token. Never log exception text.
            alarm["retries"] = min(alarm.get("retries", 0) + 1, 5)
            alarm["retry_at"] = current + min(300, 30 * 2 ** alarm["retries"])
            ok = False
        else:
            alarm["notified"] = desired
            alarm.pop("retry_at", None)
            alarm.pop("retries", None)
        save()
    return ok


def prometheus(metrics, state, conditions, current):
    values = dict(metrics, monitor_timestamp_seconds=current)
    lines = []
    for name, value in sorted(values.items()):
        value = float(value)
        if not math.isfinite(value):
            raise ValueError("nonfinite metric")
        lines.extend(["# TYPE bostan_" + name + " gauge", "bostan_" + name + " " + str(value)])
    for name in sorted(RULES):
        labels = '{check="' + name + '"}'
        lines.append("bostan_check_active" + labels + " " + str(int(state["alarms"][name]["active"])))
        lines.append("bostan_check_available" + labels + " " + str(int(conditions[name] is not None)))
    return "\n".join(lines) + "\n"


def collect(c):
    state_path = Path(c["state_file"])
    with state_path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Fail closed on state corruption; don't re-send every active incident.
        state = load(state_path) if state_path.exists() else {}
        def save():
            atomic_write(state_path, json.dumps(state, sort_keys=True))
        samples = []
        for target in ("pi", "server"):
            try:
                samples.append(ssh_probe(c[target]))
            except Exception:
                samples.append(None)
        try:
            indexed = jellyfin_tokens(c["jellyfin"])
        except Exception:
            indexed = None
        current = time.time()
        conditions, metrics = evaluate(c, state, *samples, indexed, current)
        advance(state, conditions, current)
        save()
        metrics["notifications_ok"] = int(notify(c, state, current, save=save))
        save()
        atomic_write(c["metrics_file"], prometheus(metrics, state, conditions, current), 0o644)
        print(json.dumps(dict(active_checks=[k for k, v in state["alarms"].items() if v["active"]],
                              **metrics), sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=("probe-pi", "probe-server", "collect", "test-notification"))
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    try:
        c = load(args.config)
        if args.role == "probe-pi":
            print(json.dumps(probe_pi(c)))
        elif args.role == "probe-server":
            print(json.dumps(probe_server(c)))
        elif args.role == "test-notification":
            telegram(c["telegram"], "Bostan MONITOR TEST: notification delivery works. "
                     "No live recording, network or storage failure was triggered.")
            print('{"test_notification":"delivered"}')
        else:
            collect(c)
    except Exception as exc:
        print(json.dumps(dict(error="monitor_failed", kind=type(exc).__name__)))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
