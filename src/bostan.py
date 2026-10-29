#!/usr/bin/env python3
"""Stream-copy camera recorder and verified SSH transport. Python 3.11+."""

import argparse
import csv
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import queue
import re
import resource
import shutil
import signal
import stat
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

UTC = dt.timezone.utc
LOG = logging.getLogger("bostan")
STOP = threading.Event()
RETRY_REASONS = frozenset((
    "manifest_invalid", "video_missing", "video_changed", "receipt_invalid",
    "remote_rejected", "remote_receipt_invalid", "item_io", "item_failed",
    "service_unavailable",
))


class ItemFailure(ValueError):
    """An isolated recording failure; only fixed diagnostic codes are logged."""

    def __init__(self, reason):
        self.reason = reason if reason in RETRY_REASONS else "item_failed"
        super().__init__(self.reason)


class ServiceUnavailable(ValueError):
    """Stop network work for this pass, without penalizing individual items."""


def now():
    return dt.datetime.now(UTC).isoformat()


def syncdir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path, obj):
    path = Path(path)
    tmp = path.with_name("." + path.name + "." + uuid.uuid4().hex + ".tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o666)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        syncdir(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def probe(path, binary="ffprobe"):
    # Damaged media may make probe diagnostics/metadata grow without bound.
    # Use an unnamed file and a child-only output cap, never a buffered pipe.
    # A tiny exec wrapper avoids preexec_fn in the recorder's threaded process.
    wrapper = ("import os,resource,sys; h=resource.getrlimit(resource.RLIMIT_FSIZE)[1]; "
               "n=65536 if h==resource.RLIM_INFINITY else min(65536,h); "
               "resource.setrlimit(resource.RLIMIT_CORE,(0,0)); "
               "resource.setrlimit(resource.RLIMIT_FSIZE,(n,n)); os.execvp(sys.argv[1],sys.argv[1:])")
    with tempfile.TemporaryFile() as output:
        p = subprocess.run(
            [sys.executable, "-c", wrapper, binary, "-v", "error", "-show_entries",
             "format=duration,size:stream=codec_type,codec_name,width,height,r_frame_rate,sample_rate,channels",
             "-of", "json", str(path)],
            stdout=output, stderr=subprocess.DEVNULL, timeout=90,
        )
        if p.returncode:
            raise ValueError("media probe failed")
        output.seek(0)
        encoded = output.read(65537)
    if len(encoded) > 65536:
        raise ValueError("oversize media probe")
    data = json.loads(encoded)
    duration = float(data.get("format", {}).get("duration", 0))
    if not 0 < duration < 172800 or not any(
        s["codec_type"] == "video" for s in data["streams"]
    ):
        raise ValueError("missing video or invalid duration")
    return data, duration


def credential_url(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
                v = v[1:-1]
            values[k.strip()] = v
    u = urlsplit(values.get("RTSP_URL", ""))
    if u.scheme != "rtsp" or not u.hostname:
        raise ValueError("invalid RTSP configuration")
    if values.get("RTSP_USERNAME"):
        auth = (
            quote(values["RTSP_USERNAME"], safe="")
            + ":"
            + quote(values.get("RTSP_PASSWORD", ""), safe="")
        )
        u = u._replace(netloc=auth + "@" + u.netloc.split("@")[-1])
    return u.geturl()


def spool_bytes(root):
    total = 0
    seen = set()
    for path in root.rglob("*"):
        try:
            s = path.lstat()
            inode = (s.st_dev, s.st_ino)
            if stat.S_ISREG(s.st_mode) and inode not in seen:
                total += s.st_size
                seen.add(inode)
        except FileNotFoundError:
            # The independent publisher may remove a verified file during this scan.
            continue
    return total


def space_ok(c):
    root = Path(c["spool"])
    return (
        shutil.disk_usage(root).free > c["reserve_bytes"]
        and spool_bytes(root) < c["max_spool_bytes"]
    )


def recording_id(raw):
    ident = raw.stem if raw.parent.name == "ready" else raw.parent.name + "-" + raw.stem
    if not re.fullmatch(r"[a-f0-9]{32}-\d{8}T\d{6}Z", ident):
        raise ValueError("invalid recording filename")
    dt.datetime.strptime(ident.rsplit("-", 1)[1], "%Y%m%dT%H%M%SZ")
    return ident


def manifest_for(c, raw, ident, reason):
    if not stat.S_ISREG(raw.lstat().st_mode):
        raise ValueError("not regular media")
    data, duration = probe(raw)
    start = dt.datetime.strptime(ident.rsplit("-", 1)[1], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    m = dict(
        id=ident,
        start_utc=start.isoformat(),
        end_utc=(start + dt.timedelta(seconds=duration)).isoformat(),
        closed_utc=now(),
        duration_seconds=duration,
        timezone=c["timezone"],
        camera=c["camera_name"],
        timestamp_basis="recorder segment-open UTC clock, one-second precision; end derived from media duration",
        target_seconds=c["segment_seconds"],
        close_reason=reason,
        partial=duration < c["segment_seconds"] - min(10, c["segment_seconds"] * 0.1),
        media=data,
        size=raw.stat().st_size,
        sha256=digest(raw),
    )
    check_manifest(m)
    return m


def exclusive_json(path, obj):
    """Publish durable metadata without replacing any existing evidence."""
    name = path.parent / (".seal-" + uuid.uuid4().hex)
    # Match normal umask/group permissions so the restricted read-only monitor
    # can inspect sealed manifests; mkstemp's 0600 would hide them from it.
    fd = os.open(name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o666)
    try:
        with os.fdopen(fd, "w") as out:
            json.dump(obj, out, sort_keys=True, indent=2)
            out.flush()
            os.fsync(out.fileno())
        os.link(name, path)
        syncdir(path.parent)
    finally:
        os.unlink(name)


def seal(c, raw, m):
    """Video first, manifest last; retries may complete but never replace a seal."""
    ready = Path(c["spool"]) / "ready"
    ready.mkdir(exist_ok=True)
    target = ready / (m["id"] + ".mkv")
    with raw.open("rb") as f:
        os.fsync(f.fileno())
    try:
        os.link(raw, target)
    except FileExistsError:
        if (not stat.S_ISREG(target.lstat().st_mode) or target.stat().st_size != m["size"]
                or digest(target) != m["sha256"]):
            raise ValueError("sealed video conflict") from None
    syncdir(ready)
    manifest = target.with_suffix(".json")
    try:
        exclusive_json(manifest, m)
    except FileExistsError:
        saved = check_manifest(read_regular_json(manifest))
        if any(saved.get(k) != m[k] for k in ("id", "size", "sha256", "start_utc", "end_utc")):
            raise ValueError("sealed manifest conflict") from None
    syncdir(ready)


def finalize(c, raw, reason):
    m = manifest_for(c, raw, recording_id(raw), reason)
    seal(c, raw, m)
    raw.unlink()
    syncdir(raw.parent)
    LOG.info("sealed recording %s duration=%.1fs", m["id"], m["duration_seconds"])


def recovery_key(entry):
    return hashlib.sha256(os.fsencode(str(entry))).hexdigest()


def recovery_path(c, entry):
    return Path(c["spool"]) / "recovery" / (recovery_key(entry) + ".json")


def capture_protocol_ready(root):
    """An upgrade must never mistake a legacy recorder's live file for an orphan."""
    def held(path):
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            return False
        with os.fdopen(fd, "rb") as lock:
            if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
                raise ValueError("invalid recorder lock")
            try:
                fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
        return False

    return not held(root / "recorder.lock") or held(root / "recovery-aware.lock")


def load_recovery(path):
    if not path.exists() and not path.is_symlink():
        return None
    s = read_regular_json(path)
    if (not isinstance(s, dict) or s.get("version") != 1
            or s.get("status") not in ("running", "backoff", "quarantined", "recovered")
            or type(s.get("attempts")) is not int or not 0 <= s["attempts"] <= 100
            or not isinstance(s.get("entry"), str)
            or recovery_key(s["entry"]) != path.stem
            or s.get("reason") not in ("interrupted", "failed", "timeout", "space", "artifact", "recovered")
            or any(type(s.get(k)) not in (int, float) or not math.isfinite(s[k])
                   for k in ("last_attempt", "next_attempt"))
            or not 0 <= s["next_attempt"] - s["last_attempt"] <= 21600):
        raise ValueError("invalid recovery state")
    return s


def recovery_entry(c, entry):
    """Only exact, regular files beneath real spool directories are media inputs."""
    parts = Path(entry).parts
    if (len(parts) not in (2, 3) or parts[0] not in ("active", "ready")
            or (parts[0] == "active") != (len(parts) == 3) or any(p in (".", "..") for p in parts)):
        raise ValueError("invalid recovery entry")
    root = Path(c["spool"])
    raw = root / entry
    for parent in raw.parents:
        if parent == root:
            break
        if not stat.S_ISDIR(parent.lstat().st_mode):
            raise ValueError("invalid media directory")
    if raw.suffix != ".mkv" or not stat.S_ISREG(raw.lstat().st_mode):
        raise ValueError("invalid media entry")
    return raw, recording_id(raw)


@contextlib.contextmanager
def recovery_run_lock(c, ident):
    run = Path(c["spool"]) / "active" / ident.split("-", 1)[0]
    if not run.exists():
        yield
        return
    if not stat.S_ISDIR(run.lstat().st_mode):
        raise ValueError("invalid run directory")
    fd = os.open(run / "capture.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o640)
    with os.fdopen(fd, "a+b") as lock:
        if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
            raise ValueError("invalid capture lock")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def recovery_candidates(root):
    # Include legacy partial artifacts and malformed/nonregular entries. They get
    # an observable hold, never an attempt to interpret their names as a clock.
    for run in (root / "active").iterdir():
        if stat.S_ISDIR(run.lstat().st_mode):
            for raw in run.iterdir():
                if raw.name not in ("session.json", "capture.lock"):
                    yield raw.relative_to(root)
        else:
            yield run.relative_to(root)
    for raw in (root / "ready").iterdir():
        if ((raw.suffix == ".mkv" and not os.path.lexists(raw.with_suffix(".json")))
                or raw.suffix not in (".mkv", ".json")):
            if raw.suffix == ".mkv" or time.time() - raw.lstat().st_mtime > 300:
                yield raw.relative_to(root)


def stop_recovery_child(p):
    # All media tools inherit this private process group. Kill descendants even
    # when the direct child has already exited. Never wait without a bound.
    with contextlib.suppress(ProcessLookupError):
        os.killpg(p.pid, signal.SIGKILL)
    with contextlib.suppress(subprocess.TimeoutExpired):
        p.wait(timeout=2)


def recovery_spawn(c, entry, output_limit, timeout):
    request = json.dumps(dict(config=c, entry=str(entry), output_limit=output_limit)).encode()
    p = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "_recover-one"],
                         stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        p.communicate(request, timeout=timeout)
        return "recovered" if p.returncode == 0 else "failed"
    except subprocess.TimeoutExpired:
        return "timeout"
    finally:
        stop_recovery_child(p)


def recover_one(c, entry, output_limit):
    """Run only in a disposable child: probe, remux, hash and fsync share its limit."""
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_FSIZE, (output_limit, output_limit))
    raw, ident = recovery_entry(c, entry)
    state_path = recovery_path(c, entry)
    with recovery_run_lock(c, ident):
        s = load_recovery(state_path)
        if not s or s["status"] != "running":
            raise ValueError("attempt not reserved")
        root = Path(c["spool"])
        work = root / "recovery" / "work" / state_path.stem
        work.mkdir(exist_ok=True)
        if not stat.S_ISDIR(work.lstat().st_mode):
            raise ValueError("invalid recovery work directory")
        commit = work / "commit.json"
        if os.path.lexists(commit):
            job = read_regular_json(commit)
            m = check_manifest(job["manifest"])
            if m["id"] != ident or not re.fullmatch(r"attempt-[1-9][0-9]*\.mkv", job["output"]):
                raise ValueError("invalid recovery commit")
            fixed = work / job["output"]
            if (not stat.S_ISREG(fixed.lstat().st_mode) or fixed.stat().st_size != m["size"]
                    or digest(fixed) != m["sha256"]):
                raise ValueError("recovery output changed")
        elif (raw.parent.name == "ready" or
              (root / "ready" / (ident + ".mkv")).exists()):
            # A video linked before its manifest already passed normal close.
            # Preserve those exact bytes; a damaged ready orphan stays held.
            fixed = root / "ready" / (ident + ".mkv")
            if raw != fixed and not os.path.samefile(raw, fixed):
                raise ValueError("sealed video conflict")
            m = manifest_for(c, fixed, ident, "recovered_after_restart")
        else:
            fixed = work / ("attempt-" + str(s["attempts"]) + ".mkv")
            result = subprocess.run([
                "ffmpeg", "-nostdin", "-v", "error", "-i", str(raw),
                "-map", "0:v:0", "-map", "0:a?", "-c", "copy", "-f", "matroska", "-n", str(fixed),
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=240)
            if result.returncode:
                raise ValueError("remux failed")
            m = manifest_for(c, fixed, ident, "recovered_after_restart")
            with fixed.open("rb") as source:
                os.fsync(source.fileno())
            syncdir(work)
            exclusive_json(commit, dict(output=fixed.name, manifest=m))
        seal(c, fixed, m)
        # Active originals and all failed outputs stay in place. A durable success
        # marker prevents their replay even after publisher grace cleanup.
        atomic_json(state_path, dict(s, status="recovered", reason="recovered"))


def recover(c):
    """Independent, globally bounded pass; never called by the capture loop."""
    root = Path(c["spool"])
    result = dict(attempted=0, recovered=0, quarantined=0)
    budget = min(240, max(0.1, c.get("recovery_budget_seconds", 240)))
    deadline = time.monotonic() + budget
    for name in ("active", "ready", "recovery", "recovery/work"):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        if not stat.S_ISDIR(directory.lstat().st_mode):
            raise ValueError("invalid recovery directory")
    fd = os.open(root / "recovery.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o640)
    with os.fdopen(fd, "a+b") as lock:
        if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
            raise ValueError("invalid recovery lock")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return result
        if not capture_protocol_ready(root):
            LOG.warning("recovery held reason=legacy_recorder; capture retains ownership")
            return result
        service = root / "recovery" / "service.json"
        current = time.time()
        if os.path.lexists(service):
            saved = read_regular_json(service)
            if (not isinstance(saved, dict) or saved.get("version") != 1
                    or any(type(saved.get(k)) not in (int, float) or not math.isfinite(saved[k])
                           for k in ("last_attempt", "next_attempt"))
                    or not 0 <= saved["next_attempt"] - saved["last_attempt"] <= 300):
                raise ValueError("invalid recovery pass state")
            if saved["last_attempt"] <= current < saved["next_attempt"]:
                return result
        # Persist before scanning/spawning: reconnect, boot or a crash cannot
        # reset the shared five-minute ceiling. Corrupt state requires an operator.
        atomic_json(service, dict(version=1, last_attempt=current, next_attempt=current + 300))
        for entry in recovery_candidates(root):
            if STOP.is_set() or time.monotonic() >= deadline or result["attempted"] >= 2:
                break
            state_path = recovery_path(c, entry)
            try:
                s = load_recovery(state_path)
                if s and (s["status"] in ("quarantined", "recovered")
                          or s["last_attempt"] <= current < s["next_attempt"]):
                    continue
                attempts = s["attempts"] if s else 0
                if attempts >= 3:
                    atomic_json(state_path, dict(s, status="quarantined"))
                    continue
                try:
                    raw, ident = recovery_entry(c, entry)
                    with recovery_run_lock(c, ident):
                        pass
                except BlockingIOError:
                    continue  # Recorder owns the entire run until close/finalize.
                except (ValueError, OSError):
                    atomic_json(state_path, dict(version=1, entry=str(entry), attempts=attempts,
                        status="quarantined", reason="artifact", last_attempt=current, next_attempt=current))
                    result["quarantined"] += 1
                    LOG.warning("recovery held reason=artifact entry=%s; footage retained", state_path.stem)
                    continue
                size = raw.stat().st_size
                output_limit = min(c.get("recovery_max_output_bytes", 512 * 1024**2),
                                   size + max(1024**2, size // 20))
                available = min(shutil.disk_usage(root).free - c.get("reserve_bytes", 0),
                                c.get("max_spool_bytes", 8 * 1024**3) - spool_bytes(root))
                enough = output_limit + 64 * 1024**2 < available
                s = dict(version=1, entry=str(entry), attempts=attempts + int(enough),
                         status="running" if enough else "backoff", reason="interrupted" if enough else "space",
                         last_attempt=current, next_attempt=current + min(21600, 300 * 2**min(attempts, 6)))
                atomic_json(state_path, s)
                if not enough:
                    LOG.warning("recovery deferred reason=space entry=%s; footage retained", state_path.stem)
                    continue
                result["attempted"] += 1
                reason = recovery_spawn(c, entry, output_limit,
                    max(0.01, deadline - time.monotonic()))
                saved = load_recovery(state_path)
                if saved["status"] == "recovered":
                    result["recovered"] += 1
                    LOG.info("recovery completed entry=%s; original retained", state_path.stem)
                else:
                    held = s["attempts"] >= 3
                    atomic_json(state_path, dict(s, status="quarantined" if held else "backoff", reason=reason))
                    result["quarantined"] += int(held)
                    LOG.warning("recovery deferred reason=%s entry=%s attempts=%d; footage retained",
                                reason, state_path.stem, s["attempts"])
            except Exception:
                # An unreadable state or filesystem error is a hold, never a
                # reason to retry without a durable reservation or touch capture.
                LOG.warning("recovery state/io hold entry=%s; footage retained", state_path.stem)
    return result


def record_connection(c, url, run):
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-v",
        "error",
        "-rtsp_transport",
        "tcp",
        "-timeout",
        "15000000",
        "-i",
        url,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c",
        "copy",
        "-f",
        "segment",
        "-segment_format",
        "matroska",
        "-segment_format_options",
        "cluster_time_limit=5000",
        "-segment_time",
        str(c["segment_seconds"]),
        "-segment_atclocktime",
        "1",
        "-reset_timestamps",
        "1",
        "-strftime",
        "1",
        "-segment_list",
        "pipe:1",
        "-segment_list_type",
        "csv",
        str(run / "%Y%m%dT%H%M%SZ.mkv"),
    ]
    env = dict(os.environ, TZ="UTC")
    # FFmpeg stderr can contain the credential URL: never forward it to journals.
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=env,
        text=True,
    )
    try:
        events = queue.Queue()

        def reader():
            for row in csv.reader(p.stdout):
                if row:
                    events.put(row[0])

        thread = threading.Thread(target=reader, daemon=True)
        thread.start()
        last_change = time.monotonic()
        last_size = -1
        reason = "hour_boundary"
        while p.poll() is None:
            while not events.empty():
                raw = run / Path(events.get()).name
                try:
                    finalize(c, raw, "hour_boundary")
                except Exception:
                    LOG.warning(
                        "invalid closed segment retained; capture continues"
                    )
            size = sum(f.stat().st_size for f in run.glob("*.mkv"))
            if size != last_size:
                last_size = size
                last_change = time.monotonic()
            if (
                STOP.is_set()
                or not space_ok(c)
                or time.monotonic() - last_change > 60
            ):
                reason = (
                    "shutdown" if STOP.is_set() else "storage_guard_or_stream_stall"
                )
                p.send_signal(signal.SIGINT)
                try:
                    p.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait(timeout=2)
                break
            STOP.wait(2)
        thread.join(timeout=5)
        if reason == "hour_boundary":
            reason = "stream_ended"
        while not events.empty():
            raw = run / Path(events.get()).name
            if raw.exists():
                try:
                    finalize(c, raw, reason)
                except Exception:
                    LOG.warning("incomplete segment retained for recovery")
    finally:
        if p.poll() is None:
            p.send_signal(signal.SIGINT)
            try:
                p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=2)
        p.stdout.close()


def record(c, credentials):
    root = Path(c["spool"])
    for name in ("active", "ready", "receipts"):
        (root / name).mkdir(parents=True, exist_ok=True)
    with ((root / "recorder.lock").open("w") as lock,
          (root / "recovery-aware.lock").open("a+b") as protocol_lock):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(protocol_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        url = credential_url(credentials)
        while not STOP.is_set():
            if not space_ok(c):
                LOG.warning("storage guard: recording paused; unverified footage retained")
                STOP.wait(30)
                continue
            run = root / "active" / uuid.uuid4().hex
            run.mkdir()
            # Recovery takes the same lock before looking at any file in a run.
            with (run / "capture.lock").open("a+b") as capture_lock:
                fcntl.flock(capture_lock, fcntl.LOCK_EX)
                atomic_json(run / "session.json", {
                    "started_utc": now(),
                    "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()
                    if Path("/proc/sys/kernel/random/boot_id").exists() else "test",
                })
                try:
                    record_connection(c, url, run)
                except Exception:
                    LOG.warning("capture connection failed; closed footage retained")
            # Do not remove a run that contains any footage or partial artifacts.
            if all(p.name in ("session.json", "capture.lock") for p in run.iterdir()):
                shutil.rmtree(run)
            LOG.info("capture connection ended; reconnecting")
            STOP.wait(5)



def xml_file(path, tag, values):
    root = ET.Element(tag)
    for k, value in values.items():
        ET.SubElement(root, k).text = str(value)
    ET.indent(root)
    with open(path, "wb") as f:
        f.write(ET.tostring(root, encoding="utf-8", xml_declaration=True))
        f.flush()
        os.fsync(f.fileno())


def check_manifest(m):
    if not isinstance(m, dict):
        raise ValueError("invalid manifest")
    if not isinstance(m.get("id"), str) or not re.fullmatch(
        r"[a-f0-9]{32}-\d{8}T\d{6}Z", m["id"]
    ):
        raise ValueError("invalid id")
    dt.datetime.strptime(m["id"].rsplit("-", 1)[1], "%Y%m%dT%H%M%SZ")
    if not isinstance(m.get("sha256"), str) or not re.fullmatch("[a-f0-9]{64}", m["sha256"]):
        raise ValueError("invalid hash")
    if type(m.get("size")) is not int or not 0 < m["size"] <= 8 * 1024**3:
        raise ValueError("invalid size")
    for k in ("start_utc", "end_utc"):
        if dt.datetime.fromisoformat(m[k]).utcoffset() != dt.timedelta():
            raise ValueError("timestamps must be UTC")
    if "closed_utc" in m and dt.datetime.fromisoformat(m["closed_utc"]).utcoffset() != dt.timedelta():
        raise ValueError("timestamps must be UTC")
    if dt.datetime.fromisoformat(m["end_utc"]) <= dt.datetime.fromisoformat(m["start_utc"]):
        raise ValueError("invalid time range")
    duration = m.get("duration_seconds")
    if type(duration) not in (int, float) or not 0 < duration < 172800:
        raise ValueError("invalid duration")
    if not all(isinstance(m.get(k), str) for k in ("timestamp_basis", "close_reason")):
        raise ValueError("invalid metadata")
    return m


def receipt_matches(ack, m):
    return isinstance(ack, dict) and type(ack.get("size")) is int and ack.get("status") == "committed" and all(
        ack.get(k) == m[k] for k in ("id", "sha256", "size")
    )


def check_saved_receipt(saved, m):
    if not isinstance(saved, dict) or not receipt_matches(saved.get("receipt"), m):
        raise ItemFailure("receipt_invalid")
    verified = saved.get("verified_at")
    if type(verified) not in (int, float) or not math.isfinite(verified) or verified < 0:
        raise ItemFailure("receipt_invalid")
    return saved


def receive(c, input_stream=sys.stdin.buffer):
    line = input_stream.readline(65537)
    if len(line) > 65536:
        raise ItemFailure("remote_rejected")
    try:
        req = json.loads(line)
        m = check_manifest(req["manifest"])
        if req.get("action") not in ("upload", "verify"):
            raise ValueError("invalid action")
    except (ValueError, KeyError, TypeError):
        raise ItemFailure("remote_rejected") from None
    base = Path(c["library_root"])
    state = Path(c["state"])
    state.mkdir(parents=True, exist_ok=True)
    with (state / "lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if base.stat().st_dev != state.stat().st_dev:
            raise ServiceUnavailable("staging and media must share a filesystem")
        if (
            shutil.disk_usage(base).free
            < m["size"] + c.get("server_reserve_bytes", 1024**3)
            and req.get("action") == "upload"
        ):
            raise ServiceUnavailable("server storage reserve reached")
        with contextlib.closing(sqlite3.connect(state / "receipts.sqlite3")) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS recordings (id TEXT PRIMARY KEY, sha TEXT, size INTEGER, season INTEGER, episode INTEGER, path TEXT, committed INTEGER DEFAULT 0)"
            )
            row = db.execute(
                "SELECT sha,size,season,episode,path,committed FROM recordings WHERE id=?",
                (m["id"],),
            ).fetchone()
            if row and (row[0], row[1]) != (m["sha256"], m["size"]):
                raise ItemFailure("remote_rejected")
            if row and row[5]:
                video = Path(row[4])
                try:
                    if video.stat().st_size != m["size"] or digest(video) != m["sha256"]:
                        raise ValueError("server copy changed")
                    # Verify metadata too, before authorizing any local cleanup.
                    stored = json.loads(video.with_suffix(".json").read_text())
                    if stored != m or not video.with_suffix(".nfo").is_file():
                        raise ValueError("metadata missing or changed")
                except (FileNotFoundError, ValueError, TypeError):
                    raise ItemFailure("remote_rejected") from None
                ack = dict(
                    status="committed",
                    id=m["id"],
                    sha256=m["sha256"],
                    size=m["size"],
                    path=str(video),
                )
                print(json.dumps(ack), flush=True)
                return
            if req.get("action") != "upload":
                raise ItemFailure("remote_rejected")
            local = dt.datetime.fromisoformat(m["start_utc"]).astimezone(
                ZoneInfo(c["timezone"])
            )
            year, month = map(int, c["season_epoch"].split("-"))
            season = (local.year - year) * 12 + local.month - month + 1
            if not 1 <= season <= 9999:
                raise ItemFailure("remote_rejected")
            if row:
                episode = row[3]
            else:
                episode = db.execute(
                    "SELECT COALESCE(MAX(episode),0)+1 FROM recordings WHERE season=?",
                    (season,),
                ).fetchone()[0]
                if episode > 9999:
                    raise ItemFailure("remote_rejected")
            season_dir = base / ("Season %02d" % season)
            season_dir.mkdir(parents=True, exist_ok=True)
            stem = "Recording - S%02dE%02d - %s - %s" % (
                season,
                episode,
                local.strftime("%Y-%m-%d %H-%M-%S"),
                m["id"][:8],
            )
            video = season_dir / (stem + ".mkv")
            db.execute(
                "INSERT OR IGNORE INTO recordings(id,sha,size,season,episode,path) VALUES(?,?,?,?,?,?)",
                (m["id"], m["sha256"], m["size"], season, episode, str(video)),
            )
            db.commit()
            stage = state / (m["id"] + ".partial")
            if stage.exists():
                shutil.rmtree(stage)
            stage.mkdir()
            temp_video = stage / video.name
            h = hashlib.sha256()
            remaining = m["size"]
            with temp_video.open("wb") as f:
                while remaining:
                    signal.alarm(c.get("receive_idle_timeout", 0))
                    block = input_stream.read(min(1024 * 1024, remaining))
                    if not block:
                        raise ServiceUnavailable("interrupted upload")
                    f.write(block)
                    h.update(block)
                    remaining -= len(block)
                f.flush()
                os.fsync(f.fileno())
            if h.hexdigest() != m["sha256"]:
                raise ItemFailure("remote_rejected")
            try:
                _, duration = probe(temp_video, c.get("ffprobe", "ffprobe"))
            except ValueError:
                raise ItemFailure("remote_rejected") from None
            if abs(duration - m["duration_seconds"]) > 2:
                raise ItemFailure("remote_rejected")
            title = (
                local.strftime("%Y-%m-%d %H:%M:%S")
                + " – "
                + dt.datetime.fromisoformat(m["end_utc"])
                .astimezone(ZoneInfo(c["timezone"]))
                .strftime("%H:%M:%S %z")
            )
            if m.get("partial"):
                title += " [partial]"
            if m.get("target_seconds", 3600) != 3600:
                title += " [validation]"
            atomic_json(temp_video.with_suffix(".json"), m)
            xml_file(
                temp_video.with_suffix(".nfo"),
                "episodedetails",
                dict(
                    title=title,
                    showtitle=c["show_title"],
                    season=season,
                    episode=episode,
                    aired=local.strftime("%Y-%m-%d"),
                    plot=f"Recorded {m['start_utc']} to {m['end_utc']} UTC. {m['timestamp_basis']}. Close reason: {m['close_reason']}.",
                    lockdata="false",
                ),
            )
            xml_file(
                base / "tvshow.nfo",
                "tvshow",
                dict(
                    title=c["show_title"],
                    plot="Local camera archive. Episode titles show recording times.",
                    lockdata="false",
                ),
            )
            xml_file(
                season_dir / "season.nfo",
                "season",
                dict(
                    title=local.strftime("%B %Y"), seasonnumber=season, lockdata="false"
                ),
            )
            syncdir(stage)
            if video.exists():
                # Recovery after publishing video but before the database commit.
                try:
                    if (digest(video) != m["sha256"]
                            or json.loads(video.with_suffix(".json").read_text()) != m):
                        raise ValueError("publication conflict")
                except (ValueError, FileNotFoundError):
                    raise ItemFailure("remote_rejected") from None
            else:
                # Sidecars are durable before the video becomes visible to Jellyfin.
                for suffix in (".json", ".nfo"):
                    os.replace(
                        temp_video.with_suffix(suffix), video.with_suffix(suffix)
                    )
                syncdir(season_dir)
                os.link(temp_video, video)  # Atomic publication, never overwrite.
            syncdir(season_dir)
            shutil.rmtree(stage)
            db.execute("UPDATE recordings SET committed=1 WHERE id=?", (m["id"],))
            db.commit()
            db.close()
            syncdir(state)
            print(
                json.dumps(
                    dict(
                        status="committed",
                        id=m["id"],
                        sha256=m["sha256"],
                        size=m["size"],
                        path=str(video),
                    )
                ),
                flush=True,
            )


def remote(c, m, action, video=None):
    args = [
        "ssh",
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "UserKnownHostsFile=" + c["known_hosts"],
        "-i",
        c["ssh_key"],
        c["destination"],
    ]
    with tempfile.TemporaryFile() as out:
        try:
            p = subprocess.Popen(
                args, stdin=subprocess.PIPE, stdout=out, stderr=subprocess.DEVNULL
            )
        except OSError:
            raise ServiceUnavailable("transport unavailable") from None
        errors = []

        def writer():
            try:
                p.stdin.write(
                    (json.dumps(dict(action=action, manifest=m)) + "\n").encode()
                )
                if video:
                    with open(video, "rb") as f:
                        shutil.copyfileobj(f, p.stdin, 1024 * 1024)
                p.stdin.close()
            except (OSError, ValueError) as exc:
                errors.append(type(exc).__name__)

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        try:
            p.wait(timeout=c.get("upload_timeout", 1800))
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
            raise ServiceUnavailable("upload timeout") from None
        t.join(timeout=5)
        out.seek(0)
        try:
            ack = json.loads(out.read(65537))
        except (ValueError, UnicodeError):
            if p.returncode:
                raise ServiceUnavailable("transport failed") from None
            raise ItemFailure("remote_receipt_invalid") from None
        if p.returncode:
            if (p.returncode != 255 and isinstance(ack, dict)
                    and ack.get("status") == "rejected" and ack.get("scope") == "item"):
                raise ItemFailure("remote_rejected")
            raise ServiceUnavailable("transport failed")
        if not receipt_matches(ack, m):
            raise ItemFailure("remote_receipt_invalid")
        return ack


def retry_path(root, manifest):
    # Never persist arbitrary filenames or exception payloads in retry diagnostics.
    return root / "retry" / (hashlib.sha256(os.fsencode(manifest.name)).hexdigest() + ".json")


def load_retry(path):
    if not path.exists() and not path.is_symlink():
        return None
    state = read_regular_json(path)
    if (not isinstance(state, dict) or state.get("version") != 1
            or type(state.get("failures")) is not int or state["failures"] < 1
            or state.get("reason") not in RETRY_REASONS
            or not isinstance(state.get("signature"), str)):
        raise ValueError("invalid retry state")
    for k in ("last_attempt", "next_attempt"):
        if type(state.get(k)) not in (int, float) or not math.isfinite(state[k]):
            raise ValueError("invalid retry time")
    if not 0 <= state["next_attempt"] - state["last_attempt"] <= 3600:
        raise ValueError("invalid retry delay")
    return state


def retry_due(state, signature, current):
    # Repairs are noticed promptly. A clock reversal cannot defer work forever.
    return (state is None or state["signature"] != signature
            or current >= state["next_attempt"] or current < state["last_attempt"])


def defer(path, state, signature, reason, current, maximum=3600):
    failures = (state["failures"] if state and state["signature"] == signature else 0) + 1
    delay = min(maximum, 60 * 2 ** min(failures - 1, 6))
    atomic_json(path, dict(version=1, failures=failures, signature=signature,
                           reason=reason, last_attempt=current, next_attempt=current + delay))
    LOG.warning("publication deferred reason=%s entry=%s retry_seconds=%d; footage retained",
                reason, path.stem, delay)


def clear_retry(path):
    if path.exists():
        path.unlink()
        syncdir(path.parent)


def queue_signature(manifest, receipts):
    parts = []
    for path in (manifest, manifest.with_suffix(".mkv"), receipts / manifest.name):
        try:
            s = path.lstat()
            parts.append((s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns))
        except FileNotFoundError:
            parts.append(None)
    return hashlib.sha256(json.dumps(parts).encode()).hexdigest()


def read_regular_json(path):
    if not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError("not a regular file")
    # Requests and manifests are small; do not read an accidentally substituted video.
    with path.open("rb") as source:
        data = source.read(65537)
    if len(data) > 65536:
        raise ValueError("oversize metadata")
    return json.loads(data)


def publish_item(c, manifest, m, receipts, network_ready):
    video = manifest.with_suffix(".mkv")
    rec = receipts / manifest.name
    saved = None
    if rec.exists() or rec.is_symlink():
        try:
            saved = check_saved_receipt(read_regular_json(rec), m)
        except (ValueError, KeyError, TypeError):
            raise ItemFailure("receipt_invalid") from None
    try:
        video_stat = video.lstat()
    except FileNotFoundError:
        if saved:
            # Complete a crash-interrupted, previously verified local cleanup.
            manifest.unlink()
            syncdir(manifest.parent)
            return "local"
        raise ItemFailure("video_missing") from None
    if not stat.S_ISREG(video_stat.st_mode):
        raise ItemFailure("video_changed")
    if not network_ready:
        return False
    if saved:
        if time.time() - saved["verified_at"] < c["grace_seconds"] and space_ok(c):
            return "local"
        ack = remote(c, m, "verify")  # Fresh remote rehash and sidecar checks.
        if not receipt_matches(ack, m):
            raise ItemFailure("remote_receipt_invalid")
        video.unlink()
        syncdir(manifest.parent)
        manifest.unlink()
        syncdir(manifest.parent)
        LOG.info("removed verified local recording %s", m["id"])
        return "remote"
    if video_stat.st_size != m["size"] or digest(video) != m["sha256"]:
        raise ItemFailure("video_changed")
    ack = remote(c, m, "upload", video)
    if not receipt_matches(ack, m):
        raise ItemFailure("remote_receipt_invalid")
    atomic_json(rec, dict(receipt=ack, verified_at=time.time()))
    LOG.info("server copy verified %s", m["id"])
    return "remote"


def publish_once(c):
    root = Path(c["spool"])
    ready = root / "ready"
    receipts = root / "receipts"
    ready.mkdir(parents=True, exist_ok=True)
    receipts.mkdir(exist_ok=True)
    retries = root / "retry"
    try:
        retries.mkdir()
    except FileExistsError:
        pass
    else:
        syncdir(root)
    service_path = retries / "service.json"
    current = time.time()
    try:
        service_state = load_retry(service_path)
        if service_state and (service_state["reason"] != "service_unavailable"
                              or service_state["signature"] != "service"
                              or service_state["next_attempt"] - service_state["last_attempt"] > 300):
            raise ValueError("invalid service retry state")
        network_ready = retry_due(service_state, "service", current)
    except Exception:
        # This controls scheduling only, never integrity/deletion authorization.
        # Do not let damaged shared metadata become a permanent queue blocker.
        service_state, network_ready = None, True
        LOG.warning("invalid service retry state; allowing one connection attempt")
    entries = []
    paths = list(ready.glob("*.json"))
    for manifest in paths:
        state_path = retry_path(root, manifest)
        state, signature = None, ""
        try:
            signature = queue_signature(manifest, receipts)
            state = load_retry(state_path)
            if not retry_due(state, signature, current):
                continue
            m = check_manifest(read_regular_json(manifest))
            if manifest.stem != m["id"]:
                raise ValueError("manifest filename mismatch")
            entries.append((dt.datetime.fromisoformat(m["start_utc"]), manifest, m, state, signature))
        except Exception:
            try:
                # Keep a corrupt retry file for diagnosis instead of silently resetting it.
                if state_path.exists() and state is None:
                    LOG.warning("invalid item retry state entry=%s; footage retained", state_path.stem)
                else:
                    defer(state_path, state, signature, "manifest_invalid", current)
            except Exception:
                LOG.warning("could not persist item retry; footage retained")
    network_succeeded = False
    for _, manifest, m, state, signature in sorted(entries, key=lambda entry: (entry[0], entry[1].name)):
        if STOP.is_set():
            break
        state_path = retry_path(root, manifest)
        try:
            result = publish_item(c, manifest, m, receipts, network_ready)
            if result:
                clear_retry(state_path)
                network_succeeded |= result == "remote"
        except ServiceUnavailable:
            network_ready = False
            try:
                defer(service_path, service_state, "service", "service_unavailable", time.time(), maximum=300)
            except Exception:
                LOG.warning("could not persist service retry; network work deferred")
        except Exception as exc:
            if isinstance(exc, ItemFailure):
                reason = exc.reason
            else:
                reason = "item_io" if isinstance(exc, OSError) else "item_failed"
            try:
                defer(state_path, state, signature, reason, time.time())
            except Exception:
                LOG.warning("could not persist item retry; footage retained")
    if network_ready and network_succeeded:
        clear_retry(service_path)
    present = {retry_path(root, p).name for p in ready.glob("*.json")}
    for path in retries.glob("*.json"):
        if path != service_path and path.name not in present:
            clear_retry(path)  # Obsolete scheduling metadata only; never footage/receipts.


def publish(c):
    root = Path(c["spool"])
    root.mkdir(parents=True, exist_ok=True)
    with (root / "publisher.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while not STOP.is_set():
            try:
                publish_once(c)
            except Exception as exc:
                LOG.warning(
                    "publish deferred (%s); local footage retained", type(exc).__name__
                )
            STOP.wait(c.get("publish_interval", 60))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["record", "publish", "receive", "recover", "_recover-one"])
    parser.add_argument("--config")
    parser.add_argument("--credentials")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    if args.mode == "_recover-one":
        request = json.loads(sys.stdin.buffer.read(65537))
        recover_one(request["config"], request["entry"], request["output_limit"])
        return
    if not args.config:
        parser.error("--config is required")
    c = json.loads(Path(args.config).read_text())
    if args.mode == "record":
        record(
            c,
            args.credentials
            or str(Path(os.environ["CREDENTIALS_DIRECTORY"]) / "camera.env"),
        )
    elif args.mode == "recover":
        try:
            print(json.dumps(recover(c)))
        except Exception:
            LOG.error("recovery pass held reason=state_io; footage retained")
            sys.exit(1)
    elif args.mode == "publish":
        publish(c)
    else:

        def idle_timeout(*_):
            raise TimeoutError("receiver idle timeout")

        signal.signal(signal.SIGALRM, idle_timeout)
        c.setdefault("receive_idle_timeout", 120)
        signal.alarm(c["receive_idle_timeout"])
        try:
            receive(c)
        except Exception as exc:
            # Only explicit item/validation failures bypass the shared outage backoff.
            scope = "item" if isinstance(exc, ItemFailure) else "service"
            print(json.dumps(dict(status="rejected", scope=scope)), flush=True)
            LOG.error("receiver rejected request (%s)", type(exc).__name__)
            sys.exit(1)


if __name__ == "__main__":
    main()
