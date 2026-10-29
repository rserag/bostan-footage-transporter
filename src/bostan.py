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
import os
from pathlib import Path
import queue
import re
import shutil
import signal
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
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    syncdir(path.parent)


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def probe(path, binary="ffprobe"):
    p = subprocess.run(
        [
            binary,
            "-v",
            "error",
            "-show_entries",
            "format=duration,size:stream=codec_type,codec_name,width,height,r_frame_rate,sample_rate,channels",
            "-of",
            "json",
            str(path),
        ],
        capture_output=True,
        timeout=90,
    )
    if p.returncode:
        raise ValueError("media probe failed")
    data = json.loads(p.stdout)
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
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


def space_ok(c):
    root = Path(c["spool"])
    return (
        shutil.disk_usage(root).free > c["reserve_bytes"]
        and spool_bytes(root) < c["max_spool_bytes"]
    )


def finalize(c, raw, reason):
    data, duration = probe(raw)
    start = dt.datetime.strptime(raw.stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    ident = raw.parent.name + "-" + raw.stem
    ready = Path(c["spool"]) / "ready"
    ready.mkdir(exist_ok=True)
    target = ready / (ident + ".mkv")
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
    with raw.open("rb") as f:
        os.fsync(f.fileno())
    os.replace(raw, target)
    syncdir(ready)
    atomic_json(ready / (ident + ".json"), m)
    LOG.info("sealed recording %s duration=%.1fs", ident, duration)


def recover(c):
    root = Path(c["spool"])
    # A crash between moving a video and writing its manifest is also recoverable.
    for raw in (root / "ready").glob("*.mkv"):
        if not raw.with_suffix(".json").exists():
            ident, stamp = raw.stem.rsplit("-", 1)
            dest = root / "active" / ident
            dest.mkdir(exist_ok=True)
            os.replace(raw, dest / (stamp + ".mkv"))
    for raw in (root / "active").glob("*/*.mkv"):
        fixed = raw.with_suffix(".recovering")
        result = subprocess.run(
            [
                "ffmpeg",
                "-nostdin",
                "-v",
                "error",
                "-i",
                str(raw),
                "-map",
                "0:v:0",
                "-map",
                "0:a?",
                "-c",
                "copy",
                "-f",
                "matroska",
                "-y",
                str(fixed),
            ],
            capture_output=True,
            timeout=300,
        )
        if result.returncode == 0:
            try:
                probe(fixed)
                os.replace(fixed, raw)
                finalize(c, raw, "recovered_after_restart")
            except ValueError:
                LOG.error("unrecoverable recording retained: %s", raw.name)
        else:
            LOG.error("damaged recording retained: %s", raw.name)
        fixed.unlink(missing_ok=True)


def record(c, credentials):
    root = Path(c["spool"])
    for name in ("active", "ready", "receipts"):
        (root / name).mkdir(parents=True, exist_ok=True)
    with (root / "recorder.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        recover(c)
        url = credential_url(credentials)
        while not STOP.is_set():
            if not space_ok(c):
                LOG.warning(
                    "storage guard: recording paused; unverified footage retained"
                )
                STOP.wait(30)
                continue
            run = root / "active" / uuid.uuid4().hex
            run.mkdir()
            atomic_json(
                run / "session.json",
                {
                    "started_utc": now(),
                    "boot_id": Path("/proc/sys/kernel/random/boot_id")
                    .read_text()
                    .strip()
                    if Path("/proc/sys/kernel/random/boot_id").exists()
                    else "test",
                },
            )
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
                    finalize(c, run / Path(events.get()).name, "hour_boundary")
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
                        p.wait()
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
                    except ValueError:
                        LOG.warning("incomplete segment retained for recovery")
            recover(c)
            if not list(run.glob("*.mkv")):
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
    if not re.fullmatch(r"[a-f0-9]{32}-\d{8}T\d{6}Z", m.get("id", "")):
        raise ValueError("invalid id")
    if not re.fullmatch("[a-f0-9]{64}", m.get("sha256", "")):
        raise ValueError("invalid hash")
    if not isinstance(m.get("size"), int) or not 0 < m["size"] <= 8 * 1024**3:
        raise ValueError("invalid size")
    for k in ("start_utc", "end_utc"):
        if dt.datetime.fromisoformat(m[k]).utcoffset() != dt.timedelta():
            raise ValueError("timestamps must be UTC")
    return m


def receipt_matches(ack, m):
    return ack.get("status") == "committed" and all(
        ack.get(k) == m[k] for k in ("id", "sha256", "size")
    )


def receive(c, input_stream=sys.stdin.buffer):
    line = input_stream.readline(65537)
    if len(line) > 65536:
        raise ValueError("oversize request")
    req = json.loads(line)
    m = check_manifest(req["manifest"])
    base = Path(c["library_root"])
    state = Path(c["state"])
    state.mkdir(parents=True, exist_ok=True)
    with (state / "lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if base.stat().st_dev != state.stat().st_dev:
            raise ValueError("staging and media must share a filesystem")
        if (
            shutil.disk_usage(base).free
            < m["size"] + c.get("server_reserve_bytes", 1024**3)
            and req.get("action") == "upload"
        ):
            raise ValueError("server storage reserve reached")
        with contextlib.closing(sqlite3.connect(state / "receipts.sqlite3")) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS recordings (id TEXT PRIMARY KEY, sha TEXT, size INTEGER, season INTEGER, episode INTEGER, path TEXT, committed INTEGER DEFAULT 0)"
            )
            row = db.execute(
                "SELECT sha,size,season,episode,path,committed FROM recordings WHERE id=?",
                (m["id"],),
            ).fetchone()
            if row and (row[0], row[1]) != (m["sha256"], m["size"]):
                raise ValueError("id conflict")
            if row and row[5]:
                video = Path(row[4])
                if video.stat().st_size != m["size"] or digest(video) != m["sha256"]:
                    raise ValueError("server copy changed")
                # Verify metadata still exists too, before authorizing local cleanup.
                stored = json.loads(video.with_suffix(".json").read_text())
                if stored != m or not video.with_suffix(".nfo").is_file():
                    raise ValueError("metadata missing or changed")
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
                raise ValueError("not committed")
            local = dt.datetime.fromisoformat(m["start_utc"]).astimezone(
                ZoneInfo(c["timezone"])
            )
            year, month = map(int, c["season_epoch"].split("-"))
            season = (local.year - year) * 12 + local.month - month + 1
            if not 1 <= season <= 9999:
                raise ValueError("date outside season range")
            if row:
                episode = row[3]
            else:
                episode = db.execute(
                    "SELECT COALESCE(MAX(episode),0)+1 FROM recordings WHERE season=?",
                    (season,),
                ).fetchone()[0]
                if episode > 9999:
                    raise ValueError("season episode limit")
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
                    block = input_stream.read(min(1024 * 1024, remaining))
                    if not block:
                        raise ValueError("interrupted upload")
                    f.write(block)
                    h.update(block)
                    remaining -= len(block)
                f.flush()
                os.fsync(f.fileno())
            if h.hexdigest() != m["sha256"]:
                raise ValueError("checksum mismatch")
            _, duration = probe(temp_video, c.get("ffprobe", "ffprobe"))
            if abs(duration - m["duration_seconds"]) > 2:
                raise ValueError("duration mismatch")
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
                if (
                    digest(video) != m["sha256"]
                    or json.loads(video.with_suffix(".json").read_text()) != m
                ):
                    raise ValueError("publication conflict")
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
        p = subprocess.Popen(
            args, stdin=subprocess.PIPE, stdout=out, stderr=subprocess.DEVNULL
        )
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
            raise ValueError("upload timeout")
        t.join(timeout=5)
        if p.returncode:
            raise ValueError("transport failed; local copy retained")
        out.seek(0)
        ack = json.loads(out.read(65537))
        if not receipt_matches(ack, m):
            raise ValueError("receipt mismatch; local copy retained")
        return ack


def publish_once(c):
    root = Path(c["spool"])
    ready = root / "ready"
    receipts = root / "receipts"
    ready.mkdir(parents=True, exist_ok=True)
    receipts.mkdir(exist_ok=True)
    for manifest in sorted(
        ready.glob("*.json"), key=lambda p: json.loads(p.read_text())["start_utc"]
    ):
        m = check_manifest(json.loads(manifest.read_text()))
        video = manifest.with_suffix(".mkv")
        if not video.exists():
            # Local video was removed after confirmed delivery; complete interrupted cleanup.
            r = receipts / (m["id"] + ".json")
            if r.exists() and receipt_matches(json.loads(r.read_text())["receipt"], m):
                manifest.unlink()
            continue
        rec = receipts / (m["id"] + ".json")
        if rec.exists():
            saved = json.loads(rec.read_text())
            if not receipt_matches(saved["receipt"], m):
                raise ValueError("saved receipt mismatch")
            if time.time() - saved["verified_at"] >= c["grace_seconds"] or not space_ok(
                c
            ):
                remote(
                    c, m, "verify"
                )  # Rehash the durable remote copy before deletion.
                video.unlink()
                syncdir(ready)
                manifest.unlink()
                syncdir(ready)
                LOG.info("removed verified local recording %s", m["id"])
            continue
        if video.stat().st_size != m["size"] or digest(video) != m["sha256"]:
            raise ValueError("local recording changed")
        ack = remote(c, m, "upload", video)
        atomic_json(rec, dict(receipt=ack, verified_at=time.time()))
        LOG.info("server copy verified %s", m["id"])


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
    parser.add_argument("mode", choices=["record", "publish", "receive"])
    parser.add_argument("--config", required=True)
    parser.add_argument("--credentials")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    c = json.loads(Path(args.config).read_text())
    if args.mode == "record":
        record(
            c,
            args.credentials
            or str(Path(os.environ["CREDENTIALS_DIRECTORY"]) / "camera.env"),
        )
    elif args.mode == "publish":
        publish(c)
    else:
        try:
            receive(c)
        except Exception as exc:
            LOG.error("receiver rejected request (%s)", type(exc).__name__)
            sys.exit(1)


if __name__ == "__main__":
    main()
