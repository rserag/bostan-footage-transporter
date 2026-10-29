import contextlib
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sqlite3
import tempfile
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

    def test_missing_metadata_not_acknowledged(self):
        a = self.request()
        Path(a["path"]).with_suffix(".json").unlink()
        with self.assertRaises(FileNotFoundError):
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
        with patch.object(b, "remote", side_effect=ValueError("offline")):
            with self.assertRaises(ValueError):
                b.publish_once(c)
        self.assertTrue(p.exists())

    def test_remote_recheck_required_before_delete(self):
        c, p = self.queue()
        ack = dict(self.m, status="committed")
        with patch.object(b, "remote", return_value=ack):
            b.publish_once(c)
        self.assertTrue(p.exists())
        with patch.object(b, "remote", side_effect=ValueError("remote gone")):
            with self.assertRaises(ValueError):
                b.publish_once(c)
        self.assertTrue(p.exists())
        with patch.object(b, "remote", return_value=ack):
            b.publish_once(c)
        self.assertFalse(p.exists())

    def test_server_full_does_not_publish(self):
        self.c["server_reserve_bytes"] = 10**18
        with self.assertRaises(ValueError):
            self.request()
        self.assertEqual(list(self.library.rglob("*.mkv")), [])

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
        self.assertEqual(list(run.glob("*.mkv")), [])


if __name__ == "__main__":
    unittest.main()
