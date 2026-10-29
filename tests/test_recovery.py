"""Recovery fault tests use isolated spools, real media and disposable children."""
import fcntl
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).parents[1] / 'src/bostan.py'
spec = importlib.util.spec_from_file_location('recovery_bostan', SOURCE)
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
spec = importlib.util.spec_from_file_location('recovery_monitor', SOURCE.with_name('bostan_monitor.py'))
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)


class RecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixtures = tempfile.TemporaryDirectory()
        video = Path(cls.fixtures.name) / 'fixture.mkv'
        subprocess.run(['ffmpeg', '-v', 'error', '-f', 'lavfi', '-i', 'testsrc2=size=160x120:rate=10',
                        '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=8000', '-t', '2',
                        '-c:v', 'libx264', '-preset', 'ultrafast', '-c:a', 'pcm_alaw', str(video)], check=True)
        cls.content = video.read_bytes()

    @classmethod
    def tearDownClass(cls):
        cls.fixtures.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.c = dict(spool=str(self.root / 'spool'), camera_name='Test', timezone='UTC',
                      segment_seconds=3600, reserve_bytes=0, max_spool_bytes=10**9)
        self.spool = Path(self.c['spool'])
        for name in ('active', 'ready', 'receipts'):
            (self.spool / name).mkdir(parents=True)
        self.current = time.time()

    def raw(self, number=0, content=None):
        run = self.spool / 'active' / format(number + 1, '032x')
        run.mkdir(exist_ok=True)
        raw = run / '20261029T120000Z.mkv'
        raw.write_bytes(self.content if content is None else content)
        return raw

    def entry(self, raw):
        return raw.relative_to(self.spool)

    def state(self, raw):
        return b.load_recovery(b.recovery_path(self.c, self.entry(raw)))

    def pass_at(self, seconds=0):
        with patch.object(b.time, 'time', return_value=self.current + seconds):
            return b.recover(self.c)

    def worker_with(self, code):
        # Inject a fault in a child; rlimits must never be applied to the tests.
        def spawn(c, entry, limit, timeout):
            wrapper = '''import importlib.util,json,sys
from pathlib import Path
from unittest.mock import patch
s=importlib.util.spec_from_file_location('worker',sys.argv[1]); b=importlib.util.module_from_spec(s);s.loader.exec_module(b)
r=json.load(sys.stdin)
'''+code+'''
b.recover_one(r['config'],r['entry'],r['output_limit'])
'''
            r = subprocess.run([sys.executable, '-c', wrapper, str(SOURCE)],
                input=json.dumps(dict(config=c, entry=str(entry), output_limit=limit)).encode(),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
            return 'recovered' if r.returncode == 0 else 'failed'
        return spawn

    def test_success_preserves_original_and_publishes_hash_bound_manifest(self):
        raw = self.raw()
        result = b.recover(self.c)
        self.assertEqual(result['recovered'], 1)
        self.assertEqual(raw.read_bytes(), self.content)
        self.assertEqual(self.state(raw)['status'], 'recovered')
        manifest = next((self.spool / 'ready').glob('*.json'))
        m = b.check_manifest(json.loads(manifest.read_text()))
        self.assertEqual(m['sha256'], b.digest(manifest.with_suffix('.mkv')))
        self.assertEqual(m['size'], manifest.with_suffix('.mkv').stat().st_size)
        self.assertEqual(m['close_reason'], 'recovered_after_restart')
        self.assertTrue({'h264', 'pcm_alaw'} <= {s['codec_name'] for s in m['media']['streams']})
        # A receipt/grace cleanup must not cause another publication of originals.
        manifest.with_suffix('.mkv').unlink()
        manifest.unlink()
        with patch.object(b, 'recovery_spawn') as spawned:
            self.pass_at(100000)
        spawned.assert_not_called()
        self.assertEqual(list((self.spool / 'ready').iterdir()), [])

    def test_truncated_real_media_is_salvaged_without_altering_its_original(self):
        truncated = self.content[:len(self.content) * 9 // 10]
        raw = self.raw(content=truncated)
        self.assertEqual(self.pass_at()['recovered'], 1)
        self.assertEqual(raw.read_bytes(), truncated)
        video = next((self.spool / 'ready').glob('*.mkv'))
        data, duration = b.probe(video)
        self.assertGreater(duration, 0)
        self.assertTrue(any(stream['codec_type'] == 'video' for stream in data['streams']))
        subprocess.run(['ffmpeg', '-v', 'error', '-i', str(video), '-f', 'null', '-'],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True, timeout=20)

    def test_several_bad_files_cannot_multiply_pass_attempts(self):
        raws = [self.raw(i, b'broken') for i in range(8)]
        with patch.object(b, 'recovery_spawn', return_value='timeout') as spawned:
            self.pass_at()
            self.assertEqual(spawned.call_count, 2)
            # Even an immediate new process/pass is covered by durable shared state.
            b.recover(self.c)
            self.assertEqual(spawned.call_count, 2)
        self.assertTrue(all(p.read_bytes() == b'broken' for p in raws))
        self.assertEqual(sum(self.state(p) is not None for p in raws), 2)

    def test_attempts_survive_restart_then_permanently_hold_until_operator_reset(self):
        raw = self.raw(content=b'broken')
        with patch.object(b, 'recovery_spawn', return_value='failed') as spawned:
            for seconds, attempts in ((0, 1), (301, 2), (902, 3), (99999, 3)):
                self.pass_at(seconds)
                s = self.state(raw)
                self.assertEqual(s['attempts'], attempts)
            self.assertEqual(spawned.call_count, 3)
        self.assertEqual(s['status'], 'quarantined')
        self.assertEqual(raw.read_bytes(), b'broken')

    def test_crash_after_durable_reservation_consumes_an_attempt(self):
        raw = self.raw()
        # Simulate supervisor death immediately after durable reservation.
        with patch.object(b, 'recovery_spawn', side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                self.pass_at()
        self.assertEqual(self.state(raw)['status'], 'running')
        with patch.object(b, 'recovery_spawn', return_value='timeout') as spawned:
            self.pass_at(100)
            spawned.assert_not_called()
            self.pass_at(301)
        self.assertEqual(self.state(raw)['attempts'], 2)

    def test_live_run_is_skipped_without_creating_failure_state(self):
        raw = self.raw()
        with (raw.parent / 'capture.lock').open('a+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with patch.object(b, 'recovery_spawn') as spawned:
                self.pass_at()
            spawned.assert_not_called()
        self.assertIsNone(self.state(raw))
        self.assertEqual(raw.read_bytes(), self.content)

    def test_live_legacy_recorder_blocks_media_until_protocol_is_upgraded(self):
        raw = self.raw()
        with (self.spool / 'recorder.lock').open('a+b') as recorder:
            fcntl.flock(recorder, fcntl.LOCK_EX)
            with patch.object(b, 'recovery_spawn') as spawned:
                self.pass_at()
            spawned.assert_not_called()
            self.assertIsNone(self.state(raw))
            with (self.spool / 'recovery-aware.lock').open('a+b') as protocol:
                fcntl.flock(protocol, fcntl.LOCK_EX)
                with patch.object(b, 'recovery_spawn', return_value='failed') as spawned:
                    self.pass_at()
                self.assertEqual(spawned.call_count, 1)
        self.assertEqual(raw.read_bytes(), self.content)

    def test_low_space_reserves_capture_headroom_and_does_not_spend_attempts(self):
        raw = self.raw()
        used = b.spool_bytes(self.spool)
        self.c['max_spool_bytes'] = used + 64 * 1024**2
        with patch.object(b, 'recovery_spawn') as spawned:
            self.pass_at()
        spawned.assert_not_called()
        self.assertEqual(self.state(raw)['reason'], 'space')
        self.assertEqual(self.state(raw)['attempts'], 0)
        self.assertEqual(raw.read_bytes(), self.content)

    def test_filesystem_reserve_also_prevents_remux(self):
        raw = self.raw()
        with patch.object(b.shutil, 'disk_usage', return_value=shutil._ntuple_diskusage(100, 99, 1)), \
             patch.object(b, 'recovery_spawn') as spawned:
            self.pass_at()
        spawned.assert_not_called()
        self.assertEqual(self.state(raw)['reason'], 'space')

    def test_missing_bad_names_symlink_fifo_and_legacy_partial_are_retained(self):
        valid = self.raw()
        external = self.root / 'external.mkv'
        external.write_bytes(self.content)
        odd = valid.parent / (os.fsdecode(b'\xff-PRIVATE_SENTINEL\n.mkv') if sys.platform == 'linux'
                              else 'PRIVATE_SENTINEL\n.mkv')
        odd.write_bytes(b'odd')
        link = valid.parent / '20261029T130000Z.mkv'
        link.symlink_to(external)
        fifo = valid.parent / '20261029T140000Z.mkv'
        os.mkfifo(fifo)
        legacy = valid.with_suffix('.recovering')
        legacy.write_bytes(b'partial')
        for item in (odd, link, fifo, legacy):
            with self.assertRaises(ValueError):
                b.recovery_entry(self.c, self.entry(item))
        with self.assertLogs(b.LOG, 'WARNING') as logs:
            self.pass_at()
        self.assertNotIn('PRIVATE_SENTINEL', '\n'.join(logs.output))
        for item in (odd, link, fifo, legacy):
            self.assertEqual(self.state(item)['status'], 'quarantined')
            self.assertTrue(os.path.lexists(item))
        self.assertEqual(external.read_bytes(), self.content)
        with self.assertRaises(FileNotFoundError):
            b.recovery_entry(self.c, 'active/' + 'a'*32 + '/20261029T120000Z.mkv')

    def test_symlink_run_never_followed(self):
        external = self.root / ('e'*32)
        external.mkdir()
        (external / '20261029T120000Z.mkv').write_bytes(self.content)
        link = self.spool / 'active' / external.name
        link.symlink_to(external, target_is_directory=True)
        with patch.object(b, 'recovery_spawn') as spawned:
            self.pass_at()
        spawned.assert_not_called()
        self.assertEqual((external / '20261029T120000Z.mkv').read_bytes(), self.content)
        self.assertEqual(self.state(link)['status'], 'quarantined')

    def test_corrupt_item_state_holds_only_that_file_and_global_state_fails_closed(self):
        raw, later = self.raw(), self.raw(1)
        directory = self.spool / 'recovery'
        directory.mkdir()
        b.recovery_path(self.c, self.entry(raw)).write_text('{')
        with patch.object(b, 'recovery_spawn', return_value='failed') as spawned:
            self.pass_at()
        self.assertEqual(spawned.call_count, 1)
        self.assertEqual(spawned.call_args.args[1], self.entry(later))
        (directory / 'service.json').write_text('{')
        with patch.object(b, 'recovery_spawn') as spawned, self.assertRaises(ValueError):
            self.pass_at(1000)
        spawned.assert_not_called()

    def test_state_write_failure_cannot_start_unreserved_work(self):
        raw = self.raw()
        original = b.atomic_json
        def write(path, value):
            if path.name != 'service.json':
                raise OSError('injected disk error')
            return original(path, value)
        with patch.object(b, 'atomic_json', side_effect=write), patch.object(b, 'recovery_spawn') as spawned:
            self.pass_at()
        spawned.assert_not_called()
        self.assertEqual(raw.read_bytes(), self.content)

    def test_remux_error_and_probe_failure_preserve_original_and_failed_output(self):
        for code in ("b.subprocess.run=lambda *a,**k: type('R',(),{'returncode':1})()",
                     "b.probe=lambda *a,**k: (_ for _ in ()).throw(ValueError('injected'))",
                     "b.probe=lambda *a,**k: (_ for _ in ()).throw(b.subprocess.TimeoutExpired(['probe'],90))"):
            with self.subTest(code=code):
                raw = self.raw(len(list((self.spool / 'active').iterdir())))
                with patch.object(b, 'recovery_spawn', side_effect=self.worker_with(code)):
                    self.pass_at(100000 * len(list((self.spool / 'active').iterdir())))
                self.assertEqual(raw.read_bytes(), self.content)
                self.assertEqual(self.state(raw)['status'], 'backoff')
        self.assertGreater(len(list((self.spool / 'recovery/work').glob('*/attempt-*.mkv'))), 0)
        self.assertEqual(list((self.spool / 'ready').glob('*.json')), [])

    def test_interrupted_seal_reuses_validated_output_without_overwrite(self):
        raw = self.raw()
        code = "original=b.exclusive_json\ndef fail(path,obj):\n if path.parent.name=='ready': raise OSError('injected')\n return original(path,obj)\nb.exclusive_json=fail"
        with patch.object(b, 'recovery_spawn', side_effect=self.worker_with(code)):
            self.pass_at()
        video = next((self.spool / 'ready').glob('*.mkv'))
        before = (video.stat().st_ino, b.digest(video))
        self.assertEqual(list((self.spool / 'ready').glob('*.json')), [])
        self.pass_at(301)
        self.assertEqual((video.stat().st_ino, b.digest(video)), before)
        self.assertEqual(len(list((self.spool / 'ready').glob('*.json'))), 1)
        self.assertEqual(self.state(raw)['status'], 'recovered')
        self.assertEqual(raw.read_bytes(), self.content)
        self.assertEqual(len(list((self.spool / 'recovery/work').glob('*/attempt-*.mkv'))), 1)

    def test_interrupted_normal_finalization_retains_source_and_ready_orphan_completes(self):
        raw = self.raw()
        with patch.object(b, 'exclusive_json', side_effect=OSError('injected')):
            with self.assertRaises(OSError):
                b.finalize(self.c, raw, 'test')
        self.assertEqual(raw.read_bytes(), self.content)
        self.pass_at()
        self.assertEqual(self.state(raw)['status'], 'recovered')
        ready = next((self.spool / 'ready').glob('*.mkv'))
        self.assertEqual(ready.read_bytes(), self.content)
        self.assertEqual(len(list((self.spool / 'ready').glob('*.json'))), 1)

    def test_sealed_metadata_keeps_group_read_access_and_stale_temp_symlink_is_untouched(self):
        raw = self.raw()
        outside = self.root / 'preserve.txt'
        outside.write_text('unchanged evidence')
        state = self.spool / 'schedule.json'
        stale = state.with_suffix('.json.tmp')
        stale.symlink_to(outside)
        mask = os.umask(0o027)
        try:
            b.atomic_json(state, {'ok': True})
            b.finalize(self.c, raw, 'test')
        finally:
            os.umask(mask)
        manifest = next((self.spool / 'ready').glob('*.json'))
        self.assertEqual(manifest.stat().st_mode & 0o777, 0o640)
        self.assertEqual(state.stat().st_mode & 0o777, 0o640)
        self.assertEqual(outside.read_text(), 'unchanged evidence')
        self.assertTrue(stale.is_symlink())

    def test_ready_only_orphan_and_filename_without_clock(self):
        ready = self.spool / 'ready' / ('a'*32 + '-20261029T120000Z.mkv')
        ready.write_bytes(self.content)
        bad = self.spool / 'ready' / 'missing-clock.mkv'
        bad.write_bytes(b'retain')
        self.pass_at()
        self.assertTrue(ready.with_suffix('.json').exists())
        self.assertEqual(ready.read_bytes(), self.content)
        self.assertEqual(self.state(bad)['status'], 'quarantined')

    def test_conflicting_ready_assets_are_never_overwritten(self):
        raw = self.raw()
        target = self.spool / 'ready' / (b.recording_id(raw) + '.mkv')
        target.write_bytes(b'different evidence')
        manifest = target.with_suffix('.json')
        manifest.write_text('{"preserve":"conflicting manifest"}')
        self.pass_at()
        self.assertEqual(target.read_bytes(), b'different evidence')
        self.assertEqual(manifest.read_text(), '{"preserve":"conflicting manifest"}')
        self.assertEqual(raw.read_bytes(), self.content)
        self.assertEqual(self.state(raw)['status'], 'backoff')

    def test_real_timeout_kills_worker_and_descendant_and_bounds_whole_pass(self):
        raws = [self.raw(i) for i in range(5)]
        tools = self.root / 'tools'
        tools.mkdir()
        pidfile = self.root / 'child.pid'
        tool = tools / 'ffmpeg'
        tool.write_text('#!' + sys.executable + '\nimport subprocess,sys,time\n'
                        'p=subprocess.Popen([sys.executable,"-c","import time;time.sleep(60)"])\n'
                        'open(' + repr(str(pidfile)) + ',"w").write(str(p.pid))\ntime.sleep(60)\n')
        tool.chmod(0o755)
        # Leave room for Python/tool startup on the original ARMv6 Pi. The fake
        # media operation itself sleeps for 60s, far beyond this shared budget.
        self.c['recovery_budget_seconds'] = 8
        start = time.monotonic()
        with patch.dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH']):
            result = self.pass_at()
        self.assertLess(time.monotonic() - start, 12)
        self.assertEqual(result['attempted'], 1)
        attempted = [self.state(raw) for raw in raws if self.state(raw)]
        self.assertEqual([s['reason'] for s in attempted], ['timeout'])
        self.assertTrue(pidfile.exists())
        pid = int(pidfile.read_text())
        # A reparented zombie is dead too; Linux init may reap it asynchronously.
        if sys.platform == 'linux' and Path('/proc', str(pid), 'stat').exists():
            self.assertEqual(Path('/proc', str(pid), 'stat').read_text().split()[2], 'Z')
        elif sys.platform != 'linux':
            p = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)], capture_output=True, text=True)
            self.assertTrue(not p.stdout.strip() or p.stdout.strip().startswith('Z'))
        self.assertTrue(all(p.read_bytes() == self.content for p in raws))

    def test_output_disk_limit_preserves_source_and_never_publishes_partial(self):
        raw = self.raw()
        tools = self.root / 'tools'
        tools.mkdir()
        tool = tools / 'ffmpeg'
        tool.write_text('#!' + sys.executable + '\nimport sys\nf=open(sys.argv[-1],"wb")\n'
                        'while True: f.write(b"x"*65536);f.flush()\n')
        tool.chmod(0o755)
        self.c['recovery_max_output_bytes'] = 131072
        with patch.dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH']):
            self.pass_at()
        outputs = list((self.spool / 'recovery/work').glob('*/attempt-*.mkv'))
        self.assertEqual(len(outputs), 1)
        self.assertLessEqual(outputs[0].stat().st_size, 131072)
        self.assertEqual(raw.read_bytes(), self.content)
        self.assertEqual(list((self.spool / 'ready').glob('*.json')), [])

    def test_probe_bounds_metadata_and_suppresses_error_output(self):
        tool = self.root / 'noisy-probe'
        tool.write_text('#!' + sys.executable + '\nimport sys\n'
                        'sys.stderr.write("PRIVATE_SENTINEL"*10000)\n'
                        'sys.stdout.write("x"*1000000)\n')
        tool.chmod(0o755)
        with self.assertRaises(ValueError):
            b.probe(self.raw(), binary=str(tool))

    def test_record_starts_and_reconnects_without_any_recovery_work(self):
        raw = self.raw(content=b'poison historical file')
        stop = threading.Event()
        calls = []
        def connection(c, url, run):
            calls.append(run)
            self.assertTrue(b.capture_protocol_ready(self.spool))
            # The capture lock covers normal close/finalization too.
            with self.assertRaises(BlockingIOError), (run / 'capture.lock').open('a+b') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if len(calls) == 1:
                raise OSError('connection ended')
            stop.set()
        with patch.object(b, 'STOP', stop), patch.object(stop, 'wait', return_value=False), \
             patch.object(b, 'credential_url', return_value='rtsp://camera.invalid/stream'), \
             patch.object(b, 'recover', side_effect=AssertionError('capture called recovery')), \
             patch.object(b, 'record_connection', side_effect=connection):
            b.record(self.c, 'unused')
        self.assertEqual(len(calls), 2)
        self.assertEqual(raw.read_bytes(), b'poison historical file')

    def test_connection_exit_finalization_errors_do_not_escape_or_drop_original(self):
        raw = self.raw()
        for failure in (ValueError('probe'), OSError('fsync'), subprocess.TimeoutExpired(['probe'], 90)):
            with self.subTest(failure=type(failure).__name__):
                process = types.SimpleNamespace(stdout=io.StringIO(raw.name + ',0,2\n'), poll=lambda: 0)
                with patch.object(b.subprocess, 'Popen', return_value=process), \
                     patch.object(b, 'finalize', side_effect=failure) as finalized:
                    b.record_connection(self.c, 'rtsp://camera.invalid/stream', raw.parent)
                self.assertEqual(finalized.call_count, 1)
                self.assertEqual(raw.read_bytes(), self.content)
                self.assertTrue(process.stdout.closed)

    def test_orphan_work_and_corrupt_state_are_visible_without_media_reads(self):
        raw = self.raw()
        directory = self.spool / 'recovery'
        directory.mkdir()
        state = b.recovery_path(self.c, self.entry(raw))
        state.write_text('{')
        work = directory / 'work' / ('e'*64)
        work.mkdir(parents=True)
        (work / 'attempt-1.mkv').write_bytes(b'partial output')
        data = monitor.probe_pi(self.c, services=lambda u: 1)
        self.assertEqual(data['recovery_held'], 2)
        self.assertGreaterEqual(data['invalid_entries'], 2)
        self.assertGreaterEqual(data['spool_bytes'], len(self.content) + len(b'partial output'))

    def test_invalid_filesystem_bytes_in_active_name_cannot_break_probe(self):
        raw = self.raw()
        bad = raw.parent / os.fsdecode(b'\xff-malformed.mkv' if sys.platform == 'linux' else b'malformed.mkv')
        bad.write_bytes(b'preserved')
        os.utime(bad, (self.current + 100, self.current + 100))
        data = monitor.probe_pi(self.c, services=lambda u: 1)
        self.assertGreaterEqual(data['invalid_entries'], 1)
        self.assertEqual(data['active_token'], monitor.token(self.entry(bad)))

    def test_monitor_counts_retained_bytes_holds_and_current_capture(self):
        failed = self.raw(content=b'bad')
        success = self.raw(1)
        with patch.object(b, 'recovery_spawn', return_value='timeout'):
            self.pass_at()
        # Repair the second file via a real pass; first remains held/backoff.
        with patch.object(b, 'recovery_spawn', side_effect=lambda *a: self.worker_with('')(*a)
                          if a[1] == self.entry(success) else 'timeout'):
            self.pass_at(301)
        live = self.raw(2)
        clock = self.current + 1000
        os.utime(live, (clock, clock))
        data = monitor.probe_pi(self.c, clock=lambda: clock, services=lambda u: 1)
        self.assertEqual(data['recovery_held'], 1)
        self.assertEqual(data['recovery_recovered'], 1)
        self.assertEqual(data['active_bytes'], len(self.content))
        self.assertEqual(data['active_token'], monitor.token(self.entry(live)))
        self.assertGreaterEqual(data['invalid_entries'], 1)
        self.assertEqual(data['recovery_retained_bytes'], len(self.content) + 3)
        self.assertEqual(data['spool_bytes'], b.spool_bytes(self.spool))
        # Work output and published video are hard links: capacity counts once.
        logical = sum(p.stat().st_size for p in self.spool.rglob('*') if p.is_file())
        self.assertGreater(logical, data['spool_bytes'])


if __name__ == '__main__':
    unittest.main()
