"""Clock faults are injected only into isolated spools/processes, never the host."""
import contextlib
import copy
import datetime as dt
import fcntl
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch

SOURCE = Path(__file__).parents[1] / 'src/bostan.py'
spec = importlib.util.spec_from_file_location('timestamp_bostan', SOURCE)
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
NTP_OBSERVATION = b.ntp_observation
MEDIA_PROBE = b.probe
spec = importlib.util.spec_from_file_location('timestamp_monitor', SOURCE.with_name('bostan_monitor.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class FakeClock:
    def __init__(self):
        self.wall = dt.datetime(2026, 10, 29, 12, tzinfo=b.UTC).timestamp()
        self.tick = 1000.0
        self.boot = 'test-boot-a'
        self.synced, self.marker = True, 'ntp-a'

    def advance(self, seconds):
        self.wall += seconds
        self.tick += seconds
        return False

    def iso(self):
        return dt.datetime.fromtimestamp(self.wall, b.UTC).isoformat()


class TimestampTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixtures = tempfile.TemporaryDirectory()
        cls.fixture = Path(cls.fixtures.name) / 'fixture.mkv'
        subprocess.run(['ffmpeg', '-v', 'error', '-f', 'lavfi', '-i', 'testsrc2=size=160x120:rate=10',
                        '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=8000', '-t', '2',
                        '-c:v', 'libx264', '-g', '10', '-preset', 'ultrafast', '-c:a', 'pcm_alaw',
                        str(cls.fixture)], check=True)
        cls.content = cls.fixture.read_bytes()
        cls.fixture_hash = b.digest(cls.fixture)
        cls.fixture_probe = MEDIA_PROBE(cls.fixture)

    @classmethod
    def tearDownClass(cls):
        cls.fixtures.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.c = dict(spool=str(self.root / 'spool'), camera_name='Test', timezone='Asia/Yerevan',
                      segment_seconds=3600, reserve_bytes=0, max_spool_bytes=10**9, grace_seconds=120)
        self.spool = Path(self.c['spool'])
        for name in ('active', 'ready', 'receipts'):
            (self.spool / name).mkdir(parents=True)
        self.clock = FakeClock()
        self.patches = contextlib.ExitStack()
        self.addCleanup(self.patches.close)
        for module in (b, m):
            self.patches.enter_context(patch.object(module, 'elapsed', side_effect=lambda: self.clock.tick))
            self.patches.enter_context(patch.object(module, 'boot_id', side_effect=lambda: self.clock.boot))
        self.patches.enter_context(patch.object(b.time, 'time', side_effect=lambda: self.clock.wall))
        self.patches.enter_context(patch.object(b, 'now', side_effect=self.clock.iso))
        self.patches.enter_context(patch.object(b, 'ntp_observation',
            side_effect=lambda: (self.clock.synced, self.clock.marker)))
        self.patches.enter_context(patch.object(b, 'STOP', threading.Event()))
        # Clock/schema cases reuse one genuinely probed fixture. Real capture
        # output and disposable recovery/receiver processes still run ffprobe.
        def probe(path, binary='ffprobe'):
            if binary == 'ffprobe' and b.digest(path) == self.fixture_hash:
                return copy.deepcopy(self.fixture_probe)
            return MEDIA_PROBE(path, binary)
        self.patches.enter_context(patch.object(b, 'probe', side_effect=probe))

    def raw(self, index=0, run_number=1):
        run = self.spool / 'active' / format(run_number, '032x')
        run.mkdir(exist_ok=True)
        raw = run / ('%010d-20261029T120000Z.mkv' % index)
        raw.write_bytes(self.content)
        return raw

    def observer(self, raw):
        authority = b.ClockAuthority(self.c)
        initial, _ = authority.sample()
        process = types.SimpleNamespace(send_signal=Mock())
        observer = b.CaptureClock(authority, raw.parent, initial, process)
        self.clock.advance(.5)
        observer.scan()
        return observer, process

    def manifest(self, raw=None, close=True):
        raw = raw or self.raw()
        observer, process = self.observer(raw)
        if close:
            self.clock.advance(2)
            observer.close(raw)
        return b.manifest_for(self.c, raw, b.recording_id(raw), 'test'), raw

    def receiver(self, manifest):
        library = self.root / 'show'
        library.mkdir(exist_ok=True)
        c = dict(library_root=str(library), state=str(self.root / 'state'),
                 timezone='Asia/Yerevan', season_epoch='2026-10', show_title='Camera', server_reserve_bytes=0)
        stream = io.BytesIO((json.dumps(dict(action='upload', manifest=manifest)) + '\n').encode() + self.content)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            b.receive(c, stream)
        return json.loads(output.getvalue())

    def test_startup_synchronized_does_not_wait(self):
        with patch.object(b.STOP, 'wait', side_effect=AssertionError('already synchronized')):
            self.assertEqual(b.ClockAuthority(self.c).startup()['status'], 'synchronized')

    def test_ntp_evidence_requires_kernel_flag_and_recent_boot_local_marker(self):
        marker = types.SimpleNamespace(st_dev=1, st_ino=2, st_mtime_ns=3, st_mtime=self.clock.wall - 20)
        response = types.SimpleNamespace(returncode=0, stdout='yes\n')
        with patch.object(Path, 'stat', return_value=marker), patch.object(b.subprocess, 'run', return_value=response):
            self.assertTrue(NTP_OBSERVATION()[0])
            response.stdout = 'no\n'
            self.assertFalse(NTP_OBSERVATION()[0])
            response.stdout = 'yes\n'
            marker.st_mtime = self.clock.wall - 7201
            self.assertFalse(NTP_OBSERVATION()[0])
            marker.st_mtime = self.clock.wall + 3
            self.assertFalse(NTP_OBSERVATION()[0])
            marker.st_mtime = self.clock.wall
            with patch.object(b.subprocess, 'run', side_effect=subprocess.TimeoutExpired(['timedatectl'], 1)):
                self.assertFalse(NTP_OBSERVATION()[0])

    def test_startup_unavailable_ntp_is_bounded_by_elapsed_time_despite_wall_jump(self):
        self.clock.synced, self.clock.marker = False, None
        def wait(seconds):
            self.clock.wall -= 86400
            return self.clock.advance(seconds)
        with patch.object(b.time, 'monotonic', side_effect=lambda: self.clock.tick), \
             patch.object(b.STOP, 'wait', side_effect=wait):
            value = b.ClockAuthority(self.c).startup(3)
        self.assertEqual(self.clock.tick, 1003)
        self.assertEqual(value['status'], 'uncertain')

    def test_startup_late_ntp_and_shutdown_are_handled(self):
        self.clock.synced = False
        def wait(seconds):
            self.clock.advance(seconds)
            self.clock.synced, self.clock.marker = True, 'ntp-b'
        with patch.object(b.time, 'monotonic', side_effect=lambda: self.clock.tick), \
             patch.object(b.STOP, 'wait', side_effect=wait):
            value = b.ClockAuthority(self.c).startup(15)
        self.assertEqual(value['status'], 'synchronized')
        self.assertLessEqual(self.clock.tick, 1010)
        self.clock.synced = False
        b.STOP.set()
        self.assertEqual(b.ClockAuthority(self.c).startup()['status'], 'uncertain')

    def test_clock_step_requires_fresh_ntp_and_survives_process_restart(self):
        authority = b.ClockAuthority(self.c)
        authority.sample()
        self.clock.advance(1)
        self.clock.wall -= 3600
        value, changed = authority.sample()
        self.assertTrue(changed)
        self.assertEqual(value['status'], 'uncertain')
        self.assertEqual(b.ClockAuthority(self.c).sample()[0]['status'], 'uncertain')
        self.clock.marker = 'ntp-b'
        self.clock.advance(11)
        self.assertEqual(b.ClockAuthority(self.c).sample()[0]['status'], 'synchronized')

    def test_ntp_step_with_new_marker_trusts_new_run_but_invalidates_old_segment(self):
        raw = self.raw()
        observer, process = self.observer(raw)
        self.clock.wall += 4000
        self.clock.marker = 'ntp-b'
        observer.close(raw)
        self.assertTrue(process.send_signal.called)
        value = b.manifest_for(self.c, raw, b.recording_id(raw), 'clock_change')
        self.assertIsNone(value['start_utc'])
        self.assertEqual(b.ClockAuthority(self.c).sample()[0]['status'], 'synchronized')

    def test_forward_and_backward_steps_close_and_do_not_claim_capture_utc(self):
        for index, step in enumerate((86400, -7200)):
            with self.subTest(step=step):
                raw = self.raw(run_number=index + 1)
                self.clock.marker = 'fresh-' + str(index)
                observer, process = self.observer(raw)
                self.clock.wall += step
                observer.close(raw)
                manifest = b.manifest_for(self.c, raw, b.recording_id(raw), 'clock_change')
                process.send_signal.assert_called_once_with(signal.SIGINT)
                self.assertIsNone(manifest['start_utc'])
                self.assertIsNone(manifest['end_utc'])
                self.assertEqual(raw.read_bytes(), self.content)

    def test_sync_loss_and_acquisition_close_uncertain_clip_without_relabeling(self):
        for initial, final in ((False, True), (True, False)):
            self.clock.synced, self.clock.marker = initial, 'marker-' + str(initial)
            raw = self.raw(run_number=1 if initial else 2)
            observer, process = self.observer(raw)
            self.clock.synced, self.clock.marker = final, 'changed-' + str(final)
            self.clock.advance(11)
            observer.close(raw)
            value = b.manifest_for(self.c, raw, b.recording_id(raw), 'clock_change')
            self.assertEqual(value['clock']['status'], 'uncertain')
            self.assertIsNone(value['start_utc'])
            self.assertTrue(process.send_signal.called)

    def test_numeric_identity_is_unique_for_repeated_wall_times_and_recovery(self):
        raws = [self.raw(i) for i in range(3)] + [self.raw(0, run_number=2)]
        identifiers = [b.recording_id(raw) for raw in raws]
        self.assertEqual(len(set(identifiers)), 4)
        self.clock.wall -= 7200
        self.assertEqual([b.recording_id(raw) for raw in raws], identifiers)
        for raw, ident in zip(raws, identifiers):
            self.assertEqual(b.recovery_entry(self.c, raw.relative_to(self.spool))[1], ident)

    def test_missing_and_unclosed_observations_stay_uncertain(self):
        raw = self.raw()
        value = b.manifest_for(self.c, raw, b.recording_id(raw), 'recovered_after_restart')
        self.assertIsNone(value['start_utc'])
        self.assertEqual(value['clock']['reason'], 'observation_missing')
        value, _ = self.manifest(raw, close=False)
        self.assertEqual(value['clock']['reason'], 'recovered_without_close')
        self.assertIsNone(value['start_utc'])

    def test_trusted_metadata_uses_observation_and_media_duration_not_id_suffix(self):
        self.clock.wall += 7200
        manifest, raw = self.manifest()
        self.assertTrue(manifest['start_utc'].startswith('2026-10-29T14:00:00'))
        self.assertEqual(manifest['clock']['status'], 'synchronized')
        self.assertEqual((dt.datetime.fromisoformat(manifest['end_utc']) -
                          dt.datetime.fromisoformat(manifest['start_utc'])).total_seconds(),
                         manifest['duration_seconds'])
        self.assertNotIn('14:00', manifest['id'])
        self.assertEqual(b.check_manifest(manifest), manifest)

    def test_uncertain_archive_has_no_airdate_and_uses_separate_season(self):
        self.clock.synced = False
        manifest, raw = self.manifest()
        ack = self.receiver(manifest)
        path = Path(ack['path'])
        self.assertEqual(path.parent.name, 'Season 00')
        text = path.with_suffix('.nfo').read_text()
        self.assertIn('Recording time uncertain', text)
        self.assertNotIn('<aired>', text)
        self.assertIn('<lockdata>false</lockdata>', text)
        before = path.with_suffix('.json').read_bytes()
        self.clock.synced, self.clock.marker = True, 'ntp-b'
        self.clock.wall += 86400
        self.assertEqual(self.receiver(manifest), ack)
        self.assertEqual(path.with_suffix('.json').read_bytes(), before)

    def test_month_year_rollover_is_based_on_local_capture_observation(self):
        for wall, season, date in (('2026-10-31T19:59:58+00:00', 1, '2026-10-31'),
                                   ('2026-10-31T20:00:02+00:00', 2, '2026-11-01'),
                                   ('2026-12-31T20:00:02+00:00', 4, '2027-01-01')):
            with self.subTest(wall=wall):
                self.clock.wall = dt.datetime.fromisoformat(wall).timestamp()
                self.clock.marker = wall
                # A new boot provides fresh NTP evidence, not a clock-step inference.
                self.clock.boot = wall
                manifest, raw = self.manifest(self.raw(run_number=season))
                local = b.archive_calendar(dict(timezone='Asia/Yerevan'), manifest)
                self.assertEqual(local.strftime('%Y-%m-%d'), date)
                ack = self.receiver(manifest)
                self.assertEqual(Path(ack['path']).parent.name, 'Season %02d' % season)

    def test_observation_window_straddling_month_cannot_assign_month(self):
        self.clock.wall = dt.datetime(2026, 10, 31, 19, 59, 59, 900000, tzinfo=b.UTC).timestamp()
        value, raw = self.manifest()
        self.assertIsNone(b.archive_calendar(dict(timezone='Asia/Yerevan'), value))
        self.assertEqual(Path(self.receiver(value)['path']).parent.name, 'Season 00')

    def test_capture_outside_configured_calendar_is_preserved_without_relabeling(self):
        self.clock.wall = dt.datetime(2026, 9, 15, tzinfo=b.UTC).timestamp()
        value, raw = self.manifest()
        ack = self.receiver(value)
        path = Path(ack['path'])
        self.assertEqual(path.parent.name, 'Season 00')
        self.assertNotIn('<aired>', path.with_suffix('.nfo').read_text())
        self.assertEqual(b.read_regular_json(path.with_suffix('.json')), value)

    def test_file_observation_window_includes_slow_ntp_check(self):
        authority = b.ClockAuthority(self.c)
        initial, _ = authority.sample()
        raw = self.raw()
        observer = b.CaptureClock(authority, raw.parent, initial, types.SimpleNamespace(send_signal=Mock()))
        self.clock.advance(11)
        def ntp():
            self.clock.advance(3)
            return True, 'new-marker'
        with patch.object(b, 'ntp_observation', side_effect=ntp):
            observer.close(raw)
        value = b.manifest_for(self.c, raw, b.recording_id(raw), 'test')
        self.assertEqual(value['clock']['observation_window_seconds'], 14)
        self.assertEqual(value['clock']['observed_start_utc'], self.clock.iso())
        self.assertIsNone(value['start_utc'])

    def test_bad_clock_schema_and_fabricated_utc_are_rejected(self):
        manifest, raw = self.manifest()
        for change in ('status', 'duration', 'timer', 'start', 'closed'):
            value = copy.deepcopy(manifest)
            if change == 'status':
                value['clock']['status'] = 'uncertain'
            elif change == 'duration':
                value['duration_seconds'] += 1
            elif change == 'timer':
                value['sealed_timer']['elapsed'] = float('nan')
            elif change == 'start':
                value['clock']['observed_start_utc'] = '2020-01-01T00:00:00+00:00'
            else:
                value['clock']['closed'] = False
            with self.subTest(change=change), self.assertRaises(ValueError):
                b.check_manifest(value)

    def test_seal_collision_never_overwrites_video_or_clock_metadata(self):
        value, raw = self.manifest()
        b.seal(self.c, raw, value)
        ready = self.spool / 'ready' / (value['id'] + '.mkv')
        before = (ready.read_bytes(), ready.with_suffix('.json').read_bytes())
        altered = copy.deepcopy(value)
        altered['clock']['observation_window_seconds'] = .1
        with self.assertRaises(ValueError):
            b.seal(self.c, raw, altered)
        self.assertEqual((ready.read_bytes(), ready.with_suffix('.json').read_bytes()), before)

    def test_retry_delay_uses_elapsed_time_under_repeated_clock_steps(self):
        path = self.spool / 'retry.json'
        b.defer(path, None, 'same', 'item_failed', self.clock.wall)
        for delta in (86400, -172800, 90000):
            self.clock.wall += delta
            self.clock.advance(5)
            state = b.load_retry(path)
            self.assertFalse(b.retry_due(state, 'same', path))
        self.clock.advance(45)
        self.assertTrue(b.retry_due(b.load_retry(path), 'same', path))
        self.assertTrue(b.retry_due(b.load_retry(path), 'repaired', path))

    def test_retry_reboot_and_legacy_state_rebase_once_and_survive_restart(self):
        path = self.spool / 'retry.json'
        b.defer(path, None, 'same', 'item_failed', self.clock.wall)
        self.clock.boot, self.clock.tick = 'new-boot', 2
        self.clock.wall += 86400
        state = b.load_retry(path)
        self.assertFalse(b.retry_due(state, 'same', path))
        self.clock.advance(59)
        self.assertFalse(b.retry_due(b.load_retry(path), 'same', path))
        self.clock.advance(1)
        self.assertTrue(b.retry_due(b.load_retry(path), 'same', path))
        state = b.load_retry(path)
        state.pop('timer')
        b.atomic_json(path, state)
        self.assertFalse(b.retry_due(b.load_retry(path), 'same', path))
        self.clock.advance(60)
        self.assertTrue(b.retry_due(b.load_retry(path), 'same', path))

    def test_verified_grace_never_expires_on_forward_jump_or_reboot(self):
        value, raw = self.manifest()
        b.finalize(self.c, raw, 'test')
        manifest = self.spool / 'ready' / (value['id'] + '.json')
        video = manifest.with_suffix('.mkv')
        ack = dict(value, status='committed')
        with patch.object(b, 'remote', return_value=ack) as remote:
            b.publish_once(self.c)
            self.clock.wall += 86400
            self.clock.advance(1)
            b.publish_once(self.c)
            self.assertTrue(video.exists())
            self.clock.boot, self.clock.tick = 'new-boot', 2
            b.publish_once(self.c)
            self.assertTrue(video.exists())
            self.clock.advance(119)
            b.publish_once(self.c)
            self.assertTrue(video.exists())
            self.clock.advance(1)
            b.publish_once(self.c)
            self.assertFalse(video.exists())
            self.assertEqual(remote.call_args.args[2], 'verify')
            self.assertEqual(remote.call_count, 2)

    def test_recovery_shared_delay_and_attempt_cap_ignore_wall_steps(self):
        raw = self.raw()
        with patch.object(b, 'recovery_spawn', return_value='failed') as work:
            self.assertEqual(b.recover(self.c)['attempted'], 1)
            for delta in (86400, -172800):
                self.clock.wall += delta
                self.clock.advance(10)
                self.assertEqual(b.recover(self.c)['attempted'], 0)
            self.clock.advance(280)
            self.assertEqual(b.recover(self.c)['attempted'], 1)
            self.assertEqual(work.call_count, 2)
        self.assertEqual(raw.read_bytes(), self.content)

    def test_pre_epoch_verification_observation_cannot_poison_valid_elapsed_receipt(self):
        value, raw = self.manifest()
        b.finalize(self.c, raw, 'test')
        self.clock.wall = -1000
        ack = dict(value, status='committed')
        with patch.object(b, 'remote', return_value=ack) as remote:
            b.publish_once(self.c)
            data = m.probe_pi(self.c, clock=lambda: self.clock.wall, services=lambda unit: 1)
            self.assertEqual(data['invalid_entries'], 0)
            video = self.spool / 'ready' / (value['id'] + '.mkv')
            self.assertTrue(video.exists())
            self.clock.advance(120)
            b.publish_once(self.c)
            self.assertFalse(video.exists())
            self.assertEqual(remote.call_count, 2)

    def test_monitor_preserves_active_incident_and_recovery_age_across_clock_jump(self):
        state = {}
        m.stabilize(state, self.clock.wall)
        m.advance(state, {'capture_stalled': True}, self.clock.wall)
        state['alarms']['capture_stalled']['notified'] = True
        m.advance(state, {'capture_stalled': False}, self.clock.wall)
        self.clock.advance(30)
        self.clock.wall += 86400
        m.stabilize(state, self.clock.wall)
        m.advance(state, {'capture_stalled': False}, self.clock.wall)
        self.assertTrue(state['alarms']['capture_stalled']['active'])
        self.assertTrue(state['alarms']['capture_stalled']['notified'])
        self.clock.advance(90)
        m.stabilize(state, self.clock.wall)
        m.advance(state, {'capture_stalled': False}, self.clock.wall)
        self.assertFalse(state['alarms']['capture_stalled']['active'])

    def test_monitor_new_schema_and_elapsed_active_age_despite_wall_reversal(self):
        value, raw = self.manifest()
        self.clock.wall -= 86400
        self.clock.advance(20)
        data = m.probe_pi(self.c, clock=lambda: self.clock.wall, services=lambda unit: 1)
        self.assertEqual(data['invalid_entries'], 0)
        self.assertAlmostEqual(self.clock.wall - data['oldest_active_start'], 22)
        self.assertEqual(data['clock_synchronized'], 1)
        self.clock.synced = False
        self.clock.advance(11)
        b.ClockAuthority(self.c).sample()
        b.finalize(self.c, raw, 'test')
        data = m.probe_pi(self.c, clock=lambda: self.clock.wall, services=lambda unit: 1)
        self.assertEqual(data['invalid_entries'], 0)
        self.assertEqual(data['pending_count'], 1)
        self.assertEqual(data['clock_synchronized'], 0)

    def test_malformed_clock_shape_is_isolated_by_read_only_probe(self):
        value, raw = self.manifest()
        b.finalize(self.c, raw, 'test')
        path = self.spool / 'ready' / (value['id'] + '.json')
        value['clock'] = ['bad shape']
        b.atomic_json(path, value)
        data = m.probe_pi(self.c, clock=lambda: self.clock.wall, services=lambda unit: 1)
        self.assertEqual(data['invalid_entries'], 1)
        self.assertEqual(data['record_up'], 1)

    def test_clock_state_restored_from_other_boot_never_supplies_trust(self):
        b.ClockAuthority(self.c).sample()
        self.clock.boot, self.clock.tick = 'new-boot', 1
        self.clock.synced, self.clock.marker = False, None
        value, _ = b.ClockAuthority(self.c).sample()
        self.assertEqual(value['status'], 'uncertain')

    def test_recovery_of_numeric_capture_keeps_identity_and_never_infers_missing_end_clock(self):
        value, raw = self.manifest(close=False)
        ident = value['id']
        self.assertEqual(b.recover(self.c)['recovered'], 1)
        ready = self.spool / 'ready' / (ident + '.json')
        recovered = b.check_manifest(b.read_regular_json(ready))
        self.assertEqual(recovered['clock']['reason'], 'recovered_without_close')
        self.assertIsNone(recovered['start_utc'])
        self.assertEqual(raw.read_bytes(), self.content)
        self.assertEqual(b.digest(ready.with_suffix('.mkv')), recovered['sha256'])
        self.assertEqual(m.probe_pi(self.c, services=lambda unit: 1)['invalid_entries'], 0)

    def test_numeric_identity_does_not_bypass_active_run_lock(self):
        raw = self.raw()
        self.observer(raw)
        with (raw.parent / 'capture.lock').open('a+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with patch.object(b, 'recovery_spawn') as spawn:
                self.assertEqual(b.recover(self.c)['attempted'], 0)
            spawn.assert_not_called()
        self.assertFalse(b.recovery_path(self.c, raw.relative_to(self.spool)).exists())
        self.assertEqual(raw.read_bytes(), self.content)

    def test_real_stream_copy_closes_safely_on_simulated_clock_step(self):
        run = self.spool / 'active' / ('f' * 32)
        run.mkdir()
        config = dict(self.c, segment_seconds=1)
        original = subprocess.Popen
        commands, children, errors = [], [], []

        def spawn(args, *a, **kw):
            if args[0] == 'ffmpeg':
                commands.append(args[:])
                pos = args.index('-rtsp_transport')
                args = args[:pos] + ['-re', '-stream_loop', '-1', '-i', str(self.fixture)] + args[pos + 6:]
            child = original(args, *a, **kw)
            children.append(child)
            return child

        def record():
            try:
                b.record_connection(config, 'rtsp://unused.invalid/live', run)
            except Exception as exc:
                errors.append(type(exc).__name__)

        with patch.object(b.subprocess, 'Popen', side_effect=spawn):
            thread = threading.Thread(target=record)
            thread.start()
            try:
                deadline = time.monotonic() + 20
                while not list(run.glob('*.clock')):
                    self.assertLess(time.monotonic(), deadline, 'real stream did not open')
                    time.sleep(.05)
                self.clock.advance(2)
                self.clock.wall -= 7200
                # Verify child closure separately from Pi-speed media sealing.
                children[0].wait(timeout=20)
                thread.join(timeout=60)
                self.assertFalse(thread.is_alive(), 'clock correction failed to close capture')
                self.assertEqual(errors, [])
            finally:
                b.STOP.set()
                thread.join(timeout=5)
                for child in children:
                    if child.poll() is None:
                        child.kill()
                        child.wait(timeout=2)
        manifests = [b.check_manifest(b.read_regular_json(p)) for p in (self.spool / 'ready').glob('*.json')]
        self.assertTrue(manifests)
        self.assertTrue(any(v['clock']['status'] == 'uncertain' for v in manifests))
        self.assertTrue(any(v['close_reason'] == 'clock_change' for v in manifests))
        self.assertEqual(commands[0][commands[0].index('-c') + 1], 'copy')
        self.assertNotIn('-strftime', commands[0])
        self.assertEqual(commands[0][commands[0].index('-segment_atclocktime') + 1], '1')
        self.assertEqual(len({v['id'] for v in manifests}), len(manifests))
        for value in manifests:
            self.assertEqual({s['codec_name'] for s in value['media']['streams']}, {'h264', 'pcm_alaw'})

    def test_monitor_prefers_live_sequence_to_future_mtime_of_orphan(self):
        stale = self.raw(run_number=2)
        live = self.raw(index=1)
        self.observer(stale)
        self.observer(live)
        os.utime(stale, (self.clock.wall + 86400, self.clock.wall + 86400))
        with (live.parent / 'capture.lock').open('a+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            data = m.probe_pi(self.c, clock=lambda: self.clock.wall, services=lambda unit: 1)
        self.assertEqual(data['active_token'], m.token(live.relative_to(self.spool)))
        self.assertEqual(data['invalid_entries'], 0)

    def test_ready_orphan_clock_evidence_preserves_original_numeric_run_lock(self):
        value, raw = self.manifest()
        target = self.spool / 'ready' / (value['id'] + '.mkv')
        os.link(raw, target)
        b.exclusive_json(target.with_suffix('.clock'), value['clock'])
        with (raw.parent / 'capture.lock').open('a+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with patch.object(b, 'recovery_spawn') as spawn:
                self.assertEqual(b.recover(self.c)['attempted'], 0)
            spawn.assert_not_called()
        self.assertEqual(raw.read_bytes(), self.content)

    def test_corrupt_clock_state_is_preserved_and_fresh_sync_is_required(self):
        (self.spool / 'clock.json').write_text('{broken')
        authority = b.ClockAuthority(self.c)
        self.assertEqual(authority.sample()[0]['status'], 'uncertain')
        evidence = next(self.spool.glob('clock-invalid-*'))
        self.assertEqual(evidence.read_text(), '{broken')
        self.clock.advance(11)
        self.clock.marker = 'fresh'
        self.assertEqual(authority.sample()[0]['status'], 'synchronized')


if __name__ == '__main__':
    unittest.main()
