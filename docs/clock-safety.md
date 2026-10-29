# Recorder clock safety

The recorder waits up to 60 seconds for synchronization at process startup.
`startup_sync_wait_seconds` can be 0–120; larger values are capped at 120.
The deadline and sleeps use elapsed time. One bounded NTP check (at most
three seconds) and filesystem operations can add to that deadline. Shutdown interrupts
the wait. An unavailable Internet/NTP service does not prevent camera capture
indefinitely: the recorder continues with explicitly uncertain capture time.
Network-online ordering and physical filesystem stalls have their own OS limits.

The supplied unit starts after `systemd-timesyncd.service`, requests it, and does
not wait for `time-sync.target`. Do not add an unbounded
`systemd-time-wait-sync.service` dependency to this recorder. Merely ordering a
unit after `time-sync.target` does not establish accurate time. This implementation
supports systemd-timesyncd: another NTP client without its boot-local marker
will produce uncertain timestamps until a separately reviewed adapter is added.
It never sets the system clock or changes NTP/network configuration.

A synchronized observation requires both:

- `/run/systemd/timesync/synchronized`, written by timesyncd in the current boot,
  with a modification time no more than two hours old (and not over two seconds
  ahead of the observed clock);
- `timedatectl show -p NTPSynchronized --value` returning `yes` within three seconds.

The persistent `/var/lib/systemd/timesync/clock` and fake-hardware-clock estimates
are not synchronization evidence. A Pi without an RTC cannot recover an accurate
capture date from its previous boot's saved date. An NTP-observed recorder clock
is still an estimate, not authenticated camera time or proof of exposure time.
Camera overlays, source buffering, keyframe timing, oscillator drift and NTP's
own accuracy remain outside this timestamp guarantee.

## Recording and clock changes

Each connection exclusively creates a random run directory. FFmpeg writes
increasing numeric filenames without `strftime` or counter wrapping. Each
recording ID is a deterministic UUID derived from that run and numeric filename.
The date-shaped suffix remains for wire-format compatibility; for new recordings
it is an opaque connection token, **never the recording's capture date**. Repeated
wall times, backwards steps and rebooted/restored state cannot reuse a live
filename. Existing sealed IDs, manifests, receipts and archive paths are immutable.

While synchronized, FFmpeg still copies the original compressed video and audio
and cuts on UTC hour boundaries at the next source keyframe. UTC avoids DST
repeats; configured local time is used only for archive display/month selection.
When uncertain, it uses media-duration intervals instead. There is no re-encoding
or generated timestamp overlay. First, interrupted and clock-change clips can be
partial. Source keyframes/buffering and packet outages prevent an exact 3600-second
or zero-gap guarantee.

An independent observer normally samples every 0.5 seconds, including while the
capture loop hashes/probes a closed clip. It checks NTP at most every 10 seconds
and compares realtime with Linux `CLOCK_BOOTTIME`. A difference over two seconds
between samples is a step; smaller changes/slews remain within the stated clock
estimate. Scheduling and I/O can delay observations; this is not a real-time OS.
A step, synchronization loss, or synchronization acquisition irreversibly marks
all still-open/unsealed affected observations uncertain and asks FFmpeg to close.
The capture loop bounds child shutdown, retains the clip, and reconnects. A new
connection uses the new clock status and alignment. This can cause a camera
reconnection gap; it preserves footage rather than guessing a corrected date.

After a detected step, the old NTP marker is blocked until a new successful NTP
observation changes it. That block and the last realtime/elapsed sample survive
process restarts in spool `clock.json`; old-boot state cannot establish trust.
Corrupt clock state is retained as `clock-invalid-*` and requires a fresh marker.
No prior uncertain recording is relabeled when synchronization later returns.

## Manifest v2 and calendar assignment

New seals use `manifest_version: 2`; v1 manifests remain accepted and are never
rewritten. A `.clock` sidecar records first-observed UTC, boot ID, elapsed file-open
time, the preceding observation window, status and explicit close evidence.
It is saved before sealing and preserved alongside a ready orphan until its JSON
manifest is durable. Run locks also protect numeric files from recovery.
Missing/invalid observations, a window exceeding five seconds, or an unclean
close produce uncertain time. Historical recovery never reconstructs accurate
UTC from a filename, file mtime, previous boot or the time of recovery.

For an NTP-observed, cleanly closed clip, `start_utc` is the **first file
observation**, with its measured preceding observation window. It is not the
camera's first-frame timestamp. `end_utc` is that observation plus probed media
duration, not an independently measured exposure end. Jellyfin titles use `~`
and the plot explains that estimate. `clock.status` is `synchronized`.

For uncertain clips, both `start_utc` and `end_utc` are JSON `null`.
`clock.status` is `uncertain` and a fixed reason explains why. An optional
`observed_start_utc` is evidence of what the recorder clock showed, not a valid
capture timestamp. `closed_utc` is also a recorder wall-clock observation; use
`sealed_timer` for same-boot elapsed scheduling/diagnostics.

The receiver assigns synchronized footage by its manifest observation in the
server's configured timezone, independently of server arrival time or clock.
A UTC date can belong to the next month/year in local time. If the open observation
window straddles a local month boundary, the month cannot be assigned confidently.
Uncertain clips, calendar-ambiguous clips and v2 observations outside the
configured season range go to `Season 00`, titled
`Time uncertain`, with their own sequential episodes and no invented airdate.
For a synchronized window crossing only a day boundary, the airdate is omitted.
The original manifest remains byte-for-byte unchanged. A slow upload, server
clock skew, later NTP correction or retry cannot move an already reserved clip.
The existing clock-skew alert remains active independently of this assignment.

## Elapsed scheduling and monitoring

Upload item/shared backoff, recovery item/shared delays and verified-copy grace
store boot ID, elapsed anchor and delay. Linux `CLOCK_BOOTTIME` includes suspend
and ignores wall-clock jumps. Human-readable wall attempt/verification times
remain audit observations; they no longer authorize expiration or deletion.
A same-boot service restart preserves the delay. After reboot, a future/invalid
elapsed anchor, a changed grace setting, or migration from legacy wall-only state,
the full applicable delay is conservatively rebased once and persisted. Offline
power time is not counted because it cannot be measured reliably without an RTC.
Thus reboot can retain copies longer; it cannot prematurely delete them or reset
the durable recovery attempt count. Corrupt timers retain the existing item-hold
and shared-outage error policies. Repairs still bypass item scheduling promptly.

The existing storage-pressure exception remains: an already verified local copy
may be cleaned sooner only after a fresh remote hash/manifest/NFO verification.
Unverified footage is never deleted. This changes no server retention policy.
Temporary recovery artifacts use elapsed discovery grace rather than file mtime.

Monitoring adds `clock_uncertain` after two minutes, plus
`bostan_pi_clock_synchronized`, `bostan_pi_clock_observation_age_seconds` and
`bostan_pi_timestamp_uncertain_count`. The last count describes local ready clips,
not a demand to relabel or delete historical uncertain footage. Missing/stale
clock observation is uncertainty, even while capture continues. Old probes without
clock fields remain compatible; absence is unknown, never a recovery signal.
Actual byte growth, live run locks and sequence order identify current capture,
including when a clock step makes an older file's mtime appear newer. New active
and queue ages use elapsed evidence; collector-observed backlog ages remain useful
across Pi reboot. Collector incident/recovery/notification ages also compensate
for monitor clock steps and conservatively restart grace on reboot while retaining
all active/notified flags. Never delete/reset the monitor incident state.

## Upgrade, tests and rollback

Deploy the new receiver first, then probes/collector, then reload the idle
publisher and recovery worker source, and finally gracefully restart the recorder
just after a natural hour has sealed. Retain the old sources privately. The
recorder restart is necessary to load the observer and numeric muxer pattern;
plan and report its actual capture gap. No system clock change, NTP restart,
Pi reboot, Jellyfin restart or fault on the live camera is needed for testing.

Run `python3 -m unittest discover -s tests -v` locally and on the actual Pi in
isolated test directories. Clock tests cover bounded startup, unavailable/late
NTP, forwards/backwards steps, stale/rebooted state, immutable uncertain dates,
numeric identity/locks/recovery, real stream-copy child closure, elapsed retry
and grace, v1/v2 protocol coexistence, calendar rollover and incident continuity.
All generated media and receiver databases are temporary fixtures.

An old receiver/publisher/probe cannot interpret v2 null capture dates safely.
For rollback, prefer stopping capture and repairing the new code, or drain/preserve
all v2 clips using the new publisher before restoring old code. Do not downgrade
recovery while numeric active originals or `.clock` evidence remain. Keep
receipts, retry/recovery state, incident acknowledgements, archive manifests and
intentional-deletion ledgers. No data migration or archive timestamp rewrite is
part of this upgrade or rollback.

References: [systemd timesyncd](https://www.freedesktop.org/software/systemd/man/latest/systemd-timesyncd.service.html),
[time-wait-sync](https://www.freedesktop.org/software/systemd/man/latest/systemd-time-wait-sync.service.html),
[systemd 257 marker implementation](https://github.com/systemd/systemd/blob/v257/src/timesync/timesyncd-manager.c),
[FFmpeg segment muxer](https://ffmpeg.org/ffmpeg-formats.html#segment_002c-stream_005fsegment_002c-ssegment).
