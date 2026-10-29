# Bounded recording recovery

Capture starts and reconnects without waiting for historical files. A separate
`bostan-recover.timer` runs `bostan-recover.service`; neither is a dependency of
the recorder. Normal close still probes, hashes and fsyncs footage before its
manifest enters the publisher queue. A close/probe/I/O failure retains that file
and allows the current capture or next connection to continue.

The recovery service is low priority and has no camera credentials or network
protocol. It checks closed runs and videos left in `ready/` without a manifest.
The recorder holds an exclusive `active/<run>/capture.lock` through capture and
normal finalization. Recovery must obtain that same lock before media work and
skips the entire live run without penalizing it. The new recorder also holds
`recovery-aware.lock` beside `recorder.lock` for its process lifetime. If the
recorder lock is held without that protocol lock, recovery defers all media work;
an upgrade cannot mistake an older running recorder for abandoned footage.
Restart the recorder before enabling its recovery timer.

## Bounds and durable holds

- The timer starts two minutes after boot, then five minutes after each completed
  pass, with up to 30 seconds of jitter. A durable `recovery/service.json` also
  prevents manual starts, service restarts or reboot from bypassing five minutes
  between passes. Boot/elapsed anchors ignore wall-clock steps. Reboot or legacy
  wall-only state conservatively rebases the full five-minute delay once.
- Each pass starts at most two media attempts and gives all attempts together
  240 seconds. One attempt can use that entire shared budget, including remux, probe,
  SHA-256, fsync and publication. Timed-out workers and their process-group
  descendants are killed, with a bounded two-second reap. The four-minute
  budget allows a full hourly file to remux and hash on slow ARMv6/SD storage;
  it never delays capture startup or reconnection. The service itself
  has a 260-second limit and control-group cleanup. Many damaged files therefore
  cannot turn into per-file startup delays or a recorder restart loop.
- Each file gets at most three automatically reserved attempts, with delays of
  5 and 10 minutes after the first two failures. The third failure is an
  indefinite hold. A reservation is
  fsynced **before** a child starts; death during an attempt still consumes it.
  After three failures the file is held indefinitely. No content/stat change
  automatically resets this ceiling. A filesystem or invalid-name artifact is
  held immediately without interpreting an invented timestamp.
- Scheduling is `recovery/<SHA256 of the spool-relative filename bytes>.json`.
  State contains the local entry, attempt count, fixed reason, status and times.
  Journals contain fixed codes and opaque tokens, never arbitrary filenames,
  FFmpeg errors or camera URLs. Corrupt per-file scheduling holds only that
  file; corrupt shared state stops recovery until an operator investigates.
- Before each attempt, both filesystem reserve and spool headroom must cover
  its full output allowance **plus 64 MiB for ongoing capture**. No attempt is
  spent while space is insufficient. Output is capped by a child file-size
  limit at the smaller of 512 MiB and input size plus 5% (at least 1 MiB extra).
  This limits partial-output growth even when a tool misbehaves. The optional
  `recovery_max_output_bytes` config changes that cap; use deliberate capacity
  planning before increasing it. `recovery_budget_seconds` can lower the shared
  240-second budget, but cannot increase it. These are limits, not preallocation
  or protection against another writer consuming the reserved space.

These are process-level bounds. A broken SD card/filesystem can stall kernel
I/O, and a task in uninterruptible I/O may not obey a kill deadline. Isolating
recovery removes its dependency from capture startup; it cannot make failed
physical storage usable. No live disk-full, power-loss or network-cut test is
required to operate or validate this mechanism.

## Evidence retention and interrupted commits

Recovery never replaces an active original. Each attempt writes a new,
no-overwrite `recovery/work/<token>/attempt-N.mkv`. Failed and interrupted
outputs stay there. A successful remux must pass media validation, hash and
fsync before its durable `commit.json` records the exact output and manifest.
A retry revalidates that output and can complete an interrupted commit without
remuxing it again or overwriting the existing target. A ready orphan is directly
validated and sealed with its original bytes; if those bytes fail validation,
it remains held for manual salvage rather than being silently replaced.

Video enters `ready/` by an exclusive same-filesystem hard link. The manifest
is also published without replacement and made durable before a success marker.
Existing assets must match; a conflict retains both sides. Normal finalization
now uses the same sequence and unlinks its active source only after a durable
seal. Recovery leaves its original, successful output and failed partials in
place indefinitely. Remux success does not prove that every damaged input
packet was preserved, so even a successful recovery does **not** authorize
automatic deletion of the original. This intentionally consumes extra capacity.

`recovered` state must remain after the publisher uploads/cleans its ready copy;
it prevents the retained original from being republished after a restart. All
publication receipts, hash checks, server idempotence, remote verification and
normal publisher grace rules still apply. No receiver receipt rows or episode
numbers are reset. A missing or intentionally deleted committed archive cannot
be resurrected by overwriting it through recovery.

## Monitoring and operator repair

The Pi probe exposes `recovery_held`, `recovery_recovered` and
`recovery_retained_bytes`; collector metrics add the `bostan_pi_` prefix. Any
running/backoff/quarantined or corrupt scheduling marker counts as an invalid
queue entry under the existing two-minute alert grace. Recovered originals are
excluded from the current-capture growth and late-segment checks, while held
originals remain visible through the invalid-entry alert. Both remain in spool
and conservative buffer accounting. Hard links count once by device/inode;
separate remux outputs and partial attempts count separately. The probe reads
small metadata only, never footage hashes or camera credentials. Monitor alert
state and notification acknowledgements are not reset.

Inspect privately:

```sh
systemctl status bostan-recover.timer bostan-recover.service
journalctl -u bostan-recover
```

Map an original to its scheduling token with
`hashlib.sha256(os.fsencode(str(path.relative_to(spool)))).hexdigest()`.
Reason `timeout` or `failed` calls for inspecting the source and retained
outputs on healthy storage; `space` calls for more capacity; `artifact` calls
for verified names, file type and provenance. Do not guess a missing timestamp,
follow a symlink, fabricate a manifest hash or overwrite an existing seal.

For a deliberate retry, stop only `bostan-recover.timer`, wait for its current
service to finish (or stop that service), and preserve the exact item state **and
its entire work directory** in a private evidence location. Diagnose/repair or
safely relocate the original first. Move only that item's scheduling file and
work directory aside, then restart the timer; this explicitly grants a new set
of up to three attempts. Moving just the state would collide with the retained
attempt filenames. An unreadable shared `service.json` requires the same
preserve/diagnose/move-aside procedure for that file. Do not reset another
item's state, the publisher's `retry/`, receipts or monitor incident state.

For successful recovery, keep its success marker as long as the original stays
under `active/`. Validate the archive and arrange safe additional storage for
originals/partials before relocating them. No automatic quarantine expiration,
original/partial deletion or server retention policy is supplied. Eventually
retained evidence can reach the normal storage guard and pause recording until
an operator provides capacity or arranges an explicitly authorized disposition.

## Deployment and rollback

Run the complete test suite locally and in an isolated Pi spool. Copy the core
source to the receiver as well if updating the bounded probe implementation;
its protocol and restricted account stay unchanged. Update the read-only probe
on Pi/monitor/archive hosts. Install both recovery units and reload systemd.
Gracefully restart the recorder near a segment boundary, verify its sealed
footage and the new capture lock/growth, then enable the timer:

```sh
sudo systemctl enable --now bostan-recover.timer
```

Restart the publisher while idle to load the shared hard-link-aware space
accounting. No Jellyfin/container, monitor timer or unrelated service restart is
needed. A graceful recorder restart briefly interrupts live capture and can
create a shorter segment; all captured bytes still go through normal validation.
Never change the production segment duration or damage production files to test
recovery.

For operational rollback, disable/stop only the recovery timer and service.
The new recorder continues capturing without recovery; all footage/state stays
retained. Keep the new recorder source while investigating. Restoring an old
recorder that retries every `active/*.mkv` would ignore success/hold markers and
can restore the original blocking bug; first arrange safe, audited relocation
of closed recovery originals. Never roll back data or delete receipt rows.

Clock-safe numeric recordings keep their original run lock despite per-file UUIDs.
Missing close/clock evidence stays uncertain during recovery; no capture date is
inferred from recovery time. Delay state uses persisted boot/elapsed anchors,
including a conservative full-delay rebase after reboot or legacy migration.
See [clock safety](clock-safety.md) before upgrading or rolling back v2 recordings.
