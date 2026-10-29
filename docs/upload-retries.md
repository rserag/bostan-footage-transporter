# Upload failure isolation and repair

The publisher validates each `ready/*.json` independently before sorting valid
entries by recording start time. Invalid JSON, non-UTC/invalid timestamps,
invalid IDs, mismatched filenames, missing videos, changed local bytes and bad
saved receipts defer only that item. The other recordings continue to upload
and clean up after their normal verified-copy grace period. Videos without a
manifest remain retained and are reported by monitoring after its orphan grace;
the publisher does not invent their metadata. See [bounded recorder recovery](recording-recovery.md)
for its separate worker, attempt limits and retained originals.

Failed items remain in `ready/`; they are never automatically moved or deleted.
This is an in-place hold with automatic retries, rather than a permanent
quarantine threshold. Each item has a small atomic, fsynced state file at
`retry/<SHA256 of manifest filename bytes>.json` beneath the spool. Its fixed
reason code, failure count, last attempt, next attempt and file-stat signature
contain no exception payload or arbitrary filename. Backoff is 60, 120, 240,
480, 960, 1920, then 3600 seconds, capped at one hour, with no maximum attempt
count. Actual attempts occur on publisher passes, so add up to
`publish_interval` to these delays. The schedule survives service restarts.

A changed manifest, video or receipt stat signature makes an item eligible on
the next pass, so a repair need not wait for its previous backoff. This signature
is only a scheduling hint: it never replaces manifest checks, the local upload
hash, receipt binding or fresh remote verification before local video deletion.
Repeated attempts cannot overwrite a committed remote file. A successful item
loses only its retry marker; its receipt remains for audit after local cleanup.

## Shared outages

SSH connection/authentication errors, transport timeouts, server storage reserve
failures and unexpected receiver failures use `retry/service.json`. After the
first such failure, that pass performs no more network work or expensive local
upload hashes. Subsequent network attempts back off for 60, 120, 240, then 300
seconds, capped at five minutes. Items skipped during an outage do not accumulate
individual failures or become permanently quarantined. A successful network
pass clears the shared delay. A service restart preserves it.

The receiver returns a minimal rejection with scope `item` for known validation,
ID conflict, committed-media/metadata and content-integrity failures. These use
the individual backoff and allow later items through. Its storage reserve and
unexpected I/O/receiver failures have scope `service`. No raw error or path is
returned. Legacy receivers without this response are treated conservatively as
shared failures; update the receiver before the publisher. A transport outage
cannot prove whether the server committed a recording, so retries retain the
same ID and use the receiver's existing idempotent verification.

## Monitoring and diagnosis

The read-only Pi probe counts item retry markers as invalid queue entries,
including failed cleanup verification of an already acknowledged recording.
This exposes failures that a cheap file-stat probe cannot discover itself.
Existing queue-invalid alerts apply after two minutes. Shared outage state is
not labeled a damaged item; reachability and upload-backlog alerts retain their
existing thresholds. All held footage stays in spool and free-space accounting,
reducing the conservative remaining buffer. No alert acknowledgement state is
reset or modified by the publisher.

Inspect `bostan-publish` journal reason codes and private spool files locally.
To map a marker to a manifest, hash its filename (including `.json`) with
Python's `hashlib.sha256(os.fsencode(path.name)).hexdigest()`. Avoid pasting raw
manifests, camera settings, SSH errors or credential URLs into public reports.

- `manifest_invalid`: repair the JSON/required fields and exact ID filename;
  preserve the video. Never fabricate a hash to bypass an integrity failure.
- `video_missing` / `video_changed`: locate the original bytes and investigate
  storage. Preserve all available copies and the original manifest.
- `receipt_invalid` / `remote_receipt_invalid`: preserve the receipt for audit.
  Check its ID/hash/size and finite verification time. Do not edit a receipt to
  force deletion. An operator may move a damaged local receipt to a private
  evidence directory after investigating, allowing idempotent upload/verification
  to obtain a new authentic one. Committed server media is never overwritten.
- `remote_rejected`: inspect the receiver and committed video/sidecars privately.
  Restore damaged archive assets from trusted originals; a rejection never
  authorizes local cleanup. Do not remove receipt-database rows or reset episode
  counters to make a conflict disappear.
- `item_io` / `item_failed`: inspect permissions/storage and the affected item.
- `service_unavailable`: restore connectivity, credentials or server capacity;
  the shared retry resumes automatically.

A damaged individual retry file is retained and holds only that item for manual
inspection; remove or move only that scheduling file after investigating to
retry. A damaged shared retry file allows one connection attempt instead of
blocking the queue forever; a subsequent result repairs/clears its schedule.
Neither kind of scheduling metadata can authorize deletion. Obsolete item
markers with no remaining manifest are pruned; footage and receipts are not.

The upload scheduler introduces no server retention policy, permanent upload
quarantine, network timeout policy or watchdog setting. Recorder recovery has
its own [bounded worker and holds](recording-recovery.md). Retained failures
still consume finite storage and can eventually pause recording at the existing
storage guard; an operator must repair them or arrange safe extra storage.

## Deployment and rollback

Run the full temporary-spool tests, including the real FFmpeg/receiver tests.
Install the receiver source first, then the publisher and updated Pi probe.
Restart only `bostan-publish`; an active recorder can finish its current hour
using its already loaded source. Keep the shared source installed for its next
normal start. No Jellyfin/container restart or monitor-state reset is needed.

Keep a private copy of the previous sources. To roll back, restore those sources
and restart only the publisher. Keep `ready/`, `receipts/`, `retry/` and server
receipt state intact. Older publishers ignore retry metadata and may again stop
at a damaged item. Monitor versions deployed with this change can continue to
report retained retry markers during investigation.
