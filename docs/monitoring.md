# External recording and delivery monitoring

`src/bostan_monitor.py` runs on an independent Linux host once per minute. It
pulls small, read-only snapshots from the recorder and archive over separate
restricted SSH keys, and queries Jellyfin from the monitor host. It does not
change recording, upload, retention, recovery, clocks or watchdog settings.
The recorder needs no exporter daemon, listener, third-party Python packages or
Grafana agent. The probe stats files and reads small JSON/SQLite metadata; it
does not hash or decode videos.

## Alerts and thresholds

These defaults suit hourly segments and observed uploads of about 2–3 minutes.
All durations are approximate: add up to one polling interval, network request
time and notification delivery time. Recoveries require two continuous minutes
of healthy observations. There is no initial "healthy" notification.

| Check | Failure condition and grace | Operator action |
| --- | --- | --- |
| Pi/VPN unreachable | Restricted SSH snapshot unavailable for 3 minutes | Check power, router/VPN and the monitor's pinned SSH access; capture state is unknown |
| Archive unreachable | Restricted SSH snapshot unavailable for 3 minutes | Check server/LAN/SSH; preserve the Pi queue |
| Recorder down | Service not running for 2 minutes | Check service, camera and storage guard |
| Publisher down | Service not running for 2 minutes | Check publisher, receiver and oldest queue entry |
| Capture stalled | No observed increase in current MKV bytes for over 3 minutes | Check camera/power/network, recorder and storage; a running service alone is insufficient |
| Late closed segment | Any active MKV's opening time is older than target duration + 10 minutes, for 2 minutes | Check finalization/recovery and retained raw files |
| Upload backlog | Oldest unacknowledged manifest closed over 15 minutes ago | Check VPN, receiver capacity and oldest queue entry |
| Invalid queue | Invalid manifest/receipt, or video without a manifest older than 5 minutes, for 2 minutes | Inspect privately; retain footage for repair/manual quarantine |
| Buffer low | Conservative estimated remaining recording time below 6 hours, for 5 minutes | Restore uploads or provide safe storage |
| Buffer critical | Estimate below 2 hours (including a reached storage guard), for 2 minutes | Restore delivery urgently; recording pauses at its existing guard |
| Jellyfin unavailable | Authenticated read-only indexing query fails for 3 minutes | Check Jellyfin/API access; delivery may still work |
| Jellyfin missing footage | Committed, present footage over 10 minutes old absent from index, for 2 minutes | Check mount, permissions and scan; compare archive first |
| Archive assets missing | A committed video is absent/size changed, or JSON/NFO absent, for 2 minutes | Check storage/receipt ledger and documented deletions |
| Clock skew | Probe and monitor clocks differ by over 2 minutes, for 2 minutes | Check NTP; timing diagnostics may be inaccurate |

No upstream response means dependent checks are **unknown**, not recovered.
Existing child failures stay active until actual healthy observations return.
Both capacity levels may be active during a severe shortage. A missing or
corrupt saved monitor state is not silently reset: the cycle fails, and the
independent freshness rule below detects the stopped heartbeat.

The queue counts only manifests without a matching committed receipt. Uploaded
copies kept locally during deletion grace are not an upload backlog. The buffer
estimate counts all local spool files, uses the smaller of spool-ceiling
headroom and filesystem space above the OS reserve, and assumes **no further
cleanup**. The byte rate is the larger of the configured fallback and 120% of
the median bitrate of the six newest available closed segments. Configure the
fallback from measured traffic. This conservative estimate is not a guaranteed
outage duration or a prediction of future bitrate.

The server probes every committed receipt row. A ledger at
`state/intentional-deletions.json` can exclude IDs explicitly marked
`{"status":"deleted"}`. Planned/pending entries are not exclusions. Monitoring
does not edit the ledger or delete receipt rows, files or episode counters.
Index comparisons cover all returned committed files using relative-path hashes
and paginated Jellyfin results. No filenames, camera URLs, keys or raw errors
are metric labels or notification content.

## Delivery and deduplication

Pipeline failures and recoveries use Telegram's `sendMessage` with the existing
operator chat. Each message names the failed check, its severity and a specific
action. Successful deliveries are saved to `state.json` after each transition;
restarting the collector does not repeat an unchanged incident. Failed sends
retry after 1, 2, 4 and then 5 minutes, with a per-cycle time budget. A recovered
incident whose failure was never delivered produces no stale recovery message.
The unavoidable crash window between Telegram accepting a message and saving
the acknowledgement can duplicate one message. Keep the state file private and
durable; do not delete it as a troubleshooting shortcut.

The collector exports `bostan_notifications_ok=0` while a transition is awaiting
delivery and writes an atomic Prometheus textfile after each completed cycle.
It is not responsible for noticing its own death. An independent **cloud** rule
must alert on a stale/missing heartbeat and notification failures. Running an
alert evaluator only on the collector would leave a blind spot when it stops.

## Installation

1. Prepare a private copy of `examples/monitor.json`. Set the recorder/server
   destinations, pinned hosts, Jellyfin series ID and container-visible series
   root. Keep camera credentials and deployment configuration out of Git.
2. On the independent monitor, install/configure Alloy first, then run
   `sudo bash deploy/install-monitor.sh collector /private/monitor.json`.
   Create a dedicated Ed25519 SSH key in `/etc/bostan-monitor/`, readable only
   by root and `bostan-monitor`. The monitor is unprivileged, has no sudo rule
   and is not in the Docker, recorder or archive administration groups.
3. Install the public key on the recorder with
   `sudo bash deploy/install-monitor.sh probe-pi /etc/bostan/pi.json monitor.pub MONITOR_SOURCE_IP`.
   Install `acl` on the archive, then use role `probe-server` with its receiver
   config. Use the source address actually observed at each host. The installer
   creates root-owned authorized keys with `from=`, `restrict` and a fixed
   probe command. The recorder group is read/traverse only under its existing
   `0027` umask; archive ACLs grant read/traverse without group write access.
   Preserve the uploader's separate forced-command key and restrictions.
4. Obtain each host's SSH public key through an authenticated channel and pin
   it in `/etc/bostan-monitor/known_hosts`. No automatic trust-on-first-use or
   disabled host-key checking is supported. Private credentials referenced by
   `monitor.json` must be readable by the monitor, never by the recorder. In
   particular, keep the Jellyfin API key on the independent host.
5. Give Alloy read/traverse access to `/var/lib/bostan-monitor-metrics` (the
   installer uses the `alloy` group). Append `deploy/bostan.alloy` to the
   existing Alloy configuration, adapting `forward_to` to its existing remote
   write pipeline. Validate the complete candidate with `alloy validate` and
   reload Alloy. Do not overwrite unrelated configuration. The dedicated
   textfile exporter collects only Bostan metrics and adds no public listener.
6. Run one collection and an explicitly labelled notification test:

   ```sh
   sudo -u bostan-monitor python3 /opt/bostan/bostan_monitor.py collect --config /etc/bostan/monitor.json
   sudo -u bostan-monitor python3 /opt/bostan/bostan_monitor.py test-notification --config /etc/bostan/monitor.json
   sudo systemctl enable --now bostan-monitor.timer
   ```

7. Add the cloud freshness rule below, and verify its scheduled evaluation is
   Normal with advancing metrics. Installing a timer alone is insufficient.

## Independent cloud freshness rule

The reference deployment uses Grafana Cloud's existing Prometheus source and
Telegram contact point. Its one-minute evaluation uses this instant PromQL
query (adapt `site` for another deployment):

```promql
((max(time() - bostan_monitor_timestamp_seconds{site="padval"}))
 + (1 - min(bostan_notifications_ok{site="padval"})) * 600)
or vector(1000000)
```

Alert above `300` for `2m`; keep firing for `2m` after recovery. Treat both
no-data and query errors as **Alerting**. Thus a stopped collector, stale
textfile, failed scrape/remote write, or whole monitoring-host outage alerts
after roughly 5–8 minutes. Persistent notification failures alert after about
2–3 minutes. Include instructions to check the monitor timer/service, Alloy
delivery and Telegram credentials, and link this runbook.

Use a dedicated rule notification override: group wait `30s`, group interval
`5m`, repeat `5d`. This affects only the fallback rule. Pipeline transitions
never intentionally repeat while unchanged; Grafana's fallback can remind
again during an unchanged outage lasting five days. Grafana limits repeat
suppression to its notification-log retention, documented in
[Grafana notification timing](https://grafana.com/docs/grafana/latest/alerting/fundamentals/notifications/group-alert-notifications/).
The shared Telegram bot/provider/chat remains a common delivery dependency;
independent multi-channel paging is outside this deployment.

## Diagnostics, tests and rollback

```sh
systemctl status bostan-monitor.timer
systemctl show bostan-monitor.service -p Result -p ExecMainStatus
journalctl -u bostan-monitor.service
python3 -m unittest discover -s tests -v
bash -n deploy/install.sh deploy/install-monitor.sh
```

Collector diagnostics contain numeric status and fixed check names. Never
publish raw recorder process arguments, camera credentials or arbitrary SSH/
HTTP exception text. SSH/auth/config/probe failures intentionally share a
sanitized unavailable result. Inspect credential permissions privately.

Tests use temporary spools/databases, synthetic clocks/snapshots and simulated
notifications. They exercise failure grace, byte growth versus mere file
touching, hourly rotation, buffer guards, delayed uploads/indexing, intentional
deletions, upstream unknown states, restart deduplication, send backoff, atomic
files, host-key pinning and full collection. Test cloud no-data logic by
previewing the query with a nonexistent `site` selector; never stop live
recording, networking, storage or production telemetry to test alerts.

For rollback, pause the newly added cloud rule, disable only
`bostan-monitor.timer`, and stop a running `bostan-monitor.service`. Remove
only the Bostan Alloy block, validate and reload. The recorder, publisher,
receiver and existing Padval monitoring continue independently. Keep alert
state if re-enabling; no footage or receipt-database rollback is involved.

This is availability monitoring, not a periodic content-integrity audit. File
size and sidecar existence do not prove hashes, decodability, audio continuity
or Jellyfin playback. A growing MKV also cannot prove a changing scene or every
packet's capture. Transport's existing SHA verification still applies. Physical
power/card repair, poison-entry isolation, bounded recovery, clock startup,
backups, watchdog investigation and retention decisions are separate work.
