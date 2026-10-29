# bostan-footage-transporter

Record an RTSP camera locally on a Raspberry Pi, upload completed recordings
through SSH, and organize them as a Jellyfin TV series. Designed for the original
ARMv6 Pi Zero W: video and audio are copied without re-encoding.

## Delivery and retention

- Two independent services: `bostan-record` and `bostan-publish`.
- Hourly, clock-aligned Matroska segments. Cuts follow camera keyframes; the first
  file and interrupted files can be shorter. Exact 3,600-second cuts are not
  promised with stream copy. Original camera video/audio and on-screen overlays
  are preserved. No extra timestamp is burned into the video.
- Every closed recording has a JSON manifest with SHA-256, stream information,
  duration, UTC times, time zone, close reason, and partial status. Wall-clock
  times are recorder-side estimates, not authenticated camera timestamps. The
  camera and recorder clocks must be configured correctly; NTP is recommended.
- A locked-down SSH receiver accepts only this protocol, not arbitrary commands,
  SFTP, tunnels, or an interactive shell. A two-minute receive-idle timeout
  releases abandoned transfers so a disconnected SSH session cannot hold the
  receiver lock indefinitely. Pin its host key before use.
- Hidden staging, SHA-256 verification, ffprobe validation, file/directory fsync,
  and atomic video publication precede the durable receipt. Metadata sidecars are
  made durable before the video is exposed to the library. State and media
  must be on the same filesystem. Interrupted transfers retry from the beginning.
- Duplicate delivery is idempotent. Monthly seasons and sequential episode
  numbers are reserved in SQLite; each recording ID remains stable on retry.
- Local deletion requires the matching receipt, the retention grace period
  (default two hours), and a fresh remote rehash plus intact manifest/NFO.
  Under storage pressure, already verified files can be removed sooner.
- Unverified footage is never automatically evicted. Recording pauses below the
  free-space reserve or above the spool limit, then resumes after space returns.
  While paused, or while the camera is offline, new footage cannot be captured.
- On recorder restart, incomplete MKV files are remuxed and salvaged where
  possible. Unsalvageable files are retained for inspection. Sudden power loss
  can still lose buffered packets; this is not a forensic or lossless NVR.

The default spool ceiling is 8 GiB with a 2 GiB OS reserve. Measure actual camera
bitrate before sizing the outage buffer. Continuous recording writes to the SD
card; use suitable storage and reliable power. The project does not enable a
hardware watchdog or modify VPN/router settings.

## Requirements

Linux, Python 3.11+, FFmpeg/ffprobe, OpenSSH, systemd with `LoadCredential`, and
network access from the recorder to the RTSP camera and SSH receiver. A VPN is
recommended between recorder and receiver. Configure it independently.

## Install

1. Clone this repository on the recorder and server. Copy `examples/pi.json`
   and `examples/server.json` to private working locations and edit the values.
   `season_epoch` is the first calendar month in the archive; it stays fixed.
   The server's `library_root` must be a **new dedicated series directory**
   inside the TV library. State belongs outside the scanned library on the same
   filesystem. Do not point either setting at an existing show's directory.
2. On the Pi, run `sudo bash deploy/install.sh pi /path/to/pi.json`.
3. Create `/etc/camera-recorder/camera.env` from
   `examples/camera.env.example`, owned by root and mode `0600`, in a root-only
   directory. Fill the URL and optionally separate username/password. Values are
   literal after `=`; matching quotes are optional. Do not use shell substitutions.
4. Copy the Pi's `/var/lib/bostan/.ssh/id_ed25519.pub` to the server. Run
   `sudo bash deploy/install.sh receiver /path/to/server.json /path/to/pi.pub`.
   Give Jellyfin read access to the series tree (shared group or ACL as appropriate).
5. Obtain the server's SSH host public key through an authenticated channel.
   Verify its fingerprint and write a standard known-hosts entry to
   `/etc/bostan/known_hosts` (readable by `bostan`). Do not disable host-key checks.
   Set `destination` in the Pi config to `bostan-ingest@YOUR_SERVER`.
6. Run `sudo systemctl enable --now bostan-record bostan-publish`.
7. Enable Jellyfin real-time monitoring on the existing TV library, or arrange a
   periodic library refresh. The transport receipt confirms durable server files;
   it does not wait for Jellyfin's asynchronous indexing. Local NFO files provide
   titles, dates and monthly season names. Leave the series and seasons unlocked:
   Jellyfin can propagate a series lock to newly discovered episodes before
   reading their local metadata. Disable internet metadata providers for a camera
   library to avoid unrelated TV matches. All users with access
   to that TV library can view the recordings.

Media layout:

```text
My camera/
  tvshow.nfo
  Season 01/
    season.nfo
    Recording - S01E01 - YYYY-MM-DD HH-MM-SS - identifier.mkv
    Recording - S01E01 - YYYY-MM-DD HH-MM-SS - identifier.json
    Recording - S01E01 - YYYY-MM-DD HH-MM-SS - identifier.nfo
```

The original compressed media bytes are preserved on the server. Container
creation timestamps alone are not the recording clock; use the JSON manifest
and episode metadata. Jellyfin may transcode camera audio during playback.

## Operations

```sh
systemctl status bostan-record bostan-publish
journalctl -u bostan-record -u bostan-publish
sudo -u bostan find /var/lib/bostan/ready -maxdepth 1 -name '*.json'
sudo systemctl restart bostan-record  # closes current segment and reconnects
```

Closed video enters `ready/` before its manifest; only manifests enter the upload
queue. Receipts are retained in `receipts/` after local video cleanup for audit.
The receiver keeps its receipt database and interrupted staging in `state`.
Back up server media and state together; RAID and checksums do not replace an
independent backup. No server-side retention/deletion policy is applied.

If uploads are deferred, inspect connectivity, disk capacity and the receiver.
If one recording is corrupt, it is retained and publication stops at that entry
until an operator repairs or quarantines it. Recorder FFmpeg stderr is suppressed
because it may contain the credential URL; journals contain only generic capture
status. Do not publish raw process listings or credential files.

## Test

```sh
python3 -m unittest discover -s tests -v
bash -n deploy/install.sh
```

Tests exercise real FFmpeg-generated H.264/audio media, interrupted transfer,
checksum mismatch, changed/missing remote metadata, duplicate delivery, traversal,
remote corruption, receipt binding, deletion gating and orphan recovery. For live
validation, use a separate spool with a short `segment_seconds`; never change the
production spool's segment duration for a synthetic test.

## Public repository hygiene

Only generic source, service definitions, examples and tests belong in Git.
Credentials, SSH keys, deployment configs, camera footage and environment files
must remain outside the repository or inside the ignored `.private/` directory.
Review staged files and history before publishing. Never put an RTSP credential
URL in an issue or log.
