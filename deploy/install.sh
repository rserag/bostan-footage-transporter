#!/bin/bash
# Install code and service accounts. Configuration files must already be prepared.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ $EUID == 0 ]] || { echo 'Run as root'; exit 1; }
role=${1:?Usage: install.sh pi|receiver CONFIG [PUBLIC_KEY_FILE]}
config=${2:?Configuration path required}
python3 -m json.tool "$config" >/dev/null
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ffmpeg openssh-client python3
install -d -m 0755 /opt/bostan
install -m 0644 src/bostan.py /opt/bostan/bostan.py
if [[ $role == pi ]]; then
    id bostan >/dev/null 2>&1 || useradd --system --home-dir /var/lib/bostan --shell /usr/sbin/nologin bostan
    install -d -m 0755 /etc/bostan
    install -m 0644 "$config" /etc/bostan/pi.json
    install -d -o bostan -g bostan -m 0750 /var/lib/bostan
    install -d -o bostan -g bostan -m 0700 /var/lib/bostan/.ssh
    if [[ ! -f /var/lib/bostan/.ssh/id_ed25519 ]]; then
        runuser -u bostan -- ssh-keygen -q -t ed25519 -N '' -f /var/lib/bostan/.ssh/id_ed25519
    fi
    install -m 0644 deploy/bostan-{record,publish}.service /etc/systemd/system/
    systemctl daemon-reload
    echo 'Code installed. Install camera credentials and pinned known_hosts before enabling services.'
elif [[ $role == receiver ]]; then
    apt-get install -y --no-install-recommends openssh-server
    key=${3:?Recorder public key file required}
    id bostan-ingest >/dev/null 2>&1 || useradd --system --home-dir /var/lib/bostan-ingest --shell /bin/sh bostan-ingest
    install -d -o root -g bostan-ingest -m 0750 /var/lib/bostan-ingest /var/lib/bostan-ingest/.ssh
    install -o root -g bostan-ingest -m 0640 "$config" /etc/bostan-server.json
    python3 - "$config" <<'PY'
import json,os,pwd,sys
from pathlib import Path
c=json.load(open(sys.argv[1]));uid=pwd.getpwnam('bostan-ingest').pw_uid
for k in ['library_root','state']:
    p=Path(c[k]);p.mkdir(parents=True,exist_ok=True);os.chown(p,uid,-1);os.chmod(p,0o2755)
if Path(c['library_root']).stat().st_dev != Path(c['state']).stat().st_dev:
    raise SystemExit('library_root and state must share a filesystem for atomic publication')
PY
    printf 'restrict,command="/usr/bin/python3 /opt/bostan/bostan.py receive --config /etc/bostan-server.json" %s\n' "$(cat "$key")" > /var/lib/bostan-ingest/.ssh/authorized_keys
    chown root:bostan-ingest /var/lib/bostan-ingest/.ssh/authorized_keys
    chmod 0640 /var/lib/bostan-ingest/.ssh/authorized_keys
    echo 'Receiver installed. Confirm Jellyfin can read library_root and run an SSH upload test.'
else
    echo 'Role must be pi or receiver'; exit 1
fi
