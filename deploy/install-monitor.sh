#!/bin/bash
# Installs only monitoring. Never restarts the recorder, publisher or receiver.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ $EUID == 0 ]] || { echo 'Run as root'; exit 1; }
role=${1:?Usage: install-monitor.sh collector|probe-pi|probe-server CONFIG [PUBLIC_KEY [SOURCE_IP]]}
config=${2:?Configuration path required}
case "$role" in collector|probe-pi|probe-server) ;; *) echo 'Invalid role'; exit 1 ;; esac
python3 -m json.tool "$config" >/dev/null
id bostan-monitor >/dev/null 2>&1 || useradd --system --no-create-home \
    --home-dir /var/lib/bostan-monitor-ssh --shell /bin/sh bostan-monitor
install -d -m 0755 /opt/bostan
install -m 0644 src/bostan_monitor.py /opt/bostan/bostan_monitor.py
install -d -o root -g bostan-monitor -m 0750 /etc/bostan-monitor
if [[ $role == collector ]]; then
    getent group alloy >/dev/null || { echo 'Install/configure Alloy first'; exit 1; }
    install -d -o root -g bostan-monitor -m 0750 /etc/bostan
    install -o root -g bostan-monitor -m 0640 "$config" /etc/bostan/monitor.json
    install -d -o bostan-monitor -g bostan-monitor -m 0700 /var/lib/bostan-monitor
    install -d -o bostan-monitor -g alloy -m 2750 /var/lib/bostan-monitor-metrics
    install -m 0644 deploy/bostan-monitor.{service,timer} /etc/systemd/system/
    systemctl daemon-reload
    echo 'Installed. Provision private credentials, pinned hosts and external freshness alert before enabling timer.'
else
    key=${3:?Monitor public key file required}
    # Validate rather than embedding unchecked input in authorized_keys options.
    ssh-keygen -l -f "$key" >/dev/null
    [[ $(wc -l < "$key") -eq 1 ]] || { echo 'One public key required'; exit 1; }
    source_ip=${4:-}
    [[ -z $source_ip || $source_ip =~ ^[0-9a-fA-F.:/]+$ ]] || { echo 'Invalid source IP'; exit 1; }
    install -o root -g bostan-monitor -m 0640 "$config" /etc/bostan-monitor/probe.json
    if [[ $role == probe-pi ]]; then
        # Recorder service uses 0027 umask: this group has read/traverse only.
        usermod -aG bostan bostan-monitor
    else
        command -v setfacl >/dev/null || { echo 'Install the acl package first'; exit 1; }
        python3 - "$config" <<'PY'
import json,subprocess,sys
c=json.load(open(sys.argv[1]))
for field in ('library_root','state'):
    subprocess.run(['setfacl','-m','u:bostan-monitor:r-x,d:u:bostan-monitor:r-x',c[field]],check=True)
PY
    fi
    install -d -o root -g bostan-monitor -m 0750 \
        /var/lib/bostan-monitor-ssh /var/lib/bostan-monitor-ssh/.ssh
    options='restrict'
    [[ -z $source_ip ]] || options="from=\"$source_ip\",$options"
    printf '%s,command="/usr/bin/python3 /opt/bostan/bostan_monitor.py %s --config /etc/bostan-monitor/probe.json" %s\n' \
        "$options" "$role" "$(cat "$key")" > /var/lib/bostan-monitor-ssh/.ssh/authorized_keys
    chown root:bostan-monitor /var/lib/bostan-monitor-ssh/.ssh/authorized_keys
    chmod 0640 /var/lib/bostan-monitor-ssh/.ssh/authorized_keys
    echo 'Restricted read-only probe installed; no recorder or upload service was changed.'
fi
