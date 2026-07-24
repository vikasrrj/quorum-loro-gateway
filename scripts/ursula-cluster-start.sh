#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_root="${QLG_CLUSTER_ROOT:-${repo_root}/target/ursula-cluster}"
ursula_bin="${URSULA_BIN:-/home/vik/ursula/target/release/ursula}"

if [[ ! -x "${ursula_bin}" ]]; then
  printf 'Ursula binary is missing: %s\n' "${ursula_bin}" >&2
  printf 'Build it with: (cd /home/vik/ursula && cargo build --release --locked --bin ursula)\n' >&2
  exit 1
fi

mkdir -p \
  "${cluster_root}/config" \
  "${cluster_root}/data" \
  "${cluster_root}/logs" \
  "${cluster_root}/pids" \
  "${cluster_root}/snapshots"

for node_id in 1 2 3; do
  pid_file="${cluster_root}/pids/node-${node_id}.pid"
  if [[ -f "${pid_file}" ]]; then
    pid="$(<"${pid_file}")"
    if kill -0 "${pid}" 2>/dev/null; then
      printf 'Node %s is already running as PID %s\n' "${node_id}" "${pid}" >&2
      exit 1
    fi
    rm -f "${pid_file}"
  fi
done

fresh_cluster=true
if [[ -e "${cluster_root}/data/node-1/raft-log/core-0/journal.bin" ]]; then
  fresh_cluster=false
fi

write_config() {
  local node_id="$1"
  local listen_port="$2"
  local admin_port="$3"
  local initialize="$4"
  local config_path="${cluster_root}/config/node-${node_id}.toml"

  cat >"${config_path}" <<EOF
[server]
listen = "127.0.0.1:${listen_port}"
admin_listen = "127.0.0.1:${admin_port}"

[runtime]
core_count = 1

[raft]
node_id = ${node_id}
group_count = 4
init_membership = ${initialize}
init_membership_per_group = false
snapshot_build_max_concurrency = 1
snapshot_install_max_concurrency = 1

[raft.wal]
backend = "disk"
path = "${cluster_root}/data/node-${node_id}"

[[raft.peers]]
node_id = 1
url = "http://127.0.0.1:18101"

[[raft.peers]]
node_id = 2
url = "http://127.0.0.1:18102"

[[raft.peers]]
node_id = 3
url = "http://127.0.0.1:18103"

[storage.cold]
backend = "memory"
flush_interval = "1s"
gc_interval = "1s"

[storage.snapshot]
backend = "local"
local_root = "${cluster_root}/snapshots/node-${node_id}"
drive_interval = "0s"
EOF
}

node_1_init=false
if [[ "${fresh_cluster}" == true ]]; then
  node_1_init=true
fi
write_config 1 18101 18201 "${node_1_init}"
write_config 2 18102 18202 false
write_config 3 18103 18203 false

start_node() {
  local node_id="$1"
  nohup "${ursula_bin}" \
    --config "${cluster_root}/config/node-${node_id}.toml" \
    >"${cluster_root}/logs/node-${node_id}.log" 2>&1 &
  printf '%s\n' "$!" >"${cluster_root}/pids/node-${node_id}.pid"
}

start_node 2
start_node 3
start_node 1

cleanup_failed_start() {
  "${repo_root}/scripts/ursula-cluster-stop.sh" >/dev/null 2>&1 || true
}
trap cleanup_failed_start ERR

python3 - "${cluster_root}" <<'PY'
import json
import pathlib
import sys
import time
import urllib.request

root = pathlib.Path(sys.argv[1])
urls = [f"http://127.0.0.1:{port}/__ursula/metrics" for port in (18201, 18202, 18203)]
deadline = time.monotonic() + 90
last_error = "no readiness attempt"

while time.monotonic() < deadline:
    try:
        reports = []
        for url in urls:
            with urllib.request.urlopen(url, timeout=1) as response:
                reports.append(json.load(response))
        for report in reports:
            groups = report.get("raft_groups", [])
            if len(groups) != 4:
                raise RuntimeError(f"expected 4 groups, received {len(groups)}")
            for group in groups:
                if group.get("voter_ids") != [1, 2, 3]:
                    raise RuntimeError(f"unexpected voters: {group.get('voter_ids')}")
                if group.get("current_leader") is None:
                    raise RuntimeError("group has no leader")
        print(f"three-node Ursula cluster ready under {root}")
        for node_id, report in enumerate(reports, start=1):
            leaders = [g["raft_group_id"] for g in report["raft_groups"] if g["current_leader"] == node_id]
            print(f"node {node_id}: http=1810{node_id} admin=1820{node_id} leads_groups={leaders}")
        sys.exit(0)
    except Exception as error:
        last_error = str(error)
        time.sleep(0.25)

for node_id in (1, 2, 3):
    path = root / "logs" / f"node-{node_id}.log"
    if path.exists():
        print(f"--- node {node_id} log ---", file=sys.stderr)
        print(path.read_text(errors="replace")[-4000:], file=sys.stderr)
raise SystemExit(f"cluster did not become ready: {last_error}")
PY

trap - ERR
