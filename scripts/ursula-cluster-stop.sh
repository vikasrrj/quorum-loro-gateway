#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_root="${QLG_CLUSTER_ROOT:-${repo_root}/target/ursula-cluster}"

is_running() {
  local pid="$1"
  if ! kill -0 "${pid}" 2>/dev/null; then
    return 1
  fi
  local state
  state="$(ps -o stat= -p "${pid}" 2>/dev/null || true)"
  [[ -n "${state}" && "${state}" != Z* ]]
}

for node_id in 1 2 3; do
  pid_file="${cluster_root}/pids/node-${node_id}.pid"
  [[ -f "${pid_file}" ]] || continue
  pid="$(<"${pid_file}")"
  if is_running "${pid}"; then
    kill -TERM "${pid}"
  fi
done

deadline=$((SECONDS + 25))
while (( SECONDS < deadline )); do
  running=false
  for node_id in 1 2 3; do
    pid_file="${cluster_root}/pids/node-${node_id}.pid"
    [[ -f "${pid_file}" ]] || continue
    pid="$(<"${pid_file}")"
    if is_running "${pid}"; then
      running=true
    fi
  done
  [[ "${running}" == false ]] && break
  sleep 0.25
done

for node_id in 1 2 3; do
  pid_file="${cluster_root}/pids/node-${node_id}.pid"
  [[ -f "${pid_file}" ]] || continue
  pid="$(<"${pid_file}")"
  if is_running "${pid}"; then
    kill -KILL "${pid}"
  fi
  rm -f "${pid_file}"
done

printf 'three-node Ursula cluster stopped\n'
