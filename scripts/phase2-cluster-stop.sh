#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_root="${PHASE2_CLUSTER_ROOT:-${repo_root}/target/phase2-cluster}"

for node_id in 1 2 3; do
  pid_file="${cluster_root}/pids/node-${node_id}.pid"
  [[ -f "${pid_file}" ]] || continue
  pid="$(<"${pid_file}")"
  if kill -0 "${pid}" 2>/dev/null; then
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
    if kill -0 "${pid}" 2>/dev/null; then
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
  if kill -0 "${pid}" 2>/dev/null; then
    kill -KILL "${pid}"
  fi
  rm -f "${pid_file}"
done

printf 'three-node Ursula cluster stopped\n'
