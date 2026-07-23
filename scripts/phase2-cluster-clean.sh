#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_root="${PHASE2_CLUSTER_ROOT:-${repo_root}/target/phase2-cluster}"

"${repo_root}/scripts/phase2-cluster-stop.sh"

case "${cluster_root}" in
  "${repo_root}"/target/phase2-cluster|/tmp/*/phase2-cluster|/tmp/phase2-cluster)
    rm -rf "${cluster_root}"
    ;;
  *)
    printf 'Refusing to remove unrecognized cluster root: %s\n' "${cluster_root}" >&2
    exit 1
    ;;
esac

printf 'removed cluster data: %s\n' "${cluster_root}"
