#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_root="${QLG_CLUSTER_ROOT:-${repo_root}/target/ursula-cluster}"

"${repo_root}/scripts/ursula-cluster-stop.sh"

case "${cluster_root}" in
  "${repo_root}"/target/ursula-cluster|/tmp/*/ursula-cluster|/tmp/ursula-cluster)
    rm -rf "${cluster_root}"
    ;;
  *)
    printf 'Refusing to remove unrecognized cluster root: %s\n' "${cluster_root}" >&2
    exit 1
    ;;
esac

printf 'removed cluster data: %s\n' "${cluster_root}"
