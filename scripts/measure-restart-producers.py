#!/usr/bin/env python3
import hashlib
import json
import os
import pathlib
import platform
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

REPO = pathlib.Path(__file__).resolve().parents[1]
NODE_URLS = [f"http://127.0.0.1:{port}" for port in (18101, 18102, 18103)]
ADMIN_URLS = [f"http://127.0.0.1:{port}" for port in (18201, 18202, 18203)]
ALLOWED_ORIGINS = {urllib.parse.urlsplit(url).netloc for url in NODE_URLS}
ROOM_ID = "restart-producer-benchmark"
STREAM = f"r-{hashlib.sha256(ROOM_ID.encode()).hexdigest()}-d0"
STAGES = (0, 100, 1000, 5000)
PAYLOAD = b"qlg-restart-producer-payload-v1"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


OPENER = urllib.request.build_opener(NoRedirect)


def request(method, url, body=None, headers=None, max_redirects=6):
    headers = dict(headers or {})
    visited = {url}
    for _ in range(max_redirects + 1):
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            response = OPENER.open(req, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        status = response.status
        response_headers = dict(response.headers.items())
        response_body = response.read()
        if status != 307:
            return status, response_headers, response_body
        marked = {
            key.lower(): value for key, value in response_headers.items()
        }.get("x-ursula-raft-leader-id")
        if marked is None:
            raise RuntimeError(f"unmarked redirect from {url}")
        location = response_headers.get("Location") or response_headers.get("location")
        if not location:
            raise RuntimeError(f"redirect without Location from {url}")
        parsed = urllib.parse.urlsplit(location)
        if parsed.netloc not in ALLOWED_ORIGINS:
            raise RuntimeError(f"redirect target is not an Ursula node: {location}")
        if location in visited:
            raise RuntimeError(f"redirect loop at {location}")
        visited.add(location)
        url = location
    raise RuntimeError(f"redirect limit exceeded for {method}")


def get_json(url):
    status, _, body = request("GET", url)
    if status != 200:
        raise RuntimeError(f"GET {url} returned {status}")
    return json.loads(body)


def metrics():
    return [get_json(f"{url}/__ursula/metrics") for url in ADMIN_URLS]


def groups_by_id(report):
    return {group["raft_group_id"]: group for group in report["raft_groups"]}


def wait_applied():
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        reports = metrics()
        indexed = [groups_by_id(report) for report in reports]
        ready = True
        for group_id in range(4):
            committed = max(report[group_id]["committed_index"] for report in indexed)
            if any(
                report[group_id]["last_applied_index"] < committed
                for report in indexed
            ):
                ready = False
                break
        if ready:
            return reports
        time.sleep(0.05)
    raise RuntimeError("replicas did not converge to committed indexes")


def directory_bytes(path):
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def storage_bytes(root):
    return [
        {
            "wal_bytes": directory_bytes(root / "data" / f"node-{node_id}"),
            "snapshot_file_bytes": directory_bytes(
                root / "snapshots" / f"node-{node_id}"
            ),
        }
        for node_id in (1, 2, 3)
    ]


def trigger_snapshots():
    before = metrics()
    before_builds = [report["raft_snapshot_builds"] for report in before]
    for admin_url in ADMIN_URLS:
        for group_id in range(4):
            status, _, body = request(
                "POST", f"{admin_url}/__ursula/raft/{group_id}/snapshot"
            )
            if status != 200:
                raise RuntimeError(
                    f"snapshot {admin_url} group {group_id} returned {status}: {body!r}"
                )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        reports = wait_applied()
        if all(
            report["raft_snapshot_builds"] > before_builds[index]
            for index, report in enumerate(reports)
        ):
            return {"before": before, "after": reports}
        time.sleep(0.05)
    raise RuntimeError("snapshot builds did not complete")


def boot_id(index):
    hasher = hashlib.sha256()
    hasher.update(b"qlg-restart-producer-boot-v1\0")
    hasher.update(index.to_bytes(8, "big"))
    return hasher.digest()[:16]


def producer_id(index):
    hasher = hashlib.sha256()
    hasher.update(b"quorum-loro-producer-v1\0")
    hasher.update(boot_id(index))
    hasher.update(ROOM_ID.encode())
    hasher.update((0).to_bytes(8, "big"))
    return f"qlg-{hasher.hexdigest()}"


def create_stream():
    status, _, _ = request("PUT", f"{NODE_URLS[1]}/qloro")
    if status not in (200, 201, 204):
        raise RuntimeError(f"bucket create returned {status}")
    status, _, _ = request(
        "PUT",
        f"{NODE_URLS[1]}/qloro/{STREAM}",
        headers={"Content-Type": "application/octet-stream"},
    )
    if status not in (200, 201):
        raise RuntimeError(f"stream create returned {status}")


def append(index, restart_mode):
    boot_index = index if restart_mode else 0
    sequence = 0 if restart_mode else index
    identity = producer_id(boot_index)
    status, _, body = request(
        "POST",
        f"{NODE_URLS[1]}/qloro/{STREAM}",
        PAYLOAD,
        {
            "Content-Type": "application/octet-stream",
            "producer-id": identity,
            "producer-epoch": "0",
            "producer-seq": str(sequence),
        },
    )
    if status != 200:
        raise RuntimeError(
            f"append {index} restart_mode={restart_mode} returned {status}: {body!r}"
        )
    return identity


def run_command(name, root):
    environment = os.environ.copy()
    environment["QLG_CLUSTER_ROOT"] = str(root)
    return subprocess.run(
        [str(REPO / "scripts" / name)],
        env=environment,
        cwd=REPO,
        text=True,
        capture_output=True,
        check=False,
    )


def force_clean(root):
    for pid_file in (root / "pids").glob("*.pid"):
        try:
            os.kill(int(pid_file.read_text().strip()), 9)
        except (FileNotFoundError, ProcessLookupError, ValueError):
            pass
    result = run_command("ursula-cluster-clean.sh", root)
    if result.returncode != 0:
        raise RuntimeError(f"cluster cleanup failed: {result.stderr}")
    shutil.rmtree(root.parent, ignore_errors=True)


def run_workload(restart_mode):
    mode = "restart" if restart_mode else "stable"
    root = pathlib.Path(tempfile.mkdtemp(prefix=f"qlg-restart-producer-{mode}-")) / "ursula-cluster"
    start = run_command("ursula-cluster-start.sh", root)
    if start.returncode != 0:
        raise RuntimeError(
            f"cluster start failed for {mode}:\n{start.stdout}\n{start.stderr}"
        )
    try:
        create_stream()
        identities = set()
        stages = []
        appended = 0
        for target in STAGES:
            while appended < target:
                identities.add(append(appended, restart_mode))
                appended += 1
            reports = wait_applied()
            stage = {
                "append_count": target,
                "committed_distinct_producer_ids": len(identities),
                "expected_retained_producer_count": len(identities),
                "stream_payload_bytes": target * len(PAYLOAD),
                "storage": storage_bytes(root),
                "metrics": reports,
            }
            if target in (0, STAGES[-1]):
                stage["snapshot"] = trigger_snapshots()
                stage["storage_after_snapshot"] = storage_bytes(root)
            stages.append(stage)
        return {
            "mode": mode,
            "stream": STREAM,
            "room_id": ROOM_ID,
            "payload_bytes": len(PAYLOAD),
            "producer_id_bytes": len(producer_id(0)),
            "stages": stages,
        }
    finally:
        force_clean(root)


def subtract(left, right):
    return [left[index] - right[index] for index in range(3)]


def growth(workload, storage_field):
    first = workload["stages"][0]["storage_after_snapshot"]
    last = workload["stages"][-1]["storage_after_snapshot"]
    return [last[index][storage_field] - first[index][storage_field] for index in range(3)]


def snapshot_growth(workload):
    first = workload["stages"][0]["snapshot"]
    last = workload["stages"][-1]["snapshot"]
    return [
        last["after"][index]["raft_snapshot_body_bytes"]
        - first["after"][index]["raft_snapshot_body_bytes"]
        for index in range(3)
    ]


def summarize(stable, restart):
    stable_wal = growth(stable, "wal_bytes")
    restart_wal = growth(restart, "wal_bytes")
    stable_snapshot = snapshot_growth(stable)
    restart_snapshot = snapshot_growth(restart)
    wal_overhead = subtract(restart_wal, stable_wal)
    snapshot_overhead = subtract(restart_snapshot, stable_snapshot)
    producer_count = restart["stages"][-1]["expected_retained_producer_count"]
    stable_count = stable["stages"][-1]["expected_retained_producer_count"]
    extra_producers = producer_count - stable_count
    return {
        "append_count": STAGES[-1],
        "gateway_boot_count": producer_count,
        "gateway_restart_count": max(producer_count - 1, 0),
        "stable_producer_count": stable_count,
        "restart_producer_count": producer_count,
        "extra_retained_producers_vs_stable": extra_producers,
        "stable_wal_growth_bytes_by_replica": stable_wal,
        "restart_wal_growth_bytes_by_replica": restart_wal,
        "restart_wal_metadata_overhead_bytes_by_replica": wal_overhead,
        "restart_wal_metadata_overhead_bytes_per_extra_producer_by_replica": [
            value / extra_producers for value in wal_overhead
        ],
        "stable_logical_snapshot_growth_bytes_by_replica": stable_snapshot,
        "restart_logical_snapshot_growth_bytes_by_replica": restart_snapshot,
        "restart_logical_snapshot_metadata_overhead_bytes_by_replica": snapshot_overhead,
        "restart_logical_snapshot_metadata_overhead_bytes_per_extra_producer_by_replica": [
            value / extra_producers for value in snapshot_overhead
        ],
        "producer_count_interpretation": (
            "Ursula does not expose producer-map cardinality. Counts are distinct exact "
            "gateway-derived producer IDs whose first append committed successfully."
        ),
    }


def git_revision():
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
    ).strip()


def main():
    output = REPO / "results" / "producer-state" / "restarts.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    stable = run_workload(False)
    restart = run_workload(True)
    result = {
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "gateway_revision": git_revision(),
            "ursula_binary": os.environ.get(
                "URSULA_BIN", "/home/vik/ursula/target/release/ursula"
            ),
        },
        "method": {
            "stages": STAGES,
            "cluster": "fresh three-voter disk-WAL cluster per workload arm",
            "stream": "one long-lived stream within each workload arm",
            "stable": "one exact gateway-derived producer ID, monotonically increasing sequence",
            "restart": "one deterministic boot ID and exact production producer derivation per append, sequence zero",
            "isolation": (
                "Direct Ursula appends isolate replicated producer metadata; this models "
                "successful gateway boot sessions and does not measure process launch cost."
            ),
            "payload_bytes": len(PAYLOAD),
        },
        "stable": stable,
        "restart": restart,
    }
    result["summary"] = summarize(stable, restart)
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["summary"], indent=2))
    print(f"raw results: {output}")


if __name__ == "__main__":
    main()
