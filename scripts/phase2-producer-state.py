#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import pathlib
import platform
import shutil
import statistics
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
STAGES = (0, 100, 1000, 5000)
PAYLOAD = b"phase2-producer-state-payload-32"


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
            response = OPENER.open(req, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        status = response.status
        response_headers = dict(response.headers.items())
        response_body = response.read()
        if status != 307:
            return status, response_headers, response_body
        if "x-ursula-raft-leader-id" not in {
            key.lower(): value for key, value in response_headers.items()
        }:
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


def wait_applied():
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        reports = metrics()
        ready = True
        for group_id in range(4):
            committed = max(
                report["raft_groups"][group_id]["committed_index"] for report in reports
            )
            if any(
                report["raft_groups"][group_id]["last_applied_index"] < committed
                for report in reports
            ):
                ready = False
                break
        if ready:
            return reports
        time.sleep(0.05)
    raise RuntimeError("replicas did not converge to committed indexes")


def sample_rss(sample_count=7):
    samples = [[] for _ in range(3)]
    for _ in range(sample_count):
        for node, report in enumerate(metrics()):
            samples[node].append(report["process_rss_bytes"])
        time.sleep(0.5)
    return samples


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
    for admin_url in ADMIN_URLS:
        for group_id in range(4):
            status, _, body = request(
                "POST", f"{admin_url}/__ursula/raft/{group_id}/snapshot"
            )
            if status != 200:
                raise RuntimeError(
                    f"snapshot {admin_url} group {group_id} returned {status}: {body!r}"
                )
    return wait_applied()


def producer_id(index):
    digest = hashlib.sha256(f"gateway-boot-{index}".encode()).hexdigest()
    return f"qlg-{digest}"


def create_stream(stream):
    status, _, _ = request("PUT", f"{NODE_URLS[1]}/qloro")
    if status not in (200, 201, 204):
        raise RuntimeError(f"bucket create returned {status}")
    status, _, _ = request(
        "PUT",
        f"{NODE_URLS[1]}/qloro/{stream}",
        headers={"Content-Type": "application/octet-stream"},
    )
    if status not in (200, 201):
        raise RuntimeError(f"stream create returned {status}")


def append(stream, index, with_producer):
    headers = {"Content-Type": "application/octet-stream"}
    if with_producer:
        headers.update(
            {
                "producer-id": producer_id(index),
                "producer-epoch": "0",
                "producer-seq": "0",
            }
        )
    status, _, body = request(
        "POST", f"{NODE_URLS[1]}/qloro/{stream}", PAYLOAD, headers
    )
    expected = 200 if with_producer else 204
    if status != expected:
        raise RuntimeError(
            f"append {index} producer={with_producer} returned {status}: {body!r}"
        )


def run_command(name, root):
    env = os.environ.copy()
    env["PHASE2_CLUSTER_ROOT"] = str(root)
    return subprocess.run(
        [str(REPO / "scripts" / name)],
        env=env,
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
    result = run_command("phase2-cluster-clean.sh", root)
    if result.returncode != 0:
        raise RuntimeError(f"cluster cleanup failed: {result.stderr}")
    shutil.rmtree(root.parent, ignore_errors=True)


def run_workload(with_producer):
    mode = "producer" if with_producer else "control"
    root = pathlib.Path(tempfile.mkdtemp(prefix=f"qlg-{mode}-")) / "phase2-cluster"
    start = run_command("phase2-cluster-start.sh", root)
    if start.returncode != 0:
        raise RuntimeError(
            f"cluster start failed for {mode}:\n{start.stdout}\n{start.stderr}"
        )
    try:
        stream = f"producer-state-{mode}"
        create_stream(stream)
        results = {
            "mode": mode,
            "stream": stream,
            "payload_bytes": len(PAYLOAD),
            "producer_id_bytes": len(producer_id(0)) if with_producer else 0,
            "stages": [],
        }
        appended = 0
        for target in STAGES:
            while appended < target:
                append(stream, appended, with_producer)
                appended += 1
            reports = wait_applied()
            rss_samples = sample_rss()
            stage = {
                "append_count": target,
                "producer_count": target if with_producer else 0,
                "rss_samples_bytes": rss_samples,
                "rss_median_bytes": [int(statistics.median(x)) for x in rss_samples],
                "storage": storage_bytes(root),
                "metrics": reports,
            }
            if target in (0, STAGES[-1]):
                stage["snapshot_metrics"] = trigger_snapshots()
                stage["storage_after_snapshot"] = storage_bytes(root)
            results["stages"].append(stage)
        return results
    finally:
        force_clean(root)


def summarize(control, producer):
    count = STAGES[-1]
    control_final = control["stages"][-1]
    producer_final = producer["stages"][-1]
    rss_delta = [
        producer_final["rss_median_bytes"][index]
        - control_final["rss_median_bytes"][index]
        for index in range(3)
    ]
    logical_snapshot_delta = [
        producer_final["snapshot_metrics"][index]["raft_snapshot_body_bytes"]
        - control_final["snapshot_metrics"][index]["raft_snapshot_body_bytes"]
        for index in range(3)
    ]
    physical_snapshot_delta = [
        producer_final["storage_after_snapshot"][index]["snapshot_file_bytes"]
        - control_final["storage_after_snapshot"][index]["snapshot_file_bytes"]
        for index in range(3)
    ]
    wal_delta = [
        producer_final["storage"][index]["wal_bytes"]
        - control_final["storage"][index]["wal_bytes"]
        for index in range(3)
    ]
    return {
        "producer_count_per_stream": count,
        "replica_count": 3,
        "rss_delta_bytes_by_replica": rss_delta,
        "rss_interpretation": (
            "Inconclusive: mimalloc returned large regions during sampling, and one "
            "producer-minus-control delta was negative. Raw samples are retained; no "
            "per-producer heap estimate is claimed from RSS."
        ),
        "wal_delta_bytes_by_replica": wal_delta,
        "wal_delta_bytes_per_producer_by_replica": [value / count for value in wal_delta],
        "logical_snapshot_delta_bytes_by_replica": logical_snapshot_delta,
        "logical_snapshot_delta_bytes_per_producer_by_replica": [
            value / count for value in logical_snapshot_delta
        ],
        "physical_snapshot_delta_bytes_by_replica": physical_snapshot_delta,
        "physical_snapshot_delta_bytes_per_producer_by_replica": [
            value / count for value in physical_snapshot_delta
        ],
        "physical_snapshot_interpretation": (
            "Cumulative local files are retained without pruning, so logical snapshot "
            "body bytes are the authoritative per-build comparison."
        ),
        "restart_effect": (
            "Each distinct gateway boot producer ID creates one retained producer entry; "
            "the producer workload uses gateway-shaped IDs to model this cardinality."
        ),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default=str(REPO / "results" / "phase2" / "producer-state.json"),
    )
    args = parser.parse_args()
    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    control = run_workload(False)
    producer = run_workload(True)
    result = {
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "ursula_binary": os.environ.get(
                "URSULA_BIN", "/home/vik/ursula/target/release/ursula"
            ),
            "ursula_revision": subprocess.check_output(
                ["git", "-C", "/home/vik/ursula", "rev-parse", "HEAD"], text=True
            ).strip(),
            "gateway_revision": subprocess.check_output(
                ["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True
            ).strip(),
        },
        "method": {
            "stages": STAGES,
            "payload_bytes": len(PAYLOAD),
            "rss_samples_per_stage": 7,
            "rss_sample_interval_seconds": 0.5,
            "control": "same stream and payloads without producer headers",
            "producer": "one distinct 68-byte gateway-shaped producer ID at sequence zero per append",
        },
        "control": control,
        "producer": producer,
    }
    result["summary"] = summarize(control, producer)
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["summary"], indent=2))
    print(f"raw results: {output}")


if __name__ == "__main__":
    main()
