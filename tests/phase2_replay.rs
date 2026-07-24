use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use loro::ExportMode;
use loro::LoroDoc;
use loro_protocol::BatchId;
use loro_protocol::ProtocolMessage;
use quorum_loro_gateway::HttpUrsula;
use quorum_loro_gateway::HttpUrsulaConfig;
use quorum_loro_gateway::RoomLifecycle;
use quorum_loro_gateway::RoomManager;
use quorum_loro_gateway::actor::ActorConfig;
use quorum_loro_gateway::actor::Outbound;
use quorum_loro_gateway::frame::DeltaFrame;
use quorum_loro_gateway::frame::ProducerTuple;
use quorum_loro_gateway::names::delta_stream;
use quorum_loro_gateway::ursula::AppendOutcome;
use quorum_loro_gateway::ursula::StoreError;
use quorum_loro_gateway::ursula::UrsulaStore;
use serde_json::json;
use sha2::Digest;
use sha2::Sha256;

const DEFAULT_COUNTS: [usize; 4] = [25_000, 50_000, 100_000, 250_000];
const DEFAULT_REPETITIONS: usize = 3;
const LOAD_CHUNK_BYTES: usize = 1024 * 1024;
const READ_WINDOW_BYTES: usize = 64 * 1024;
const RESULT_MARKER: &str = "PHASE2_REPLAY_RESULT=";
const NODE_URLS: [&str; 3] = [
    "http://127.0.0.1:18101",
    "http://127.0.0.1:18102",
    "http://127.0.0.1:18103",
];

struct ClusterGuard {
    root: PathBuf,
}

impl ClusterGuard {
    fn start() -> anyhow::Result<Self> {
        let root = env::temp_dir()
            .join(format!("qlg-replay-{}", uuid::Uuid::new_v4()))
            .join("phase2-cluster");
        let output = Command::new(script("phase2-cluster-start.sh"))
            .env("PHASE2_CLUSTER_ROOT", &root)
            .output()?;
        if !output.status.success() {
            anyhow::bail!(
                "cluster start failed:\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(Self { root })
    }
}

impl Drop for ClusterGuard {
    fn drop(&mut self) {
        for node_id in 1..=3 {
            let path = self.root.join("pids").join(format!("node-{node_id}.pid"));
            if let Ok(pid) = std::fs::read_to_string(path) {
                let _ = Command::new("kill")
                    .arg("-KILL")
                    .arg(pid.trim())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
        let _ = Command::new(script("phase2-cluster-clean.sh"))
            .env("PHASE2_CLUSTER_ROOT", &self.root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Some(parent) = self.root.parent() {
            let _ = std::fs::remove_dir_all(parent);
        }
    }
}

struct History {
    chunks: Vec<Vec<u8>>,
    stored_bytes: usize,
    loro_bytes: usize,
    min_loro_blob_bytes: usize,
    max_loro_blob_bytes: usize,
    expected_state_hash: String,
}

fn script(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join(name)
}

fn build_history(count: usize) -> anyhow::Result<History> {
    let doc = LoroDoc::new();
    doc.set_peer_id(700)?;
    let text = doc.get_text("text");
    let mut chunks = Vec::new();
    let mut chunk = Vec::new();
    let mut stored_bytes = 0_usize;
    let mut loro_bytes = 0_usize;
    let mut min_loro_blob_bytes = usize::MAX;
    let mut max_loro_blob_bytes = 0_usize;

    for index in 0..count {
        let before = doc.oplog_vv();
        text.insert(text.len_unicode(), "x")?;
        doc.commit();
        let update = doc.export(ExportMode::updates(&before))?;
        loro_bytes = loro_bytes
            .checked_add(update.len())
            .context("Loro byte count overflow")?;
        min_loro_blob_bytes = min_loro_blob_bytes.min(update.len());
        max_loro_blob_bytes = max_loro_blob_bytes.max(update.len());
        let frame = DeltaFrame::new(
            ProducerTuple {
                id: "phase2-5-replay-frame".into(),
                epoch: 0,
                sequence: u64::try_from(index)?,
            },
            BatchId(u64::try_from(index)?.to_be_bytes()),
            vec![update],
        )
        .encode()?;
        if !chunk.is_empty() && chunk.len() + frame.len() > LOAD_CHUNK_BYTES {
            chunks.push(std::mem::take(&mut chunk));
        }
        stored_bytes = stored_bytes
            .checked_add(frame.len())
            .context("encoded stream byte count overflow")?;
        chunk.extend_from_slice(&frame);
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }

    Ok(History {
        chunks,
        stored_bytes,
        loro_bytes,
        min_loro_blob_bytes,
        max_loro_blob_bytes,
        expected_state_hash: benchmark_state_hash(&"x".repeat(count)),
    })
}

fn benchmark_state_hash(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"qlg-phase2-5-state-v1\0");
    hasher.update(u64::try_from(text.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(text.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn proc_status_bytes(name: &str) -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix(name)
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
                .and_then(|kib| kib.checked_mul(1024))
        })
        .unwrap_or(0)
}

async fn collect_snapshot(
    room: &quorum_loro_gateway::actor::RoomHandle,
) -> anyhow::Result<Vec<u8>> {
    let (peer, mut outbound) = tokio::sync::mpsc::unbounded_channel();
    room.join(1, Vec::new(), peer).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let mut fragments = BTreeMap::new();
    let mut expected_fragments = None;
    let mut expected_bytes = None;

    loop {
        let message = tokio::time::timeout_at(deadline, outbound.recv())
            .await
            .context("timed out collecting reconstructed state")?
            .context("room closed while collecting reconstructed state")?;
        match message {
            Outbound::Protocol(ProtocolMessage::DocUpdate { mut updates, .. }) => {
                anyhow::ensure!(updates.len() == 1, "expected one reconstructed snapshot");
                return Ok(updates.remove(0));
            }
            Outbound::Protocol(ProtocolMessage::DocUpdateFragmentHeader {
                fragment_count,
                total_size_bytes,
                ..
            }) => {
                expected_fragments = Some(fragment_count);
                expected_bytes = Some(total_size_bytes);
            }
            Outbound::Protocol(ProtocolMessage::DocUpdateFragment {
                index, fragment, ..
            }) => {
                anyhow::ensure!(
                    fragments.insert(index, fragment).is_none(),
                    "duplicate fragment"
                );
                if Some(u64::try_from(fragments.len())?) == expected_fragments {
                    let mut snapshot = Vec::new();
                    for expected_index in
                        0..expected_fragments.context("missing fragment header")?
                    {
                        snapshot.extend_from_slice(
                            fragments
                                .get(&expected_index)
                                .context("missing reconstructed-state fragment")?,
                        );
                    }
                    anyhow::ensure!(
                        u64::try_from(snapshot.len())?
                            == expected_bytes.context("missing byte count")?,
                        "reconstructed-state fragment length mismatch"
                    );
                    return Ok(snapshot);
                }
            }
            Outbound::Protocol(ProtocolMessage::RoomError { message, .. }) => {
                anyhow::bail!("room returned error while exporting state: {message}");
            }
            _ => {}
        }
    }
}

async fn measure_recovery(
    store: Arc<HttpUrsula>,
    room_id: &str,
    expected_count: usize,
    expected_hash: &str,
) -> anyhow::Result<serde_json::Value> {
    let baseline_rss = proc_status_bytes("VmRSS:");
    let pre_activation_hwm = proc_status_bytes("VmHWM:");
    let sampling = Arc::new(AtomicBool::new(true));
    let peak_rss = Arc::new(AtomicU64::new(baseline_rss));
    let sampling_task = {
        let sampling = sampling.clone();
        let peak_rss = peak_rss.clone();
        tokio::spawn(async move {
            while sampling.load(Ordering::Relaxed) {
                peak_rss.fetch_max(proc_status_bytes("VmRSS:"), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };

    let started = Instant::now();
    let manager = RoomManager::with_random_boot(store, ActorConfig::default());
    let room = manager.room(room_id);
    let status = room
        .wait_for_activation()
        .await
        .context("room actor closed during activation")?;
    let activation_micros = duration_micros(started.elapsed());
    peak_rss.fetch_max(proc_status_bytes("VmRSS:"), Ordering::Relaxed);
    sampling.store(false, Ordering::Relaxed);
    sampling_task.await?;
    anyhow::ensure!(
        status.state == RoomLifecycle::Ready,
        "room recovery failed: {:?}",
        status.last_error
    );
    let peak_rss = peak_rss.load(Ordering::Relaxed);
    let post_activation_hwm = proc_status_bytes("VmHWM:");

    let snapshot = collect_snapshot(&room).await?;
    let verifier = LoroDoc::new();
    verifier.import(&snapshot)?;
    let text = verifier.get_text("text").to_string();
    anyhow::ensure!(
        text.len() == expected_count,
        "reconstructed text length mismatch"
    );
    anyhow::ensure!(
        text.bytes().all(|byte| byte == b'x'),
        "reconstructed text mismatch"
    );
    let reconstructed_hash = benchmark_state_hash(&text);
    anyhow::ensure!(
        reconstructed_hash == expected_hash,
        "reconstructed state hash mismatch"
    );

    Ok(json!({
        "activation_wall_micros": activation_micros,
        "recovery_total_micros": status.recovery_total_micros,
        "ursula_read_micros": status.recovery_read_micros,
        "frame_decode_micros": status.recovery_decode_micros,
        "loro_import_micros": status.recovery_import_micros,
        "ursula_read_request_count": status.recovery_read_requests,
        "recovered_stream_bytes": status.recovered_stream_bytes,
        "recovered_update_count": status.recovered_update_count,
        "baseline_rss_bytes": baseline_rss,
        "peak_rss_bytes": peak_rss,
        "peak_rss_delta_bytes": peak_rss.saturating_sub(baseline_rss),
        "pre_activation_vm_hwm_bytes": pre_activation_hwm,
        "post_activation_vm_hwm_bytes": post_activation_hwm,
        "reconstructed_state_sha256": reconstructed_hash,
    }))
}

async fn leader_url(stream: &str) -> anyhow::Result<String> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let url = format!("{}/qloro/{stream}", NODE_URLS[0]);
    for _ in 0..200 {
        if let Ok(response) = client
            .put(&url)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/vnd.quorum-loro.delta-frame.v1",
            )
            .send()
            .await
        {
            if response.status().is_success() {
                return Ok(NODE_URLS[0].into());
            }
            if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT
                && let Some(leader_id) = response
                    .headers()
                    .get("x-ursula-raft-leader-id")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<usize>().ok())
                && (1..=NODE_URLS.len()).contains(&leader_id)
            {
                return Ok(NODE_URLS[leader_id - 1].into());
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("could not determine leader for replay stream")
}

#[tokio::test]
#[ignore = "invoked as an isolated worker by full_replay_benchmark"]
async fn full_replay_worker() -> anyhow::Result<()> {
    let Ok(room_id) = env::var("PHASE2_REPLAY_WORKER_ROOM") else {
        return Ok(());
    };
    let leader = env::var("PHASE2_REPLAY_WORKER_LEADER")?;
    let count = env::var("PHASE2_REPLAY_WORKER_COUNT")?.parse::<usize>()?;
    let expected_hash = env::var("PHASE2_REPLAY_WORKER_HASH")?;
    let store = Arc::new(HttpUrsula::new(HttpUrsulaConfig {
        base_url: leader,
        redirect_base_urls: NODE_URLS.iter().map(ToString::to_string).collect(),
        response_timeout: Duration::from_secs(10),
        read_chunk_bytes: READ_WINDOW_BYTES,
        safe_retries: 10,
        ..HttpUrsulaConfig::default()
    })?);
    let result = measure_recovery(store, &room_id, count, &expected_hash).await?;
    println!("{RESULT_MARKER}{result}");
    Ok(())
}

#[tokio::test]
#[ignore = "release-mode benchmark requiring isolated real Phase 2 Ursula clusters"]
async fn full_replay_benchmark() -> anyhow::Result<()> {
    anyhow::ensure!(
        !cfg!(debug_assertions),
        "scale benchmark must use --release"
    );
    let counts = benchmark_counts()?;
    let repetitions = env::var("PHASE2_REPLAY_REPETITIONS")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(DEFAULT_REPETITIONS);
    anyhow::ensure!(repetitions >= 3, "at least three repetitions are required");
    let mut cases = Vec::new();

    for repetition in 1..=repetitions {
        for order_index in 0..counts.len() {
            let count = counts[(order_index + repetition - 1) % counts.len()];
            let _cluster = ClusterGuard::start()?;
            let room_id = format!("phase2-5-replay-{count}");
            let stream_name = delta_stream(&room_id).physical;
            let generation_started = Instant::now();
            let history = build_history(count)?;
            let generation_micros = duration_micros(generation_started.elapsed());
            let store = Arc::new(HttpUrsula::new(HttpUrsulaConfig {
                base_url: NODE_URLS[1].into(),
                redirect_base_urls: NODE_URLS.iter().map(ToString::to_string).collect(),
                response_timeout: Duration::from_secs(30),
                read_chunk_bytes: READ_WINDOW_BYTES,
                safe_retries: 10,
                ..HttpUrsulaConfig::default()
            })?);
            store.ensure_stream(&stream_name).await?;
            let load_started = Instant::now();
            for (sequence, chunk) in history.chunks.iter().enumerate() {
                let loader = ProducerTuple {
                    id: format!("phase2-5-loader-{count}-{repetition}"),
                    epoch: 0,
                    sequence: u64::try_from(sequence)?,
                };
                append_history_chunk(&store, &stream_name, &loader, chunk).await?;
            }
            let load_micros = duration_micros(load_started.elapsed());
            let leader = leader_url(&stream_name).await?;
            let mut result = run_worker(&room_id, &leader, count, &history.expected_state_hash)?;
            let object = result
                .as_object_mut()
                .context("benchmark result is not an object")?;
            object.insert("update_count".into(), json!(count));
            object.insert("repetition".into(), json!(repetition));
            object.insert("order_index".into(), json!(order_index));
            object.insert("stored_bytes".into(), json!(history.stored_bytes));
            object.insert("load_chunk_count".into(), json!(history.chunks.len()));
            object.insert("loro_blob_bytes".into(), json!(history.loro_bytes));
            object.insert(
                "average_loro_blob_bytes".into(),
                json!(history.loro_bytes as f64 / count as f64),
            );
            object.insert(
                "min_loro_blob_bytes".into(),
                json!(history.min_loro_blob_bytes),
            );
            object.insert(
                "max_loro_blob_bytes".into(),
                json!(history.max_loro_blob_bytes),
            );
            object.insert("history_generation_micros".into(), json!(generation_micros));
            object.insert("history_load_micros".into(), json!(load_micros));
            println!("replay case: {result}");
            cases.push(result);
        }
    }

    let summary = counts
        .iter()
        .map(|count| summarize_count(&cases, *count))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let output = json!({
        "environment": environment(),
        "method": {
            "cluster": "fresh three-voter cluster per repetition, disk WAL, local snapshot store, four Raft groups",
            "gateway_process": "fresh release-mode worker process per repetition",
            "storage_load": "deterministic complete QLGD frames appended in frame-aligned chunks",
            "measurement": "production RoomActor activation through HttpUrsula against the stream leader",
            "repetitions": repetitions,
            "order": "count order rotated by one position for each repetition",
            "read_window_bytes": READ_WINDOW_BYTES,
            "load_chunk_target_bytes": LOAD_CHUNK_BYTES,
            "rss_sampling_interval_millis": 1,
            "rss_scope": "isolated gateway worker process only; state export and hash verification occur after sampling",
            "ursula_read_request_count": "actual HTTP GET attempts including retries and redirects",
            "percentile_method": "nearest rank; with three repetitions p95 is the maximum observation",
            "payload_distribution": {
                "document": "one root text named text",
                "operation": "append one ASCII x and commit",
                "operations_per_update": 1,
                "loro_peer_id": 700,
                "frames_per_update": 1,
                "workload_seed": "fixed algorithm; no random input",
            },
            "ursula_configuration": {
                "node_count": 3,
                "raft_group_count": 4,
                "runtime_cores_per_node": 1,
                "wal": "disk",
                "snapshot_store": "local",
                "cold_store": "memory",
                "snapshot_drive_interval": "0s",
            },
        },
        "cases": cases,
        "summary": summary,
    });
    let output_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("results")
        .join("phase2_5")
        .join("scale-replay.json");
    std::fs::create_dir_all(output_path.parent().context("result parent")?)?;
    std::fs::write(&output_path, serde_json::to_vec_pretty(&output)?)?;
    println!("raw results: {}", output_path.display());
    Ok(())
}

async fn append_history_chunk(
    store: &HttpUrsula,
    stream: &str,
    producer: &ProducerTuple,
    chunk: &[u8],
) -> anyhow::Result<()> {
    let mut last_error = None;
    for _ in 0..20 {
        match store.append(stream, producer, chunk).await {
            Ok(AppendOutcome::Committed { .. } | AppendOutcome::Duplicate { .. }) => return Ok(()),
            Err(StoreError::Ambiguous(error)) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::bail!(
        "history append remained ambiguous: {}",
        last_error.as_deref().unwrap_or("no append attempt")
    )
}

fn run_worker(
    room_id: &str,
    leader: &str,
    count: usize,
    expected_hash: &str,
) -> anyhow::Result<serde_json::Value> {
    let output = Command::new(env::current_exe()?)
        .arg("full_replay_worker")
        .arg("--exact")
        .arg("--ignored")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env("PHASE2_REPLAY_WORKER_ROOM", room_id)
        .env("PHASE2_REPLAY_WORKER_LEADER", leader)
        .env("PHASE2_REPLAY_WORKER_COUNT", count.to_string())
        .env("PHASE2_REPLAY_WORKER_HASH", expected_hash)
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "recovery worker failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find_map(|line| {
            line.find(RESULT_MARKER)
                .map(|index| &line[index + RESULT_MARKER.len()..])
        })
        .with_context(|| {
            format!(
                "recovery worker did not emit a result:\nstdout: {stdout}\nstderr: {}",
                String::from_utf8_lossy(&output.stderr)
            )
        })
        .and_then(|value| serde_json::from_str(value).context("invalid recovery worker result"))
}

fn benchmark_counts() -> anyhow::Result<Vec<usize>> {
    let Some(value) = env::var_os("PHASE2_REPLAY_COUNTS") else {
        return Ok(DEFAULT_COUNTS.to_vec());
    };
    let value = value.to_string_lossy();
    let counts = value
        .split(',')
        .map(str::trim)
        .map(str::parse::<usize>)
        .collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(!counts.is_empty(), "at least one replay count is required");
    anyhow::ensure!(
        counts.iter().all(|count| *count > 0),
        "replay counts must be non-zero"
    );
    Ok(counts)
}

fn summarize_count(cases: &[serde_json::Value], count: usize) -> anyhow::Result<serde_json::Value> {
    let matching = cases
        .iter()
        .filter(|case| case["update_count"].as_u64() == Some(count as u64))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        matching.len() >= 3,
        "fewer than three cases for {count} updates"
    );
    let hashes = matching
        .iter()
        .filter_map(|case| case["reconstructed_state_sha256"].as_str())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        hashes.len() == matching.len() && hashes.iter().all(|hash| *hash == hashes[0]),
        "state hashes differ for {count} updates"
    );
    Ok(json!({
        "update_count": count,
        "stored_bytes": matching[0]["stored_bytes"],
        "average_loro_blob_bytes": matching[0]["average_loro_blob_bytes"],
        "median_activation_wall_micros": percentile_field(&matching, "activation_wall_micros", 50),
        "p95_activation_wall_micros": percentile_field(&matching, "activation_wall_micros", 95),
        "median_recovery_total_micros": percentile_field(&matching, "recovery_total_micros", 50),
        "median_ursula_read_micros": percentile_field(&matching, "ursula_read_micros", 50),
        "median_frame_decode_micros": percentile_field(&matching, "frame_decode_micros", 50),
        "median_loro_import_micros": percentile_field(&matching, "loro_import_micros", 50),
        "median_ursula_read_request_count": percentile_field(&matching, "ursula_read_request_count", 50),
        "median_peak_rss_bytes": percentile_field(&matching, "peak_rss_bytes", 50),
        "p95_peak_rss_bytes": percentile_field(&matching, "peak_rss_bytes", 95),
        "median_peak_rss_delta_bytes": percentile_field(&matching, "peak_rss_delta_bytes", 50),
        "reconstructed_state_sha256": hashes[0],
    }))
}

fn percentile_field(cases: &[&serde_json::Value], field: &str, percentile: usize) -> u64 {
    let mut values = cases
        .iter()
        .filter_map(|case| case[field].as_u64())
        .collect::<Vec<_>>();
    values.sort_unstable();
    let rank = percentile.saturating_mul(values.len()).div_ceil(100);
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

fn environment() -> serde_json::Value {
    let cpu_model = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("model name\t:")
                    .map(str::trim)
                    .map(str::to_owned)
            })
        })
        .unwrap_or_default();
    let memory_total_bytes = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("MemTotal:")
                    .and_then(|value| value.split_whitespace().next())
                    .and_then(|value| value.parse::<u64>().ok())
                    .and_then(|kib| kib.checked_mul(1024))
            })
        })
        .unwrap_or(0);
    json!({
        "platform": std::fs::read_to_string("/proc/version").unwrap_or_default().trim(),
        "cpu_model": cpu_model,
        "logical_cpu_count": std::thread::available_parallelism().map(usize::from).unwrap_or(0),
        "memory_total_bytes": memory_total_bytes,
        "profile": "release",
        "rustc": command_output(env!("CARGO_MANIFEST_DIR"), "rustc", &["-Vv"]),
        "ursula_binary": "/home/vik/ursula/target/release/ursula",
        "gateway_revision": command_output(env!("CARGO_MANIFEST_DIR"), "git", &["rev-parse", "HEAD"]),
    })
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn command_output(directory: &str, command: &str, arguments: &[&str]) -> String {
    Command::new(command)
        .current_dir(directory)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default()
}
