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
use quorum_loro_gateway::HttpUrsula;
use quorum_loro_gateway::HttpUrsulaConfig;
use quorum_loro_gateway::RoomLifecycle;
use quorum_loro_gateway::RoomManager;
use quorum_loro_gateway::actor::ActorConfig;
use quorum_loro_gateway::frame::DeltaFrame;
use quorum_loro_gateway::frame::ProducerTuple;
use quorum_loro_gateway::names::delta_stream;
use quorum_loro_gateway::ursula::AppendOutcome;
use quorum_loro_gateway::ursula::UrsulaStore;
use serde_json::json;

const COUNTS: [usize; 3] = [100, 1000, 10_000];
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
        let root = std::env::temp_dir()
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
            let _ = std::fs::remove_dir(parent);
        }
    }
}

fn script(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join(name)
}

fn build_history(count: usize) -> anyhow::Result<(Vec<u8>, usize)> {
    let doc = LoroDoc::new();
    doc.set_peer_id(700)?;
    let text = doc.get_text("text");
    let mut stream = Vec::new();
    let mut loro_bytes = 0_usize;
    for index in 0..count {
        let before = doc.oplog_vv();
        text.insert(text.len_unicode(), "x")?;
        doc.commit();
        let update = doc.export(ExportMode::updates(&before))?;
        loro_bytes = loro_bytes
            .checked_add(update.len())
            .context("Loro byte count overflow")?;
        let frame = DeltaFrame::new(
            ProducerTuple {
                id: "phase2-replay-frame".into(),
                epoch: 0,
                sequence: u64::try_from(index)?,
            },
            BatchId(u64::try_from(index)?.to_be_bytes()),
            vec![update],
        )
        .encode()?;
        stream.extend_from_slice(&frame);
    }
    Ok((stream, loro_bytes))
}

fn rss_bytes() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
                .and_then(|kib| kib.checked_mul(1024))
        })
        .unwrap_or(0)
}

async fn measure_recovery(
    store: Arc<HttpUrsula>,
    room_id: &str,
) -> anyhow::Result<serde_json::Value> {
    let baseline_rss = rss_bytes();
    let sampling = Arc::new(AtomicBool::new(true));
    let peak_rss = Arc::new(AtomicU64::new(baseline_rss));
    let sampling_task = {
        let sampling = sampling.clone();
        let peak_rss = peak_rss.clone();
        tokio::spawn(async move {
            while sampling.load(Ordering::Relaxed) {
                peak_rss.fetch_max(rss_bytes(), Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };

    let started = Instant::now();
    let manager = RoomManager::with_random_boot(store, ActorConfig::default());
    let room = manager.room(room_id);
    let status = loop {
        let status = room.status();
        if status.state != RoomLifecycle::Recovering {
            break status;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    let activation_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    peak_rss.fetch_max(rss_bytes(), Ordering::Relaxed);
    sampling.store(false, Ordering::Relaxed);
    sampling_task.await?;
    anyhow::ensure!(
        status.state == RoomLifecycle::Ready,
        "room recovery failed: {:?}",
        status.last_error
    );
    let peak_rss = peak_rss.load(Ordering::Relaxed);
    drop(room);
    drop(manager);

    Ok(json!({
        "activation_wall_micros": activation_micros,
        "recovery_total_micros": status.recovery_total_micros,
        "loro_import_micros": status.recovery_import_micros,
        "recovered_stream_bytes": status.recovered_stream_bytes,
        "recovered_update_count": status.recovered_update_count,
        "baseline_rss_bytes": baseline_rss,
        "peak_rss_bytes": peak_rss,
        "peak_rss_delta_bytes": peak_rss.saturating_sub(baseline_rss),
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
#[ignore = "release-mode benchmark requiring the real Phase 2 Ursula cluster binary"]
async fn full_replay_benchmark() -> anyhow::Result<()> {
    let _cluster = ClusterGuard::start()?;
    let store = Arc::new(HttpUrsula::new(HttpUrsulaConfig {
        base_url: NODE_URLS[1].into(),
        redirect_base_urls: NODE_URLS.iter().map(ToString::to_string).collect(),
        response_timeout: Duration::from_secs(10),
        read_chunk_bytes: 64 * 1024,
        safe_retries: 10,
        ..HttpUrsulaConfig::default()
    })?);
    let mut cases = Vec::new();

    for count in COUNTS {
        for repetition in 1..=3 {
            let room_id = format!("phase2-replay-{count}-{}", uuid::Uuid::new_v4());
            let stream_name = delta_stream(&room_id).physical;
            let generation_started = Instant::now();
            let (stream_bytes, loro_bytes) = build_history(count)?;
            let generation_micros =
                u64::try_from(generation_started.elapsed().as_micros()).unwrap_or(u64::MAX);
            store.ensure_stream(&stream_name).await?;
            let loader = ProducerTuple {
                id: format!("phase2-replay-loader-{count}-{repetition}"),
                epoch: 0,
                sequence: 0,
            };
            anyhow::ensure!(
                matches!(
                    store.append(&stream_name, &loader, &stream_bytes).await?,
                    AppendOutcome::Committed { .. }
                ),
                "benchmark history append was unexpectedly deduplicated"
            );
            let stored_bytes = stream_bytes.len();
            drop(stream_bytes);
            tokio::time::sleep(Duration::from_millis(100)).await;
            let recovery_store = Arc::new(HttpUrsula::new(HttpUrsulaConfig {
                base_url: leader_url(&stream_name).await?,
                redirect_base_urls: NODE_URLS.iter().map(ToString::to_string).collect(),
                response_timeout: Duration::from_secs(10),
                read_chunk_bytes: 64 * 1024,
                safe_retries: 10,
                ..HttpUrsulaConfig::default()
            })?);
            let mut result = measure_recovery(recovery_store, &room_id).await?;
            let object = result
                .as_object_mut()
                .context("benchmark result is not an object")?;
            object.insert("update_count".into(), json!(count));
            object.insert("repetition".into(), json!(repetition));
            object.insert("stored_bytes".into(), json!(stored_bytes));
            object.insert("loro_blob_bytes".into(), json!(loro_bytes));
            object.insert(
                "average_loro_blob_bytes".into(),
                json!(loro_bytes as f64 / count as f64),
            );
            object.insert("history_generation_micros".into(), json!(generation_micros));
            println!("replay case: {result}");
            cases.push(result);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let summary = COUNTS
        .iter()
        .map(|count| {
            let matching = cases
                .iter()
                .filter(|case| case["update_count"].as_u64() == Some(*count as u64))
                .collect::<Vec<_>>();
            json!({
                "update_count": count,
                "stored_bytes": matching[0]["stored_bytes"],
                "average_loro_blob_bytes": matching[0]["average_loro_blob_bytes"],
                "median_activation_wall_micros": median_field(&matching, "activation_wall_micros"),
                "median_recovery_total_micros": median_field(&matching, "recovery_total_micros"),
                "median_loro_import_micros": median_field(&matching, "loro_import_micros"),
                "max_observed_peak_rss_delta_bytes": max_field(&matching, "peak_rss_delta_bytes"),
                "rss_interpretation": "Process allocator state is retained across repetitions; report the maximum observed delta, not a per-room heap estimate.",
            })
        })
        .collect::<Vec<_>>();

    let output = json!({
        "environment": {
            "platform": std::fs::read_to_string("/proc/version").unwrap_or_default().trim(),
            "profile": "release",
            "ursula_binary": "/home/vik/ursula/target/release/ursula",
            "ursula_revision": command_output("/home/vik/ursula", &["rev-parse", "HEAD"]),
            "gateway_revision": command_output(env!("CARGO_MANIFEST_DIR"), &["rev-parse", "HEAD"]),
        },
        "method": {
            "cluster": "three voters, disk WAL, local snapshot store",
            "storage_load": "all encoded frames appended to delta/0 in one Ursula command",
            "measurement": "production RoomActor activation through HttpUrsula against the stream leader",
            "rss_sampling_interval_millis": 1,
        },
        "cases": cases,
        "summary": summary,
    });
    let output_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("results")
        .join("phase2")
        .join("full-replay.json");
    std::fs::create_dir_all(output_path.parent().context("result parent")?)?;
    std::fs::write(&output_path, serde_json::to_vec_pretty(&output)?)?;
    println!("raw results: {}", output_path.display());
    Ok(())
}

fn median_field(cases: &[&serde_json::Value], field: &str) -> u64 {
    let mut values = cases
        .iter()
        .filter_map(|case| case[field].as_u64())
        .collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

fn max_field(cases: &[&serde_json::Value], field: &str) -> u64 {
    cases
        .iter()
        .filter_map(|case| case[field].as_u64())
        .max()
        .unwrap_or(0)
}

fn command_output(directory: &str, arguments: &[&str]) -> String {
    Command::new("git")
        .current_dir(directory)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default()
}
