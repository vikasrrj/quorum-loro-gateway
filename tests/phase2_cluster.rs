use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::response::Response;
use axum::routing::any;
use futures_util::SinkExt;
use futures_util::StreamExt;
use loro::ExportMode;
use loro::LoroDoc;
use loro_protocol::BatchId;
use loro_protocol::CrdtType;
use loro_protocol::ProtocolMessage;
use loro_protocol::UpdateStatusCode;
use quorum_loro_gateway::frame::decode_all;
use quorum_loro_gateway::names::delta_stream;
use tokio::sync::Notify;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const NODE_URLS: [&str; 3] = [
    "http://127.0.0.1:18101",
    "http://127.0.0.1:18102",
    "http://127.0.0.1:18103",
];

#[derive(Clone)]
struct CommitHoldingProxy {
    backend: Arc<Mutex<String>>,
    client: reqwest::Client,
    hold_next_append: Arc<AtomicBool>,
    committed: Arc<Notify>,
    release: Arc<Notify>,
    duplicate_seen: Arc<AtomicBool>,
}

async fn proxy_request(
    State(proxy): State<CommitHoldingProxy>,
    method: Method,
    uri: Uri,
    mut headers: HeaderMap,
    body: Bytes,
) -> Response {
    let backend = proxy.backend.lock().expect("proxy backend lock").clone();
    let target = format!(
        "{backend}{}",
        uri.path_and_query()
            .map(|value| value.as_str())
            .unwrap_or(uri.path())
    );
    headers.remove(reqwest::header::HOST);
    let upstream = match proxy
        .client
        .request(method.clone(), target)
        .headers(headers)
        .body(body)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(axum::body::Body::from(error.to_string()))
                .expect("proxy transport error response");
        }
    };
    let status = upstream.status();
    if status == reqwest::StatusCode::NO_CONTENT {
        proxy.duplicate_seen.store(true, Ordering::SeqCst);
    }
    if method == Method::POST
        && status == reqwest::StatusCode::OK
        && proxy.hold_next_append.swap(false, Ordering::SeqCst)
    {
        proxy.committed.notify_waiters();
        proxy.release.notified().await;
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(axum::body::Body::empty())
            .expect("proxy ambiguous response");
    }
    let response_headers = upstream.headers().clone();
    let response_body = match upstream.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            return Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(axum::body::Body::from(error.to_string()))
                .expect("proxy body error response");
        }
    };
    let mut response = Response::builder().status(status);
    for (name, value) in response_headers {
        if let Some(name) = name {
            response = response.header(name, value);
        }
    }
    response
        .body(axum::body::Body::from(response_body))
        .expect("proxy upstream response")
}

struct ClusterGuard {
    root: PathBuf,
}

impl ClusterGuard {
    fn start() -> anyhow::Result<Self> {
        let root = std::env::temp_dir()
            .join(format!("qlg-phase2-{}", uuid::Uuid::new_v4()))
            .join("phase2-cluster");
        let output = Command::new(script("phase2-cluster-start.sh"))
            .env("PHASE2_CLUSTER_ROOT", &root)
            .output()
            .context("start three-node Ursula cluster")?;
        if !output.status.success() {
            anyhow::bail!(
                "cluster start failed:\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(Self { root })
    }

    fn kill_node(&self, node_id: u8) -> anyhow::Result<()> {
        let pid =
            std::fs::read_to_string(self.root.join("pids").join(format!("node-{node_id}.pid")))?;
        let status = Command::new("kill").arg("-KILL").arg(pid.trim()).status()?;
        anyhow::ensure!(status.success(), "failed to kill Ursula node {node_id}");
        Ok(())
    }
}

impl Drop for ClusterGuard {
    fn drop(&mut self) {
        for node_id in 1..=3 {
            let pid_path = self.root.join("pids").join(format!("node-{node_id}.pid"));
            if let Ok(pid) = std::fs::read_to_string(pid_path) {
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

struct GatewayGuard {
    child: Child,
}

impl GatewayGuard {
    fn spawn(listen: std::net::SocketAddr) -> anyhow::Result<Self> {
        Self::spawn_with_base(listen, NODE_URLS[1])
    }

    fn spawn_with_base(listen: std::net::SocketAddr, base_url: &str) -> anyhow::Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_quorum-loro-gateway"));
        command
            .arg("--listen")
            .arg(listen.to_string())
            .arg("--ursula-url")
            .arg(base_url)
            .arg("--ursula-peer-url")
            .arg(NODE_URLS[0])
            .arg("--ursula-peer-url")
            .arg(NODE_URLS[1])
            .arg("--ursula-peer-url")
            .arg(NODE_URLS[2])
            .arg("--ursula-timeout-seconds")
            .arg("2")
            .arg("--ambiguous-retries")
            .arg("120")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        Ok(Self {
            child: command.spawn().context("spawn gateway")?,
        })
    }

    fn kill_and_wait(&mut self) -> anyhow::Result<()> {
        self.child.kill().context("kill gateway")?;
        let _ = self.child.wait().context("wait for killed gateway")?;
        Ok(())
    }
}

impl Drop for GatewayGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn script(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join(name)
}

fn reserve_address() -> anyhow::Result<std::net::SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_when_ready(url: &str) -> anyhow::Result<Socket> {
    for _ in 0..200 {
        if let Ok((socket, _)) = connect_async(url).await {
            return Ok(socket);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("gateway did not start at {url}")
}

async fn send_protocol(socket: &mut Socket, message: &ProtocolMessage) -> anyhow::Result<()> {
    let encoded = loro_protocol::encode(message).map_err(anyhow::Error::msg)?;
    socket.send(Message::Binary(encoded.into())).await?;
    Ok(())
}

async fn next_protocol(socket: &mut Socket) -> anyhow::Result<ProtocolMessage> {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(10), socket.next())
            .await
            .context("WebSocket receive timeout")?
            .context("WebSocket closed")??;
        if let Message::Binary(bytes) = message {
            return loro_protocol::decode(&bytes).map_err(anyhow::Error::msg);
        }
    }
}

async fn join(socket: &mut Socket, room_id: &str) -> anyhow::Result<()> {
    send_protocol(
        socket,
        &ProtocolMessage::JoinRequest {
            crdt: CrdtType::Loro,
            room_id: room_id.into(),
            auth: Vec::new(),
            version: Vec::new(),
        },
    )
    .await?;
    loop {
        match next_protocol(socket).await? {
            ProtocolMessage::JoinResponseOk { .. } => return Ok(()),
            ProtocolMessage::JoinError { message, .. } => {
                anyhow::bail!("gateway join failed: {message}")
            }
            _ => {}
        }
    }
}

fn update_blob() -> Vec<u8> {
    update_blob_with("survives-one-voter-failure", 600)
}

fn update_blob_with(text: &str, peer_id: u64) -> Vec<u8> {
    let doc = LoroDoc::new();
    doc.set_peer_id(peer_id).expect("set cluster-test peer ID");
    doc.get_text("text")
        .insert(0, text)
        .expect("insert cluster-test text");
    doc.commit();
    doc.export(ExportMode::all_updates())
        .expect("export cluster-test update")
}

async fn submit_update(
    socket: &mut Socket,
    room_id: &str,
    batch_id: BatchId,
    update: Vec<u8>,
) -> anyhow::Result<UpdateStatusCode> {
    send_protocol(
        socket,
        &ProtocolMessage::DocUpdate {
            crdt: CrdtType::Loro,
            room_id: room_id.into(),
            updates: vec![update],
            batch_id,
        },
    )
    .await?;
    loop {
        if let ProtocolMessage::Ack { status, ref_id, .. } = next_protocol(socket).await?
            && ref_id == batch_id
        {
            return Ok(status);
        }
    }
}

async fn wait_for_surviving_leaders(surviving_node_ids: &[u64]) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    for _ in 0..240 {
        let mut ready = true;
        for node_id in surviving_node_ids {
            let url = format!("http://127.0.0.1:{}/__ursula/metrics", 18200 + node_id);
            let report = match client.get(url).send().await {
                Ok(response) if response.status().is_success() => {
                    serde_json::from_str::<serde_json::Value>(&response.text().await?)?
                }
                _ => {
                    ready = false;
                    break;
                }
            };
            let Some(groups) = report.get("raft_groups").and_then(|value| value.as_array()) else {
                ready = false;
                break;
            };
            if groups.len() != 4
                || groups.iter().any(|group| {
                    group
                        .get("current_leader")
                        .and_then(|value| value.as_u64())
                        .is_none_or(|leader| !surviving_node_ids.contains(&leader))
                })
            {
                ready = false;
                break;
            }
        }
        if ready {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("surviving Ursula voters did not elect leaders")
}

async fn leader_for_stream(stream: &str) -> anyhow::Result<u8> {
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
                return Ok(1);
            }
            if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT
                && let Some(leader) = response
                    .headers()
                    .get("x-ursula-raft-leader-id")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u8>().ok())
            {
                return Ok(leader);
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("could not determine leader for stream {stream}")
}

#[tokio::test]
#[ignore = "requires /home/vik/ursula/target/release/ursula and free cluster-test ports"]
async fn acknowledged_update_survives_one_voter_and_gateway_crash() -> anyhow::Result<()> {
    let _cluster = ClusterGuard::start()?;
    let listen = reserve_address()?;
    let url = format!("ws://{listen}/ws");
    let mut gateway = GatewayGuard::spawn(listen)?;
    let mut socket = connect_when_ready(&url).await?;
    let room_id = format!("phase2-quorum-{}", uuid::Uuid::new_v4());
    join(&mut socket, &room_id).await?;

    send_protocol(
        &mut socket,
        &ProtocolMessage::DocUpdate {
            crdt: CrdtType::Loro,
            room_id: room_id.clone(),
            updates: vec![update_blob()],
            batch_id: BatchId([60; 8]),
        },
    )
    .await?;
    loop {
        if let ProtocolMessage::Ack { status, .. } = next_protocol(&mut socket).await? {
            anyhow::ensure!(
                status == UpdateStatusCode::Ok,
                "update was not acknowledged"
            );
            break;
        }
    }

    _cluster.kill_node(3)?;
    gateway.kill_and_wait()?;
    drop(socket);

    let mut restarted_gateway = GatewayGuard::spawn(listen)?;
    let mut restarted = connect_when_ready(&url).await?;
    join(&mut restarted, &room_id).await?;
    let updates = loop {
        if let ProtocolMessage::DocUpdate { updates, .. } = next_protocol(&mut restarted).await? {
            break updates;
        }
    };
    let recovered = LoroDoc::new();
    recovered.import_batch(&updates)?;
    anyhow::ensure!(
        recovered.get_text("text").to_string() == "survives-one-voter-failure",
        "acknowledged update was not reconstructed after voter and gateway failure"
    );

    restarted_gateway.kill_and_wait()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires /home/vik/ursula/target/release/ursula and free cluster-test ports"]
async fn one_voter_allows_writes_but_quorum_loss_never_acks() -> anyhow::Result<()> {
    let cluster = ClusterGuard::start()?;
    let listen = reserve_address()?;
    let url = format!("ws://{listen}/ws");
    let mut gateway = GatewayGuard::spawn(listen)?;
    let mut socket = connect_when_ready(&url).await?;
    let room_id = format!("phase2-minority-{}", uuid::Uuid::new_v4());
    join(&mut socket, &room_id).await?;

    anyhow::ensure!(
        submit_update(
            &mut socket,
            &room_id,
            BatchId([61; 8]),
            update_blob_with("before-failure", 601),
        )
        .await?
            == UpdateStatusCode::Ok,
        "baseline write was not acknowledged"
    );

    cluster.kill_node(3)?;
    wait_for_surviving_leaders(&[1, 2]).await?;
    anyhow::ensure!(
        submit_update(
            &mut socket,
            &room_id,
            BatchId([62; 8]),
            update_blob_with("one-voter-down", 602),
        )
        .await?
            == UpdateStatusCode::Ok,
        "two-voter quorum did not acknowledge a proven write"
    );

    cluster.kill_node(1)?;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let quorum_loss_status = submit_update(
        &mut socket,
        &room_id,
        BatchId([63; 8]),
        update_blob_with("no-quorum", 603),
    )
    .await?;
    println!("quorum-loss append status: {quorum_loss_status:?}");
    anyhow::ensure!(
        quorum_loss_status != UpdateStatusCode::Ok,
        "quorum loss was converted into Ack(Ok)"
    );
    anyhow::ensure!(
        submit_update(
            &mut socket,
            &room_id,
            BatchId([64; 8]),
            update_blob_with("must-remain-blocked", 604),
        )
        .await?
            == UpdateStatusCode::RateLimited,
        "gateway advanced the producer after an unresolved quorum-loss append"
    );

    let stream = delta_stream(&room_id).physical;
    let response = reqwest::Client::new()
        .get(format!(
            "{}/qloro/{stream}?offset=0&max_bytes=1048576",
            NODE_URLS[1]
        ))
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "sole surviving voter did not serve a finite read: {}",
        response.status()
    );
    let claims_up_to_date = response
        .headers()
        .get("stream-up-to-date")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let body = response.bytes().await?;
    println!(
        "minority finite read: bytes={} stream_up_to_date={claims_up_to_date}",
        body.len()
    );
    anyhow::ensure!(
        !body.is_empty(),
        "sole survivor returned no committed history"
    );
    anyhow::ensure!(
        !claims_up_to_date,
        "minority follower incorrectly claimed its finite read was up to date"
    );

    gateway.kill_and_wait()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires /home/vik/ursula/target/release/ursula and free cluster-test ports"]
async fn leader_failure_before_append_never_advances_unresolved_sequence() -> anyhow::Result<()> {
    let cluster = ClusterGuard::start()?;
    let listen = reserve_address()?;
    let url = format!("ws://{listen}/ws");
    let mut gateway = GatewayGuard::spawn(listen)?;
    let mut socket = connect_when_ready(&url).await?;
    let room_id = format!("phase2-before-leader-{}", uuid::Uuid::new_v4());
    join(&mut socket, &room_id).await?;
    let stream = delta_stream(&room_id).physical;
    let leader = leader_for_stream(&stream).await?;

    cluster.kill_node(leader)?;
    let status = submit_update(
        &mut socket,
        &room_id,
        BatchId([65; 8]),
        update_blob_with("leader-stopped-before-append", 605),
    )
    .await?;
    println!("pre-request leader-failure append status: {status:?}");
    anyhow::ensure!(
        matches!(status, UpdateStatusCode::Ok | UpdateStatusCode::Unknown),
        "unexpected status after pre-request leader failure: {status:?}"
    );
    if status == UpdateStatusCode::Unknown {
        anyhow::ensure!(
            submit_update(
                &mut socket,
                &room_id,
                BatchId([66; 8]),
                update_blob_with("must-not-advance", 606),
            )
            .await?
                == UpdateStatusCode::RateLimited,
            "gateway advanced Producer-Seq after unresolved leader failure"
        );
    }

    gateway.kill_and_wait()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires /home/vik/ursula/target/release/ursula and free cluster-test ports"]
async fn acknowledged_update_survives_current_leader_failure() -> anyhow::Result<()> {
    let cluster = ClusterGuard::start()?;
    let listen = reserve_address()?;
    let url = format!("ws://{listen}/ws");
    let mut gateway = GatewayGuard::spawn(listen)?;
    let mut socket = connect_when_ready(&url).await?;
    let room_id = format!("phase2-after-ack-{}", uuid::Uuid::new_v4());
    join(&mut socket, &room_id).await?;
    anyhow::ensure!(
        submit_update(
            &mut socket,
            &room_id,
            BatchId([67; 8]),
            update_blob_with("acked-before-leader-failure", 607),
        )
        .await?
            == UpdateStatusCode::Ok,
        "baseline update was not acknowledged"
    );
    let stream = delta_stream(&room_id).physical;
    let leader = leader_for_stream(&stream).await?;
    cluster.kill_node(leader)?;
    let survivors = [1_u64, 2, 3]
        .into_iter()
        .filter(|node_id| *node_id != u64::from(leader))
        .collect::<Vec<_>>();
    wait_for_surviving_leaders(&survivors).await?;
    gateway.kill_and_wait()?;
    drop(socket);

    let base_url = NODE_URLS[usize::try_from(survivors[0] - 1)?];
    let mut restarted_gateway = GatewayGuard::spawn_with_base(listen, base_url)?;
    let mut restarted = connect_when_ready(&url).await?;
    join(&mut restarted, &room_id).await?;
    let updates = loop {
        if let ProtocolMessage::DocUpdate { updates, .. } = next_protocol(&mut restarted).await? {
            break updates;
        }
    };
    let recovered = LoroDoc::new();
    recovered.import_batch(&updates)?;
    anyhow::ensure!(
        recovered.get_text("text").to_string() == "acked-before-leader-failure",
        "acknowledged update was lost with the former leader"
    );

    restarted_gateway.kill_and_wait()?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires /home/vik/ursula/target/release/ursula and free cluster-test ports"]
async fn leader_failure_after_commit_before_response_requires_verified_duplicate()
-> anyhow::Result<()> {
    let cluster = ClusterGuard::start()?;
    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let proxy_address = proxy_listener.local_addr()?;
    let proxy = CommitHoldingProxy {
        backend: Arc::new(Mutex::new(NODE_URLS[0].to_owned())),
        client: reqwest::Client::new(),
        hold_next_append: Arc::new(AtomicBool::new(true)),
        committed: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        duplicate_seen: Arc::new(AtomicBool::new(false)),
    };
    let proxy_server = tokio::spawn(
        axum::serve(
            proxy_listener,
            Router::new()
                .fallback(any(proxy_request))
                .with_state(proxy.clone()),
        )
        .into_future(),
    );

    let listen = reserve_address()?;
    let url = format!("ws://{listen}/ws");
    let mut gateway = GatewayGuard::spawn_with_base(listen, &format!("http://{proxy_address}"))?;
    let mut socket = connect_when_ready(&url).await?;
    let room_id = format!("phase2-held-response-{}", uuid::Uuid::new_v4());
    join(&mut socket, &room_id).await?;
    let stream = delta_stream(&room_id).physical;
    let leader = leader_for_stream(&stream).await?;
    let survivors = [1_u64, 2, 3]
        .into_iter()
        .filter(|node_id| *node_id != u64::from(leader))
        .collect::<Vec<_>>();
    let survivor_url = NODE_URLS[usize::try_from(survivors[0] - 1)?].to_owned();

    let committed = proxy.committed.notified();
    let submit_room = room_id.clone();
    let submit = tokio::spawn(async move {
        let status = submit_update(
            &mut socket,
            &submit_room,
            BatchId([68; 8]),
            update_blob_with("committed-response-withheld", 608),
        )
        .await;
        (socket, status)
    });
    tokio::time::timeout(Duration::from_secs(10), committed)
        .await
        .context("proxy did not observe committed append")?;

    cluster.kill_node(leader)?;
    *proxy.backend.lock().expect("proxy backend lock") = survivor_url.clone();
    wait_for_surviving_leaders(&survivors).await?;
    proxy.release.notify_one();
    let (returned_socket, status) = submit.await?;
    socket = returned_socket;
    anyhow::ensure!(
        status? == UpdateStatusCode::Ok,
        "gateway did not acknowledge the verified duplicate"
    );
    anyhow::ensure!(
        proxy.duplicate_seen.load(Ordering::SeqCst),
        "retry did not receive Ursula's duplicate response"
    );
    println!("post-commit withheld response resolved through verified duplicate");

    let response = reqwest::get(format!(
        "{survivor_url}/qloro/{stream}?offset=0&max_bytes=1048576"
    ))
    .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "failed to read committed stream"
    );
    let frames = decode_all(&response.bytes().await?)?;
    anyhow::ensure!(
        frames.len() == 1,
        "ambiguous retry appended more than one frame"
    );

    drop(socket);
    gateway.kill_and_wait()?;
    proxy_server.abort();
    Ok(())
}
