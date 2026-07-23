#![cfg(feature = "crash-injection")]

use std::collections::HashMap;
use std::process::Child;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::put;
use futures_util::SinkExt;
use futures_util::StreamExt;
use loro::ExportMode;
use loro::LoroDoc;
use loro_protocol::BatchId;
use loro_protocol::CrdtType;
use loro_protocol::ProtocolMessage;
use loro_protocol::UpdateStatusCode;
use serde::Deserialize;
use tokio::sync::Notify;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Default)]
struct FakeUrsula {
    state: Arc<Mutex<FakeState>>,
    committed: Arc<Notify>,
}

#[derive(Default)]
struct FakeState {
    streams: HashMap<String, Vec<u8>>,
    producers: HashMap<(String, String), (u64, u64)>,
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Deserialize)]
struct ReadQuery {
    offset: u64,
    max_bytes: usize,
}

async fn create_bucket() -> StatusCode {
    StatusCode::OK
}

async fn create_stream(
    State(fake): State<FakeUrsula>,
    Path((_bucket, stream)): Path<(String, String)>,
) -> StatusCode {
    fake.state
        .lock()
        .expect("fake Ursula lock")
        .streams
        .entry(stream)
        .or_default();
    StatusCode::CREATED
}

async fn append(
    State(fake): State<FakeUrsula>,
    Path((_bucket, stream)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let producer_id = header(&headers, "producer-id");
    let sequence = header(&headers, "producer-seq")
        .parse::<u64>()
        .expect("producer sequence");
    let mut state = fake.state.lock().expect("fake Ursula lock");
    if let Some((saved_sequence, next_offset)) =
        state.producers.get(&(stream.clone(), producer_id.clone()))
        && sequence <= *saved_sequence
    {
        return response(StatusCode::NO_CONTENT, *next_offset, false, Vec::new());
    }
    let bytes = state.streams.entry(stream.clone()).or_default();
    bytes.extend_from_slice(&body);
    let next_offset = bytes.len() as u64;
    state
        .producers
        .insert((stream, producer_id), (sequence, next_offset));
    drop(state);
    fake.committed.notify_waiters();
    response(StatusCode::OK, next_offset, false, Vec::new())
}

async fn read_stream(
    State(fake): State<FakeUrsula>,
    Path((_bucket, stream)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Response {
    let state = fake.state.lock().expect("fake Ursula lock");
    let bytes = state.streams.get(&stream).map(Vec::as_slice).unwrap_or(&[]);
    let start = usize::try_from(query.offset)
        .unwrap_or(usize::MAX)
        .min(bytes.len());
    let end = start.saturating_add(query.max_bytes).min(bytes.len());
    response(
        StatusCode::OK,
        end as u64,
        end == bytes.len(),
        bytes[start..end].to_vec(),
    )
}

fn response(status: StatusCode, next_offset: u64, up_to_date: bool, body: Vec<u8>) -> Response {
    Response::builder()
        .status(status)
        .header("stream-next-offset", next_offset.to_string())
        .header(
            "stream-up-to-date",
            if up_to_date { "true" } else { "false" },
        )
        .body(axum::body::Body::from(body))
        .expect("fake Ursula response")
}

fn header(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .expect("required producer header")
        .to_owned()
}

fn spawn_gateway(
    listen: std::net::SocketAddr,
    ursula: std::net::SocketAddr,
    crash: bool,
) -> ChildGuard {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_quorum-loro-gateway"));
    command
        .arg("--listen")
        .arg(listen.to_string())
        .arg("--ursula-url")
        .arg(format!("http://{ursula}"))
        .arg("--ambiguous-retries")
        .arg("0")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if crash {
        command.env("QLG_TEST_CRASH_AFTER_COMMIT", "1");
    } else {
        command.env_remove("QLG_TEST_CRASH_AFTER_COMMIT");
    }
    ChildGuard {
        child: command.spawn().expect("spawn gateway child"),
    }
}

fn reserve_address() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve gateway address");
    let address = listener.local_addr().expect("reserved gateway address");
    drop(listener);
    address
}

async fn connect_when_ready(
    url: &str,
) -> anyhow::Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    for _ in 0..100 {
        if let Ok((socket, _)) = connect_async(url).await {
            return Ok(socket);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    anyhow::bail!("gateway did not start")
}

async fn send_protocol(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    message: &ProtocolMessage,
) -> anyhow::Result<()> {
    let encoded = loro_protocol::encode(message).map_err(anyhow::Error::msg)?;
    socket.send(Message::Binary(encoded.into())).await?;
    Ok(())
}

async fn next_protocol(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> anyhow::Result<ProtocolMessage> {
    loop {
        let message = socket
            .next()
            .await
            .context("WebSocket closed")?
            .context("WebSocket receive")?;
        if let Message::Binary(bytes) = message {
            return loro_protocol::decode(&bytes).map_err(anyhow::Error::msg);
        }
    }
}

fn test_update() -> Vec<u8> {
    let doc = LoroDoc::new();
    doc.set_peer_id(500).expect("set crash test peer");
    doc.get_text("text")
        .insert(0, "survived-process-crash")
        .expect("insert crash test text");
    doc.commit();
    doc.export(ExportMode::all_updates())
        .expect("export crash test update")
}

#[tokio::test]
#[ignore = "aborts a feature-gated gateway child after Ursula commit"]
async fn process_crash_after_commit_recovers_without_success_ack() -> anyhow::Result<()> {
    let fake = FakeUrsula::default();
    let ursula_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let ursula_address = ursula_listener.local_addr()?;
    let ursula_router = Router::new()
        .route("/{bucket}", put(create_bucket))
        .route(
            "/{bucket}/{stream}",
            put(create_stream).post(append).get(read_stream),
        )
        .with_state(fake.clone());
    let ursula_server = tokio::spawn(axum::serve(ursula_listener, ursula_router).into_future());

    let gateway_address = reserve_address();
    let mut crashed_child = spawn_gateway(gateway_address, ursula_address, true);
    let url = format!("ws://{gateway_address}/ws");
    let mut socket = connect_when_ready(&url).await?;
    let room_id = "process-crash".to_owned();
    send_protocol(
        &mut socket,
        &ProtocolMessage::JoinRequest {
            crdt: CrdtType::Loro,
            room_id: room_id.clone(),
            auth: Vec::new(),
            version: Vec::new(),
        },
    )
    .await?;
    while !matches!(
        next_protocol(&mut socket).await?,
        ProtocolMessage::JoinResponseOk { .. }
    ) {}

    let committed = fake.committed.notified();
    send_protocol(
        &mut socket,
        &ProtocolMessage::DocUpdate {
            crdt: CrdtType::Loro,
            room_id: room_id.clone(),
            updates: vec![test_update()],
            batch_id: BatchId([50; 8]),
        },
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(5), committed).await?;

    let exit = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = crashed_child.try_wait().expect("poll crashed child") {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    if exit.success() {
        anyhow::bail!("crash-injected child exited successfully")
    }
    if let Ok(Ok(message)) =
        tokio::time::timeout(Duration::from_millis(200), next_protocol(&mut socket)).await
        && matches!(
            message,
            ProtocolMessage::Ack {
                status: UpdateStatusCode::Ok,
                ..
            }
        )
    {
        anyhow::bail!("success ACK escaped before process crash")
    }

    let restarted_child = spawn_gateway(gateway_address, ursula_address, false);
    let mut restarted = connect_when_ready(&url).await?;
    send_protocol(
        &mut restarted,
        &ProtocolMessage::JoinRequest {
            crdt: CrdtType::Loro,
            room_id,
            auth: Vec::new(),
            version: Vec::new(),
        },
    )
    .await?;
    let mut recovered = None;
    for _ in 0..3 {
        if let ProtocolMessage::DocUpdate { updates, .. } = next_protocol(&mut restarted).await? {
            recovered = Some(updates);
            break;
        }
    }
    let doc = LoroDoc::new();
    doc.import_batch(&recovered.context("missing recovered update")?)?;
    if doc.get_text("text").to_string() != "survived-process-crash" {
        anyhow::bail!("restarted gateway did not recover the committed update")
    }

    drop(restarted_child);
    ursula_server.abort();
    Ok(())
}
