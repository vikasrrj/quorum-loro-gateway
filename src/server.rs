use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::extract::ws::WebSocketUpgrade;
use axum::response::Response;
use axum::routing::get;
use futures_util::SinkExt;
use futures_util::StreamExt;
use loro_protocol::BatchId;
use loro_protocol::CrdtType;
use loro_protocol::JoinErrorCode;
use loro_protocol::ProtocolMessage;
use loro_protocol::UpdateStatusCode;
use loro_protocol::encode;
use tokio::sync::mpsc;
use tracing::warn;

use crate::actor::Outbound;
use crate::actor::PeerSender;
use crate::actor::RoomManager;
use crate::protocol::ProtocolLimits;
use crate::protocol::decode_bounded;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub protocol_limits: ProtocolLimits,
    pub max_fragment_batches: usize,
    pub max_fragments_per_batch: u64,
    pub max_reassembled_bytes: u64,
    pub max_fragment_bytes_per_connection: u64,
    pub fragment_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            protocol_limits: ProtocolLimits::default(),
            max_fragment_batches: 8,
            max_fragments_per_batch: 4096,
            max_reassembled_bytes: 32 * 1024 * 1024,
            max_fragment_bytes_per_connection: 64 * 1024 * 1024,
            fragment_timeout: Duration::from_secs(10),
        }
    }
}

static CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct AppState {
    rooms: RoomManager,
    config: ServerConfig,
}

pub fn app(rooms: RoomManager) -> Router {
    app_with_config(rooms, ServerConfig::default())
}

pub fn app_with_config(rooms: RoomManager, config: ServerConfig) -> Router {
    Router::new()
        .route("/", get(upgrade))
        .route("/ws", get(upgrade))
        .route("/healthz", get(health))
        .route("/debug/rooms", get(debug_rooms))
        .with_state(AppState { rooms, config })
}

async fn health() -> &'static str {
    "ok"
}

async fn debug_rooms(State(state): State<AppState>) -> Json<Vec<crate::actor::RoomStatus>> {
    Json(state.rooms.room_statuses())
}

async fn upgrade(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    let wire_limit = state
        .config
        .protocol_limits
        .max_message_bytes
        .min(loro_protocol::MAX_MESSAGE_SIZE);
    ws.max_message_size(wire_limit)
        .max_frame_size(wire_limit)
        .on_upgrade(move |socket| connection(socket, state.rooms, state.config))
}

async fn connection(socket: WebSocket, rooms: RoomManager, config: ServerConfig) {
    let connection_id = CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let (mut sink, mut source) = socket.split();
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<Outbound>();
    let writer = tokio::spawn(async move {
        while let Some(outbound) = outbound_rx.recv().await {
            let message = match outbound {
                Outbound::Protocol(protocol) => match encode(&protocol) {
                    Ok(bytes) => Message::Binary(bytes.into()),
                    Err(error) => {
                        warn!(%error, "failed to encode outbound protocol message");
                        continue;
                    }
                },
                Outbound::Text(text) => Message::Text(text.into()),
            };
            if sink.send(message).await.is_err() {
                break;
            }
        }
    });

    let mut joined = HashSet::new();
    let mut fragments = HashMap::<FragmentKey, FragmentBatch>::new();
    let fragment_poll_interval = config
        .fragment_timeout
        .min(Duration::from_secs(1))
        .max(Duration::from_millis(1));
    loop {
        expire_fragments(&mut fragments, &outbound_tx);
        let result = match tokio::time::timeout(fragment_poll_interval, source.next()).await {
            Ok(Some(result)) => result,
            Ok(None) => break,
            Err(_) => continue,
        };
        match result {
            Ok(Message::Text(text)) if text == "ping" => {
                let _ = outbound_tx.send(Outbound::Text("pong"));
            }
            Ok(Message::Text(_)) => {}
            Ok(Message::Binary(bytes)) => {
                let message = match decode_bounded(&bytes, config.protocol_limits) {
                    Ok(message) => message,
                    Err(error) => {
                        warn!(%error, "invalid Loro protocol message");
                        continue;
                    }
                };
                handle_message(
                    &rooms,
                    connection_id,
                    &outbound_tx,
                    &mut joined,
                    &mut fragments,
                    &config,
                    message,
                )
                .await;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(Message::Ping(payload)) => {
                let _ = outbound_tx.send(Outbound::Text("pong"));
                drop(payload);
            }
            Ok(Message::Pong(_)) => {}
        }
    }
    for room_id in joined {
        rooms.room(&room_id).leave(connection_id).await;
    }
    fragments.clear();
    drop(outbound_tx);
    let _ = writer.await;
}

async fn handle_message(
    rooms: &RoomManager,
    connection_id: u64,
    outbound: &PeerSender,
    joined: &mut HashSet<String>,
    fragments: &mut HashMap<FragmentKey, FragmentBatch>,
    config: &ServerConfig,
    message: ProtocolMessage,
) {
    match message {
        ProtocolMessage::JoinRequest {
            crdt,
            room_id,
            version,
            ..
        } => {
            if crdt != CrdtType::Loro {
                send(
                    outbound,
                    ProtocolMessage::JoinError {
                        crdt,
                        room_id,
                        code: JoinErrorCode::AppError,
                        message: "only %LOR rooms are supported".into(),
                        receiver_version: None,
                        app_code: Some("unsupported_crdt".into()),
                    },
                );
                return;
            }
            joined.insert(room_id.clone());
            rooms
                .room(&room_id)
                .join(connection_id, version, outbound.clone())
                .await;
        }
        ProtocolMessage::DocUpdate {
            crdt,
            room_id,
            updates,
            batch_id,
        } => {
            if crdt != CrdtType::Loro || !joined.contains(&room_id) {
                send_ack(
                    outbound,
                    crdt,
                    room_id,
                    batch_id,
                    UpdateStatusCode::PermissionDenied,
                );
                return;
            }
            rooms
                .room(&room_id)
                .update(connection_id, batch_id, updates, outbound.clone())
                .await;
        }
        ProtocolMessage::DocUpdateFragmentHeader {
            crdt,
            room_id,
            batch_id,
            fragment_count,
            total_size_bytes,
        } => {
            if crdt != CrdtType::Loro || !joined.contains(&room_id) {
                send_ack(
                    outbound,
                    crdt,
                    room_id,
                    batch_id,
                    UpdateStatusCode::PermissionDenied,
                );
                return;
            }
            if fragment_count == 0
                || fragment_count > config.max_fragments_per_batch
                || total_size_bytes == 0
                || total_size_bytes > config.max_reassembled_bytes
            {
                send_ack(
                    outbound,
                    crdt,
                    room_id,
                    batch_id,
                    UpdateStatusCode::PayloadTooLarge,
                );
                return;
            }
            let key = FragmentKey { room_id, batch_id };
            if let Some(existing) = fragments.get(&key) {
                if existing.total_size == total_size_bytes
                    && u64::try_from(existing.chunks.len()).ok() == Some(fragment_count)
                {
                    return;
                }
                fragments.remove(&key);
                send_ack(
                    outbound,
                    crdt,
                    key.room_id,
                    key.batch_id,
                    UpdateStatusCode::InvalidUpdate,
                );
                return;
            }
            if fragments.len() >= config.max_fragment_batches {
                send_ack(
                    outbound,
                    crdt,
                    key.room_id,
                    key.batch_id,
                    UpdateStatusCode::RateLimited,
                );
                return;
            }
            let reserved_bytes = fragments
                .values()
                .try_fold(0_u64, |sum, batch| sum.checked_add(batch.total_size));
            if reserved_bytes
                .and_then(|bytes| bytes.checked_add(total_size_bytes))
                .is_none_or(|bytes| bytes > config.max_fragment_bytes_per_connection)
            {
                send_ack(
                    outbound,
                    crdt,
                    key.room_id,
                    key.batch_id,
                    UpdateStatusCode::PayloadTooLarge,
                );
                return;
            }
            let count = match usize::try_from(fragment_count) {
                Ok(count) => count,
                Err(_) => return,
            };
            fragments.insert(
                key,
                FragmentBatch {
                    total_size: total_size_bytes,
                    chunks: vec![None; count],
                    received: 0,
                    received_bytes: 0,
                    deadline: Instant::now() + config.fragment_timeout,
                },
            );
        }
        ProtocolMessage::DocUpdateFragment {
            crdt,
            room_id,
            batch_id,
            index,
            fragment,
        } => {
            if crdt != CrdtType::Loro || !joined.contains(&room_id) {
                send_ack(
                    outbound,
                    crdt,
                    room_id,
                    batch_id,
                    UpdateStatusCode::PermissionDenied,
                );
                return;
            }
            let key = FragmentKey {
                room_id: room_id.clone(),
                batch_id,
            };
            let Some(batch) = fragments.get_mut(&key) else {
                send_ack(
                    outbound,
                    crdt,
                    room_id,
                    batch_id,
                    UpdateStatusCode::FragmentTimeout,
                );
                return;
            };
            let index = match usize::try_from(index) {
                Ok(index) if index < batch.chunks.len() => index,
                _ => {
                    send_ack(
                        outbound,
                        crdt,
                        room_id,
                        batch_id,
                        UpdateStatusCode::InvalidUpdate,
                    );
                    return;
                }
            };
            let fragment_len = match u64::try_from(fragment.len()) {
                Ok(length) => length,
                Err(_) => {
                    fragments.remove(&key);
                    send_ack(
                        outbound,
                        crdt,
                        room_id,
                        batch_id,
                        UpdateStatusCode::PayloadTooLarge,
                    );
                    return;
                }
            };
            if let Some(existing) = &batch.chunks[index] {
                if existing != &fragment {
                    fragments.remove(&key);
                    send_ack(
                        outbound,
                        crdt,
                        room_id,
                        batch_id,
                        UpdateStatusCode::InvalidUpdate,
                    );
                }
                return;
            } else {
                let Some(received_bytes) = batch.received_bytes.checked_add(fragment_len) else {
                    fragments.remove(&key);
                    send_ack(
                        outbound,
                        crdt,
                        room_id,
                        batch_id,
                        UpdateStatusCode::PayloadTooLarge,
                    );
                    return;
                };
                if received_bytes > batch.total_size {
                    fragments.remove(&key);
                    send_ack(
                        outbound,
                        crdt,
                        room_id,
                        batch_id,
                        UpdateStatusCode::InvalidUpdate,
                    );
                    return;
                }
                batch.chunks[index] = Some(fragment);
                batch.received = batch.received.saturating_add(1);
                batch.received_bytes = received_bytes;
            }
            if batch.received == batch.chunks.len() {
                let Some(batch) = fragments.remove(&key) else {
                    return;
                };
                let mut update = Vec::new();
                for chunk in batch.chunks {
                    let Some(chunk) = chunk else {
                        return;
                    };
                    update.extend_from_slice(&chunk);
                }
                if u64::try_from(update.len()).ok() != Some(batch.total_size) {
                    send_ack(
                        outbound,
                        crdt,
                        room_id,
                        batch_id,
                        UpdateStatusCode::InvalidUpdate,
                    );
                    return;
                }
                rooms
                    .room(&room_id)
                    .update(connection_id, batch_id, vec![update], outbound.clone())
                    .await;
            }
        }
        ProtocolMessage::Leave { crdt, room_id } => {
            if crdt == CrdtType::Loro && joined.remove(&room_id) {
                rooms.room(&room_id).leave(connection_id).await;
                fragments.retain(|key, _| key.room_id != room_id);
            }
        }
        ProtocolMessage::Ack { .. } => {
            // Success acknowledgements for server-originated updates are not required.
        }
        ProtocolMessage::JoinResponseOk { .. }
        | ProtocolMessage::JoinError { .. }
        | ProtocolMessage::RoomError { .. } => {
            // These are receiver-to-requester messages and invalid from clients.
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FragmentKey {
    room_id: String,
    batch_id: BatchId,
}

struct FragmentBatch {
    total_size: u64,
    chunks: Vec<Option<Vec<u8>>>,
    received: usize,
    received_bytes: u64,
    deadline: Instant,
}

fn expire_fragments(fragments: &mut HashMap<FragmentKey, FragmentBatch>, outbound: &PeerSender) {
    let now = Instant::now();
    let expired = fragments
        .iter()
        .filter(|(_, batch)| batch.deadline <= now)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in expired {
        if fragments.remove(&key).is_some() {
            send_ack(
                outbound,
                CrdtType::Loro,
                key.room_id,
                key.batch_id,
                UpdateStatusCode::FragmentTimeout,
            );
        }
    }
}

fn send(outbound: &PeerSender, message: ProtocolMessage) {
    let _ = outbound.send(Outbound::Protocol(message));
}

fn send_ack(
    outbound: &PeerSender,
    crdt: CrdtType,
    room_id: String,
    batch_id: BatchId,
    status: UpdateStatusCode,
) {
    send(
        outbound,
        ProtocolMessage::Ack {
            crdt,
            room_id,
            ref_id: batch_id,
            status,
        },
    );
}
