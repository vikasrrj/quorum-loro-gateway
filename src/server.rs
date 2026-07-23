use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

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
use loro_protocol::decode;
use loro_protocol::encode;
use tokio::sync::mpsc;
use tracing::warn;

use crate::actor::Outbound;
use crate::actor::PeerSender;
use crate::actor::RoomManager;

const MAX_FRAGMENTS: u64 = 4096;
const MAX_REASSEMBLED_BYTES: u64 = 32 * 1024 * 1024;
const FRAGMENT_TIMEOUT: Duration = Duration::from_secs(10);

static CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct AppState {
    rooms: RoomManager,
}

pub fn app(rooms: RoomManager) -> Router {
    Router::new()
        .route("/", get(upgrade))
        .route("/ws", get(upgrade))
        .with_state(AppState { rooms })
}

async fn upgrade(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| connection(socket, state.rooms))
}

async fn connection(socket: WebSocket, rooms: RoomManager) {
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
    loop {
        expire_fragments(&mut fragments, &outbound_tx);
        let result = match tokio::time::timeout(Duration::from_secs(1), source.next()).await {
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
                if bytes.len() > loro_protocol::MAX_MESSAGE_SIZE {
                    continue;
                }
                let message = match decode(&bytes) {
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
    drop(outbound_tx);
    let _ = writer.await;
}

async fn handle_message(
    rooms: &RoomManager,
    connection_id: u64,
    outbound: &PeerSender,
    joined: &mut HashSet<String>,
    fragments: &mut HashMap<FragmentKey, FragmentBatch>,
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
                        message: "Phase 1 supports only %LOR rooms".into(),
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
                || fragment_count > MAX_FRAGMENTS
                || total_size_bytes == 0
                || total_size_bytes > MAX_REASSEMBLED_BYTES
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
                    deadline: Instant::now() + FRAGMENT_TIMEOUT,
                    outbound: outbound.clone(),
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
            if batch.chunks[index].is_none() {
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
    outbound: PeerSender,
}

fn expire_fragments(fragments: &mut HashMap<FragmentKey, FragmentBatch>, outbound: &PeerSender) {
    let now = Instant::now();
    let expired = fragments
        .iter()
        .filter(|(_, batch)| batch.deadline <= now)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for key in expired {
        if let Some(batch) = fragments.remove(&key) {
            let target = if batch.outbound.is_closed() {
                outbound
            } else {
                &batch.outbound
            };
            send_ack(
                target,
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
