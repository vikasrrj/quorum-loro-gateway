use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::SinkExt;
use futures_util::StreamExt;
use loro::ExportMode;
use loro::LoroDoc;
use loro_protocol::BatchId;
use loro_protocol::CrdtType;
use loro_protocol::Permission;
use loro_protocol::ProtocolMessage;
use loro_protocol::UpdateStatusCode;
use quorum_loro_gateway::HttpUrsula;
use quorum_loro_gateway::HttpUrsulaConfig;
use quorum_loro_gateway::RoomManager;
use quorum_loro_gateway::ServerConfig;
use quorum_loro_gateway::actor::ActorConfig;
use quorum_loro_gateway::actor::Outbound;
use quorum_loro_gateway::app;
use quorum_loro_gateway::app_with_config;
use quorum_loro_gateway::frame::ProducerTuple;
use quorum_loro_gateway::frame::decode_all;
use quorum_loro_gateway::names::delta_stream;
use quorum_loro_gateway::ursula::AppendOutcome;
use quorum_loro_gateway::ursula::RejectionKind;
use quorum_loro_gateway::ursula::StoreError;
use quorum_loro_gateway::ursula::UrsulaStore;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone)]
struct MemoryStore {
    inner: Arc<Mutex<MemoryState>>,
    append_started: Arc<Notify>,
}

#[derive(Default)]
struct MemoryState {
    streams: HashMap<String, Vec<u8>>,
    producers: HashMap<(String, String), ProducerRecord>,
    behaviors: VecDeque<Behavior>,
    attempts: Vec<Attempt>,
}

#[derive(Clone)]
struct ProducerRecord {
    sequence: u64,
    next_offset: u64,
}

enum Behavior {
    Commit,
    Reject,
    Ambiguous,
    DuplicateAt(u64),
    CommitThenAmbiguous,
    CommitCorruptThenAmbiguous,
    WaitFor(Arc<Notify>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Attempt {
    producer: ProducerTuple,
    body: Vec<u8>,
}

impl MemoryStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryState::default())),
            append_started: Arc::new(Notify::new()),
        }
    }

    fn push_behavior(&self, behavior: Behavior) {
        self.inner
            .lock()
            .expect("memory store lock")
            .behaviors
            .push_back(behavior);
    }

    fn attempts(&self) -> Vec<Attempt> {
        self.inner
            .lock()
            .expect("memory store lock")
            .attempts
            .clone()
    }

    fn stream_bytes(&self, stream: &str) -> Vec<u8> {
        self.inner
            .lock()
            .expect("memory store lock")
            .streams
            .get(stream)
            .cloned()
            .unwrap_or_default()
    }

    fn commit(
        state: &mut MemoryState,
        stream: &str,
        producer: &ProducerTuple,
        body: &[u8],
    ) -> Result<u64, StoreError> {
        let bytes = state.streams.entry(stream.to_owned()).or_default();
        bytes.extend_from_slice(body);
        let next_offset = u64::try_from(bytes.len())
            .map_err(|_| StoreError::Integrity("test stream is too large".into()))?;
        state.producers.insert(
            (stream.to_owned(), producer.id.clone()),
            ProducerRecord {
                sequence: producer.sequence,
                next_offset,
            },
        );
        Ok(next_offset)
    }
}

#[async_trait]
impl UrsulaStore for MemoryStore {
    async fn ensure_stream(&self, stream: &str) -> Result<(), StoreError> {
        self.inner
            .lock()
            .expect("memory store lock")
            .streams
            .entry(stream.to_owned())
            .or_default();
        Ok(())
    }

    async fn read_all(&self, stream: &str) -> Result<Vec<u8>, StoreError> {
        Ok(self.stream_bytes(stream))
    }

    async fn read_range(
        &self,
        stream: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, StoreError> {
        let start = usize::try_from(offset)
            .map_err(|_| StoreError::Integrity("test offset is too large".into()))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| StoreError::Integrity("test range overflow".into()))?;
        let state = self.inner.lock().expect("memory store lock");
        state
            .streams
            .get(stream)
            .and_then(|bytes| bytes.get(start..end))
            .map(ToOwned::to_owned)
            .ok_or_else(|| StoreError::Integrity("test range is absent".into()))
    }

    async fn append(
        &self,
        stream: &str,
        producer: &ProducerTuple,
        body: &[u8],
    ) -> Result<AppendOutcome, StoreError> {
        let behavior = {
            let mut state = self.inner.lock().expect("memory store lock");
            state.attempts.push(Attempt {
                producer: producer.clone(),
                body: body.to_vec(),
            });
            if let Some(record) = state
                .producers
                .get(&(stream.to_owned(), producer.id.clone()))
            {
                if producer.sequence <= record.sequence {
                    return Ok(AppendOutcome::Duplicate {
                        next_offset: record.next_offset,
                    });
                }
                if producer.sequence != record.sequence.saturating_add(1) {
                    return Err(StoreError::Rejected {
                        kind: RejectionKind::Conflict,
                        message: "test sequence gap".into(),
                    });
                }
            } else if producer.sequence != 0 {
                return Err(StoreError::Rejected {
                    kind: RejectionKind::Conflict,
                    message: "test producer must begin at zero".into(),
                });
            }
            state.behaviors.pop_front().unwrap_or(Behavior::Commit)
        };
        self.append_started.notify_waiters();
        match behavior {
            Behavior::Reject => Err(StoreError::Rejected {
                kind: RejectionKind::Conflict,
                message: "definite test rejection".into(),
            }),
            Behavior::Ambiguous => Err(StoreError::Ambiguous("test outcome unknown".into())),
            Behavior::DuplicateAt(next_offset) => Ok(AppendOutcome::Duplicate { next_offset }),
            Behavior::WaitFor(gate) => {
                gate.notified().await;
                let mut state = self.inner.lock().expect("memory store lock");
                let next_offset = Self::commit(&mut state, stream, producer, body)?;
                Ok(AppendOutcome::Committed { next_offset })
            }
            Behavior::CommitThenAmbiguous => {
                let mut state = self.inner.lock().expect("memory store lock");
                let _ = Self::commit(&mut state, stream, producer, body)?;
                Err(StoreError::Ambiguous("test response lost".into()))
            }
            Behavior::CommitCorruptThenAmbiguous => {
                let mut state = self.inner.lock().expect("memory store lock");
                let _ = Self::commit(&mut state, stream, producer, body)?;
                if let Some(last) = state
                    .streams
                    .get_mut(stream)
                    .and_then(|bytes| bytes.last_mut())
                {
                    *last ^= 1;
                }
                Err(StoreError::Ambiguous("test response lost".into()))
            }
            Behavior::Commit => {
                let mut state = self.inner.lock().expect("memory store lock");
                let next_offset = Self::commit(&mut state, stream, producer, body)?;
                Ok(AppendOutcome::Committed { next_offset })
            }
        }
    }
}

fn manager(store: MemoryStore, boot: u8) -> RoomManager {
    RoomManager::new(
        Arc::new(store),
        [boot; 16],
        ActorConfig {
            ambiguous_retries: 3,
            retry_delay: Duration::ZERO,
            ..ActorConfig::default()
        },
    )
}

fn update_for(text: &str, peer: u64) -> Vec<u8> {
    let doc = LoroDoc::new();
    doc.set_peer_id(peer).expect("set peer ID");
    doc.get_text("text")
        .insert(0, text)
        .expect("insert test text");
    doc.commit();
    doc.export(ExportMode::all_updates())
        .expect("export test update")
}

async fn joined_room(
    rooms: &RoomManager,
    room_id: &str,
    connection_id: u64,
) -> (
    quorum_loro_gateway::actor::RoomHandle,
    mpsc::UnboundedSender<Outbound>,
    mpsc::UnboundedReceiver<Outbound>,
) {
    let room = rooms.room(room_id);
    let (tx, mut rx) = mpsc::unbounded_channel();
    room.join(connection_id, Vec::new(), tx.clone()).await;
    let message = recv_protocol(&mut rx).await;
    assert!(matches!(
        message,
        ProtocolMessage::JoinResponseOk {
            permission: Permission::Write,
            ..
        }
    ));
    (room, tx, rx)
}

async fn recv_protocol(rx: &mut mpsc::UnboundedReceiver<Outbound>) -> ProtocolMessage {
    match tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("outbound timeout")
        .expect("outbound closed")
    {
        Outbound::Protocol(message) => message,
        Outbound::Text(text) => {
            assert_eq!(text, "", "unexpected text frame");
            ProtocolMessage::Leave {
                crdt: CrdtType::Loro,
                room_id: String::new(),
            }
        }
    }
}

async fn recv_ack(rx: &mut mpsc::UnboundedReceiver<Outbound>) -> UpdateStatusCode {
    loop {
        if let ProtocolMessage::Ack { status, .. } = recv_protocol(rx).await {
            return status;
        }
    }
}

#[tokio::test]
async fn ack_ok_waits_for_commit() {
    let store = MemoryStore::new();
    let gate = Arc::new(Notify::new());
    store.push_behavior(Behavior::WaitFor(gate.clone()));
    let rooms = manager(store.clone(), 1);
    let (room, tx, mut rx) = joined_room(&rooms, "delayed", 1).await;
    let started = store.append_started.notified();
    room.update(1, BatchId([1; 8]), vec![update_for("delayed", 1)], tx)
        .await;
    started.await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), rx.recv())
            .await
            .is_err(),
        "Ack arrived before the store committed"
    );
    gate.notify_waiters();
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::Ok);
}

#[tokio::test]
async fn definite_rejection_never_returns_success_ack() {
    let store = MemoryStore::new();
    store.push_behavior(Behavior::Reject);
    let rooms = manager(store, 2);
    let (room, tx, mut rx) = joined_room(&rooms, "rejected", 1).await;
    room.update(1, BatchId([2; 8]), vec![update_for("rejected", 2)], tx)
        .await;
    assert_ne!(recv_ack(&mut rx).await, UpdateStatusCode::Ok);
}

#[tokio::test]
async fn exhausted_ambiguity_never_returns_success_ack_or_advances_sequence() {
    let store = MemoryStore::new();
    for _ in 0..4 {
        store.push_behavior(Behavior::Ambiguous);
    }
    let rooms = manager(store.clone(), 23);
    let (room, tx, mut rx) = joined_room(&rooms, "outcome-unknown", 1).await;
    room.update(
        1,
        BatchId([23; 8]),
        vec![update_for("unknown", 23)],
        tx.clone(),
    )
    .await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::Unknown);
    assert!(
        store
            .stream_bytes(&delta_stream("outcome-unknown").physical)
            .is_empty()
    );

    room.update(1, BatchId([24; 8]), vec![update_for("blocked", 24)], tx)
        .await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::RateLimited);
    let attempts = store.attempts();
    assert_eq!(attempts.len(), 4);
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt.producer.sequence == 0)
    );
}

#[tokio::test]
async fn lost_response_retries_identical_tuple_and_body() {
    let store = MemoryStore::new();
    store.push_behavior(Behavior::CommitThenAmbiguous);
    let rooms = manager(store.clone(), 3);
    let (room, tx, mut rx) = joined_room(&rooms, "ambiguous", 1).await;
    room.update(1, BatchId([3; 8]), vec![update_for("once", 3)], tx)
        .await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::Ok);
    let attempts = store.attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], attempts[1]);
    let stream = delta_stream("ambiguous").physical;
    assert_eq!(
        decode_all(&store.stream_bytes(&stream))
            .expect("frames")
            .len(),
        1
    );
}

#[tokio::test]
async fn duplicate_offset_underflow_never_returns_success_ack() {
    let store = MemoryStore::new();
    store.push_behavior(Behavior::DuplicateAt(0));
    let rooms = manager(store, 25);
    let (room, tx, mut rx) = joined_room(&rooms, "duplicate-underflow", 1).await;
    room.update(1, BatchId([25; 8]), vec![update_for("underflow", 25)], tx)
        .await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::Unknown);
}

#[tokio::test]
async fn duplicate_with_changed_stored_bytes_never_returns_success_ack() {
    let store = MemoryStore::new();
    store.push_behavior(Behavior::CommitCorruptThenAmbiguous);
    let rooms = manager(store, 26);
    let (room, tx, mut rx) = joined_room(&rooms, "duplicate-corrupt", 1).await;
    room.update(1, BatchId([26; 8]), vec![update_for("corrupt", 26)], tx)
        .await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::Unknown);
}

#[tokio::test]
async fn malformed_loro_update_is_rejected_before_storage() {
    let store = MemoryStore::new();
    let rooms = manager(store.clone(), 4);
    let (room, tx, mut rx) = joined_room(&rooms, "malformed", 1).await;
    room.update(1, BatchId([4; 8]), vec![b"not a Loro update".to_vec()], tx)
        .await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::InvalidUpdate);
    assert!(store.attempts().is_empty());
}

#[tokio::test]
async fn official_empty_receiver_snapshot_is_accepted() {
    let source = LoroDoc::new();
    source.set_peer_id(40).expect("set snapshot peer");
    source
        .get_text("text")
        .insert(0, "snapshot")
        .expect("insert snapshot text");
    source.commit();
    let snapshot = source
        .export(ExportMode::Snapshot)
        .expect("export client snapshot");

    let store = MemoryStore::new();
    let rooms = manager(store, 40);
    let (room, tx, mut rx) = joined_room(&rooms, "snapshot-upload", 1).await;
    room.update(1, BatchId([40; 8]), vec![snapshot], tx).await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::Ok);
}

#[tokio::test]
async fn restart_replays_without_local_durable_state() {
    let store = MemoryStore::new();
    let first = manager(store.clone(), 5);
    let (room, tx, mut rx) = joined_room(&first, "restart", 1).await;
    let update = update_for("durable", 5);
    room.update(1, BatchId([5; 8]), vec![update], tx).await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::Ok);
    drop(first);

    let restarted = manager(store, 6);
    let (_room, _tx, mut restarted_rx) = joined_room(&restarted, "restart", 2).await;
    let backfill = recv_protocol(&mut restarted_rx).await;
    let updates = doc_updates(backfill).expect("expected replay backfill");
    let doc = LoroDoc::new();
    doc.import_batch(&updates).expect("import replay");
    assert_eq!(doc.get_text("text").to_string(), "durable");
}

#[tokio::test]
async fn crash_after_commit_before_ack_recovers_from_ursula() {
    let store = MemoryStore::new();
    let first = manager(store.clone(), 7);
    let (room, tx, rx) = joined_room(&first, "crash", 1).await;
    drop(rx);
    room.update(1, BatchId([7; 8]), vec![update_for("survived", 7)], tx)
        .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.attempts().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("append did not reach store");
    drop(first);

    let restarted = manager(store, 8);
    let (_room, _tx, mut rx) = joined_room(&restarted, "crash", 2).await;
    let updates = doc_updates(recv_protocol(&mut rx).await).expect("expected recovered update");
    let doc = LoroDoc::new();
    doc.import_batch(&updates).expect("import recovered update");
    assert_eq!(doc.get_text("text").to_string(), "survived");
}

#[tokio::test]
async fn duplicate_logical_operations_do_not_change_state_twice() {
    let store = MemoryStore::new();
    let rooms = manager(store.clone(), 9);
    let (room, tx, mut rx) = joined_room(&rooms, "duplicate-ops", 1).await;
    let update = update_for("one", 9);
    room.update(1, BatchId([9; 8]), vec![update.clone()], tx.clone())
        .await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::Ok);
    room.update(1, BatchId([10; 8]), vec![update], tx).await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::Ok);

    let restarted = manager(store.clone(), 10);
    let (_room, _tx, mut rx) = joined_room(&restarted, "duplicate-ops", 2).await;
    let updates = doc_updates(recv_protocol(&mut rx).await).expect("expected replay update");
    let doc = LoroDoc::new();
    doc.import_batch(&updates).expect("import duplicate replay");
    assert_eq!(doc.get_text("text").to_string(), "one");
    let stream = delta_stream("duplicate-ops").physical;
    assert_eq!(
        decode_all(&store.stream_bytes(&stream))
            .expect("frames")
            .len(),
        2
    );
}

struct ProtocolClient {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    doc: LoroDoc,
    room_id: String,
}

impl ProtocolClient {
    async fn connect(url: &str, room_id: &str, peer: u64) -> Self {
        let (mut ws, _) = connect_async(url).await.expect("connect protocol client");
        let doc = LoroDoc::new();
        doc.set_peer_id(peer).expect("set client peer");
        let join = ProtocolMessage::JoinRequest {
            crdt: CrdtType::Loro,
            room_id: room_id.into(),
            auth: Vec::new(),
            version: doc.oplog_vv().encode(),
        };
        ws.send(Message::Binary(
            loro_protocol::encode(&join).expect("encode join").into(),
        ))
        .await
        .expect("send join");
        loop {
            let message = next_ws_protocol(&mut ws).await;
            if matches!(message, ProtocolMessage::JoinResponseOk { .. }) {
                break;
            }
        }
        Self {
            ws,
            doc,
            room_id: room_id.into(),
        }
    }

    async fn edit_and_send(&mut self, text: &str, batch: BatchId) {
        let before = self.doc.oplog_vv();
        self.doc
            .get_text("text")
            .insert(self.doc.get_text("text").len_unicode(), text)
            .expect("client edit");
        self.doc.commit();
        let update = self
            .doc
            .export(ExportMode::updates(&before))
            .expect("export client update");
        let message = ProtocolMessage::DocUpdate {
            crdt: CrdtType::Loro,
            room_id: self.room_id.clone(),
            updates: vec![update],
            batch_id: batch,
        };
        self.ws
            .send(Message::Binary(
                loro_protocol::encode(&message)
                    .expect("encode client update")
                    .into(),
            ))
            .await
            .expect("send client update");
        loop {
            if let ProtocolMessage::Ack { status, .. } = next_ws_protocol(&mut self.ws).await {
                assert_eq!(status, UpdateStatusCode::Ok);
                return;
            }
        }
    }

    async fn receive_update(&mut self) {
        loop {
            if let ProtocolMessage::DocUpdate { updates, .. } = next_ws_protocol(&mut self.ws).await
            {
                self.doc
                    .import_batch(&updates)
                    .expect("import remote update");
                return;
            }
        }
    }
}

async fn next_ws_protocol(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> ProtocolMessage {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("WebSocket receive timeout")
            .expect("WebSocket closed")
            .expect("WebSocket error");
        if let Message::Binary(bytes) = message {
            return loro_protocol::decode(&bytes).expect("decode official protocol message");
        }
    }
}

async fn send_ws_protocol(client: &mut ProtocolClient, message: ProtocolMessage) {
    client
        .ws
        .send(Message::Binary(
            loro_protocol::encode(&message)
                .expect("encode test protocol message")
                .into(),
        ))
        .await
        .expect("send test protocol message");
}

async fn configured_gateway(
    store: MemoryStore,
    config: ServerConfig,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind configured gateway");
    let address = listener.local_addr().expect("configured gateway address");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app_with_config(manager(store, 27), config)).await;
    });
    (format!("ws://{address}/ws"), server)
}

fn doc_updates(message: ProtocolMessage) -> Result<Vec<Vec<u8>>, String> {
    if let ProtocolMessage::DocUpdate { updates, .. } = message {
        Ok(updates)
    } else {
        Err(format!("expected DocUpdate, received {message:?}"))
    }
}

fn fragment_header(message: ProtocolMessage) -> Result<(u64, u64), String> {
    if let ProtocolMessage::DocUpdateFragmentHeader {
        fragment_count,
        total_size_bytes,
        ..
    } = message
    {
        Ok((fragment_count, total_size_bytes))
    } else {
        Err(format!("expected fragment header, received {message:?}"))
    }
}

fn fragment(message: ProtocolMessage) -> Result<(u64, Vec<u8>), String> {
    if let ProtocolMessage::DocUpdateFragment {
        index, fragment, ..
    } = message
    {
        Ok((index, fragment))
    } else {
        Err(format!("expected fragment, received {message:?}"))
    }
}

#[tokio::test]
async fn two_official_protocol_clients_converge() {
    let store = MemoryStore::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gateway");
    let address = listener.local_addr().expect("gateway address");
    let server = tokio::spawn(axum::serve(listener, app(manager(store, 11))).into_future());
    let url = format!("ws://{address}/ws");
    let mut first = ProtocolClient::connect(&url, "convergence", 100).await;
    let mut second = ProtocolClient::connect(&url, "convergence", 200).await;

    first.edit_and_send("A", BatchId([11; 8])).await;
    second.receive_update().await;
    second.edit_and_send("B", BatchId([12; 8])).await;
    first.receive_update().await;

    assert_eq!(first.doc.get_text("text").to_string(), "AB");
    assert_eq!(
        first.doc.get_text("text").to_string(),
        second.doc.get_text("text").to_string()
    );
    server.abort();
}

#[tokio::test]
async fn fragment_limits_conflicts_and_timeout_fail_closed() {
    let config = ServerConfig {
        max_fragment_batches: 2,
        max_fragment_bytes_per_connection: 5,
        fragment_timeout: Duration::from_millis(500),
        ..ServerConfig::default()
    };
    let (url, server) = configured_gateway(MemoryStore::new(), config).await;
    let mut client = ProtocolClient::connect(&url, "fragment-limits", 300).await;

    send_ws_protocol(
        &mut client,
        ProtocolMessage::DocUpdateFragmentHeader {
            crdt: CrdtType::Loro,
            room_id: "fragment-limits".into(),
            batch_id: BatchId([31; 8]),
            fragment_count: 2,
            total_size_bytes: 4,
        },
    )
    .await;
    send_ws_protocol(
        &mut client,
        ProtocolMessage::DocUpdateFragmentHeader {
            crdt: CrdtType::Loro,
            room_id: "fragment-limits".into(),
            batch_id: BatchId([32; 8]),
            fragment_count: 1,
            total_size_bytes: 2,
        },
    )
    .await;
    assert!(matches!(
        next_ws_protocol(&mut client.ws).await,
        ProtocolMessage::Ack {
            status: UpdateStatusCode::PayloadTooLarge,
            ref_id,
            ..
        } if ref_id == BatchId([32; 8])
    ));

    send_ws_protocol(
        &mut client,
        ProtocolMessage::DocUpdateFragmentHeader {
            crdt: CrdtType::Loro,
            room_id: "fragment-limits".into(),
            batch_id: BatchId([31; 8]),
            fragment_count: 3,
            total_size_bytes: 4,
        },
    )
    .await;
    assert!(matches!(
        next_ws_protocol(&mut client.ws).await,
        ProtocolMessage::Ack {
            status: UpdateStatusCode::InvalidUpdate,
            ref_id,
            ..
        } if ref_id == BatchId([31; 8])
    ));

    send_ws_protocol(
        &mut client,
        ProtocolMessage::DocUpdateFragmentHeader {
            crdt: CrdtType::Loro,
            room_id: "fragment-limits".into(),
            batch_id: BatchId([33; 8]),
            fragment_count: 2,
            total_size_bytes: 2,
        },
    )
    .await;
    assert!(matches!(
        next_ws_protocol(&mut client.ws).await,
        ProtocolMessage::Ack {
            status: UpdateStatusCode::FragmentTimeout,
            ref_id,
            ..
        } if ref_id == BatchId([33; 8])
    ));

    for (batch, total_size_bytes) in [(37, 2), (38, 2), (39, 1)] {
        send_ws_protocol(
            &mut client,
            ProtocolMessage::DocUpdateFragmentHeader {
                crdt: CrdtType::Loro,
                room_id: "fragment-limits".into(),
                batch_id: BatchId([batch; 8]),
                fragment_count: 1,
                total_size_bytes,
            },
        )
        .await;
    }
    assert!(matches!(
        next_ws_protocol(&mut client.ws).await,
        ProtocolMessage::Ack {
            status: UpdateStatusCode::RateLimited,
            ref_id,
            ..
        } if ref_id == BatchId([39; 8])
    ));
    server.abort();
}

#[tokio::test]
async fn fragments_reassemble_out_of_order_and_reject_conflicting_duplicates() {
    let (url, server) = configured_gateway(MemoryStore::new(), ServerConfig::default()).await;
    let mut client = ProtocolClient::connect(&url, "fragment-order", 301).await;
    let update = update_for("fragmented", 301);
    let split = update.len() / 2;

    send_ws_protocol(
        &mut client,
        ProtocolMessage::DocUpdateFragmentHeader {
            crdt: CrdtType::Loro,
            room_id: "fragment-order".into(),
            batch_id: BatchId([34; 8]),
            fragment_count: 2,
            total_size_bytes: update.len() as u64,
        },
    )
    .await;
    for (index, fragment) in [(1, update[split..].to_vec()), (0, update[..split].to_vec())] {
        send_ws_protocol(
            &mut client,
            ProtocolMessage::DocUpdateFragment {
                crdt: CrdtType::Loro,
                room_id: "fragment-order".into(),
                batch_id: BatchId([34; 8]),
                index,
                fragment,
            },
        )
        .await;
    }
    assert!(matches!(
        next_ws_protocol(&mut client.ws).await,
        ProtocolMessage::Ack {
            status: UpdateStatusCode::Ok,
            ref_id,
            ..
        } if ref_id == BatchId([34; 8])
    ));

    send_ws_protocol(
        &mut client,
        ProtocolMessage::DocUpdateFragmentHeader {
            crdt: CrdtType::Loro,
            room_id: "fragment-order".into(),
            batch_id: BatchId([35; 8]),
            fragment_count: 2,
            total_size_bytes: 2,
        },
    )
    .await;
    for fragment in [b"a".to_vec(), b"b".to_vec()] {
        send_ws_protocol(
            &mut client,
            ProtocolMessage::DocUpdateFragment {
                crdt: CrdtType::Loro,
                room_id: "fragment-order".into(),
                batch_id: BatchId([35; 8]),
                index: 0,
                fragment,
            },
        )
        .await;
    }
    assert!(matches!(
        next_ws_protocol(&mut client.ws).await,
        ProtocolMessage::Ack {
            status: UpdateStatusCode::InvalidUpdate,
            ref_id,
            ..
        } if ref_id == BatchId([35; 8])
    ));
    server.abort();
}

#[tokio::test]
async fn oversized_committed_live_update_is_fragmented_for_subscribers() {
    let mut value = 0x1234_5678_u64;
    let text = (0..600_000)
        .map(|_| {
            value = value
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            char::from(32 + ((value >> 32) % 95) as u8)
        })
        .collect::<String>();
    let update = update_for(&text, 302);
    assert!(update.len() > loro_protocol::MAX_MESSAGE_SIZE);

    let store = MemoryStore::new();
    let rooms = manager(store, 28);
    let (room, first_tx, mut first_rx) = joined_room(&rooms, "large-live", 1).await;
    let (_same_room, _second_tx, mut second_rx) = joined_room(&rooms, "large-live", 2).await;
    room.update(1, BatchId([36; 8]), vec![update.clone()], first_tx)
        .await;

    let (fragment_count, total_size) =
        fragment_header(recv_protocol(&mut second_rx).await).expect("fragment header");
    let mut reconstructed = Vec::new();
    for expected_index in 0..fragment_count {
        let (index, bytes) =
            fragment(recv_protocol(&mut second_rx).await).expect("update fragment");
        assert_eq!(index, expected_index);
        reconstructed.extend_from_slice(&bytes);
    }
    assert_eq!(reconstructed.len() as u64, total_size);
    assert_eq!(reconstructed, update);
    assert_eq!(recv_ack(&mut first_rx).await, UpdateStatusCode::Ok);
}

#[tokio::test]
#[ignore = "requires a local Ursula server on 127.0.0.1:4437"]
async fn real_ursula_commit_duplicate_and_restart_replay() {
    let store = Arc::new(
        HttpUrsula::new(HttpUrsulaConfig {
            response_timeout: Duration::from_secs(5),
            ..HttpUrsulaConfig::default()
        })
        .expect("create Ursula HTTP client"),
    );
    let room_id = format!("real-ursula-{}", uuid::Uuid::new_v4());
    let first = RoomManager::new(
        store.clone(),
        [21; 16],
        ActorConfig {
            retry_delay: Duration::ZERO,
            ..ActorConfig::default()
        },
    );
    let (room, tx, mut rx) = joined_room(&first, &room_id, 1).await;
    room.update(1, BatchId([21; 8]), vec![update_for("real", 21)], tx)
        .await;
    assert_eq!(recv_ack(&mut rx).await, UpdateStatusCode::Ok);

    let stream = delta_stream(&room_id).physical;
    let committed = store.read_all(&stream).await.expect("read committed frame");
    let frames = decode_all(&committed).expect("decode committed frame");
    let producer = frames
        .first()
        .expect("one committed frame")
        .producer
        .clone();
    assert!(matches!(
        store
            .append(&stream, &producer, &committed)
            .await
            .expect("retry committed tuple"),
        AppendOutcome::Duplicate { .. }
    ));

    let restarted = RoomManager::new(
        store,
        [22; 16],
        ActorConfig {
            retry_delay: Duration::ZERO,
            ..ActorConfig::default()
        },
    );
    let (_room, _tx, mut rx) = joined_room(&restarted, &room_id, 2).await;
    let updates =
        doc_updates(recv_protocol(&mut rx).await).expect("expected replay from real Ursula");
    let restored = LoroDoc::new();
    restored
        .import_batch(&updates)
        .expect("import Ursula replay");
    assert_eq!(restored.get_text("text").to_string(), "real");
}
