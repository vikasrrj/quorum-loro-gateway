use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use loro::ExportMode;
use loro::LoroDoc;
use loro::VersionVector;
use loro_protocol::BatchId;
use loro_protocol::CrdtType;
use loro_protocol::JoinErrorCode;
use loro_protocol::Permission;
use loro_protocol::ProtocolMessage;
use loro_protocol::RoomErrorCode;
use loro_protocol::UpdateStatusCode;
use tokio::sync::mpsc;
use tracing::error;
use tracing::warn;

use crate::frame::DeltaFrame;
use crate::frame::ProducerTuple;
use crate::frame::decode_all;
use crate::names::delta_stream;
use crate::names::producer_id;
use crate::ursula::AppendOutcome;
use crate::ursula::RejectionKind;
use crate::ursula::StoreError;
use crate::ursula::UrsulaStore;

static SERVER_BATCH_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub enum Outbound {
    Protocol(ProtocolMessage),
    Text(&'static str),
}

pub type PeerSender = mpsc::UnboundedSender<Outbound>;

#[derive(Clone)]
pub struct RoomManager {
    inner: Arc<ManagerInner>,
}

struct ManagerInner {
    store: Arc<dyn UrsulaStore>,
    boot_id: [u8; 16],
    rooms: Mutex<HashMap<String, RoomHandle>>,
    config: ActorConfig,
}

#[derive(Debug, Clone)]
pub struct ActorConfig {
    pub command_capacity: usize,
    pub ambiguous_retries: usize,
    pub retry_delay: Duration,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            command_capacity: 128,
            ambiguous_retries: 5,
            retry_delay: Duration::from_millis(25),
        }
    }
}

impl RoomManager {
    pub fn new(store: Arc<dyn UrsulaStore>, boot_id: [u8; 16], config: ActorConfig) -> Self {
        Self {
            inner: Arc::new(ManagerInner {
                store,
                boot_id,
                rooms: Mutex::new(HashMap::new()),
                config,
            }),
        }
    }

    pub fn with_random_boot(store: Arc<dyn UrsulaStore>, config: ActorConfig) -> Self {
        Self::new(store, *uuid::Uuid::new_v4().as_bytes(), config)
    }

    pub fn room(&self, room_id: &str) -> RoomHandle {
        let mut rooms = self
            .inner
            .rooms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(room) = rooms.get(room_id) {
            return room.clone();
        }
        let (tx, rx) = mpsc::channel(self.inner.config.command_capacity);
        let handle = RoomHandle { tx };
        let actor = RoomActor::new(
            room_id.to_owned(),
            self.inner.boot_id,
            self.inner.store.clone(),
            self.inner.config.clone(),
        );
        tokio::spawn(actor.run(rx));
        rooms.insert(room_id.to_owned(), handle.clone());
        handle
    }
}

#[derive(Clone)]
pub struct RoomHandle {
    tx: mpsc::Sender<Command>,
}

impl RoomHandle {
    pub async fn join(&self, connection_id: u64, version: Vec<u8>, peer: PeerSender) {
        let _ = self
            .tx
            .send(Command::Join {
                connection_id,
                version,
                peer,
            })
            .await;
    }

    pub async fn update(
        &self,
        connection_id: u64,
        batch_id: BatchId,
        updates: Vec<Vec<u8>>,
        peer: PeerSender,
    ) {
        if self
            .tx
            .send(Command::Update {
                connection_id,
                batch_id,
                updates,
                peer: peer.clone(),
            })
            .await
            .is_err()
        {
            send_ack(&peer, "", batch_id, UpdateStatusCode::Unknown);
        }
    }

    pub async fn leave(&self, connection_id: u64) {
        let _ = self.tx.send(Command::Leave { connection_id }).await;
    }
}

enum Command {
    Join {
        connection_id: u64,
        version: Vec<u8>,
        peer: PeerSender,
    },
    Update {
        connection_id: u64,
        batch_id: BatchId,
        updates: Vec<Vec<u8>>,
        peer: PeerSender,
    },
    Leave {
        connection_id: u64,
    },
}

struct PendingAppend {
    producer: ProducerTuple,
    bytes: Vec<u8>,
}

struct RoomActor {
    room_id: String,
    stream: String,
    store: Arc<dyn UrsulaStore>,
    producer_id: String,
    producer_sequence: u64,
    doc: LoroDoc,
    history: Vec<Vec<u8>>,
    peers: HashMap<u64, PeerSender>,
    blocked: Option<PendingAppend>,
    config: ActorConfig,
}

impl RoomActor {
    fn new(
        room_id: String,
        boot_id: [u8; 16],
        store: Arc<dyn UrsulaStore>,
        config: ActorConfig,
    ) -> Self {
        let stream = delta_stream(&room_id).physical;
        let producer_id = producer_id(&boot_id, &room_id);
        Self {
            room_id,
            stream,
            store,
            producer_id,
            producer_sequence: 0,
            doc: LoroDoc::new(),
            history: Vec::new(),
            peers: HashMap::new(),
            blocked: None,
            config,
        }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<Command>) {
        let initialization = self.initialize().await;
        while let Some(command) = rx.recv().await {
            if let Err(message) = &initialization {
                self.reject_unavailable(command, message);
                continue;
            }
            match command {
                Command::Join {
                    connection_id,
                    version,
                    peer,
                } => self.join(connection_id, version, peer),
                Command::Update {
                    connection_id,
                    batch_id,
                    updates,
                    peer,
                } => {
                    self.update(connection_id, batch_id, updates, peer).await;
                }
                Command::Leave { connection_id } => {
                    self.peers.remove(&connection_id);
                }
            }
        }
    }

    async fn initialize(&mut self) -> Result<(), String> {
        self.store
            .ensure_stream(&self.stream)
            .await
            .map_err(|error| error.to_string())?;
        let bytes = self
            .store
            .read_all(&self.stream)
            .await
            .map_err(|error| error.to_string())?;
        let frames = decode_all(&bytes).map_err(|error| error.to_string())?;
        let mut history = Vec::new();
        for frame in frames {
            validate_update_blobs(&frame.updates)?;
            history.extend(frame.updates);
        }
        let doc = replay(&history)?;
        self.history = history;
        self.doc = doc;
        Ok(())
    }

    fn reject_unavailable(&self, command: Command, message: &str) {
        match command {
            Command::Join { peer, .. } => {
                send_protocol(
                    &peer,
                    ProtocolMessage::JoinError {
                        crdt: CrdtType::Loro,
                        room_id: self.room_id.clone(),
                        code: JoinErrorCode::AppError,
                        message: message.to_owned(),
                        receiver_version: None,
                        app_code: Some("storage_unavailable".into()),
                    },
                );
            }
            Command::Update { batch_id, peer, .. } => {
                send_ack(&peer, &self.room_id, batch_id, UpdateStatusCode::Unknown);
            }
            Command::Leave { .. } => {}
        }
    }

    fn join(&mut self, connection_id: u64, version: Vec<u8>, peer: PeerSender) {
        let client_version = if version.is_empty() {
            None
        } else {
            match VersionVector::decode(&version) {
                Ok(version) => Some(version),
                Err(error) => {
                    send_protocol(
                        &peer,
                        ProtocolMessage::JoinError {
                            crdt: CrdtType::Loro,
                            room_id: self.room_id.clone(),
                            code: JoinErrorCode::VersionUnknown,
                            message: error.to_string(),
                            receiver_version: Some(self.doc.oplog_vv().encode()),
                            app_code: None,
                        },
                    );
                    return;
                }
            }
        };
        self.peers.insert(connection_id, peer.clone());
        send_protocol(
            &peer,
            ProtocolMessage::JoinResponseOk {
                crdt: CrdtType::Loro,
                room_id: self.room_id.clone(),
                permission: Permission::Write,
                version: self.doc.oplog_vv().encode(),
                extra: Some(Vec::new()),
            },
        );
        if self.doc.len_ops() == 0 {
            return;
        }
        let update = match client_version {
            Some(version) => self.doc.export(ExportMode::updates(&version)),
            None => self.doc.export(ExportMode::Snapshot),
        };
        match update {
            Ok(update) => send_update(&peer, &self.room_id, update),
            Err(error) => send_room_error(&peer, &self.room_id, error.to_string()),
        }
    }

    async fn update(
        &mut self,
        connection_id: u64,
        batch_id: BatchId,
        updates: Vec<Vec<u8>>,
        peer: PeerSender,
    ) {
        if !self.peers.contains_key(&connection_id) {
            send_ack(
                &peer,
                &self.room_id,
                batch_id,
                UpdateStatusCode::PermissionDenied,
            );
            return;
        }
        if self.blocked.is_some() {
            send_ack(
                &peer,
                &self.room_id,
                batch_id,
                UpdateStatusCode::RateLimited,
            );
            return;
        }
        if let Err(error) = validate_update_blobs(&updates) {
            warn!(room = %self.room_id, %error, "invalid Loro update rejected");
            send_ack(
                &peer,
                &self.room_id,
                batch_id,
                UpdateStatusCode::InvalidUpdate,
            );
            return;
        }

        let mut candidate_history = self.history.clone();
        candidate_history.extend(updates.clone());
        let candidate = match replay(&candidate_history) {
            Ok(doc) => doc,
            Err(error) => {
                warn!(room = %self.room_id, %error, "Loro import rejected");
                send_ack(
                    &peer,
                    &self.room_id,
                    batch_id,
                    UpdateStatusCode::InvalidUpdate,
                );
                return;
            }
        };
        let producer = ProducerTuple {
            id: self.producer_id.clone(),
            epoch: 0,
            sequence: self.producer_sequence,
        };
        let frame = DeltaFrame::new(producer.clone(), batch_id, updates.clone());
        let bytes = match frame.encode() {
            Ok(bytes) => bytes,
            Err(error) => {
                error!(room = %self.room_id, %error, "failed to encode update frame");
                send_ack(
                    &peer,
                    &self.room_id,
                    batch_id,
                    UpdateStatusCode::PayloadTooLarge,
                );
                return;
            }
        };
        let pending = PendingAppend {
            producer: producer.clone(),
            bytes: bytes.clone(),
        };

        match self.resolve_append(&pending).await {
            Ok(()) => {
                self.producer_sequence = self.producer_sequence.saturating_add(1);
                self.history = candidate_history;
                self.doc = candidate;
                for (id, subscriber) in &self.peers {
                    if *id != connection_id {
                        send_protocol(
                            subscriber,
                            ProtocolMessage::DocUpdate {
                                crdt: CrdtType::Loro,
                                room_id: self.room_id.clone(),
                                updates: updates.clone(),
                                batch_id: server_batch_id(),
                            },
                        );
                    }
                }
                send_ack(&peer, &self.room_id, batch_id, UpdateStatusCode::Ok);
            }
            Err(StoreError::Rejected { kind, message }) => {
                warn!(room = %self.room_id, ?kind, %message, "Ursula append rejected");
                let status = match kind {
                    RejectionKind::PermissionDenied => UpdateStatusCode::PermissionDenied,
                    RejectionKind::PayloadTooLarge => UpdateStatusCode::PayloadTooLarge,
                    RejectionKind::RateLimited => UpdateStatusCode::RateLimited,
                    RejectionKind::Invalid => UpdateStatusCode::InvalidUpdate,
                    _ => UpdateStatusCode::AppError,
                };
                send_ack(&peer, &self.room_id, batch_id, status);
            }
            Err(error) => {
                error!(room = %self.room_id, %error, "append remains unresolved");
                self.blocked = Some(pending);
                send_ack(&peer, &self.room_id, batch_id, UpdateStatusCode::Unknown);
                send_room_error(&peer, &self.room_id, error.to_string());
            }
        }
    }

    async fn resolve_append(&self, pending: &PendingAppend) -> Result<(), StoreError> {
        let mut attempts = 0_usize;
        loop {
            match self
                .store
                .append(&self.stream, &pending.producer, &pending.bytes)
                .await
            {
                Ok(AppendOutcome::Committed { .. }) => return Ok(()),
                Ok(AppendOutcome::Duplicate { next_offset }) => {
                    let frame_len = pending.bytes.len();
                    let frame_len_u64 = u64::try_from(frame_len).map_err(|_| {
                        StoreError::Integrity("frame length does not fit u64".into())
                    })?;
                    let start = next_offset.checked_sub(frame_len_u64).ok_or_else(|| {
                        StoreError::Integrity(
                            "duplicate next offset is smaller than retry frame".into(),
                        )
                    })?;
                    let stored = self
                        .store
                        .read_range(&self.stream, start, frame_len)
                        .await?;
                    if stored != pending.bytes {
                        return Err(StoreError::Integrity(
                            "deduplicated tuple is bound to different frame bytes".into(),
                        ));
                    }
                    let (stored_frame, consumed) = DeltaFrame::decode_one(&stored)
                        .map_err(|error| StoreError::Integrity(error.to_string()))?;
                    if consumed != stored.len() || stored_frame.producer != pending.producer {
                        return Err(StoreError::Integrity(
                            "deduplicated committed range has wrong producer tuple".into(),
                        ));
                    }
                    return Ok(());
                }
                Err(error @ StoreError::Ambiguous(_)) => {
                    if attempts >= self.config.ambiguous_retries {
                        return Err(error);
                    }
                    attempts = attempts.saturating_add(1);
                    tokio::time::sleep(self.config.retry_delay).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn validate_update_blobs(updates: &[Vec<u8>]) -> Result<(), String> {
    if updates.is_empty() {
        return Err("DocUpdate contains no updates".into());
    }
    for update in updates {
        if update.is_empty() {
            return Err("DocUpdate contains an empty update".into());
        }
        LoroDoc::decode_import_blob_meta(update, true).map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn replay(updates: &[Vec<u8>]) -> Result<LoroDoc, String> {
    let doc = LoroDoc::new();
    if !updates.is_empty() {
        doc.import_batch(updates)
            .map_err(|error| error.to_string())?;
    }
    Ok(doc)
}

fn send_protocol(peer: &PeerSender, message: ProtocolMessage) {
    let _ = peer.send(Outbound::Protocol(message));
}

fn send_ack(peer: &PeerSender, room_id: &str, batch_id: BatchId, status: UpdateStatusCode) {
    send_protocol(
        peer,
        ProtocolMessage::Ack {
            crdt: CrdtType::Loro,
            room_id: room_id.to_owned(),
            ref_id: batch_id,
            status,
        },
    );
}

fn send_room_error(peer: &PeerSender, room_id: &str, message: String) {
    send_protocol(
        peer,
        ProtocolMessage::RoomError {
            crdt: CrdtType::Loro,
            room_id: room_id.to_owned(),
            code: RoomErrorCode::Unknown,
            message,
        },
    );
}

fn send_update(peer: &PeerSender, room_id: &str, update: Vec<u8>) {
    const FRAGMENT_SIZE: usize = 240 * 1024;
    let batch_id = server_batch_id();
    if update.len() <= FRAGMENT_SIZE {
        send_protocol(
            peer,
            ProtocolMessage::DocUpdate {
                crdt: CrdtType::Loro,
                room_id: room_id.to_owned(),
                updates: vec![update],
                batch_id,
            },
        );
        return;
    }
    let count = update.len().div_ceil(FRAGMENT_SIZE);
    send_protocol(
        peer,
        ProtocolMessage::DocUpdateFragmentHeader {
            crdt: CrdtType::Loro,
            room_id: room_id.to_owned(),
            batch_id,
            fragment_count: u64::try_from(count).unwrap_or(u64::MAX),
            total_size_bytes: u64::try_from(update.len()).unwrap_or(u64::MAX),
        },
    );
    for (index, fragment) in update.chunks(FRAGMENT_SIZE).enumerate() {
        send_protocol(
            peer,
            ProtocolMessage::DocUpdateFragment {
                crdt: CrdtType::Loro,
                room_id: room_id.to_owned(),
                batch_id,
                index: u64::try_from(index).unwrap_or(u64::MAX),
                fragment: fragment.to_vec(),
            },
        );
    }
}

fn server_batch_id() -> BatchId {
    BatchId(
        SERVER_BATCH_ID
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes(),
    )
}
