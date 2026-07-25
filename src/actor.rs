use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use loro::EncodedBlobMode;
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
use serde::Serialize;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::error;
use tracing::warn;

use crate::frame::DeltaFrame;
use crate::frame::FrameLimits;
use crate::frame::ProducerTuple;
use crate::frame::decode_all_with_limits;
use crate::names::GenerationId;
use crate::names::delta_stream;
use crate::names::producer_id;
use crate::names::producer_id_for_generation;
use crate::recovery::RecoveryError;
use crate::recovery::recover_from_manifest;
use crate::rotation::rotate_room;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoomLifecycle {
    Recovering,
    Ready,
    AppendAmbiguous,
    Corrupt,
    Unavailable,
}

impl RoomLifecycle {
    fn as_str(self) -> &'static str {
        match self {
            Self::Recovering => "recovering",
            Self::Ready => "ready",
            Self::AppendAmbiguous => "append_ambiguous",
            Self::Corrupt => "corrupt",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoomStatus {
    pub stream: String,
    pub state: RoomLifecycle,
    pub producer_id: String,
    pub producer_epoch: u64,
    pub producer_sequence: u64,
    pub pending_sequence: Option<u64>,
    pub peer_count: usize,
    pub last_error: Option<String>,
    pub recovered_stream_bytes: usize,
    pub recovered_update_count: usize,
    pub recovery_total_micros: u64,
    pub recovery_read_micros: u64,
    pub recovery_read_requests: u64,
    pub recovery_decode_micros: u64,
    pub recovery_import_micros: u64,
}

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
    pub frame_limits: FrameLimits,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            command_capacity: 128,
            ambiguous_retries: 5,
            retry_delay: Duration::from_millis(25),
            frame_limits: FrameLimits::default(),
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
        let actor = RoomActor::new(
            room_id.to_owned(),
            self.inner.boot_id,
            self.inner.store.clone(),
            self.inner.config.clone(),
        );
        let handle = RoomHandle {
            tx,
            status: actor.status.clone(),
        };
        tokio::spawn(actor.run(rx));
        rooms.insert(room_id.to_owned(), handle.clone());
        handle
    }

    pub fn room_statuses(&self) -> Vec<RoomStatus> {
        let rooms = self
            .inner
            .rooms
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut statuses = rooms.values().map(RoomHandle::status).collect::<Vec<_>>();
        statuses.sort_by(|left, right| left.stream.cmp(&right.stream));
        statuses
    }

    pub async fn retry_ambiguous(&self, room_id: &str) -> bool {
        let room = {
            let rooms = self
                .inner
                .rooms
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            rooms.get(room_id).cloned()
        };
        match room {
            Some(room) => room.retry_ambiguous().await,
            None => false,
        }
    }
}

#[derive(Clone)]
pub struct RoomHandle {
    tx: mpsc::Sender<Command>,
    status: Arc<Mutex<RoomStatus>>,
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

    pub fn status(&self) -> RoomStatus {
        self.status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub async fn wait_for_activation(&self) -> Option<RoomStatus> {
        let (response, result) = oneshot::channel();
        self.tx
            .send(Command::WaitForActivation { response })
            .await
            .ok()?;
        result.await.ok()?;
        Some(self.status())
    }

    pub async fn retry_ambiguous(&self) -> bool {
        let (response, result) = oneshot::channel();
        if self
            .tx
            .send(Command::RetryAmbiguous { response })
            .await
            .is_err()
        {
            return false;
        }
        result.await.unwrap_or(false)
    }
    pub async fn rotate(&self) -> bool {
        let (response, result) = oneshot::channel();

        if self.tx.send(Command::Rotate { response }).await.is_err() {
            return false;
        }

        result.await.unwrap_or(false)
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
    RetryAmbiguous {
        response: oneshot::Sender<bool>,
    },
    WaitForActivation {
        response: oneshot::Sender<()>,
    },
    Rotate {
        response: oneshot::Sender<bool>,
    },
}

struct PendingAppend {
    target_stream: String,
    producer: ProducerTuple,
    frame: DeltaFrame,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableAppend {
    Committed { next_offset: u64 },
    VerifiedDuplicate { next_offset: u64 },
}

impl DurableAppend {
    fn next_offset(self) -> u64 {
        match self {
            Self::Committed { next_offset } | Self::VerifiedDuplicate { next_offset } => {
                next_offset
            }
        }
    }
}

#[derive(Debug)]
enum AppendFailure {
    DefinitelyRejected {
        kind: RejectionKind,
        message: String,
    },
    OutcomeUnknown(StoreError),
}

#[derive(Debug)]
struct InitializationFailure {
    state: RoomLifecycle,
    message: String,
}

struct RoomActor {
    room_id: String,
    boot_id: [u8; 16],
    stream: String,
    active_delta_generation: GenerationId,
    active_delta_end_offset: u64,
    store: Arc<dyn UrsulaStore>,
    producer_id: String,
    producer_sequence: u64,
    doc: LoroDoc,
    history: Vec<Vec<u8>>,
    peers: HashMap<u64, PeerSender>,
    blocked: Option<PendingAppend>,
    lifecycle: RoomLifecycle,
    last_error: Option<String>,
    status: Arc<Mutex<RoomStatus>>,
    recovered_stream_bytes: usize,
    recovered_update_count: usize,
    recovery_total_micros: u64,
    recovery_read_micros: u64,
    recovery_read_requests: u64,
    recovery_decode_micros: u64,
    recovery_import_micros: u64,
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
        let status = Arc::new(Mutex::new(RoomStatus {
            stream: stream.clone(),
            state: RoomLifecycle::Recovering,
            producer_id: producer_id.clone(),
            producer_epoch: 0,
            producer_sequence: 0,
            pending_sequence: None,
            peer_count: 0,
            last_error: None,
            recovered_stream_bytes: 0,
            recovered_update_count: 0,
            recovery_total_micros: 0,
            recovery_read_micros: 0,
            recovery_read_requests: 0,
            recovery_decode_micros: 0,
            recovery_import_micros: 0,
        }));
        Self {
            room_id,
            boot_id,
            stream,
            active_delta_generation: GenerationId::ZERO,
            active_delta_end_offset: 0,
            store,
            producer_id,
            producer_sequence: 0,
            doc: LoroDoc::new(),
            history: Vec::new(),
            peers: HashMap::new(),
            blocked: None,
            lifecycle: RoomLifecycle::Recovering,
            last_error: None,
            status,
            recovered_stream_bytes: 0,
            recovered_update_count: 0,
            recovery_total_micros: 0,
            recovery_read_micros: 0,
            recovery_read_requests: 0,
            recovery_decode_micros: 0,
            recovery_import_micros: 0,
            config,
        }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<Command>) {
        match self.initialize().await {
            Ok(()) => self.transition(RoomLifecycle::Ready, None),
            Err(failure) => self.transition(failure.state, Some(failure.message)),
        }
        while let Some(command) = rx.recv().await {
            match command {
                Command::WaitForActivation { response } => {
                    let _ = response.send(());
                }
                Command::RetryAmbiguous { response } => {
                    let _ = response.send(self.retry_pending().await);
                }
                Command::Rotate { response } => {
                    let rotated = self.rotate_generation().await;
                    let _ = response.send(rotated);
                }
                command
                    if matches!(
                        self.lifecycle,
                        RoomLifecycle::Recovering
                            | RoomLifecycle::Corrupt
                            | RoomLifecycle::Unavailable
                    ) =>
                {
                    self.reject_unavailable(command);
                }
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
                    self.publish_status();
                }
            }
        }
    }

    async fn initialize(&mut self) -> Result<(), InitializationFailure> {
        let started = Instant::now();

        match recover_from_manifest(self.store.as_ref(), &self.room_id, self.config.frame_limits)
            .await
            .map_err(manifest_recovery_failure)?
        {
            Some(recovered) => {
                self.stream = recovered.active_delta_stream;
                self.active_delta_generation = recovered.active_delta_generation;
                self.active_delta_end_offset = recovered.active_delta_end_offset;

                self.producer_id = producer_id_for_generation(
                    &self.boot_id,
                    &self.room_id,
                    self.active_delta_generation,
                );
                self.producer_sequence = 0;

                self.history = recovered.history;
                self.doc = recovered.doc;

                self.recovered_stream_bytes = recovered
                    .recovered_checkpoint_bytes
                    .saturating_add(recovered.recovered_delta_bytes);

                self.recovered_update_count = self.history.len();
                self.recovery_read_requests = 3;

                tracing::info!(
                    room = %self.room_id,
                    stream = %self.stream,
                    checkpoint_generation =
                        recovered.checkpoint_generation.value(),
                    active_delta_generation =
                        self.active_delta_generation.value(),
                    active_delta_end_offset =
                        self.active_delta_end_offset,
                    "room recovered from checkpoint manifest"
                );
            }
            None => {
                self.store
                    .ensure_stream(&self.stream)
                    .await
                    .map_err(initialization_store_failure)?;

                let (history, doc) = self.load_from_store().await?;
                self.history = history;
                self.doc = doc;

                tracing::info!(
                    room = %self.room_id,
                    stream = %self.stream,
                    "room recovered through legacy full replay"
                );
            }
        }

        self.recovery_total_micros = duration_micros(started.elapsed());

        Ok(())
    }

    async fn load_from_store(&mut self) -> Result<(Vec<Vec<u8>>, LoroDoc), InitializationFailure> {
        let read_started = Instant::now();
        let observation = self.store.read_all_observed(&self.stream).await;
        self.recovery_read_micros = duration_micros(read_started.elapsed());
        let observation = observation.map_err(initialization_store_failure)?;
        self.recovery_read_requests = observation.request_count;
        let bytes = observation.bytes;
        self.active_delta_end_offset =
            u64::try_from(bytes.len()).map_err(|_| InitializationFailure {
                state: RoomLifecycle::Corrupt,
                message: "delta stream length does not fit u64".into(),
            })?;
        let decode_started = Instant::now();
        let frames = decode_all_with_limits(&bytes, self.config.frame_limits);
        self.recovery_decode_micros = duration_micros(decode_started.elapsed());
        let frames = frames.map_err(|error| InitializationFailure {
            state: RoomLifecycle::Corrupt,
            message: error.to_string(),
        })?;
        let mut history = Vec::new();
        for frame in frames {
            validate_update_blobs(&frame.updates, history.is_empty()).map_err(|message| {
                InitializationFailure {
                    state: RoomLifecycle::Corrupt,
                    message,
                }
            })?;
            history.extend(frame.updates);
        }
        let import_started = Instant::now();
        let replayed = replay(&history).map_err(|message| InitializationFailure {
            state: RoomLifecycle::Corrupt,
            message,
        })?;

        tracing::debug!(
            room = %self.room_id,
            has_pending = replayed.has_pending,
            "replayed durable room history"
        );

        let doc = replayed.doc;
        self.recovered_stream_bytes = bytes.len();
        self.recovered_update_count = history.len();
        self.recovery_import_micros = duration_micros(import_started.elapsed());
        Ok((history, doc))
    }

    fn reject_unavailable(&self, command: Command) {
        let message = self
            .last_error
            .as_deref()
            .unwrap_or_else(|| self.lifecycle.as_str());
        match command {
            Command::Rotate { response } => {
                let _ = response.send(false);
            }
            Command::Join { peer, .. } => {
                send_protocol(
                    &peer,
                    ProtocolMessage::JoinError {
                        crdt: CrdtType::Loro,
                        room_id: self.room_id.clone(),
                        code: JoinErrorCode::AppError,
                        message: message.to_owned(),
                        receiver_version: None,
                        app_code: Some(self.lifecycle.as_str().into()),
                    },
                );
            }
            Command::Update { batch_id, peer, .. } => {
                send_ack(&peer, &self.room_id, batch_id, UpdateStatusCode::Unknown);
            }
            Command::Leave { .. } => {}
            Command::RetryAmbiguous { response } => {
                let _ = response.send(false);
            }

            Command::WaitForActivation { response } => {
                let _ = response.send(());
            }
        }
    }

    fn transition(&mut self, state: RoomLifecycle, last_error: Option<String>) {
        let previous = self.lifecycle;
        self.lifecycle = state;
        self.last_error = last_error;
        self.publish_status();
        tracing::info!(
            stream = %self.stream,
            producer_id = %self.producer_id,
            producer_epoch = 0_u64,
            producer_sequence = self.producer_sequence,
            pending_sequence = self.blocked.as_ref().map(|pending| pending.producer.sequence),
            from = previous.as_str(),
            to = state.as_str(),
            "room lifecycle transition"
        );
    }

    fn publish_status(&self) {
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        status.stream.clone_from(&self.stream);
        status.producer_id.clone_from(&self.producer_id);
        status.state = self.lifecycle;
        status.producer_sequence = self.producer_sequence;
        status.pending_sequence = self
            .blocked
            .as_ref()
            .map(|pending| pending.producer.sequence);
        status.peer_count = self.peers.len();
        status.last_error.clone_from(&self.last_error);
        status.recovered_stream_bytes = self.recovered_stream_bytes;
        status.recovered_update_count = self.recovered_update_count;
        status.recovery_total_micros = self.recovery_total_micros;
        status.recovery_read_micros = self.recovery_read_micros;
        status.recovery_read_requests = self.recovery_read_requests;
        status.recovery_decode_micros = self.recovery_decode_micros;
        status.recovery_import_micros = self.recovery_import_micros;
    }

    fn join(&mut self, connection_id: u64, version: Vec<u8>, peer: PeerSender) {
        if self.lifecycle != RoomLifecycle::Ready {
            send_protocol(
                &peer,
                ProtocolMessage::JoinError {
                    crdt: CrdtType::Loro,
                    room_id: self.room_id.clone(),
                    code: JoinErrorCode::AppError,
                    message: "room state is not authoritative".into(),
                    receiver_version: None,
                    app_code: Some(self.lifecycle.as_str().into()),
                },
            );
            return;
        }
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
        self.publish_status();
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
        if self.lifecycle != RoomLifecycle::Ready || self.blocked.is_some() {
            send_ack(
                &peer,
                &self.room_id,
                batch_id,
                UpdateStatusCode::RateLimited,
            );
            return;
        }
        if let Err(error) = validate_update_blobs(&updates, self.history.is_empty()) {
            warn!(room = %self.room_id, %error, "invalid Loro update rejected");
            send_ack(
                &peer,
                &self.room_id,
                batch_id,
                UpdateStatusCode::InvalidUpdate,
            );
            return;
        }
        let candidate = match build_candidate(&self.doc, &self.history, &updates) {
            Ok(replayed) => {
                tracing::debug!(
                    room = %self.room_id,
                    has_pending = replayed.has_pending,
                    "validated candidate room state"
                );

                replayed.doc
            }
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

        let mut candidate_history = self.history.clone();
        candidate_history.extend(updates.clone());
        let producer = ProducerTuple {
            id: self.producer_id.clone(),
            epoch: 0,
            sequence: self.producer_sequence,
        };
        let Some(next_producer_sequence) = self.producer_sequence.checked_add(1) else {
            error!(room = %self.room_id, "producer sequence exhausted");
            send_ack(&peer, &self.room_id, batch_id, UpdateStatusCode::AppError);
            return;
        };
        let frame = DeltaFrame::new(producer.clone(), batch_id, updates.clone());
        let bytes = match frame.encode_with_limits(self.config.frame_limits) {
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
            target_stream: self.stream.clone(),
            producer: producer.clone(),
            frame,
            bytes: bytes.clone(),
        };

        match self.resolve_append(&pending).await {
            Ok(proof) => {
                #[cfg(feature = "crash-injection")]
                crash_after_commit_for_test();
                self.producer_sequence = next_producer_sequence;
                self.active_delta_end_offset = proof.next_offset();
                self.history = candidate_history;
                self.doc = candidate;
                self.transition(RoomLifecycle::Ready, None);
                for (id, subscriber) in &self.peers {
                    if *id != connection_id {
                        for update in &updates {
                            send_update(subscriber, &self.room_id, update.clone());
                        }
                    }
                }
                send_durable_ack(&peer, &self.room_id, batch_id, proof);
            }
            Err(AppendFailure::DefinitelyRejected { kind, message }) => {
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
            Err(AppendFailure::OutcomeUnknown(error)) => {
                error!(room = %self.room_id, %error, "append remains unresolved");
                let message = error.to_string();
                self.blocked = Some(pending);
                self.transition(RoomLifecycle::AppendAmbiguous, Some(message.clone()));
                send_ack(&peer, &self.room_id, batch_id, UpdateStatusCode::Unknown);
                send_room_error(&peer, &self.room_id, message);
            }
        }
    }

    async fn rotate_generation(&mut self) -> bool {
        if self.lifecycle != RoomLifecycle::Ready || self.blocked.is_some() {
            return false;
        }

        let rotated = match rotate_room(
            self.store.as_ref(),
            &self.room_id,
            self.active_delta_generation,
            self.active_delta_end_offset,
            &self.doc,
            &self.history,
            self.config.ambiguous_retries,
        )
        .await
        {
            Ok(rotated) => rotated,
            Err(error) => {
                self.transition(RoomLifecycle::Unavailable, Some(error.to_string()));

                return false;
            }
        };

        self.stream = rotated.next_delta_stream;
        self.active_delta_generation = rotated.next_delta_generation;
        self.active_delta_end_offset = 0;

        self.producer_id =
            producer_id_for_generation(&self.boot_id, &self.room_id, self.active_delta_generation);

        self.producer_sequence = 0;
        self.history = rotated.retained_history;

        tracing::info!(
            room = %self.room_id,
            stream = %self.stream,
            active_delta_generation =
                self.active_delta_generation.value(),
            "room rotation completed"
        );

        self.transition(RoomLifecycle::Ready, None);

        true
    }

    async fn retry_pending(&mut self) -> bool {
        let Some(pending) = self.blocked.take() else {
            return false;
        };
        self.publish_status();
        match self.resolve_append(&pending).await {
            Ok(proof) => match self.load_from_store().await {
                Ok((history, doc)) => {
                    let Some(next_sequence) = pending.producer.sequence.checked_add(1) else {
                        self.blocked = Some(pending);
                        self.transition(
                            RoomLifecycle::Unavailable,
                            Some("producer sequence exhausted during reconciliation".into()),
                        );
                        return false;
                    };
                    self.producer_sequence = next_sequence;
                    self.history = history;
                    self.doc = doc;
                    tracing::info!(
                        stream = %self.stream,
                        producer_id = %pending.producer.id,
                        producer_epoch = pending.producer.epoch,
                        producer_sequence = pending.producer.sequence,
                        ?proof,
                        reconciliation = "reloaded_from_ursula",
                        "ambiguous append reconciled"
                    );
                    self.transition(RoomLifecycle::Ready, None);
                    true
                }
                Err(failure) => {
                    self.blocked = Some(pending);
                    self.transition(failure.state, Some(failure.message));
                    false
                }
            },
            Err(AppendFailure::DefinitelyRejected { kind, message }) => {
                self.blocked = Some(pending);
                self.transition(
                    RoomLifecycle::Unavailable,
                    Some(format!(
                        "ambiguous append retry was definitely rejected ({kind:?}): {message}"
                    )),
                );
                false
            }
            Err(AppendFailure::OutcomeUnknown(error)) => {
                self.blocked = Some(pending);
                self.transition(RoomLifecycle::AppendAmbiguous, Some(error.to_string()));
                false
            }
        }
    }

    async fn resolve_append(
        &self,
        pending: &PendingAppend,
    ) -> Result<DurableAppend, AppendFailure> {
        let mut attempts = 0_usize;
        loop {
            tracing::info!(
                stream = %self.stream,
                active_delta_generation = self.active_delta_generation.value(),
                active_delta_end_offset = self.active_delta_end_offset,
                producer_id = %pending.producer.id,
                producer_epoch = pending.producer.epoch,
                producer_sequence = pending.producer.sequence,
                retry = attempts,
                frame_bytes = pending.bytes.len(),
                "submitting Ursula append"
            );
            match self
                .store
                .append(&pending.target_stream, &pending.producer, &pending.bytes)
                .await
            {
                Ok(AppendOutcome::Committed { next_offset }) => {
                    tracing::info!(
                        stream = %self.stream,
                        producer_id = %pending.producer.id,
                        producer_epoch = pending.producer.epoch,
                        producer_sequence = pending.producer.sequence,
                        retry = attempts,
                        outcome = "committed",
                        next_offset,
                        "Ursula append resolved"
                    );
                    return Ok(DurableAppend::Committed { next_offset });
                }
                Ok(AppendOutcome::Duplicate { next_offset }) => {
                    let frame_len = pending.bytes.len();
                    let frame_len_u64 = u64::try_from(frame_len).map_err(|_| {
                        AppendFailure::OutcomeUnknown(StoreError::Integrity(
                            "frame length does not fit u64".into(),
                        ))
                    })?;
                    let start = next_offset.checked_sub(frame_len_u64).ok_or_else(|| {
                        AppendFailure::OutcomeUnknown(StoreError::Integrity(
                            "duplicate next offset is smaller than retry frame".into(),
                        ))
                    })?;
                    tracing::info!(
                        stream = %self.stream,
                        producer_id = %pending.producer.id,
                        producer_epoch = pending.producer.epoch,
                        producer_sequence = pending.producer.sequence,
                        retry = attempts,
                        outcome = "duplicate",
                        range_start = start,
                        range_end = next_offset,
                        "verifying deduplicated append range"
                    );
                    let stored = self
                        .store
                        .read_range(&pending.target_stream, start, frame_len)
                        .await
                        .map_err(AppendFailure::OutcomeUnknown)?;
                    verify_duplicate_bytes(pending, &stored, self.config.frame_limits)
                        .map_err(AppendFailure::OutcomeUnknown)?;
                    tracing::info!(
                        stream = %pending.target_stream,                        producer_id = %pending.producer.id,
                        producer_epoch = pending.producer.epoch,
                        producer_sequence = pending.producer.sequence,
                        retry = attempts,
                        outcome = "verified_duplicate",
                        range_start = start,
                        range_end = next_offset,
                        reconciliation = "exact_frame_match",
                        "Ursula append resolved"
                    );
                    return Ok(DurableAppend::VerifiedDuplicate { next_offset });
                }
                Err(error @ StoreError::Ambiguous(_)) => {
                    if attempts >= self.config.ambiguous_retries {
                        return Err(AppendFailure::OutcomeUnknown(error));
                    }
                    attempts = attempts.saturating_add(1);
                    tokio::time::sleep(self.config.retry_delay).await;
                }
                Err(StoreError::Rejected { kind, message }) => {
                    return Err(AppendFailure::DefinitelyRejected { kind, message });
                }
                Err(error) => return Err(AppendFailure::OutcomeUnknown(error)),
            }
        }
    }
}

fn verify_duplicate_bytes(
    pending: &PendingAppend,
    stored: &[u8],
    limits: FrameLimits,
) -> Result<(), StoreError> {
    if stored.len() != pending.bytes.len() {
        return Err(StoreError::Integrity(format!(
            "duplicate range length mismatch: expected {}, received {}",
            pending.bytes.len(),
            stored.len()
        )));
    }
    if stored != pending.bytes {
        return Err(StoreError::Integrity(
            "deduplicated tuple is bound to different frame bytes".into(),
        ));
    }
    let stored_frame = DeltaFrame::decode_exact(stored, limits)
        .map_err(|error| StoreError::Integrity(error.to_string()))?;
    if stored_frame.producer != pending.producer {
        return Err(StoreError::Integrity(
            "deduplicated committed range has wrong producer tuple".into(),
        ));
    }
    if stored_frame != pending.frame {
        return Err(StoreError::Integrity(
            "deduplicated committed range has different frame fields".into(),
        ));
    }
    Ok(())
}

fn initialization_store_failure(error: StoreError) -> InitializationFailure {
    let state = if matches!(error, StoreError::Integrity(_)) {
        RoomLifecycle::Corrupt
    } else {
        RoomLifecycle::Unavailable
    };
    InitializationFailure {
        state,
        message: error.to_string(),
    }
}
fn manifest_recovery_failure(error: RecoveryError) -> InitializationFailure {
    let state = match &error {
        RecoveryError::Store(StoreError::Integrity(_)) => RoomLifecycle::Corrupt,
        RecoveryError::Store(_) => RoomLifecycle::Unavailable,
        _ => RoomLifecycle::Corrupt,
    };

    InitializationFailure {
        state,
        message: error.to_string(),
    }
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn validate_update_blobs(updates: &[Vec<u8>], allow_snapshot: bool) -> Result<(), String> {
    if updates.is_empty() {
        return Err("DocUpdate contains no updates".into());
    }
    for update in updates {
        if update.is_empty() {
            return Err("DocUpdate contains an empty update".into());
        }
        let metadata =
            LoroDoc::decode_import_blob_meta(update, true).map_err(|error| error.to_string())?;
        match metadata.mode {
            EncodedBlobMode::Snapshot if allow_snapshot && updates.len() == 1 => {}
            EncodedBlobMode::Snapshot => {
                return Err(
                    "full snapshots are accepted only as the sole blob in an empty room".into(),
                );
            }
            EncodedBlobMode::Updates => {}
            EncodedBlobMode::ShallowSnapshot => {
                return Err("shallow snapshots are not supported".into());
            }
            EncodedBlobMode::OutdatedSnapshot | EncodedBlobMode::OutdatedRle => {
                return Err("outdated Loro encodings are not supported".into());
            }
        }
    }
    Ok(())
}

struct ReplayOutcome {
    doc: LoroDoc,
    has_pending: bool,
}
fn build_candidate(
    current_doc: &LoroDoc,
    retained_history: &[Vec<u8>],
    new_updates: &[Vec<u8>],
) -> Result<ReplayOutcome, String> {
    let snapshot = current_doc
        .export(ExportMode::Snapshot)
        .map_err(|error| error.to_string())?;

    let candidate = LoroDoc::from_snapshot(&snapshot).map_err(|error| error.to_string())?;

    let mut updates = Vec::with_capacity(retained_history.len() + new_updates.len());

    updates.extend(retained_history.iter().cloned());
    updates.extend(new_updates.iter().cloned());

    let has_pending = if updates.is_empty() {
        false
    } else {
        candidate
            .import_batch(&updates)
            .map_err(|error| error.to_string())?
            .pending
            .is_some()
    };

    Ok(ReplayOutcome {
        doc: candidate,
        has_pending,
    })
}

fn replay(updates: &[Vec<u8>]) -> Result<ReplayOutcome, String> {
    let doc = LoroDoc::new();

    let has_pending = if updates.is_empty() {
        false
    } else {
        doc.import_batch(updates)
            .map_err(|error| error.to_string())?
            .pending
            .is_some()
    };

    Ok(ReplayOutcome { doc, has_pending })
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

fn send_durable_ack(peer: &PeerSender, room_id: &str, batch_id: BatchId, proof: DurableAppend) {
    match proof {
        DurableAppend::Committed { next_offset } => {
            tracing::info!(room = %room_id, producer_next_offset = next_offset, resolution = "committed", "durable update acknowledged");
        }
        DurableAppend::VerifiedDuplicate { next_offset } => {
            tracing::info!(room = %room_id, producer_next_offset = next_offset, resolution = "verified_duplicate", "durable update acknowledged");
        }
    }
    send_ack(peer, room_id, batch_id, UpdateStatusCode::Ok);
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

#[cfg(feature = "crash-injection")]
fn crash_after_commit_for_test() {
    if std::env::var_os("QLG_TEST_CRASH_AFTER_COMMIT").is_some() {
        std::process::abort();
    }
}
