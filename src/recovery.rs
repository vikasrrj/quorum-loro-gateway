use loro::LoroDoc;
use thiserror::Error;

use crate::checkpoint::CheckpointError;
use crate::checkpoint::CheckpointLimits;
use crate::checkpoint::CheckpointRecord;
use crate::frame::FrameError;
use crate::frame::FrameLimits;
use crate::frame::decode_all_with_limits;
use crate::manifest::ManifestError;
use crate::manifest::ManifestLimits;
use crate::manifest::decode_manifest_stream;
use crate::manifest::validate_manifest_chain;
use crate::names::GenerationId;
use crate::names::checkpoint_stream;
use crate::names::delta_stream_for_generation;
use crate::names::manifest_stream;
use crate::ursula::StoreError;
use crate::ursula::UrsulaStore;

pub struct ManifestRecovery {
    pub doc: LoroDoc,
    pub history: Vec<Vec<u8>>,
    pub checkpoint_generation: GenerationId,
    pub active_delta_generation: GenerationId,
    pub active_delta_stream: String,
    pub active_delta_end_offset: u64,
    pub recovered_checkpoint_bytes: usize,
    pub recovered_delta_bytes: usize,
}

pub async fn recover_from_manifest(
    store: &dyn UrsulaStore,
    room_id: &str,
    frame_limits: FrameLimits,
) -> Result<Option<ManifestRecovery>, RecoveryError> {
    let manifest_name = manifest_stream(room_id).physical;

    store.ensure_stream(&manifest_name).await?;
    let manifest_bytes = store.read_all(&manifest_name).await?;

    if manifest_bytes.is_empty() {
        return Ok(None);
    }

    let records = decode_manifest_stream(&manifest_bytes, ManifestLimits::default())?;
    let latest = validate_manifest_chain(&records, room_id)?;

    let checkpoint_generation = GenerationId::new(latest.checkpoint_generation);
    let checkpoint_name = checkpoint_stream(room_id, checkpoint_generation).physical;
    let checkpoint_bytes = store.read_all(&checkpoint_name).await?;

    if checkpoint_bytes.is_empty() {
        return Err(RecoveryError::MissingCheckpoint {
            generation: checkpoint_generation.value(),
        });
    }

    if !latest.verifies_checkpoint_bytes(&checkpoint_bytes) {
        return Err(RecoveryError::CheckpointManifestMismatch);
    }

    let checkpoint =
        CheckpointRecord::decode_exact(&checkpoint_bytes, CheckpointLimits::default())?;

    validate_checkpoint_link(
        room_id,
        latest.checkpoint_generation,
        latest.active_delta_generation,
        &checkpoint,
    )?;

    let doc = LoroDoc::from_snapshot(&checkpoint.snapshot)
        .map_err(|error| RecoveryError::Loro(error.to_string()))?;

    if !checkpoint.pending_updates.is_empty() {
        doc.import_batch(&checkpoint.pending_updates)
            .map_err(|error| RecoveryError::Loro(error.to_string()))?;
    }

    let active_delta_generation = GenerationId::new(latest.active_delta_generation);
    let active_delta_stream =
        delta_stream_for_generation(room_id, active_delta_generation).physical;

    let delta_bytes = store.read_all(&active_delta_stream).await?;
    let frames = decode_all_with_limits(&delta_bytes, frame_limits)?;

    let mut history = checkpoint.pending_updates.clone();

    for frame in frames {
        history.extend(frame.updates);
    }

    if !history.is_empty() {
        let snapshot_pending_count = checkpoint.pending_updates.len();
        let active_updates = history
            .get(snapshot_pending_count..)
            .ok_or(RecoveryError::HistoryRange)?;

        if !active_updates.is_empty() {
            doc.import_batch(active_updates)
                .map_err(|error| RecoveryError::Loro(error.to_string()))?;
        }
    }

    let active_delta_end_offset =
        u64::try_from(delta_bytes.len()).map_err(|_| RecoveryError::LengthOverflow)?;

    Ok(Some(ManifestRecovery {
        doc,
        history,
        checkpoint_generation,
        active_delta_generation,
        active_delta_stream,
        active_delta_end_offset,
        recovered_checkpoint_bytes: checkpoint_bytes.len(),
        recovered_delta_bytes: delta_bytes.len(),
    }))
}

fn validate_checkpoint_link(
    room_id: &str,
    manifest_checkpoint_generation: u64,
    manifest_active_delta_generation: u64,
    checkpoint: &CheckpointRecord,
) -> Result<(), RecoveryError> {
    if !checkpoint.belongs_to_room(room_id) {
        return Err(RecoveryError::WrongCheckpointRoom);
    }

    if checkpoint.checkpoint_generation != manifest_checkpoint_generation {
        return Err(RecoveryError::CheckpointGenerationMismatch {
            manifest: manifest_checkpoint_generation,
            checkpoint: checkpoint.checkpoint_generation,
        });
    }

    if checkpoint.source_delta_generation != checkpoint.checkpoint_generation {
        return Err(RecoveryError::CheckpointSourceMismatch {
            checkpoint_generation: checkpoint.checkpoint_generation,
            source_delta_generation: checkpoint.source_delta_generation,
        });
    }

    let expected_active = checkpoint
        .checkpoint_generation
        .checked_add(1)
        .ok_or(RecoveryError::GenerationOverflow)?;

    if manifest_active_delta_generation != expected_active {
        return Err(RecoveryError::ActiveDeltaGenerationMismatch {
            expected: expected_active,
            actual: manifest_active_delta_generation,
        });
    }

    Ok(())
}

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("recovery store operation failed: {0}")]
    Store(#[from] StoreError),

    #[error("manifest recovery failed: {0}")]
    Manifest(#[from] ManifestError),

    #[error("checkpoint recovery failed: {0}")]
    Checkpoint(#[from] CheckpointError),

    #[error("active delta recovery failed: {0}")]
    Frame(#[from] FrameError),

    #[error("manifest references missing checkpoint generation {generation}")]
    MissingCheckpoint { generation: u64 },

    #[error("manifest checkpoint digest or length does not match stored bytes")]
    CheckpointManifestMismatch,

    #[error("checkpoint belongs to a different room")]
    WrongCheckpointRoom,

    #[error("checkpoint generation mismatch: manifest {manifest}, checkpoint {checkpoint}")]
    CheckpointGenerationMismatch { manifest: u64, checkpoint: u64 },

    #[error(
        "checkpoint source mismatch: checkpoint generation {checkpoint_generation}, source delta {source_delta_generation}"
    )]
    CheckpointSourceMismatch {
        checkpoint_generation: u64,
        source_delta_generation: u64,
    },

    #[error("active delta generation mismatch: expected {expected}, found {actual}")]
    ActiveDeltaGenerationMismatch { expected: u64, actual: u64 },

    #[error("generation overflow during recovery")]
    GenerationOverflow,

    #[error("recovered stream length does not fit u64")]
    LengthOverflow,

    #[error("recovered history range is invalid")]
    HistoryRange,

    #[error("Loro recovery failed: {0}")]
    Loro(String),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use loro::ExportMode;
    use loro_protocol::BatchId;

    use super::*;
    use crate::checkpoint::build_checkpoint_record;
    use crate::frame::DeltaFrame;
    use crate::frame::ProducerTuple;
    use crate::manifest::GENESIS_DIGEST;
    use crate::manifest::ManifestRecord;
    use crate::ursula::AppendOutcome;

    #[derive(Default)]
    struct MemoryStore {
        streams: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl MemoryStore {
        fn put(&self, stream: String, bytes: Vec<u8>) {
            self.streams
                .lock()
                .expect("memory store lock")
                .insert(stream, bytes);
        }
    }

    #[async_trait]
    impl UrsulaStore for MemoryStore {
        async fn ensure_stream(&self, stream: &str) -> Result<(), StoreError> {
            self.streams
                .lock()
                .expect("memory store lock")
                .entry(stream.to_owned())
                .or_default();

            Ok(())
        }

        async fn read_all(&self, stream: &str) -> Result<Vec<u8>, StoreError> {
            Ok(self
                .streams
                .lock()
                .expect("memory store lock")
                .get(stream)
                .cloned()
                .unwrap_or_default())
        }

        async fn read_range(
            &self,
            stream: &str,
            offset: u64,
            length: usize,
        ) -> Result<Vec<u8>, StoreError> {
            let start = usize::try_from(offset)
                .map_err(|_| StoreError::Integrity("test offset does not fit usize".into()))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| StoreError::Integrity("test read range overflow".into()))?;

            self.streams
                .lock()
                .expect("memory store lock")
                .get(stream)
                .and_then(|bytes| bytes.get(start..end))
                .map(ToOwned::to_owned)
                .ok_or_else(|| StoreError::Integrity("test read range is absent".into()))
        }

        async fn append(
            &self,
            _stream: &str,
            _producer: &ProducerTuple,
            _body: &[u8],
        ) -> Result<AppendOutcome, StoreError> {
            Err(StoreError::Integrity(
                "append is not used by recovery tests".into(),
            ))
        }
    }

    fn build_fixture(room_id: &str) -> (MemoryStore, Vec<u8>, Vec<u8>) {
        let source = LoroDoc::new();
        source.set_peer_id(500).expect("set peer");

        let before_base = source.oplog_vv();
        source
            .get_text("text")
            .insert(0, "base")
            .expect("insert base");
        source.commit();

        let after_base = source.oplog_vv();
        let base_update = source
            .export(ExportMode::updates(&before_base))
            .expect("export base");

        let checkpoint_doc = LoroDoc::new();
        checkpoint_doc.import(&base_update).expect("import base");

        let checkpoint =
            build_checkpoint_record(room_id, 0, 0, 91, &checkpoint_doc, &[base_update])
                .expect("build checkpoint");
        let checkpoint_bytes = checkpoint.encode().expect("encode checkpoint");

        source
            .get_text("text")
            .insert(4, "-next")
            .expect("insert next");
        source.commit();

        let next_update = source
            .export(ExportMode::updates(&after_base))
            .expect("export next");

        let delta = DeltaFrame::new(
            ProducerTuple {
                id: "delta-producer".into(),
                epoch: 0,
                sequence: 0,
            },
            BatchId([7; 8]),
            vec![next_update.clone()],
        );
        let delta_bytes = delta.encode().expect("encode delta");

        let manifest = ManifestRecord::new(room_id, 0, GENESIS_DIGEST, 0, &checkpoint_bytes, 1)
            .expect("build manifest");
        let manifest_bytes = manifest.encode().expect("encode manifest");

        let store = MemoryStore::default();
        store.put(
            checkpoint_stream(room_id, GenerationId::ZERO).physical,
            checkpoint_bytes.clone(),
        );
        store.put(
            delta_stream_for_generation(room_id, GenerationId::new(1)).physical,
            delta_bytes,
        );
        store.put(manifest_stream(room_id).physical, manifest_bytes);

        (store, checkpoint_bytes, next_update)
    }

    #[tokio::test]
    async fn empty_manifest_returns_legacy_fallback() {
        let store = MemoryStore::default();

        let recovered = recover_from_manifest(&store, "legacy-room", FrameLimits::default())
            .await
            .expect("check manifest");

        assert!(recovered.is_none());
    }

    #[tokio::test]
    async fn recovers_checkpoint_and_only_active_delta() {
        let room_id = "bounded-room";
        let (store, checkpoint_bytes, next_update) = build_fixture(room_id);

        let recovered = recover_from_manifest(&store, room_id, FrameLimits::default())
            .await
            .expect("recover manifest room")
            .expect("manifest recovery");

        assert_eq!(recovered.doc.get_text("text").to_string(), "base-next");
        assert_eq!(recovered.active_delta_generation, GenerationId::new(1));
        assert_eq!(recovered.history, vec![next_update]);
        assert_eq!(recovered.recovered_checkpoint_bytes, checkpoint_bytes.len());
    }

    #[tokio::test]
    async fn corrupted_checkpoint_fails_closed() {
        let room_id = "corrupt-room";
        let (store, mut checkpoint_bytes, _next_update) = build_fixture(room_id);

        checkpoint_bytes[0] ^= 0xff;
        store.put(
            checkpoint_stream(room_id, GenerationId::ZERO).physical,
            checkpoint_bytes,
        );

        let result = recover_from_manifest(&store, room_id, FrameLimits::default()).await;

        assert!(result.is_err(), "corrupt checkpoint must fail");

        let error = result.err().expect("error was checked above");
        assert!(matches!(error, RecoveryError::CheckpointManifestMismatch));
    }
}
