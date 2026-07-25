use loro::LoroDoc;
use thiserror::Error;

use crate::checkpoint::CheckpointBuildError;
use crate::checkpoint::CheckpointRecord;
use crate::checkpoint::build_checkpoint_record;
use crate::checkpoint_store::CheckpointWriteError;
use crate::checkpoint_store::persist_checkpoint;
use crate::manifest::GENESIS_DIGEST;
use crate::manifest::ManifestError;
use crate::manifest::ManifestLimits;
use crate::manifest::ManifestRecord;
use crate::manifest::decode_manifest_stream;
use crate::manifest::validate_manifest_chain;
use crate::manifest_store::ManifestWriteError;
use crate::manifest_store::publish_manifest_record;
use crate::names::GenerationId;
use crate::names::delta_stream_for_generation;
use crate::names::manifest_stream;
use crate::ursula::StoreError;
use crate::ursula::UrsulaStore;

#[derive(Debug)]
pub struct RotationResult {
    pub checkpoint: CheckpointRecord,
    pub manifest: ManifestRecord,
    pub next_delta_generation: GenerationId,
    pub next_delta_stream: String,
    pub retained_history: Vec<Vec<u8>>,
}

pub async fn rotate_room(
    store: &dyn UrsulaStore,
    room_id: &str,
    current_delta_generation: GenerationId,
    current_delta_end_offset: u64,
    doc: &LoroDoc,
    history: &[Vec<u8>],
    max_ambiguous_retries: usize,
) -> Result<RotationResult, RotationError> {
    let next_delta_generation = current_delta_generation
        .checked_next()
        .ok_or(RotationError::GenerationOverflow)?;

    let checkpoint = build_checkpoint_record(
        room_id,
        current_delta_generation.value(),
        current_delta_generation.value(),
        current_delta_end_offset,
        doc,
        history,
    )?;

    let checkpoint_bytes = checkpoint.encode()?;

    persist_checkpoint(
        store,
        room_id,
        current_delta_generation,
        &checkpoint,
        max_ambiguous_retries,
    )
    .await?;

    let next_delta_stream = delta_stream_for_generation(room_id, next_delta_generation).physical;

    store.ensure_stream(&next_delta_stream).await?;

    let next_delta_bytes = store.read_all(&next_delta_stream).await?;
    if !next_delta_bytes.is_empty() {
        return Err(RotationError::NextDeltaNotEmpty {
            generation: next_delta_generation.value(),
        });
    }

    let (revision, previous_record_digest) =
        next_manifest_position(store, room_id, current_delta_generation).await?;

    let manifest = ManifestRecord::new(
        room_id,
        revision,
        previous_record_digest,
        current_delta_generation.value(),
        &checkpoint_bytes,
        next_delta_generation.value(),
    )?;

    publish_manifest_record(store, room_id, &manifest, max_ambiguous_retries).await?;

    Ok(RotationResult {
        retained_history: checkpoint.pending_updates.clone(),
        checkpoint,
        manifest,
        next_delta_generation,
        next_delta_stream,
    })
}

async fn next_manifest_position(
    store: &dyn UrsulaStore,
    room_id: &str,
    current_delta_generation: GenerationId,
) -> Result<(u64, [u8; 32]), RotationError> {
    let stream = manifest_stream(room_id).physical;

    store.ensure_stream(&stream).await?;
    let bytes = store.read_all(&stream).await?;

    if bytes.is_empty() {
        if current_delta_generation != GenerationId::ZERO {
            return Err(RotationError::ManifestActiveGenerationMismatch {
                manifest: None,
                actor: current_delta_generation.value(),
            });
        }

        return Ok((0, GENESIS_DIGEST));
    }

    let records = decode_manifest_stream(&bytes, ManifestLimits::default())?;
    let latest = validate_manifest_chain(&records, room_id)?;

    if latest.active_delta_generation != current_delta_generation.value() {
        return Err(RotationError::ManifestActiveGenerationMismatch {
            manifest: Some(latest.active_delta_generation),
            actor: current_delta_generation.value(),
        });
    }

    let revision = latest
        .revision
        .checked_add(1)
        .ok_or(RotationError::RevisionOverflow)?;

    Ok((revision, latest.digest))
}

#[derive(Debug, Error)]
pub enum RotationError {
    #[error("checkpoint construction failed: {0}")]
    CheckpointBuild(#[from] CheckpointBuildError),

    #[error("checkpoint encoding failed: {0}")]
    CheckpointEncode(#[from] crate::checkpoint::CheckpointError),

    #[error("checkpoint persistence failed: {0}")]
    CheckpointWrite(#[from] CheckpointWriteError),

    #[error("manifest construction or validation failed: {0}")]
    Manifest(#[from] ManifestError),

    #[error("manifest publication failed: {0}")]
    ManifestWrite(#[from] ManifestWriteError),

    #[error("rotation store operation failed: {0}")]
    Store(#[from] StoreError),

    #[error("delta generation overflow")]
    GenerationOverflow,

    #[error("manifest revision overflow")]
    RevisionOverflow,

    #[error("next delta generation {generation} already contains bytes")]
    NextDeltaNotEmpty { generation: u64 },

    #[error("manifest active generation mismatch: manifest {manifest:?}, actor {actor}")]
    ManifestActiveGenerationMismatch { manifest: Option<u64>, actor: u64 },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use loro::ExportMode;

    use super::*;
    use crate::frame::ProducerTuple;
    use crate::names::checkpoint_stream;
    use crate::ursula::AppendOutcome;
    use crate::ursula::RejectionKind;

    #[derive(Default)]
    struct MemoryStore {
        state: Mutex<State>,
    }

    #[derive(Default)]
    struct State {
        streams: HashMap<String, Vec<u8>>,
        producers: HashMap<(String, String), (u64, u64)>,
    }

    impl MemoryStore {
        fn set_stream(&self, stream: String, bytes: Vec<u8>) {
            self.state
                .lock()
                .expect("memory store lock")
                .streams
                .insert(stream, bytes);
        }

        fn stream(&self, stream: &str) -> Vec<u8> {
            self.state
                .lock()
                .expect("memory store lock")
                .streams
                .get(stream)
                .cloned()
                .unwrap_or_default()
        }
    }

    #[async_trait]
    impl UrsulaStore for MemoryStore {
        async fn ensure_stream(&self, stream: &str) -> Result<(), StoreError> {
            self.state
                .lock()
                .expect("memory store lock")
                .streams
                .entry(stream.to_owned())
                .or_default();

            Ok(())
        }

        async fn read_all(&self, stream: &str) -> Result<Vec<u8>, StoreError> {
            Ok(self.stream(stream))
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

            self.state
                .lock()
                .expect("memory store lock")
                .streams
                .get(stream)
                .and_then(|bytes| bytes.get(start..end))
                .map(ToOwned::to_owned)
                .ok_or_else(|| StoreError::Integrity("test read range is absent".into()))
        }

        async fn append(
            &self,
            stream: &str,
            producer: &ProducerTuple,
            body: &[u8],
        ) -> Result<AppendOutcome, StoreError> {
            let mut state = self.state.lock().expect("memory store lock");
            let key = (stream.to_owned(), producer.id.clone());

            if let Some((sequence, next_offset)) = state.producers.get(&key)
                && producer.sequence <= *sequence
            {
                return Ok(AppendOutcome::Duplicate {
                    next_offset: *next_offset,
                });
            }

            if let Some((sequence, _)) = state.producers.get(&key)
                && producer.sequence != sequence.saturating_add(1)
            {
                return Err(StoreError::Rejected {
                    kind: RejectionKind::Conflict,
                    message: "test producer sequence gap".into(),
                });
            }

            if !state.producers.contains_key(&key) && producer.sequence != 0 {
                return Err(StoreError::Rejected {
                    kind: RejectionKind::Conflict,
                    message: "test producer must begin at zero".into(),
                });
            }

            let bytes = state.streams.entry(stream.to_owned()).or_default();
            bytes.extend_from_slice(body);

            let next_offset = u64::try_from(bytes.len())
                .map_err(|_| StoreError::Integrity("test stream length does not fit u64".into()))?;

            state
                .producers
                .insert(key, (producer.sequence, next_offset));

            Ok(AppendOutcome::Committed { next_offset })
        }
    }

    fn live_document() -> (LoroDoc, Vec<Vec<u8>>) {
        let doc = LoroDoc::new();
        doc.set_peer_id(901).expect("set peer");

        let before = doc.oplog_vv();
        doc.get_text("text")
            .insert(0, "durable")
            .expect("insert text");
        doc.commit();

        let update = doc
            .export(ExportMode::updates(&before))
            .expect("export update");

        (doc, vec![update])
    }

    #[tokio::test]
    async fn rotates_legacy_delta_zero_to_generation_one() {
        let store = MemoryStore::default();
        let (doc, history) = live_document();

        let result = rotate_room(&store, "room-a", GenerationId::ZERO, 123, &doc, &history, 1)
            .await
            .expect("rotate room");

        assert_eq!(result.next_delta_generation, GenerationId::new(1));

        assert!(
            store
                .stream(&checkpoint_stream("room-a", GenerationId::ZERO,).physical)
                .starts_with(b"QLGC")
        );

        assert!(
            store
                .stream(&delta_stream_for_generation("room-a", GenerationId::new(1),).physical)
                .is_empty()
        );

        let manifest_bytes = store.stream(&manifest_stream("room-a").physical);
        let records = decode_manifest_stream(&manifest_bytes, ManifestLimits::default())
            .expect("decode manifest");

        let latest = validate_manifest_chain(&records, "room-a").expect("validate manifest");

        assert_eq!(latest.checkpoint_generation, 0);
        assert_eq!(latest.active_delta_generation, 1);
    }

    #[tokio::test]
    async fn second_rotation_extends_manifest_chain() {
        let store = MemoryStore::default();
        let (doc, history) = live_document();

        let first = rotate_room(&store, "room-a", GenerationId::ZERO, 123, &doc, &history, 1)
            .await
            .expect("first rotation");

        let second = rotate_room(
            &store,
            "room-a",
            first.next_delta_generation,
            0,
            &doc,
            &first.retained_history,
            1,
        )
        .await
        .expect("second rotation");

        assert_eq!(second.next_delta_generation, GenerationId::new(2));

        let records = decode_manifest_stream(
            &store.stream(&manifest_stream("room-a").physical),
            ManifestLimits::default(),
        )
        .expect("decode manifest");

        assert_eq!(records.len(), 2);

        let latest = validate_manifest_chain(&records, "room-a").expect("validate manifest");

        assert_eq!(latest.revision, 1);
        assert_eq!(latest.checkpoint_generation, 1);
        assert_eq!(latest.active_delta_generation, 2);
    }

    #[tokio::test]
    async fn nonempty_next_delta_fails_closed() {
        let store = MemoryStore::default();
        let (doc, history) = live_document();

        store.set_stream(
            delta_stream_for_generation("room-a", GenerationId::new(1)).physical,
            b"unexpected".to_vec(),
        );

        let result =
            rotate_room(&store, "room-a", GenerationId::ZERO, 123, &doc, &history, 1).await;

        assert!(matches!(
            result,
            Err(RotationError::NextDeltaNotEmpty { generation: 1 })
        ));
    }
}
