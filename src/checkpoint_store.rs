use crate::checkpoint::CheckpointError;
use crate::checkpoint::CheckpointRecord;
use crate::exact_append::ExactAppendOutcome;
use crate::exact_append::append_exact_with_retry;
use crate::frame::ProducerTuple;
use crate::names::GenerationId;
use crate::names::checkpoint_stream;
use crate::names::document_hash;
use crate::ursula::StoreError;
use crate::ursula::UrsulaStore;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointWriteOutcome {
    Committed { stream: String, next_offset: u64 },
    VerifiedDuplicate { stream: String, next_offset: u64 },
    VerifiedExisting { stream: String, next_offset: u64 },
}

pub async fn persist_checkpoint(
    store: &dyn UrsulaStore,
    room_id: &str,
    generation: GenerationId,
    record: &CheckpointRecord,
    max_ambiguous_retries: usize,
) -> Result<CheckpointWriteOutcome, CheckpointWriteError> {
    if !record.belongs_to_room(room_id) {
        return Err(CheckpointWriteError::WrongRoom);
    }

    if record.checkpoint_generation != generation.value() {
        return Err(CheckpointWriteError::GenerationMismatch {
            expected: generation.value(),
            actual: record.checkpoint_generation,
        });
    }

    let bytes = record.encode()?;
    let stream = checkpoint_stream(room_id, generation).physical;

    store.ensure_stream(&stream).await?;

    let existing = store.read_all(&stream).await?;
    if !existing.is_empty() {
        if existing != bytes {
            return Err(CheckpointWriteError::ExistingBytesMismatch);
        }

        let next_offset =
            u64::try_from(existing.len()).map_err(|_| CheckpointWriteError::LengthOverflow)?;

        return Ok(CheckpointWriteOutcome::VerifiedExisting {
            stream,
            next_offset,
        });
    }

    let producer = ProducerTuple {
        id: checkpoint_producer_id(room_id, generation),
        epoch: 0,
        sequence: 0,
    };

    let appended =
        append_exact_with_retry(store, &stream, &producer, &bytes, max_ambiguous_retries).await?;

    let next_offset = match appended {
        ExactAppendOutcome::Committed { next_offset }
        | ExactAppendOutcome::VerifiedDuplicate { next_offset } => next_offset,
    };

    let expected_offset =
        u64::try_from(bytes.len()).map_err(|_| CheckpointWriteError::LengthOverflow)?;

    if next_offset != expected_offset {
        return Err(CheckpointWriteError::UnexpectedEndOffset {
            expected: expected_offset,
            actual: next_offset,
        });
    }

    Ok(match appended {
        ExactAppendOutcome::Committed { .. } => CheckpointWriteOutcome::Committed {
            stream,
            next_offset,
        },
        ExactAppendOutcome::VerifiedDuplicate { .. } => CheckpointWriteOutcome::VerifiedDuplicate {
            stream,
            next_offset,
        },
    })
}

fn checkpoint_producer_id(room_id: &str, generation: GenerationId) -> String {
    format!(
        "qlg-checkpoint-{}-{}",
        document_hash(room_id),
        generation.value()
    )
}

#[derive(Debug, Error)]
pub enum CheckpointWriteError {
    #[error("failed to encode checkpoint: {0}")]
    Encode(#[from] CheckpointError),

    #[error("checkpoint store operation failed: {0}")]
    Store(#[from] StoreError),

    #[error("checkpoint record belongs to a different room")]
    WrongRoom,

    #[error("checkpoint generation mismatch: expected {expected}, found {actual}")]
    GenerationMismatch { expected: u64, actual: u64 },

    #[error("checkpoint stream already contains different bytes")]
    ExistingBytesMismatch,

    #[error("checkpoint append ended at unexpected offset: expected {expected}, found {actual}")]
    UnexpectedEndOffset { expected: u64, actual: u64 },

    #[error("checkpoint length does not fit u64")]
    LengthOverflow,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::ursula::AppendOutcome;

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
        fn set_stream(&self, stream: &str, bytes: Vec<u8>) {
            self.state
                .lock()
                .expect("memory store lock")
                .streams
                .insert(stream.to_owned(), bytes);
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

    fn record(room_id: &str, generation: GenerationId) -> CheckpointRecord {
        CheckpointRecord::new(
            room_id,
            generation.value(),
            generation.value().saturating_sub(1),
            123,
            b"snapshot".to_vec(),
            vec![b"pending".to_vec()],
        )
    }

    #[tokio::test]
    async fn writes_checkpoint_to_generation_stream() {
        let store = MemoryStore::default();
        let generation = GenerationId::new(4);
        let record = record("room-a", generation);

        let outcome = persist_checkpoint(&store, "room-a", generation, &record, 1)
            .await
            .expect("persist checkpoint");

        let expected = record.encode().expect("encode checkpoint");
        let stream = checkpoint_stream("room-a", generation).physical;

        assert_eq!(store.stream(&stream), expected);
        assert_eq!(
            outcome,
            CheckpointWriteOutcome::Committed {
                stream,
                next_offset: u64::try_from(expected.len()).expect("checkpoint length fits u64"),
            }
        );
    }

    #[tokio::test]
    async fn verifies_existing_identical_checkpoint() {
        let store = MemoryStore::default();
        let generation = GenerationId::new(4);
        let record = record("room-a", generation);
        let bytes = record.encode().expect("encode checkpoint");
        let stream = checkpoint_stream("room-a", generation).physical;

        store.set_stream(&stream, bytes.clone());

        let outcome = persist_checkpoint(&store, "room-a", generation, &record, 1)
            .await
            .expect("verify existing checkpoint");

        assert_eq!(
            outcome,
            CheckpointWriteOutcome::VerifiedExisting {
                stream,
                next_offset: u64::try_from(bytes.len()).expect("checkpoint length fits u64"),
            }
        );
    }

    #[tokio::test]
    async fn existing_different_checkpoint_fails_closed() {
        let store = MemoryStore::default();
        let generation = GenerationId::new(4);
        let record = record("room-a", generation);
        let stream = checkpoint_stream("room-a", generation).physical;

        store.set_stream(&stream, b"different".to_vec());

        let error = persist_checkpoint(&store, "room-a", generation, &record, 1)
            .await
            .expect_err("different checkpoint must fail");

        assert!(matches!(error, CheckpointWriteError::ExistingBytesMismatch));
    }

    #[tokio::test]
    async fn wrong_generation_is_rejected_before_write() {
        let store = MemoryStore::default();
        let record = record("room-a", GenerationId::new(4));

        let error = persist_checkpoint(&store, "room-a", GenerationId::new(5), &record, 1)
            .await
            .expect_err("wrong generation must fail");

        assert!(matches!(
            error,
            CheckpointWriteError::GenerationMismatch {
                expected: 5,
                actual: 4,
            }
        ));
    }
}
