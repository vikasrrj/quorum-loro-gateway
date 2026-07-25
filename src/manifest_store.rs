use crate::exact_append::ExactAppendOutcome;
use crate::exact_append::append_exact_with_retry;
use crate::frame::ProducerTuple;
use crate::manifest::GENESIS_DIGEST;
use crate::manifest::ManifestError;
use crate::manifest::ManifestLimits;
use crate::manifest::ManifestRecord;
use crate::manifest::decode_manifest_stream;
use crate::manifest::validate_manifest_chain;
use crate::names::document_hash;
use crate::names::manifest_stream;
use crate::ursula::StoreError;
use crate::ursula::UrsulaStore;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestWriteOutcome {
    Committed { stream: String, next_offset: u64 },
    VerifiedDuplicate { stream: String, next_offset: u64 },
    VerifiedExisting { stream: String, next_offset: u64 },
}

pub async fn publish_manifest_record(
    store: &dyn UrsulaStore,
    room_id: &str,
    record: &ManifestRecord,
    max_ambiguous_retries: usize,
) -> Result<ManifestWriteOutcome, ManifestWriteError> {
    if !record.belongs_to_room(room_id) {
        return Err(ManifestWriteError::WrongRoom);
    }

    let encoded = record.encode()?;
    let stream = manifest_stream(room_id).physical;

    store.ensure_stream(&stream).await?;

    let existing = store.read_all(&stream).await?;
    let existing_len =
        u64::try_from(existing.len()).map_err(|_| ManifestWriteError::LengthOverflow)?;

    let mut records = if existing.is_empty() {
        if record.revision != 0 {
            return Err(ManifestWriteError::UnexpectedRevision {
                expected: 0,
                actual: record.revision,
            });
        }

        if record.previous_record_digest != GENESIS_DIGEST {
            return Err(ManifestWriteError::PreviousDigestMismatch);
        }

        Vec::new()
    } else {
        let records = decode_manifest_stream(&existing, ManifestLimits::default())?;
        let latest = validate_manifest_chain(&records, room_id)?;

        if latest == record && existing.ends_with(&encoded) {
            return Ok(ManifestWriteOutcome::VerifiedExisting {
                stream,
                next_offset: existing_len,
            });
        }

        let expected_revision = latest
            .revision
            .checked_add(1)
            .ok_or(ManifestWriteError::RevisionOverflow)?;

        if record.revision != expected_revision {
            return Err(ManifestWriteError::UnexpectedRevision {
                expected: expected_revision,
                actual: record.revision,
            });
        }

        if record.previous_record_digest != latest.digest {
            return Err(ManifestWriteError::PreviousDigestMismatch);
        }

        records
    };

    records.push(record.clone());
    validate_manifest_chain(&records, room_id)?;

    let producer = ProducerTuple {
        id: manifest_producer_id(room_id),
        epoch: 0,
        sequence: record.revision,
    };

    let appended =
        append_exact_with_retry(store, &stream, &producer, &encoded, max_ambiguous_retries).await?;

    let encoded_len =
        u64::try_from(encoded.len()).map_err(|_| ManifestWriteError::LengthOverflow)?;
    let expected_end = existing_len
        .checked_add(encoded_len)
        .ok_or(ManifestWriteError::LengthOverflow)?;

    let next_offset = match appended {
        ExactAppendOutcome::Committed { next_offset }
        | ExactAppendOutcome::VerifiedDuplicate { next_offset } => next_offset,
    };

    if next_offset != expected_end {
        return Err(ManifestWriteError::UnexpectedEndOffset {
            expected: expected_end,
            actual: next_offset,
        });
    }

    Ok(match appended {
        ExactAppendOutcome::Committed { .. } => ManifestWriteOutcome::Committed {
            stream,
            next_offset,
        },
        ExactAppendOutcome::VerifiedDuplicate { .. } => ManifestWriteOutcome::VerifiedDuplicate {
            stream,
            next_offset,
        },
    })
}

fn manifest_producer_id(room_id: &str) -> String {
    format!("qlg-manifest-{}", document_hash(room_id))
}

#[derive(Debug, Error)]
pub enum ManifestWriteError {
    #[error("failed to encode or validate manifest: {0}")]
    Manifest(#[from] ManifestError),

    #[error("manifest store operation failed: {0}")]
    Store(#[from] StoreError),

    #[error("manifest record belongs to a different room")]
    WrongRoom,

    #[error("unexpected manifest revision: expected {expected}, found {actual}")]
    UnexpectedRevision { expected: u64, actual: u64 },

    #[error("manifest previous-record digest does not match")]
    PreviousDigestMismatch,

    #[error("manifest revision overflow")]
    RevisionOverflow,

    #[error("manifest length does not fit or overflows u64")]
    LengthOverflow,

    #[error("manifest append ended at unexpected offset: expected {expected}, found {actual}")]
    UnexpectedEndOffset { expected: u64, actual: u64 },
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

            if let Some((sequence, _)) = state.producers.get(&key)
                && producer.sequence != sequence.saturating_add(1)
            {
                return Err(StoreError::Rejected {
                    kind: crate::ursula::RejectionKind::Conflict,
                    message: "test producer sequence gap".into(),
                });
            }

            if !state.producers.contains_key(&key) && producer.sequence != 0 {
                return Err(StoreError::Rejected {
                    kind: crate::ursula::RejectionKind::Conflict,
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

    fn record(
        room_id: &str,
        revision: u64,
        previous: [u8; 32],
        checkpoint_generation: u64,
        active_delta_generation: u64,
        checkpoint_bytes: &[u8],
    ) -> ManifestRecord {
        ManifestRecord::new(
            room_id,
            revision,
            previous,
            checkpoint_generation,
            checkpoint_bytes,
            active_delta_generation,
        )
        .expect("create manifest record")
    }

    #[tokio::test]
    async fn publishes_genesis_manifest_record() {
        let store = MemoryStore::default();
        let record = record("room-a", 0, GENESIS_DIGEST, 0, 1, b"checkpoint-zero");

        let outcome = publish_manifest_record(&store, "room-a", &record, 1)
            .await
            .expect("publish genesis manifest");

        let encoded = record.encode().expect("encode manifest");
        let stream = manifest_stream("room-a").physical;

        assert_eq!(store.stream(&stream), encoded);
        assert_eq!(
            outcome,
            ManifestWriteOutcome::Committed {
                stream,
                next_offset: u64::try_from(encoded.len()).expect("manifest length fits u64"),
            }
        );
    }

    #[tokio::test]
    async fn publishes_next_chained_manifest_record() {
        let store = MemoryStore::default();

        let first = record("room-a", 0, GENESIS_DIGEST, 0, 1, b"checkpoint-zero");

        publish_manifest_record(&store, "room-a", &first, 1)
            .await
            .expect("publish first manifest");

        let second = record("room-a", 1, first.digest, 1, 2, b"checkpoint-one");

        publish_manifest_record(&store, "room-a", &second, 1)
            .await
            .expect("publish second manifest");

        let stream = manifest_stream("room-a").physical;
        let records = decode_manifest_stream(&store.stream(&stream), ManifestLimits::default())
            .expect("decode manifest stream");

        let latest = validate_manifest_chain(&records, "room-a").expect("validate manifest chain");

        assert_eq!(latest, &second);
    }

    #[tokio::test]
    async fn verifies_existing_identical_latest_record() {
        let store = MemoryStore::default();

        let record = record("room-a", 0, GENESIS_DIGEST, 0, 1, b"checkpoint-zero");

        let encoded = record.encode().expect("encode manifest");
        let stream = manifest_stream("room-a").physical;
        store.set_stream(&stream, encoded.clone());

        let outcome = publish_manifest_record(&store, "room-a", &record, 1)
            .await
            .expect("verify existing manifest");

        assert_eq!(
            outcome,
            ManifestWriteOutcome::VerifiedExisting {
                stream,
                next_offset: u64::try_from(encoded.len()).expect("manifest length fits u64"),
            }
        );
    }

    #[tokio::test]
    async fn wrong_previous_digest_fails_closed() {
        let store = MemoryStore::default();

        let first = record("room-a", 0, GENESIS_DIGEST, 0, 1, b"checkpoint-zero");

        publish_manifest_record(&store, "room-a", &first, 1)
            .await
            .expect("publish first manifest");

        let second = record("room-a", 1, [9; 32], 1, 2, b"checkpoint-one");

        let error = publish_manifest_record(&store, "room-a", &second, 1)
            .await
            .expect_err("wrong chain must fail");

        assert!(matches!(error, ManifestWriteError::PreviousDigestMismatch));
    }
}
