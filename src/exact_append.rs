use crate::frame::ProducerTuple;
use crate::ursula::AppendOutcome;
use crate::ursula::StoreError;
use crate::ursula::UrsulaStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactAppendOutcome {
    Committed { next_offset: u64 },
    VerifiedDuplicate { next_offset: u64 },
}

pub async fn append_exact_with_retry(
    store: &dyn UrsulaStore,
    stream: &str,
    producer: &ProducerTuple,
    body: &[u8],
    max_ambiguous_retries: usize,
) -> Result<ExactAppendOutcome, StoreError> {
    if body.is_empty() {
        return Err(StoreError::Integrity("exact append body is empty".into()));
    }

    let mut retries = 0_usize;

    loop {
        match store.append(stream, producer, body).await {
            Ok(AppendOutcome::Committed { next_offset }) => {
                return Ok(ExactAppendOutcome::Committed { next_offset });
            }
            Ok(AppendOutcome::Duplicate { next_offset }) => {
                verify_duplicate_bytes(store, stream, next_offset, body).await?;

                return Ok(ExactAppendOutcome::VerifiedDuplicate { next_offset });
            }
            Err(StoreError::Ambiguous(_)) if retries < max_ambiguous_retries => {
                retries = retries.saturating_add(1);
                tracing::warn!(
                    stream,
                    producer_id = %producer.id,
                    producer_epoch = producer.epoch,
                    producer_sequence = producer.sequence,
                    retries,
                    "retrying ambiguous exact append"
                );
            }
            Err(error) => return Err(error),
        }
    }
}

async fn verify_duplicate_bytes(
    store: &dyn UrsulaStore,
    stream: &str,
    next_offset: u64,
    expected: &[u8],
) -> Result<(), StoreError> {
    let expected_len = u64::try_from(expected.len())
        .map_err(|_| StoreError::Integrity("exact append length does not fit u64".into()))?;

    let start = next_offset.checked_sub(expected_len).ok_or_else(|| {
        StoreError::Integrity("duplicate next offset is smaller than exact append body".into())
    })?;

    let stored = store.read_range(stream, start, expected.len()).await?;

    if stored.len() != expected.len() {
        return Err(StoreError::Integrity(format!(
            "duplicate exact append length mismatch: expected {}, received {}",
            expected.len(),
            stored.len(),
        )));
    }

    if stored != expected {
        return Err(StoreError::Integrity(
            "duplicate producer tuple is bound to different bytes".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;

    #[derive(Debug, Clone, Copy)]
    enum Behavior {
        Commit,
        CommitThenAmbiguous,
        Ambiguous,
    }

    #[derive(Default)]
    struct FakeState {
        streams: HashMap<String, Vec<u8>>,
        producers: HashMap<(String, String), (u64, u64)>,
        behaviors: VecDeque<Behavior>,
    }

    #[derive(Default)]
    struct FakeStore {
        state: Mutex<FakeState>,
    }

    impl FakeStore {
        fn with_behaviors(behaviors: impl IntoIterator<Item = Behavior>) -> Self {
            Self {
                state: Mutex::new(FakeState {
                    behaviors: behaviors.into_iter().collect(),
                    ..FakeState::default()
                }),
            }
        }

        fn set_duplicate(&self, stream: &str, producer: &ProducerTuple, stored: Vec<u8>) {
            let mut state = self.state.lock().expect("fake store lock");
            let next_offset = u64::try_from(stored.len()).expect("stored length fits u64");

            state.streams.insert(stream.to_owned(), stored);
            state.producers.insert(
                (stream.to_owned(), producer.id.clone()),
                (producer.sequence, next_offset),
            );
        }
    }

    #[async_trait]
    impl UrsulaStore for FakeStore {
        async fn ensure_stream(&self, stream: &str) -> Result<(), StoreError> {
            self.state
                .lock()
                .expect("fake store lock")
                .streams
                .entry(stream.to_owned())
                .or_default();

            Ok(())
        }

        async fn read_all(&self, stream: &str) -> Result<Vec<u8>, StoreError> {
            Ok(self
                .state
                .lock()
                .expect("fake store lock")
                .streams
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
                .map_err(|_| StoreError::Integrity("fake offset does not fit usize".into()))?;

            let end = start
                .checked_add(length)
                .ok_or_else(|| StoreError::Integrity("fake read range overflow".into()))?;

            self.state
                .lock()
                .expect("fake store lock")
                .streams
                .get(stream)
                .and_then(|bytes| bytes.get(start..end))
                .map(ToOwned::to_owned)
                .ok_or_else(|| StoreError::Integrity("fake read range is absent".into()))
        }

        async fn append(
            &self,
            stream: &str,
            producer: &ProducerTuple,
            body: &[u8],
        ) -> Result<AppendOutcome, StoreError> {
            let mut state = self.state.lock().expect("fake store lock");
            let key = (stream.to_owned(), producer.id.clone());

            if let Some((sequence, next_offset)) = state.producers.get(&key)
                && producer.sequence <= *sequence
            {
                return Ok(AppendOutcome::Duplicate {
                    next_offset: *next_offset,
                });
            }

            let behavior = state.behaviors.pop_front().unwrap_or(Behavior::Commit);

            match behavior {
                Behavior::Commit => {
                    let next_offset = commit(&mut state, stream, producer, body)?;
                    Ok(AppendOutcome::Committed { next_offset })
                }
                Behavior::CommitThenAmbiguous => {
                    commit(&mut state, stream, producer, body)?;
                    Err(StoreError::Ambiguous("commit outcome was lost".into()))
                }
                Behavior::Ambiguous => {
                    Err(StoreError::Ambiguous("append outcome is unknown".into()))
                }
            }
        }
    }

    fn commit(
        state: &mut FakeState,
        stream: &str,
        producer: &ProducerTuple,
        body: &[u8],
    ) -> Result<u64, StoreError> {
        let bytes = state.streams.entry(stream.to_owned()).or_default();
        bytes.extend_from_slice(body);

        let next_offset = u64::try_from(bytes.len())
            .map_err(|_| StoreError::Integrity("fake stream length does not fit u64".into()))?;

        state.producers.insert(
            (stream.to_owned(), producer.id.clone()),
            (producer.sequence, next_offset),
        );

        Ok(next_offset)
    }

    fn producer() -> ProducerTuple {
        ProducerTuple {
            id: "exact-producer".into(),
            epoch: 0,
            sequence: 0,
        }
    }

    #[tokio::test]
    async fn committed_append_returns_committed_proof() {
        let store = FakeStore::default();

        let outcome =
            append_exact_with_retry(&store, "checkpoint", &producer(), b"checkpoint-bytes", 1)
                .await
                .expect("exact append");

        assert_eq!(outcome, ExactAppendOutcome::Committed { next_offset: 16 });
    }

    #[tokio::test]
    async fn ambiguous_commit_is_verified_as_exact_duplicate() {
        let store = FakeStore::with_behaviors([Behavior::CommitThenAmbiguous]);

        let outcome =
            append_exact_with_retry(&store, "checkpoint", &producer(), b"checkpoint-bytes", 1)
                .await
                .expect("resolve ambiguous append");

        assert_eq!(
            outcome,
            ExactAppendOutcome::VerifiedDuplicate { next_offset: 16 }
        );
    }

    #[tokio::test]
    async fn duplicate_with_different_bytes_fails_closed() {
        let store = FakeStore::default();
        let producer = producer();

        store.set_duplicate("checkpoint", &producer, b"different-bytes".to_vec());

        let error =
            append_exact_with_retry(&store, "checkpoint", &producer, b"checkpoint-bytes", 0)
                .await
                .expect_err("mismatched duplicate must fail");

        assert!(matches!(error, StoreError::Integrity(_)));
    }

    #[tokio::test]
    async fn unresolved_ambiguity_stops_after_retry_budget() {
        let store = FakeStore::with_behaviors([Behavior::Ambiguous, Behavior::Ambiguous]);

        let error =
            append_exact_with_retry(&store, "checkpoint", &producer(), b"checkpoint-bytes", 1)
                .await
                .expect_err("ambiguity should remain unresolved");

        assert!(matches!(error, StoreError::Ambiguous(_)));
    }
}
