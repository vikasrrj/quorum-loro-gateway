use std::collections::HashMap;
use std::env;
use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use loro::ExportMode;
use loro::LoroDoc;
use loro_protocol::BatchId;
use quorum_loro_gateway::checkpoint::build_checkpoint_record;
use quorum_loro_gateway::frame::DeltaFrame;
use quorum_loro_gateway::frame::FrameLimits;
use quorum_loro_gateway::frame::ProducerTuple;
use quorum_loro_gateway::frame::decode_all_with_limits;
use quorum_loro_gateway::manifest::GENESIS_DIGEST;
use quorum_loro_gateway::manifest::ManifestRecord;
use quorum_loro_gateway::names::GenerationId;
use quorum_loro_gateway::names::checkpoint_stream;
use quorum_loro_gateway::names::delta_stream_for_generation;
use quorum_loro_gateway::names::manifest_stream;
use quorum_loro_gateway::recovery::recover_from_manifest;
use quorum_loro_gateway::ursula::AppendOutcome;
use quorum_loro_gateway::ursula::StoreError;
use quorum_loro_gateway::ursula::UrsulaStore;

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
            .map_err(|_| StoreError::Integrity("benchmark offset does not fit usize".into()))?;

        let end = start
            .checked_add(length)
            .ok_or_else(|| StoreError::Integrity("benchmark read range overflow".into()))?;

        self.streams
            .lock()
            .expect("memory store lock")
            .get(stream)
            .and_then(|bytes| bytes.get(start..end))
            .map(ToOwned::to_owned)
            .ok_or_else(|| StoreError::Integrity("benchmark read range is absent".into()))
    }

    async fn append(
        &self,
        _stream: &str,
        _producer: &ProducerTuple,
        _body: &[u8],
    ) -> Result<AppendOutcome, StoreError> {
        Err(StoreError::Integrity(
            "append is not used by this benchmark".into(),
        ))
    }
}

fn configured_count(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn generate_updates(count: usize) -> Vec<Vec<u8>> {
    let doc = LoroDoc::new();
    doc.set_peer_id(7_001).expect("set benchmark peer");

    let text = doc.get_text("text");
    let mut updates = Vec::with_capacity(count);

    for index in 0..count {
        let before = doc.oplog_vv();

        text.insert(index, "x").expect("insert benchmark character");
        doc.commit();

        updates.push(
            doc.export(ExportMode::updates(&before))
                .expect("export incremental update"),
        );
    }

    updates
}

fn encode_frames(producer_id: &str, updates: &[Vec<u8>]) -> Vec<u8> {
    let mut stream = Vec::new();

    for (index, update) in updates.iter().enumerate() {
        let sequence = u64::try_from(index).expect("benchmark sequence fits u64");

        let frame = DeltaFrame::new(
            ProducerTuple {
                id: producer_id.to_owned(),
                epoch: 0,
                sequence,
            },
            BatchId(sequence.to_be_bytes()),
            vec![update.clone()],
        );

        stream.extend(
            frame
                .encode_with_limits(FrameLimits::default())
                .expect("encode benchmark frame"),
        );
    }

    stream
}

fn recover_legacy(bytes: &[u8]) -> (LoroDoc, usize) {
    let frames =
        decode_all_with_limits(bytes, FrameLimits::default()).expect("decode legacy frames");

    let mut updates = Vec::with_capacity(frames.len());

    for frame in frames {
        updates.extend(frame.updates);
    }

    let doc = LoroDoc::new();

    if !updates.is_empty() {
        doc.import_batch(&updates).expect("import legacy updates");
    }

    (doc, updates.len())
}

#[tokio::test]
#[ignore = "manual benchmark; run with --ignored --nocapture"]
async fn bounded_recovery_replays_only_active_generation() {
    let total_updates = configured_count("BENCH_TOTAL_UPDATES", 2_000);
    let active_updates = configured_count("BENCH_ACTIVE_UPDATES", 50);

    assert!(
        active_updates > 0 && active_updates < total_updates,
        "active updates must be between zero and total updates"
    );

    let checkpoint_updates = total_updates - active_updates;
    let room_id = "bounded-recovery-benchmark";

    let generation_zero_updates = generate_updates(total_updates);
    let checkpoint_history = &generation_zero_updates[..checkpoint_updates];
    let active_history = &generation_zero_updates[checkpoint_updates..];

    let full_delta_bytes = encode_frames("legacy-benchmark", &generation_zero_updates);
    let sealed_delta_bytes = encode_frames("sealed-benchmark", checkpoint_history);
    let active_delta_bytes = encode_frames("active-benchmark", active_history);

    let checkpoint_doc = LoroDoc::new();
    checkpoint_doc
        .import_batch(checkpoint_history)
        .expect("construct checkpoint document");

    let sealed_delta_end_offset =
        u64::try_from(sealed_delta_bytes.len()).expect("sealed delta length fits u64");

    let checkpoint = build_checkpoint_record(
        room_id,
        0,
        0,
        sealed_delta_end_offset,
        &checkpoint_doc,
        checkpoint_history,
    )
    .expect("build benchmark checkpoint");

    let checkpoint_bytes = checkpoint.encode().expect("encode checkpoint");

    let manifest = ManifestRecord::new(room_id, 0, GENESIS_DIGEST, 0, &checkpoint_bytes, 1)
        .expect("build benchmark manifest");

    let manifest_bytes = manifest.encode().expect("encode manifest");

    let store = MemoryStore::default();

    store.put(
        checkpoint_stream(room_id, GenerationId::ZERO).physical,
        checkpoint_bytes.clone(),
    );
    store.put(
        delta_stream_for_generation(room_id, GenerationId::new(1)).physical,
        active_delta_bytes.clone(),
    );
    store.put(manifest_stream(room_id).physical, manifest_bytes.clone());

    let legacy_started = Instant::now();
    let (legacy_doc, legacy_replayed_updates) = recover_legacy(&full_delta_bytes);
    let legacy_elapsed = legacy_started.elapsed();

    let bounded_started = Instant::now();
    let bounded = recover_from_manifest(&store, room_id, FrameLimits::default())
        .await
        .expect("bounded recovery succeeds")
        .expect("manifest exists");
    let bounded_elapsed = bounded_started.elapsed();

    assert_eq!(legacy_doc.get_text("text").to_string().len(), total_updates);
    assert_eq!(
        bounded.doc.get_text("text").to_string().len(),
        total_updates
    );
    assert_eq!(legacy_replayed_updates, total_updates);
    assert_eq!(bounded.history.len(), active_updates);
    assert!(
        active_delta_bytes.len() < full_delta_bytes.len(),
        "active generation should be smaller than full history"
    );

    println!("bounded recovery benchmark");
    println!("total updates: {total_updates}");
    println!("checkpointed updates: {checkpoint_updates}");
    println!("active delta updates replayed: {active_updates}");
    println!("legacy delta bytes read: {}", full_delta_bytes.len());
    println!("manifest bytes read: {}", manifest_bytes.len());
    println!("checkpoint bytes read: {}", checkpoint_bytes.len());
    println!("active delta bytes read: {}", active_delta_bytes.len());
    println!("legacy full replay elapsed: {:?}", legacy_elapsed);
    println!("bounded recovery elapsed: {:?}", bounded_elapsed);
}
