use loro::ExportMode;
use loro::LoroDoc;
use quorum_loro_gateway::checkpoint::CheckpointLimits;
use quorum_loro_gateway::checkpoint::CheckpointRecord;
use quorum_loro_gateway::checkpoint::build_checkpoint_record;

#[test]
fn checkpoint_builder_retains_only_updates_missing_from_snapshot() {
    let source = LoroDoc::new();
    source.set_peer_id(404).expect("set source peer");

    let before_base = source.oplog_vv();

    source
        .get_text("text")
        .insert(0, "base")
        .expect("insert base");
    source.commit();

    let after_base = source.oplog_vv();

    let base_update = source
        .export(ExportMode::updates(&before_base))
        .expect("export base update");

    source
        .get_text("text")
        .insert(4, "-one")
        .expect("insert first dependent update");
    source.commit();

    let after_first = source.oplog_vv();

    let first_update = source
        .export(ExportMode::updates(&after_base))
        .expect("export first dependent update");

    source
        .get_text("text")
        .insert(8, "-two")
        .expect("insert second dependent update");
    source.commit();

    let second_update = source
        .export(ExportMode::updates(&after_first))
        .expect("export second dependent update");

    let live = LoroDoc::new();

    let status = live
        .import_batch(&[base_update.clone(), second_update.clone()])
        .expect("import live history");

    assert!(status.pending.is_some());
    assert_eq!(live.get_text("text").to_string(), "base");

    let record = build_checkpoint_record(
        "checkpoint-builder",
        4,
        3,
        123,
        &live,
        &[base_update, second_update.clone()],
    )
    .expect("build checkpoint record");

    assert_eq!(record.checkpoint_generation, 4);
    assert_eq!(record.source_delta_generation, 3);
    assert_eq!(record.source_delta_end_offset, 123);
    assert_eq!(record.pending_updates, vec![second_update]);

    let encoded = record.encode().expect("encode checkpoint");

    let decoded = CheckpointRecord::decode_exact(&encoded, CheckpointLimits::default())
        .expect("decode checkpoint");

    assert!(decoded.belongs_to_room("checkpoint-builder"));

    let recovered = LoroDoc::from_snapshot(&decoded.snapshot).expect("restore checkpoint snapshot");

    let pending_status = recovered
        .import_batch(&decoded.pending_updates)
        .expect("restore pending updates");

    assert!(pending_status.pending.is_some());

    let resolved_status = recovered
        .import(&first_update)
        .expect("import missing dependency");

    assert!(resolved_status.pending.is_none());
    assert_eq!(recovered.get_text("text").to_string(), "base-one-two");
}
