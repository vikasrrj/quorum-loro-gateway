use loro::{ExportMode, LoroDoc};

fn new_doc(peer_id: u64) -> LoroDoc {
    let doc = LoroDoc::new();
    doc.set_peer_id(peer_id)
        .expect("checkpoint semantics test operation should succeed");
    doc
}

fn import_without_pending(doc: &LoroDoc, bytes: &[u8]) {
    let status = doc
        .import(bytes)
        .expect("checkpoint semantics test operation should succeed");
    assert!(
        status.pending.is_none(),
        "expected a complete import, but Loro reported missing dependencies"
    );
}

/// A client may remain offline while the server creates a checkpoint.
///
/// When that client returns, its causally old update must still merge correctly.
/// Storage generation age must not become an event-time or causality cutoff.
#[test]
fn checkpoint_retains_only_raw_blobs_still_pending_against_snapshot() {
    let source = LoroDoc::new();
    source.set_peer_id(303).expect("set source peer");

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

    // The base is applied, but the second update remains pending because
    // the first dependent update is missing.
    let live = LoroDoc::new();
    let status = live
        .import_batch(&[base_update.clone(), second_update.clone()])
        .expect("import live history");

    assert!(status.pending.is_some());
    assert_eq!(live.get_text("text").to_string(), "base");

    let snapshot = live
        .export(ExportMode::Snapshot)
        .expect("export checkpoint snapshot");

    let history = vec![base_update.clone(), second_update.clone()];
    let mut retained_pending_blobs: Vec<Vec<u8>> = Vec::new();

    for blob in &history {
        let probe = LoroDoc::from_snapshot(&snapshot).expect("restore checkpoint probe");

        let probe_status = probe.import(blob).expect("probe historical update blob");

        if probe_status.pending.is_some() {
            retained_pending_blobs.push(blob.to_vec());
        }
    }

    assert_eq!(
        retained_pending_blobs,
        vec![second_update.clone()],
        "checkpoint should retain only the still-pending raw blob"
    );

    let recovered = LoroDoc::from_snapshot(&snapshot).expect("restore checkpoint");

    let pending_status = recovered
        .import_batch(&retained_pending_blobs)
        .expect("restore retained pending blobs");

    assert!(pending_status.pending.is_some());

    let resolved_status = recovered
        .import(&first_update)
        .expect("import missing dependency");

    assert!(resolved_status.pending.is_none());
    assert_eq!(recovered.get_text("text").to_string(), "base-one-two");

    let control = LoroDoc::new();
    let control_status = control
        .import_batch(&[base_update, second_update, first_update])
        .expect("build control document");

    assert!(control_status.pending.is_none());
    assert_eq!(recovered.get_deep_value(), control.get_deep_value());
    assert_eq!(recovered.oplog_vv(), control.oplog_vv());
}
#[test]
fn snapshot_accepts_update_from_client_offline_before_checkpoint() {
    // Initial shared state.
    let initial = new_doc(1);
    initial
        .get_text("text")
        .insert(0, "base")
        .expect("checkpoint semantics test operation should succeed");
    initial.commit();

    let initial_updates = initial
        .export(ExportMode::all_updates())
        .expect("checkpoint semantics test operation should succeed");
    let initial_vv = initial.oplog_vv();

    // This client goes offline at the initial version.
    let offline_client = new_doc(2);
    import_without_pending(&offline_client, &initial_updates);

    offline_client
        .get_text("text")
        .insert(4, "-offline")
        .expect("checkpoint semantics test operation should succeed");
    offline_client.commit();

    // Export only the operation created while offline.
    let offline_update = offline_client
        .export(ExportMode::updates(&initial_vv))
        .expect("checkpoint semantics test operation should succeed");

    // Meanwhile, the server receives another update and creates a checkpoint.
    let server = new_doc(3);
    import_without_pending(&server, &initial_updates);

    server
        .get_text("text")
        .insert(4, "-server")
        .expect("checkpoint semantics test operation should succeed");
    server.commit();

    // This represents the current full-replay recovery path.
    let complete_history = server
        .export(ExportMode::all_updates())
        .expect("checkpoint semantics test operation should succeed");

    // This is the proposed checkpoint base.
    let snapshot = server
        .export(ExportMode::Snapshot)
        .expect("checkpoint semantics test operation should succeed");

    // Path A: reconstruct by replaying complete history.
    let full_replay = new_doc(4);
    import_without_pending(&full_replay, &complete_history);
    import_without_pending(&full_replay, &offline_update);

    // Path B: reconstruct from checkpoint, then receive the same offline update.
    let checkpoint_replay = LoroDoc::from_snapshot(&snapshot)
        .expect("checkpoint semantics test operation should succeed");
    import_without_pending(&checkpoint_replay, &offline_update);

    assert_eq!(
        checkpoint_replay.get_deep_value(),
        full_replay.get_deep_value(),
        "checkpoint recovery and complete replay produced different document states"
    );

    assert_eq!(
        checkpoint_replay.oplog_vv(),
        full_replay.oplog_vv(),
        "checkpoint recovery and complete replay retained different operation histories"
    );
}

/// Characterizes the dangerous case for checkpoint creation:
///
/// the gateway has received update two, but update one—its dependency—is missing.
/// Update two is stored by Loro as pending and is not yet visible in document state.
///
/// A checkpoint containing only an ordinary snapshot is expected not to preserve
/// that pending import. This test documents that limitation.
#[test]
fn snapshot_alone_does_not_preserve_causally_pending_import() {
    let initial = new_doc(10);
    initial
        .get_text("text")
        .insert(0, "base")
        .expect("checkpoint semantics test operation should succeed");
    initial.commit();

    let initial_updates = initial
        .export(ExportMode::all_updates())
        .expect("checkpoint semantics test operation should succeed");
    let initial_vv = initial.oplog_vv();

    // Create two dependent updates from one peer.
    let remote_client = new_doc(11);
    import_without_pending(&remote_client, &initial_updates);

    remote_client
        .get_text("text")
        .insert(4, "-one")
        .expect("checkpoint semantics test operation should succeed");
    remote_client.commit();

    let after_first_vv = remote_client.oplog_vv();

    let first_update = remote_client
        .export(ExportMode::updates(&initial_vv))
        .expect("checkpoint semantics test operation should succeed");

    remote_client
        .get_text("text")
        .insert(8, "-two")
        .expect("checkpoint semantics test operation should succeed");
    remote_client.commit();

    // This contains only the second update. It depends on the first update.
    let second_update = remote_client
        .export(ExportMode::updates(&after_first_vv))
        .expect("checkpoint semantics test operation should succeed");

    // Control path: receive update two first, then its missing dependency.
    let full_replay = new_doc(12);
    import_without_pending(&full_replay, &initial_updates);

    let pending_status = full_replay
        .import(&second_update)
        .expect("checkpoint semantics test operation should succeed");
    assert!(
        pending_status.pending.is_some(),
        "the second update was expected to wait for the first update"
    );

    import_without_pending(&full_replay, &first_update);

    assert_eq!(
        full_replay.get_text("text").to_string(),
        "base-one-two",
        "the control path did not resolve the pending update"
    );

    // Checkpoint path: snapshot while update two is still pending.
    let checkpoint_source = new_doc(13);
    import_without_pending(&checkpoint_source, &initial_updates);

    let pending_status = checkpoint_source
        .import(&second_update)
        .expect("checkpoint semantics test operation should succeed");
    assert!(
        pending_status.pending.is_some(),
        "the checkpoint source was expected to contain a pending import"
    );

    let snapshot = checkpoint_source
        .export(ExportMode::Snapshot)
        .expect("checkpoint semantics test operation should succeed");

    let checkpoint_replay = LoroDoc::from_snapshot(&snapshot)
        .expect("checkpoint semantics test operation should succeed");
    import_without_pending(&checkpoint_replay, &first_update);

    // If the plain snapshot discarded update two, only update one appears.
    assert_eq!(
        checkpoint_replay.get_text("text").to_string(),
        "base-one",
        "Loro snapshot behavior changed: the pending update appears to have survived"
    );

    assert_ne!(
        checkpoint_replay.get_deep_value(),
        full_replay.get_deep_value(),
        "a plain snapshot unexpectedly preserved the pending import; \
         re-evaluate the proposed checkpoint format"
    );
}
