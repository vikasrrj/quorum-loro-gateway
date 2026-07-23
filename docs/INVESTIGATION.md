# Phase 1 Investigation

This document records evidence used by the Phase 1 implementation. Statements
are grouped by confidence so estimates and future requirements are not mistaken
for verified behavior.

## Verified Source Behavior

### Loro protocol

- The official synchronization protocol is maintained separately from the core
  CRDT at <https://github.com/loro-dev/protocol>.
- Protocol version 1 defines `%LOR`, `JoinRequest`, `JoinResponseOk`,
  `JoinError`, `DocUpdate`, fragment header/fragment messages, `Ack`,
  `RoomError`, `Leave`, and text `ping`/`pong` frames.
- `loro-protocol` 0.3.0 is the published Rust codec containing protocol v1.
  Published 0.1.0 is protocol v0 even though a later repository tag contained
  v1 source with a stale 0.1.0 manifest version.
- A protocol batch ID is an eight-byte acknowledgement correlation value. It is
  not a stable semantic identity for a Loro update.
- Protocol messages are limited to 256 KiB. Larger individual updates use the
  official fragmentation messages.

Sources:

- <https://github.com/loro-dev/protocol/blob/loro-websocket-v0.6.2/protocol.md>
- <https://docs.rs/crate/loro-protocol/0.3.0/source/src/protocol.rs>
- <https://docs.rs/crate/loro-protocol/0.3.0/source/src/encoding.rs>

### Loro update validation and identity

- `LoroDoc::decode_import_blob_meta(bytes, true)` parses import metadata and
  checks Loro's encoded checksum without mutating a document.
- Metadata decoding does not prove that an update can be imported into a
  particular document.
- Importing into a reconstructed disposable `LoroDoc` exercises the full import
  path. Phase 1 reconstructs that probe from all committed raw updates.
- One update blob may contain multiple operation ranges and overlap previous
  exports. Loro exposes stable operation identities `(PeerID, counter)`, but no
  singular canonical update-blob identity.
- Duplicate operation imports do not apply document state twice.
- Official clients may send a full snapshot in `DocUpdate` when the receiver's
  version is empty. Phase 1 accepts checksum-valid snapshots when contextual
  replay can import them, rather than imposing a non-standard update-only rule.
- `LoroDoc::clone()` shares the original document. `fork()` is independent but
  O(n) and omits pending changes.
- Loro full snapshots omit causally pending updates. Later checkpoint phases
  must carry unresolved raw updates separately.

Sources:

- <https://docs.rs/loro/1.13.7/loro/struct.LoroDoc.html>
- <https://docs.rs/loro/1.13.7/loro/struct.ImportBlobMetadata.html>
- <https://docs.rs/loro/1.13.7/loro/struct.ImportStatus.html>
- <https://github.com/loro-dev/loro/blob/loro-v1.13.7/crates/loro-internal/src/oplog.rs>

### Ursula append and replay

- A producer append receives HTTP `200` only after the replicated command has
  committed and applied. A deduplicated producer retry receives HTTP `204`.
- HTTP success returns `Stream-Next-Offset`, producer epoch, and producer
  sequence. It does not return the append start offset or producer ID.
- Producer deduplication is checked before closed-stream rejection.
- Within one epoch, Ursula treats every `producer_seq <= stored_seq` as a
  duplicate and returns the latest producer result. Phase 1 therefore permits
  one unresolved append and never pipelines producer sequences.
- Ursula does not compare the retry body with the originally committed body.
  Phase 1 retains exact bytes, derives the expected start from next offset and
  exact frame length, reads that range, and compares it byte-for-byte before a
  duplicate can produce `Ack(Ok)`.
- Catch-up reads are byte ranges and may split logical appends. Phase 1 uses a
  self-delimiting storage frame and replays from offset zero.
- Producer state is a per-stream `HashMap<String, ProducerState>` replicated in
  Raft state and snapshots. It has no producer-specific cap, TTL, or eviction.
- Producer state is removed with the containing stream.

Sources in the inspected checkout `/home/vik/ursula`:

- `crates/ursula/src/lib.rs:1119-1132`
- `crates/ursula/src/tests.rs:1005-1079`
- `crates/ursula-stream/src/state_machine/append.rs:63-109`
- `crates/ursula-stream/src/state_machine/append.rs:486-568`
- `crates/ursula-stream/src/state_machine.rs:96-106`
- `crates/ursula-stream/src/state_machine/persist.rs:49-93`

### Ursula naming constraint

- Ursula stream IDs reject `/` and the combined bucket/stream name is limited
  to 122 bytes.
- Phase 1 preserves `room/{sha256(document-id)}/delta/0` as the logical name and
  maps it deterministically to physical stream ID `r-{hash}-d0` in bucket
  `qloro`.

Source: `/home/vik/ursula/crates/ursula-stream/src/validate.rs:18-41`.

## Measured Behavior

No production performance or memory measurements have been made in Phase 1.

The Phase 1 test suite measures only functional outcomes under deterministic
mock storage: append ordering, retry identity, replay, acknowledgement timing,
frame integrity, and CRDT convergence. Test counts and timings are reported by
`cargo test --workspace`; they are not capacity measurements.

## Estimates

- Ursula producer state is estimated from Rust layout at roughly 180–300 bytes
  per short-ID, single-item producer per replica, excluding Raft log copies,
  allocator fragmentation, and snapshot peaks.
- Snapshot building can temporarily hold multiple clones of producer state.

These are unmeasured planning estimates and must not be used for capacity
planning.

## Unverified Assumptions

- Deriving a duplicate range start as `next_offset - exact_retry_frame_length`
  is safe only because the actor retains one exact unresolved frame. A future
  Ursula start-offset response would remove this inference.
- Phase 1 assumes clients obey the official protocol join-before-update flow.
- Phase 1 has no authorization policy; every valid `%LOR` join receives write
  permission.
- A gateway process that exhausts ambiguous retries keeps the append unresolved
  and blocks later producer sequences. Automated cross-process resolution is a
  later phase.
- In-memory fan-out is not cross-gateway delivery. Restart/rejoin relies on full
  Ursula replay and Loro version reconciliation.
- The local single-node Ursula development preset validates the HTTP contract,
  not production quorum failure behavior or disk durability configuration.

## Later-Phase Requirements

The following findings are verified but intentionally not implemented in Phase
1:

- Ursula currently has no atomic `If-Match` append. Manifest transitions and
  multi-gateway coordination require expected-offset CAS inside Ursula's
  replicated state machine.
- Ursula whole-stream `DELETE` is quorum-applied logically, returns `404` on a
  repeated hard delete, and performs physical cold-object deletion
  asynchronously.
- Ursula stream deletion currently omits accepted externalized objects under
  the `external/` prefix. Large-checkpoint physical retention requires an
  Ursula GC fix.
- Catalog discovery, manifests, generation rotation, checkpoints, pending raw
  update carry-forward, retention, DELETE, and multi-gateway live delivery are
  later phases.
- Producer-state memory and snapshot amplification require a dedicated measured
  benchmark before production approval.
- Multi-node quorum partitions, leader loss, and durable disk-WAL tests belong
  to a later integration phase.
