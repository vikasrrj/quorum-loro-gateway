# Bounded Recovery Architecture

This document describes the bounded-recovery design implemented on the
`bounded-recovery` branch of `quorum-loro-gateway`.

## Goal

The gateway accepts Loro protocol updates and persists the exact update bytes
to Ursula before returning `Ack(Ok)`.

The original implementation recovered a room by replaying its entire delta
stream from offset zero. Recovery cost therefore increased with the complete
lifetime history of the room.

Bounded recovery changes the durable layout to:

```text
manifest -> immutable checkpoint -> active delta generation
```

Ordinary restart recovery now loads the latest checkpoint and replays only the
currently active delta generation.

This bounds historical replay work. It does not make recovery independent of
the current document size, because the checkpoint snapshot still represents
the current document state.

## Durable stream layout

Each room uses independent Ursula streams:

```text
room/{room-hash}/manifest
room/{room-hash}/checkpoint/{generation}
room/{room-hash}/delta/{generation}
```

Names are derived from a stable room hash and contain only Ursula-safe
characters.

Generation-specific stream names are never reused.

The legacy stream is treated as delta generation zero:

```text
room/{room-hash}/delta/0
```

Old checkpoint and delta generations are retained. There is currently no
garbage-collection policy.

## Delta frames

Every committed client batch is stored in a versioned binary `DeltaFrame`.

A frame binds:

- Producer ID, epoch, and sequence
- Loro batch ID
- Exact raw update blobs
- SHA-256 digest
- CRC32 checksum
- Encoded-length trailer

The decoder enforces configured limits before allocation and rejects truncated,
corrupt, oversized, or trailing input.

The exact raw Loro update bytes are retained rather than decoded and re-encoded.

## Acknowledgement boundary

The gateway returns `Ack(Ok)` only after one of these outcomes:

1. Ursula reports the exact frame committed.
2. A retry reports a duplicate and the gateway reads the committed range and
   verifies it byte-for-byte against the original frame.

Ambiguous append outcomes never produce `Ack(Ok)` until this verification
succeeds.

A pending ambiguous append stores its original target stream. A later retry
therefore remains bound to the original delta generation even if other room
state changes.

## Checkpoints

A checkpoint is an immutable binary record containing:

- Room hash
- Checkpoint generation
- Source delta generation
- Stable source delta end offset
- Loro snapshot bytes
- Exact raw updates still causally pending against that snapshot
- SHA-256 digest
- CRC32 checksum
- Encoded-length trailer

The checkpoint builder exports a snapshot from the live document, probes the
retained update blobs against that snapshot, and preserves blobs whose import
still reports pending operations.

This matters because a normal Loro snapshot represents visible document state
but does not by itself preserve causally pending imported operations.

Before accepting a checkpoint, the builder reconstructs:

```text
snapshot + retained pending updates
```

and checks that the reconstructed document matches the live document state.

A checkpoint stream is immutable:

- Empty stream: append the checkpoint with deterministic producer identity.
- Existing identical bytes: treat as verified existing state.
- Existing different bytes: fail closed.

## Manifest

The manifest is an append-only chain of fixed binary records.

Each record contains:

- Room hash
- Monotonic revision
- Previous manifest-record digest
- Checkpoint generation
- Checkpoint stream end offset
- Checkpoint record digest
- Active delta generation
- Record digest
- CRC32 checksum
- Encoded-length trailer

The genesis record uses the all-zero predecessor digest.

Every later record must:

- Increment the manifest revision by exactly one
- Reference the previous record digest
- Advance the checkpoint generation
- Advance the active delta generation
- Point to the next delta generation after its checkpoint

The manifest binds both the exact checkpoint byte length and its digest.

## Rotation protocol

Room commands are processed by one actor, so rotation is serialized with joins,
updates, retries, and leaves.

Rotation is allowed only when the room is ready and has no unresolved append.

The implemented order is:

1. Determine the next delta generation.
2. Build a checkpoint from the current document and retained history.
3. Persist or verify the immutable checkpoint stream.
4. Ensure the next delta stream exists.
5. Verify that the next delta stream is empty.
6. Build the next chained manifest record.
7. Append or byte-verify the manifest record.
8. Switch the actor's in-memory stream and generation.
9. Reset generation-scoped producer sequence and active-delta counters.
10. Retain only the checkpoint's causally pending raw updates as history.

The manifest is published before the in-memory switch.

There is no Ursula close-stream operation in the current store interface.
A sealed generation becomes immutable because the actor stops writing to it.

## Automatic rotation

Rotation can be triggered by any configured active-generation threshold:

- Delta bytes
- Update-blob count
- Generation age

Current defaults:

```text
64 MiB active delta
10,000 update blobs
15 minutes
```

Threshold checks occur after a successful durable append and acknowledgement.
Manual rotation remains available through `RoomHandle::rotate()`.

Age is measured from actor activation for a recovered generation because the
manifest currently does not persist a generation-start timestamp.

## Startup recovery

Startup first reads the room manifest.

### Empty manifest

An empty manifest selects legacy compatibility mode:

1. Ensure delta generation zero.
2. Read the full legacy delta stream.
3. Decode all frames.
4. Replay all updates.

### Non-empty manifest

A non-empty manifest selects bounded recovery:

1. Decode and validate the complete manifest chain.
2. Select the latest manifest record.
3. Read the referenced checkpoint stream.
4. Verify checkpoint length and digest against the manifest.
5. Decode and validate the checkpoint.
6. Restore the Loro snapshot.
7. Import checkpoint-retained pending updates.
8. Read the active delta generation.
9. Decode and import only its frames.
10. Start a generation-scoped producer identity for the new gateway boot.

Recovered retained history is:

```text
checkpoint pending updates + active delta updates
```

This is enough to build the next checkpoint without retaining all pre-checkpoint
delta history in memory.

## Candidate update validation after a checkpoint

After bounded recovery, retained history no longer contains every operation
represented by the document.

For a new client batch, the actor therefore does not rebuild a candidate
document from retained history alone.

It instead constructs:

```text
snapshot of current document
+ retained pending/active history
+ incoming updates
```

Applied operations already represented in the snapshot import as duplicates.
Causally pending operations are restored from exact retained blobs.

## Crash behavior

The deterministic crash-window tests cover these cases.

### Checkpoint committed, next-delta creation fails

No manifest record points to the new checkpoint. Restart continues from the
previous manifest state or legacy generation.

The immutable checkpoint can be reused by a retry.

### Next delta created, manifest publication fails

The empty next-delta stream is not authoritative because the manifest did not
advance.

A retry verifies and reuses the empty stream.

### Manifest committed, response lost

Exact append retry detects the duplicate and verifies the stored manifest bytes
before treating publication as successful.

### Manifest published, actor crashes before in-memory switch

The manifest is authoritative. Restart loads the published checkpoint and the
new active delta generation.

The previous generation is not reopened for writes.

## Corruption behavior

Recovery fails closed when it encounters:

- Invalid manifest chain
- Wrong room hash
- Wrong predecessor digest
- Invalid generation transition
- Missing checkpoint
- Checkpoint length or digest mismatch
- Checkpoint checksum or format corruption
- Corrupt active-delta frame
- Hostile or oversized encoded lengths
- Duplicate append whose stored bytes differ

A corrupt room is not silently reconstructed from an alternative state.

## Concurrency model

One room actor serializes durable operations for that room.

This implementation does not provide:

- Multiple active gateway writers for the same room
- Distributed room leases
- Cross-gateway fencing
- Active-active producer coordination

Generation-scoped producer IDs prevent sequence reuse inside one gateway boot,
but they are not a replacement for multi-writer fencing.

## Recovery bound

Let:

- `M` be manifest bytes
- `C` be latest checkpoint bytes
- `D` be active delta bytes
- `H` be the room's complete historical delta bytes

Legacy recovery reads and decodes approximately:

```text
H
```

Bounded recovery reads approximately:

```text
M + C + D
```

Rotation keeps `D` below configured thresholds during normal operation.

`C` can still grow with current document state, and `M` grows by one small
record per rotation. The design bounds delta replay history, not total state
size or indefinite manifest growth.

## Benchmark

The ignored benchmark compares full-history replay with checkpoint recovery:

```bash
cargo test --test bounded_recovery_benchmark \
  -- --ignored --nocapture
```

Larger release run:

```bash
BENCH_TOTAL_UPDATES=10000 \
BENCH_ACTIVE_UPDATES=100 \
cargo test --release \
  --test bounded_recovery_benchmark \
  -- --ignored --nocapture
```

The benchmark prints:

- Total generated updates
- Checkpointed updates
- Active-delta updates replayed
- Legacy delta bytes
- Manifest bytes
- Checkpoint bytes
- Active delta bytes
- Legacy replay duration
- Bounded recovery duration

Timing is an in-memory microbenchmark and should not be presented as an Ursula
cluster performance result. The important invariant is that active-delta replay
depends on the configured active generation rather than complete room history.

## Verification

Run deterministic verification:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Run the manual benchmark:

```bash
cargo test --test bounded_recovery_benchmark \
  -- --ignored --nocapture
```

Optional integration tests remain ignored unless their external dependencies
are available:

- Local Ursula cluster binary and free test ports
- Node.js dependencies for official TypeScript client interop

## Current limitations and follow-up work

The bounded-recovery implementation intentionally leaves these outside scope:

- Checkpoint and old-generation garbage collection
- Manifest compaction
- Persisted generation creation time
- Multi-gateway leases and fencing
- Authentication and authorization
- Generic storage backends
- Background checkpoint workers
- Ursula-native snapshot integration

These are follow-up system-design areas, not requirements for the current
single-gateway bounded-recovery prototype.
