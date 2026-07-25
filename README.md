# Quorum Loro Gateway

A Rust gateway between the official Loro sync protocol and Ursula.

The gateway accepts Loro document updates, stores the exact update bytes in Ursula, and returns `Ack(Ok)` only after the append is committed or an uncertain retry is verified byte for byte.

## Overview

Each room is handled by a single actor.

The actor serializes updates, retries, joins, recovery, and stream rotation for that room.

Updates are stored inside versioned `DeltaFrame` records containing the original Loro update bytes, batch ID, producer tuple, digest, checksum, and encoded length.

## Write Guarantee

```text
Loro update
    ↓
validate update
    ↓
encode DeltaFrame
    ↓
append to Ursula
    ↓
commit or exact duplicate verification
    ↓
Ack(Ok)
```

An ambiguous append does not return success.

The gateway retries using the same producer identity, sequence, stream, and frame bytes.

When Ursula reports a duplicate, the stored range must exactly match the original frame before the gateway returns `Ack(Ok)`.

## Recovery

Rooms without a manifest use the original full replay path.

Rooms with checkpoints recover using:

```text
manifest → checkpoint → active delta
```

The checkpoint restores the Loro document state and retains exact update blobs that are still causally pending.

The active delta contains updates committed after the latest checkpoint.

## Rotation

Rotation creates an immutable checkpoint, creates the next delta generation, publishes a new manifest record, and then switches the room actor to the new stream.

Generation names are never reused.

Rotation is currently triggered through `RoomHandle::rotate()`.

## Benchmark

The included benchmark compares complete history replay with checkpoint recovery.

| Measurement            | Full replay | Checkpoint recovery |
| ---------------------- | ----------: | ------------------: |
| Total document updates |       2,000 |               2,000 |
| Updates replayed       |       2,000 |                  50 |
| Delta bytes replayed   |     395,679 |               9,900 |
| Recovery time          |   261.48 ms |             8.13 ms |

The remaining 1,950 updates are represented by the checkpoint.

This is an in-memory comparison benchmark and not a production Ursula cluster benchmark.

Run it with:

```bash
cargo test --test bounded_recovery_benchmark \
  -- --ignored --nocapture
```

## Running

```bash
cargo run
```

The gateway connects Loro protocol clients to the configured Ursula server.

Health and room status endpoints are available for inspecting gateway state without exposing document contents.

## Testing

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The tests cover durable acknowledgements, ambiguous append outcomes, exact duplicate verification, gateway restart recovery, pending Loro updates, corrupt frames, checkpoints, manifests, protocol limits, and generation rotation.

Some Ursula cluster and TypeScript interoperability tests require external dependencies and remain ignored by default.

## Scope

This is a small database and distributed systems prototype.

It currently assumes one active gateway writer for each room.

Multi-gateway coordination, distributed writer fencing, authentication, automatic rotation, old-generation cleanup, manifest compaction, and production cluster benchmarks are outside the current scope.

## License

Apache-2.0
