# Quorum Loro Gateway

A Rust gateway that connects the official Loro sync protocol to Ursula.

The gateway sends `Ack(Ok)` only after the exact update bytes are committed to Ursula or an uncertain retry is found and verified byte-for-byte.

## Why this project exists

The original gateway recovered a room by replaying its complete Ursula delta stream from offset zero.

That recovery method is correct, but the amount of data and work grows with the room’s entire history.

This project adds bounded recovery using:

```text
manifest → checkpoint → active delta generation
```

A restart now restores the latest checkpoint and replays only the current active delta.

## Write flow

```text
Loro client update
        ↓
room actor validates the update
        ↓
exact DeltaFrame is encoded
        ↓
frame is appended to Ursula
        ↓
commit or exact duplicate is verified
        ↓
Ack(Ok) is returned
```

Ambiguous append results do not return success until the gateway verifies that the stored bytes exactly match the original frame.

## Bounded recovery

Each room uses:

```text
manifest
checkpoint/{generation}
delta/{generation}
```

A checkpoint contains:

* The current Loro snapshot
* The source delta generation and offset
* Exact raw updates that are still causally pending
* Digest, checksum, and length validation

The manifest records:

* The latest checkpoint generation
* The checkpoint digest and length
* The active delta generation
* A digest link to the previous manifest record

On restart, the gateway:

1. Reads and validates the manifest
2. Loads the referenced checkpoint
3. Restores the snapshot and pending updates
4. Replays only the active delta generation

Rooms without a manifest continue using the legacy full-replay path.

## Rotation

Rotation is serialized through the room actor.

The gateway:

1. Builds an immutable checkpoint
2. Creates the next empty delta generation
3. Publishes the next manifest record
4. Switches the actor to the new delta stream

The manifest is published before the in-memory switch, so it remains the durable source of truth after a restart.

Rotation is currently triggered manually through `RoomHandle::rotate()`.

## Benchmark

An in-memory benchmark compared full replay with bounded recovery.

```text
Total updates:                 2,000
Checkpointed updates:         1,950
Active updates replayed:         50

Legacy delta bytes:         395,679
Active delta bytes:           9,900

Legacy replay time:          261.48 ms
Bounded recovery time:         8.13 ms
```

This benchmark is not an Ursula cluster performance result. It demonstrates that active-delta replay depends on the current generation rather than the room’s complete update history.

Run it with:

```bash
cargo test --test bounded_recovery_benchmark \
  -- --ignored --nocapture
```

## Correctness properties

The prototype focuses on these guarantees:

* `Ack(Ok)` is returned only after durable commit or exact duplicate verification
* Ambiguous retries reuse the same producer tuple and exact frame bytes
* Checkpoint, manifest, and delta corruption fail closed
* Stream and generation names are never reused
* Causally pending Loro updates are retained across checkpoints
* Legacy rooms remain compatible
* Rotation is serialized with room writes

## Testing

Run the deterministic test suite:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The repository includes tests for:

* Durable acknowledgements
* Ambiguous and duplicate append outcomes
* Gateway restart recovery
* Corrupt frames, checkpoints, and manifests
* Pending Loro updates
* Checkpoint creation
* Manifest chaining
* Generation rotation
* Official protocol compatibility

Some Ursula cluster and TypeScript interoperability tests are ignored unless their external dependencies are available.

## Scope

This is a database and distributed-systems research prototype, not a production service.

It intentionally does not include:

* Multiple active gateway writers for one room
* Distributed leases or fencing
* Authentication and authorization
* Old-generation garbage collection
* Manifest compaction
* Automatic background rotation
* Production Ursula cluster benchmarks

The goal is to demonstrate a correct durable acknowledgement boundary and a practical checkpoint-and-generation design for bounded Loro recovery.

## License

Apache-2.0
