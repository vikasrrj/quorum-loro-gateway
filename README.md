# Quorum Loro Gateway

A Loro sync server that stores updates in Ursula and sends `Ack(Ok)` only after the exact update bytes are durably committed.

This is a working prototype, not a production service yet.

**Current version: `v0.1.0`**

## What it does

```text
Loro client
    │ WebSocket
    ▼
Quorum Loro Gateway
    │ append exact update bytes
    ▼
Ursula durable stream
    │ quorum commit
    ▼
Ack(Ok)
```

Loro handles CRDT merging and offline edits. Ursula handles durable ordering, replication, and replay. The gateway sits between them and controls when a client update is acknowledged.

## What works

- Official Loro Synchronization Protocol v1 over WebSocket
- `%LOR` rooms
- Rust and TypeScript client interoperability
- Normal and fragmented updates
- Exact Loro bytes stored in versioned, checksummed frames
- `Ack(Ok)` only after a commit or a byte-verified duplicate commit
- Safe retry after a lost Ursula response
- Full room recovery after a gateway restart
- Recovery after a commit succeeds but the client ACK is lost
- Corrupt, truncated, or malformed history is rejected
- Ursula leader redirects
- Three-voter tests covering one-voter failure, leader failure, and quorum loss
- `/healthz` and payload-free room diagnostics at `/debug/rooms`

The gateway keeps no local durable database. A fresh process rebuilds room state from Ursula.

## What `Ack(Ok)` means

`Ack(Ok)` means Ursula committed the frame, or Ursula recognized a retry and the gateway read the stored frame back and verified it byte-for-byte.

Timeouts, transport errors, malformed responses, and unresolved writes never turn into a successful ACK. When a write is ambiguous, the room stops accepting joins and updates until the exact append is resolved.

## Run locally

Start Ursula on `127.0.0.1:4437`, then run:

```bash
cargo run -- \
  --listen 127.0.0.1:8080 \
  --ursula-url http://127.0.0.1:4437
```

Connect a protocol v1 client to:

```text
ws://127.0.0.1:8080/ws
```

For a multi-node Ursula cluster, pass each possible redirect target with `--ursula-peer-url`.

## Checks

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Real-cluster, crash-injection, benchmark, and TypeScript tests are ignored by default because they need extra processes or dependencies. See [Testing](docs/TESTING.md).

## Replay cost

Rooms currently rebuild by replaying their full Ursula stream.

| Updates | Encoded history | p95 activation |
| ---: | ---: | ---: |
| 10,000 | 1.94 MiB | 237 ms |
| 50,000 | 9.78 MiB | 912 ms |
| 100,000 | 19.61 MiB | 1.90 s |
| 250,000 | 49.08 MiB | 4.82 s |

The measured curve was close to linear for this workload. More detail is in [Benchmarks](docs/BENCHMARKS.md).

## Still missing

- Checkpoints and bounded recovery
- Stream generations and rotation
- Retention and cleanup
- Authentication and authorization
- Safe multi-gateway writing
- Cross-gateway live fan-out
- Actor eviction and slow-client queue limits
- Recovery from complete Ursula cluster loss

The next major piece is checkpointed recovery, likely starting around 40,000 updates or 8 MiB of post-checkpoint history for the measured workload. That threshold is provisional and must be re-measured on real deployment hardware.

## Docs

- [Design](docs/DESIGN.md)
- [Testing](docs/TESTING.md)
- [Benchmarks](docs/BENCHMARKS.md)

## Bounded recovery

The gateway now supports generation-based bounded recovery.

For every accepted Loro batch, `Ack(Ok)` is sent only after the exact encoded
frame commits to Ursula or an ambiguous retry is located and verified
byte-for-byte.

Rooms use:

```text
manifest -> immutable checkpoint -> active delta generation
```

Rotation checkpoints the current document, preserves exact causally pending
update blobs, creates a new empty delta generation, publishes a chained manifest
record, and only then switches the room actor to the new stream.

On restart, rooms with a manifest restore the latest checkpoint and replay only
the active delta generation. Rooms without a manifest retain legacy full-replay
compatibility through delta generation zero.

Automatic rotation defaults to:

- 64 MiB active-delta bytes
- 10,000 active-delta update blobs
- 15 minutes of active-generation age

See [`docs/BOUNDED_RECOVERY.md`](docs/BOUNDED_RECOVERY.md) for the complete
write, rotation, recovery, corruption, and crash semantics.

Run deterministic verification:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Run the ignored in-memory comparison benchmark:

```bash
cargo test --test bounded_recovery_benchmark \
  -- --ignored --nocapture
```

