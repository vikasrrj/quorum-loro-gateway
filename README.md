# Quorum Loro Gateway

A self-hosted Loro synchronization server backed by Ursula’s quorum-replicated durable streams.

The gateway stores the exact Loro update bytes in Ursula and returns `Ack(Ok)` only after the update is durably committed or an earlier duplicate commit is verified byte-for-byte.

> **Current version: v0.1 prototype**

## How it works

```text
Loro client
    │ WebSocket
    ▼
Quorum Loro Gateway
    │ Durable append
    ▼
Ursula quorum
    │ Commit confirmed
    ▼
Ack(Ok)
```

Loro handles CRDT merging and offline edits. Ursula handles durable ordering, replication, and replay. The gateway connects them and decides when an update is safe to acknowledge.

## Available in v0.1

- Loro Synchronization Protocol v1 over WebSocket
- `%LOR` rooms
- Rust and TypeScript client interoperability
- Normal and fragmented Loro updates
- Exact Loro byte storage without decode-and-reencode changes
- Durable `Ack(Ok)` after commit
- Producer retries and byte-verified duplicate handling
- Recovery after gateway restart
- Recovery when a commit succeeds but the ACK is lost
- Complete room reconstruction from Ursula
- Corrupt and malformed history detection
- Ursula leader redirect handling
- Health and room-debug endpoints
- Tested three-voter Ursula quorum
- Tested voter failure, leader failure, gateway crash, and quorum loss

The gateway does not require its own local durable database. After restarting, it rebuilds room state directly from Ursula.

## Durability guarantee

`Ack(Ok)` means one of the following is true:

1. Ursula confirmed that the exact update frame was committed.
2. Ursula recognized a retry, and the gateway read the committed frame back and verified it byte-for-byte.

Timeouts, transport failures, invalid responses, uncertain writes, and failed duplicate verification never return `Ack(Ok)`.

When the result of a write cannot be safely determined, the room stops accepting joins and updates until the write is resolved.

## Storage and recovery

Each room currently uses one permanent append-only Ursula stream.

```text
Room stream
├── update 1
├── update 2
├── update 3
└── ...
```

Every append is stored in a versioned `QLGD` frame containing the producer identity, sequence, batch ID, exact Loro blobs, SHA-256 digest, and CRC32 checksum.

During recovery, every frame is replayed and verified in order. Invalid, incomplete, or corrupted frames are rejected rather than skipped.

## Replay performance

Room activation currently requires replaying the full stream.

| Updates | Encoded history | p95 activation |
|--------:|----------------:|---------------:|
| 10,000 | 1.94 MiB | 237 ms |
| 25,000 | 4.87 MiB | 533 ms |
| 50,000 | 9.78 MiB | 912 ms |
| 100,000 | 19.61 MiB | 1.90 s |
| 250,000 | 49.08 MiB | 4.82 s |

These measurements used a release-mode gateway and a real three-voter Ursula cluster. Replay remained approximately linear through 250,000 updates.

## Run locally

Start Ursula on `127.0.0.1:4437`, then run:

```bash
cargo run -- \
  --listen 127.0.0.1:8080 \
  --ursula-url http://127.0.0.1:4437
```

Connect a Loro protocol client to:

```text
ws://127.0.0.1:8080/ws
```

Operational endpoints:

```text
GET /healthz
GET /debug/rooms
```

The debug endpoint reports room lifecycle and recovery metrics without returning document contents or update bytes.

## Verification

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Some real-cluster, crash-injection, interoperability, and benchmark tests are ignored by default because they require Ursula, Node.js, or separate child processes.

## Main limitations

- Full room history is replayed after every activation
- One permanent stream is used per room
- No checkpoints or stream rotation
- No retention or old-history cleanup
- No authentication or authorization
- No safe multi-gateway writing
- Fan-out exists only inside one gateway process
- No complete Ursula cluster-loss guarantee
- Not production-ready

## Remaining work

The next version will focus on bounded recovery using immutable Loro checkpoints and sealed stream generations.

Later work includes retention, authentication, actor cleanup, slow-client limits, multi-gateway coordination, and broader production-style failure testing.

## Documentation

Detailed design notes, audits, failure experiments, and benchmark results are available in the [`docs`](docs/) directory.
