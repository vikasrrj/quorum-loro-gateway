# Quorum Loro Gateway

A self-hosted Loro sync server backed by Ursula durable streams.

The gateway accepts Loro updates over WebSocket, stores the exact update bytes in Ursula, and returns `Ack(Ok)` only after the write is durably committed or a previous duplicate commit is verified.

This is currently a hardened prototype.

## How it works

```text
Loro client
    ↓ WebSocket
Quorum Loro Gateway
    ↓ durable append
Ursula quorum-backed stream
    ↓ commit confirmed
Ack(Ok)

Loro handles CRDT merging and offline edits.

Ursula handles durable ordering, replication, and replay.

The gateway connects them and controls when an update is acknowledged.

Current version

The current version supports:

Loro Synchronization Protocol v1
%LOR rooms
official Rust and TypeScript clients
normal and fragmented Loro updates
exact update-byte storage
durable Ack(Ok)
producer retry and duplicate verification
gateway crash recovery
complete room reconstruction from Ursula
corruption and malformed-frame detection
Ursula leader redirects
tested three-voter quorum behavior
one-voter and leader failure testing
health and room debug endpoints

The gateway does not need its own local durable database. It rebuilds room state from Ursula after restarting.

Durability guarantee

Ack(Ok) means the exact update frame was committed to Ursula, or an already committed retry was read back and verified byte-for-byte.

Timeouts, transport failures, malformed responses, failed duplicate verification, and uncertain writes never return Ack(Ok).

When the gateway cannot determine whether a write committed, the room fails closed until the write is safely resolved.

Current storage model

Each room currently uses one append-only Ursula stream.

On startup, the gateway replays the full stream to rebuild the Loro document.

Measured replay results:

Updates	History size	p95 activation
10,000	1.94 MiB	237 ms
50,000	9.78 MiB	912 ms
100,000	19.61 MiB	1.90 s
250,000	49.08 MiB	4.82 s

These results suggest checkpointing becomes useful as room history grows.

Run

Start Ursula locally, then run:

cargo run -- \
  --listen 127.0.0.1:8080 \
  --ursula-url http://127.0.0.1:4437

Connect a Loro client to:

ws://127.0.0.1:8080/ws

Run the standard checks:

cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
Main limitations
full room history is replayed after restart
no checkpoints or stream rotation yet
one permanent stream per room
no authentication or authorization
no safe multi-gateway writing
no retention or cleanup system
no complete-cluster disaster recovery guarantee
not production-ready
Remaining work

The next major step is bounded recovery using checkpoints and stream generations.

Later work includes authentication, retention, multi-gateway coordination, actor cleanup, slow-client limits, and larger production-style testing.

Detailed documentation

More detailed design notes, failure tests, and benchmark results are available in the docs directory.


Then commit it manually:

```bash
