# Quorum Loro Gateway

Phase 1 implements the official Loro Synchronization Protocol v1 over
WebSocket and persists updates to one permanent Ursula stream per document.

Only `%LOR` rooms are supported. `Ack(Ok)` is sent only after Ursula reports a
committed append or the gateway verifies that an exact retry was deduplicated.

## Run

Start a local Ursula server, then run:

```bash
cargo run -- \
  --listen 127.0.0.1:8080 \
  --ursula-url http://127.0.0.1:4437
```

Connect an official Loro protocol v1 client to `ws://127.0.0.1:8080/ws`.

## Phase 1 Boundaries

- One permanent delta stream per room.
- In-memory fan-out only within one gateway process.
- Full replay from Ursula on room activation.
- No manifests, catalog, checkpoints, rotation, retention, deletion, or
  multi-gateway coordination.

See `docs/INVESTIGATION.md` for verified dependency behavior and limitations.
