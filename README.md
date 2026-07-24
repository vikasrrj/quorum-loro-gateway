# Quorum Loro Gateway

Phase 1.5 implements the official Loro Synchronization Protocol v1 over
WebSocket and stores exact Loro blobs in one permanent Ursula delta stream per
room. Phase 2 validates that design against a real three-voter Ursula cluster.
Only `%LOR` rooms are supported.

This is a hardened prototype, not a production-ready service.

## Durable ACK Guarantee

`Ack(Ok)` has one construction path. It requires one of two internal proofs:

- `Committed`: Ursula returned an explicit successful append result.
- `VerifiedDuplicate`: Ursula reported the producer tuple as a duplicate, and
  the gateway derived the candidate range with checked subtraction, read the
  exact expected byte count, compared every stored byte to the retained frame,
  decoded exactly one checksum-valid frame, and compared the full decoded
  frame including producer tuple, batch ID, updates, and digest.

Timeouts, transport failures, 5xx responses, malformed responses, exhausted
retries, offset inconsistencies, and failed duplicate verification never map to
`Ack(Ok)`. One room actor serializes appends and advances its producer sequence
only after one of the durable proofs above.

When an append remains ambiguous, the exact tuple and frame bytes are retained,
the room enters `append_ambiguous`, and both joins and writes fail closed. A
controlled `RoomHandle::retry_ambiguous` or `RoomManager::retry_ambiguous`
attempt reuses those exact values. Returning to `ready` additionally requires a
complete authoritative reload from Ursula.

## Durable Frame

Each append is one versioned `QLGD` frame containing:

- frame magic, version, flags, and duplicated total length;
- producer ID, epoch, and sequence;
- official protocol batch ID;
- count and lengths of the exact received Loro blobs;
- exact blob bytes without decode/re-encode transformation;
- SHA-256 domain-separated update digest;
- CRC32 over the frame body.

Recovery starts at stream offset zero and decodes every frame in order. It
rejects unsupported versions, bad magic or flags, truncation, trailing bytes,
checksum/digest failures, invalid Loro blobs, snapshot-policy violations, and
contextual import failures. Frame errors identify the durable stream offset.
No malformed frame or Loro update is skipped.

Frame, producer ID, update count, individual update, aggregate update, complete
history, WebSocket message, fragment batch, fragment count, reassembled update,
per-connection fragment bytes, HTTP response body, and HTTP timing are bounded.
The binary exposes the principal limits as command-line flags; library users can
configure `FrameLimits`, `ProtocolLimits`, `ServerConfig`, and
`HttpUrsulaConfig` directly.

## Loro Policy

- Updates are checksum-checked and contextually imported into an isolated
  document before storage.
- One full snapshot is accepted only as the sole blob initializing a room with
  no durable history. This preserves the official empty-receiver flow.
- Full snapshot replacement, shallow snapshots, outdated encodings, empty
  batches, malformed blobs, and unresolved dependency imports are rejected
  before storage.
- Authoritative in-memory state changes only after durable append proof. If
  reconciliation after an already committed ambiguous append fails, the frame
  remains durable and the room stays fail-closed until Ursula reload succeeds.

## Run

Start a local Ursula server, then run:

```bash
cargo run -- \
  --listen 127.0.0.1:8080 \
  --ursula-url http://127.0.0.1:4437
```

Connect an official protocol v1 client to `ws://127.0.0.1:8080/ws`.

Operational endpoints:

- `GET /healthz` is process liveness only.
- `GET /debug/rooms` reports hashed stream name, lifecycle, producer sequence,
  pending sequence, peer count, recovery byte/update counts and timings, and
  last error. It never returns document or frame bytes.

These endpoints have no authentication. Keep the service on a trusted network.

## Verification

Required local gates:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Official TypeScript/JavaScript interoperability uses pinned
`loro-protocol@0.3.0` and `loro-crdt@1.13.7` clients:

```bash
npm ci --prefix interop
cargo test --test phase1 official_typescript_clients_converge -- --ignored --nocapture
```

The genuine process-crash harness enables only the test injection feature,
aborts a gateway child after Ursula commit and before ACK, then starts a fresh
child and verifies Ursula-only recovery:

```bash
cargo test --features crash-injection --test process_crash -- --ignored --nocapture
```

The real-Ursula acceptance test expects Ursula at `127.0.0.1:4437`:

```bash
cargo test --test phase1 real_ursula_commit_duplicate_and_restart_replay -- --ignored --nocapture
```

The Phase 2 harness starts isolated real Ursula and gateway processes for
one-voter durability, quorum loss, and leader failure:

```bash
cargo test --test phase2_cluster \
  acknowledged_update_survives_one_voter_and_gateway_crash \
  -- --ignored --nocapture
cargo test --test phase2_cluster \
  one_voter_allows_writes_but_quorum_loss_never_acks \
  -- --ignored --nocapture
cargo test --test phase2_cluster leader_failure \
  -- --ignored --nocapture --test-threads=1
```

Producer-state and release full-replay measurements, including raw results, are
documented in `docs/PHASE_2.md`.

The isolated Phase 2.5 scale study extends full replay through 250,000 updates,
records component timings and state hashes, and derives its checkpoint decision
from an explicit p95 recovery SLO. See `docs/PHASE_2_5_SCALE.md`.

## Observability

Structured append and reconciliation logs include the hashed stream, producer
ID/epoch/sequence, retry number, result class, committed offset or duplicate
range, and reconciliation result. Room lifecycle transitions include producer
and pending sequence state.

Room states are:

- `recovering`: activation is replaying Ursula; commands remain queued.
- `ready`: authoritative replay/install succeeded; joins and writes are allowed.
- `append_ambiguous`: one exact append is unresolved; joins and writes fail.
- `corrupt`: durable bytes, frame policy, or Loro replay failed integrity checks.
- `unavailable`: Ursula or reconciliation is unavailable or definitely unsafe.

## Non-Production Boundaries

- One permanent, unbounded logical delta history per room. Configured byte
  limits fail closed; there is no checkpoint or compaction path.
- Full replay and candidate validation are O(history), and active actors retain
  raw history in memory.
- In-memory fan-out only; no active-active or multi-gateway coordination.
- No authentication, authorization, actor eviction, slow-consumer queue bound,
  or debug-endpoint access control.
- Three-voter one-voter durability, quorum loss, and leader failure are tested;
  no two-replica-loss or complete-cluster disaster recovery guarantee is
  claimed.
- No manifests, catalog, checkpoints, generation rotation, retention, DELETE,
  or later-phase lifecycle features.

See `docs/INVESTIGATION.md` for dependency behavior and
`docs/PHASE_1_5_AUDIT.md` for the pre-hardening audit and resolution record.
See `docs/PHASE_2.md` for cluster topology, failure experiments, producer-state
growth, and full-replay measurements.
See `docs/PHASE_2_5_SCALE.md` for the measured full-replay scaling and checkpoint
decision.
