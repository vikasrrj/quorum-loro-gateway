# Phase 1.5 Audit

This audit was written from the source and tests at commit `f1fc6b6`, before
Phase 1.5 runtime changes. The baseline command `cargo test --workspace` passed
13 tests and ignored the real-Ursula test.

## Confirmed Guarantees

### Durable acknowledgement path

The baseline has one direct `Ack(Ok)` call at `src/actor.rs:423`. It is reached
only after `RoomActor::resolve_append` returns success. That function returns
success for:

- `AppendOutcome::Committed` at `src/actor.rs:453`; or
- `AppendOutcome::Duplicate` after reading a candidate range, comparing it to
  the retained bytes, decoding one frame, and checking the producer tuple at
  `src/actor.rs:454-480`.

Parsing, contextual Loro import, queue insertion, fan-out, timeout, and
connection closure do not directly construct a successful acknowledgement.
Definite rejection and unresolved outcomes use non-success statuses.

### Producer serialization

One Tokio task owns each room's producer state. Commands are processed
serially by `RoomActor::run` at `src/actor.rs:212-238`. The next producer
sequence is submitted only after the previous update command finishes. The
sequence advances only on `resolve_append` success at `src/actor.rs:405-409`.
After exhausted ambiguity, `blocked` retains the tuple and exact frame bytes,
and later updates receive `RateLimited`.

### Pre-commit validation and state mutation

Each received blob is inspected with `LoroDoc::decode_import_blob_meta`, then
all committed history plus the candidate bytes are imported into a disposable
document at `src/actor.rs:355-380`. The authoritative document is replaced
only after durable append success at `src/actor.rs:405-409`. Exact received
blob bytes, not re-exported bytes, are placed in the storage frame.

### Recovery fails closed on ordinary parse/import errors

Room activation reads the complete stream, decodes every frame, validates the
contained Loro blobs, and imports them in stream order at
`src/actor.rs:240-259`. Initialization failure causes `JoinError` and prevents
updates from reaching storage. No malformed frame is intentionally skipped.

## Code Locations

| Guarantee | Baseline enforcement |
| --- | --- |
| One room writer | `src/actor.rs:96-105`, `src/actor.rs:212-238` |
| One unresolved producer append | `src/actor.rs:171-186`, `src/actor.rs:346-353` |
| Exact retry tuple and bytes | `src/actor.rs:400-403`, `src/actor.rs:448-451` |
| Sequence advance after success | `src/actor.rs:405-409` |
| Duplicate byte comparison | `src/actor.rs:454-480` |
| Loro preflight before append | `src/actor.rs:355-399` |
| Authoritative state after durability | `src/actor.rs:405-423` |
| Full replay from offset zero | `src/ursula.rs:176-191`, `src/actor.rs:240-259` |
| Official fragment reassembly | `src/server.rs:181-329` |
| Only `%LOR` joins | `src/server.rs:134-158` |

## Missing Tests

- Every durable acknowledgement transition as a typed unit: committed,
  verified duplicate, definite rejection, and unknown outcome.
- Duplicate verification underflow, wrong next offset, short/long range,
  changed body, changed tuple, digest mismatch, and valid exact duplicate.
- Decoder limits for frame size, producer ID, update count, individual update,
  and aggregate update bytes.
- Arbitrary-byte decoder tests proving no panic.
- Recovery corruption in first, middle, and final frames; truncated final frame;
  trailing junk; unsupported version; and errors containing stream offset.
- Fragment missing-final, timeout, duplicate, conflict, oversize, disconnect,
  two concurrent batches, aggregate memory, and valid out-of-order assembly.
- Oversized committed update fan-out through official fragmentation.
- Join/update behavior while a room is recovering, ambiguous, corrupt, or
  unavailable.
- Full snapshot policy on empty and non-empty rooms, malformed snapshot,
  unresolved dependencies, definite commit failure, and post-commit rebuild.
- HTTP delayed body, oversized body, offset/body mismatch, redirect,
  classification, retry, and backoff tests.
- A real child-process crash after backend commit and before WebSocket ACK.
- Official TypeScript client interoperability.

## Correctness Risks

### Untrusted allocation before validation

`DeltaFrame::decode_one` allocates from an untrusted `u32` update count before
proving its length table fits the frame. The pinned official codec similarly
allocates from an untrusted protocol update count. Small attacker-controlled
inputs can therefore panic or attempt excessive allocation before application
checks run.

### Offset/body mismatch can omit durable bytes

The HTTP reader accepts any increasing `Stream-Next-Offset`; it does not prove
that `next_offset == requested_offset + body.len()`. A malformed response can
skip committed bytes while replay continues. The same issue weakens duplicate
range verification.

### Duplicate-range proof is incomplete

Ursula offsets count exact appended bytes. With exactly one unresolved append,
an exact duplicate's saved `next_offset` is the end of that original frame, so
`next_offset - retained_frame_length` is its candidate start. This derivation
is valid only if checked subtraction succeeds and a read of exactly that byte
range returns exactly the retained frame. The baseline compares bytes but does
not independently validate all requested semantic fields or protect the HTTP
range contract from inconsistent offsets.

### Fragment and fan-out risks

Fragment limits are per batch, while concurrent batch count and aggregate
connection memory are unbounded. Conflicting duplicate headers/fragments are
not rejected. Incomplete batches retain outbound senders, causing disconnect
cleanup to wait forever. Updates larger than the protocol message limit commit
and receive `Ack(Ok)`, but live fan-out sends an oversized unfragmented message
that the writer silently drops.

### Ambiguous rooms serve stale state

After retry exhaustion, later writes are blocked but joins still receive
`JoinResponseOk` from the pre-append document. The unresolved append may have
committed, so the advertised state can be stale.

### Snapshot policy is too broad

All checksum-valid blob modes are accepted. The official empty-receiver flow
requires a full snapshot, but a snapshot must not replace an already
authoritative room and shallow snapshots must not enter Phase 1 storage.

### Process crash claim is unproven

`crash_after_commit_before_ack_recovers_from_ursula` drops an in-memory ACK
receiver and manager. It does not terminate an operating-system process; the
detached room actor can continue running.

## Operational Limitations

- One permanent, unbounded delta stream and O(history) validation/recovery.
- Complete raw history remains in each active actor.
- In-memory fan-out and no active-active gateway coordination.
- Per-boot producer identity; restart relies on replay and Loro reconciliation.
- No authentication, actor eviction, or slow-consumer memory policy.
- Single-node Ursula verification only; no quorum failure test.
- No total Ursula cluster-loss recovery guarantee.
- No checkpoint, generation, retention, or bounded recovery-time guarantee.
- The official TypeScript client has not yet passed an interoperability suite.

## Fixes Selected For Phase 1.5

1. Add `FrameLimits`, validate all lengths before allocation, add offset-aware
   typed errors, strict one-frame decoding, and arbitrary-byte panic tests.
2. Introduce typed append resolution outcomes: `Committed`,
   `VerifiedDuplicate`, `DefinitelyRejected`, and `OutcomeUnknown`.
3. Centralize `Ack(Ok)` construction behind a durable-success type that can be
   created only by committed or byte-verified duplicate transitions.
4. Move exact duplicate verification into one tested function that validates
   range arithmetic, exact bytes, every frame field, checksum, and digest.
5. Require HTTP body length to equal offset progress; bound response bodies and
   total replay; classify redirects explicitly; add bounded exponential retry
   with jitter for safe idempotent reads/creates while preserving actor-owned
   exact append retries.
6. Add explicit `Recovering`, `Ready`, `AppendAmbiguous`, `Corrupt`, and
   `Unavailable` room states, a controlled retry command, structured state
   logs, and a payload-free debug endpoint.
7. Bound concurrent fragment batches and aggregate bytes per connection;
   reject conflicting metadata/fragments; ensure timeout/disconnect/leave
   cleanup; and fragment oversized live fan-out.
8. Restrict client snapshots to one full snapshot that contextually initializes
   an empty authoritative room; reject shallow or replacement snapshots.
9. If authoritative installation cannot be verified after commit, reconstruct
   from Ursula before sending success; preserve the durable update on failure.
10. Add an ignored, reproducible child-process crash test with a fake Ursula
    endpoint and document the exact command.
11. Add a concrete official TypeScript harness if practical; otherwise keep it
    as an explicit unresolved acceptance gate and do not claim interoperability.
12. Update README guarantees and observability documentation without claiming
    production readiness.

## Explicitly Deferred

Phase 1.5 does not implement manifests, checkpoints, generation rotation,
retention, DELETE, multi-gateway coordination, authorization, or Ursula source
changes.

## Phase 1.5 Resolution

The selected fixes were implemented in focused milestones after this baseline
audit:

- Durable frame and history limits are enforced before attacker-controlled
  allocation, and recovery errors carry stream offsets.
- `Ack(Ok)` requires a typed committed or fully verified duplicate proof.
- HTTP reads enforce exact offset/body progress, bounded timed bodies, bounded
  total replay, redirect rejection, and safe-operation retries with jitter.
- A structural precheck protects the pinned official decoder from hostile
  update counts before invoking it.
- Fragment batches, counts, aggregate reservation, conflicts, timeout, leave,
  and disconnect cleanup are bounded; oversized live fan-out is fragmented.
- Room lifecycle is explicit and visible at `/debug/rooms`; ambiguous joins and
  writes fail closed and controlled reconciliation reloads Ursula.
- Full snapshots are empty-room initialization only; shallow and replacement
  snapshots are rejected before storage.
- The feature-gated child-process crash test now proves the post-commit/pre-ACK
  boundary and Ursula-only restart recovery.
- The pinned official TypeScript packages now pass bidirectional convergence
  through the gateway.

The non-production limitations and later-phase deferrals above remain in force.
