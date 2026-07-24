# Design

Quorum Loro Gateway connects the official Loro WebSocket protocol to Ursula durable streams.

The core rule is simple: receiving an update is not enough to acknowledge it. The gateway waits until Ursula has committed the exact stored frame, or proves that the same frame was already committed during an earlier attempt.

## Responsibilities

**Loro** owns CRDT semantics, version vectors, offline edits, and convergence.

**Ursula** owns durable byte ordering, replication, producer deduplication, and replay.

**The gateway** owns protocol handling, validation, framing, retries, acknowledgement policy, in-process fan-out, and reconstruction after restart.

Only `%LOR` rooms from Loro Synchronization Protocol v1 are accepted.

## Write path

A room is owned by one Tokio actor. That actor serializes writes and allows only one unresolved Ursula append at a time.

```text
DocUpdate
   │
   ├─ validate protocol and Loro encoding
   ├─ test the update against a reconstructed document
   ├─ build one QLGD storage frame
   └─ append with Producer-Id / Epoch / Seq
              │
              ├─ committed ───────────────┐
              │                           │
              └─ duplicate                │
                    └─ read stored range  │
                       and compare bytes  │
                                          ▼
                                       Ack(Ok)
```

The producer sequence advances only after a committed append or a verified duplicate. A rejected append does not consume the sequence.

## Lost responses and duplicate proof

A network failure can happen after Ursula commits but before the gateway receives the response. The gateway keeps the exact producer tuple and frame bytes and retries them unchanged.

Ursula producer deduplication does not compare request bodies, so a duplicate response alone is not enough. The gateway derives the committed byte range, reads it from Ursula, checks the range length, compares every byte, decodes exactly one frame, and verifies its producer tuple, batch ID, updates, digest, and checksums.

Only then can a duplicate retry produce `Ack(Ok)`.

If the result remains unknown after the configured retries, the room enters `append_ambiguous`. Joins and writes fail closed until a controlled retry and a full Ursula reload succeed.

## Stored frame

Every append is one self-delimiting `QLGD` frame containing:

- format version and total length
- producer ID, epoch, and sequence
- the Loro protocol batch ID
- the exact received Loro blobs and their lengths
- a SHA-256 digest over the ordered updates
- a CRC32 checksum over the frame body

The gateway stores the original update bytes. It does not decode and export them into a new representation before persistence.

Frame sizes, update counts, update bytes, complete history, HTTP bodies, WebSocket messages, and fragment buffers all have configurable limits.

## Room recovery

Each room currently maps to one permanent Ursula stream. The physical stream name is derived from a SHA-256 hash of the room ID because Ursula stream IDs do not allow `/`.

On activation, the gateway:

1. reads the stream from offset zero in bounded windows;
2. checks that every response advances by exactly the number of returned bytes;
3. decodes every frame in order;
4. validates every Loro blob and snapshot rule;
5. imports the full history into a new `LoroDoc`;
6. marks the room ready only after the complete replay succeeds.

No malformed frame is skipped. Corruption, truncation, unsupported versions, invalid Loro data, and failed imports leave the room unavailable or corrupt.

The active actor keeps the raw update history in memory because candidate updates are validated against the reconstructed history. This is one reason checkpoints are planned.

## Snapshot policy

The official protocol may send a full snapshot when an empty receiver joins. The gateway accepts one full snapshot only when it is the sole blob initializing a room with no durable history.

It rejects:

- a full snapshot replacing an existing room
- shallow snapshots
- outdated encodings
- empty update batches
- malformed blobs
- updates whose dependencies cannot be imported

Loro full snapshots do not include causally pending updates. A future checkpoint format therefore cannot be just a snapshot blob; it also needs to preserve unresolved raw updates required for correct recovery.

## Ursula transport

Writes may be sent to any configured Ursula node. The client follows Ursula `307` leader redirects only when:

- the response includes a valid Ursula leader ID;
- the target origin is explicitly allowlisted;
- the path and query are unchanged;
- the redirect chain has no loop and stays within the configured hop limit.

A redirect is never commit proof. The exact body and producer tuple are rebuilt for every append attempt.

Finite reads can be served by a follower and may be stale when a quorum is unavailable. The gateway therefore does not treat an ordinary follower read as proof that it has reached the latest committed tail.

## Room states

- `recovering`: Ursula history is being loaded
- `ready`: joins and updates are accepted
- `append_ambiguous`: one append may or may not have committed
- `corrupt`: durable bytes or Loro replay failed validation
- `unavailable`: Ursula or reconciliation is unavailable

`GET /debug/rooms` exposes these states, producer progress, peer count, recovery timings, and the last error. It does not expose document contents or frame bytes.

## Current boundary

The current storage model is intentionally simple: one unbounded append-only stream per room and full replay on activation.

There is no manifest, checkpoint, stream rotation, retention, active-active writer coordination, or cross-gateway live delivery yet. The service also has no built-in authentication or authorization policy.
