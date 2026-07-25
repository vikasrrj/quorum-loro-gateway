Quorum Loro Gateway

A Rust gateway connecting the official Loro sync protocol to Ursula.

The gateway sends Ack(Ok) only after the exact update bytes are committed to Ursula, or after an uncertain retry is verified byte for byte against what's actually stored.

Why this exists

#why-this-exists

The original gateway recovered a room by replaying its entire Ursula delta stream from offset zero. That's correct, but the work grows with the room's full history, forever.

This project fixes that with bounded recovery: manifest → checkpoint → active delta generation. A restart now loads the latest checkpoint and replays only the current generation, not everything that ever happened.

How a write actually flows

#how-a-write-actually-flows

loro client update
        ↓
room actor validates it
        ↓
exact delta frame gets encoded
        ↓
frame appended to ursula
        ↓
commit or exact duplicate is verified
        ↓
ack(ok)

If an append result is ever ambiguous, the gateway never returns success until it's confirmed the stored bytes match the original frame exactly. No guessing.

Bounded recovery

#bounded-recovery

Each room has three streams: manifest, checkpoint/{generation}, delta/{generation}.

A checkpoint holds the current Loro snapshot, the source delta generation and offset, any raw updates still causally pending, and a digest, checksum, and length for validation.

The manifest tracks the latest checkpoint generation, its digest and length, the active delta generation, and a digest link back to the previous manifest record.

On restart, the gateway reads and validates the manifest, loads the checkpoint it points to, restores the snapshot plus pending updates, and replays only the active generation forward. Rooms without a manifest just keep using the old full-replay path, so nothing older breaks.

Rotation

#rotation

Rotation is serialized through the room actor. When it happens, the gateway builds an immutable checkpoint, creates the next empty delta generation, publishes the new manifest record, then switches over. The manifest is published before the in-memory switch, so it stays the true source of record even across a crash mid-rotation.

Right now rotation is manual, triggered through RoomHandle::rotate().

Benchmark

#benchmark

An in-memory comparison of full replay versus bounded recovery, on 2,000 updates:

Metric	Legacy (full replay)	Bounded recovery
Updates covered	1,950 checkpointed	50 active
Delta bytes	395,679	9,900
Recovery time	261.48 ms	8.13 ms

This isn't a real Ursula cluster benchmark, just proof that recovery cost now scales with the active generation, not the room's whole history.

Run it yourself:

cargo test --test bounded_recovery_benchmark -- --ignored --nocapture
What it actually guarantees

#what-it-actually-guarantees

Ack(Ok) only comes after a durable commit or an exact duplicate verification. Ambiguous retries always reuse the same producer tuple and exact frame bytes. Corrupt checkpoints, manifests, or deltas fail closed rather than silently proceeding. Stream and generation names are never reused. Causally pending Loro updates survive across checkpoints. Old rooms stay compatible. Rotation is fully serialized with regular writes.

Testing

#testing

cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings

Covers durable acks, ambiguous and duplicate appends, gateway restart recovery, corrupted frames/checkpoints/manifests, pending Loro updates, checkpoint creation, manifest chaining, generation rotation, and official protocol compatibility. A few Ursula cluster and TypeScript interop tests are skipped unless their external dependencies are available.

Scope

#scope

This is a distributed systems research prototype, not a production service. On purpose, it doesn't include multiple active writers per room, distributed leases or fencing, auth, old generation cleanup, manifest compaction, automatic rotation, or real cluster benchmarks.

The goal was narrower than all that: prove a correct durable acknowledgement boundary, and a practical checkpoint plus generation design for bounded Loro recovery.

License

#license

Apache 2.0
