# Quorum Loro Gateway

Quorum Loro Gateway is a Rust prototype that connects the official Loro sync protocol with Ursula.

The main guarantee is simple: the gateway sends `Ack(Ok)` only after the exact update has been committed to Ursula or an uncertain retry has been verified against the stored bytes.

## Why I built it

The original recovery flow replayed the complete update history every time a room restarted.

That works, but recovery becomes slower as the room grows.

This project adds bounded recovery using:

```text
manifest → checkpoint → active delta
```

The gateway can now restore the latest checkpoint and replay only the current delta generation.

## How it works

A client update is validated, encoded into a durable frame, and appended to Ursula.

If the result is uncertain, the gateway retries using the same producer identity and verifies that the stored bytes match before returning success.

During rotation, the gateway creates a checkpoint, creates the next delta generation, publishes a manifest record, and switches the room to the new stream.

Rotation is currently triggered manually through `RoomHandle::rotate()`.

## Benchmark

A small in memory benchmark used 2,000 updates.

```text
Full replay:        261.48 ms
Bounded recovery:     8.13 ms

Full delta:        395,679 bytes
Active delta:        9,900 bytes
```

The benchmark is not a real Ursula cluster benchmark. It only shows that restart replay depends on the active generation instead of the complete room history.

Run it with:

```bash
cargo test --test bounded_recovery_benchmark \
  -- --ignored --nocapture
```

## Testing

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Scope

This is a small database and distributed systems prototype, not a production service.

It does not include multiple active gateway writers, distributed leases, authentication, old generation cleanup, automatic background rotation, or production cluster benchmarks.

The goal was to explore durable acknowledgements, safe retries, checkpoints, manifests, stream generations, and bounded recovery without turning it into a much larger system.

## License

Apache 2.0
