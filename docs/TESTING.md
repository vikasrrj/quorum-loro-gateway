# Testing

The test suite is split between fast local tests and slower tests that start real processes.

## Normal checks

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

The default suite covers frame parsing, protocol bounds, Loro validation, ACK timing, duplicate verification, ambiguous writes, corruption handling, restart replay, fragmentation, HTTP response checks, retries, and redirect handling.

## What the local tests cover

The main integration tests in `tests/gateway.rs` use deterministic in-memory storage to exercise the gateway logic without requiring Ursula.

They check that:

- `Ack(Ok)` waits for durable proof;
- definite rejection and unknown outcomes never become success;
- a lost response retries the exact same tuple and body;
- duplicate ranges with the wrong length, bytes, or offsets are rejected;
- an ambiguous room stops serving joins and writes;
- a restarted manager reconstructs state without local durable data;
- corrupt history fails closed;
- full snapshots only initialize empty rooms;
- fragmented updates are bounded and reassembled safely;
- oversized live updates are fragmented for subscribers;
- debug output does not leak document content.

Unit tests also feed arbitrary bytes into the frame and protocol decoders to make sure malformed input returns an error instead of panicking.

## TypeScript interoperability

The repository pins `loro-protocol@0.3.0` and `loro-crdt@1.13.7` for the JavaScript client test.

```bash
npm ci --prefix interop
cargo test --test gateway official_typescript_clients_converge \
  -- --ignored --nocapture
```

This starts the Rust gateway and verifies bidirectional convergence with official TypeScript protocol clients.

## Single-node Ursula acceptance test

With Ursula running on `127.0.0.1:4437`:

```bash
cargo test --test gateway real_ursula_commit_duplicate_and_restart_replay \
  -- --ignored --nocapture
```

This checks the real HTTP contract, duplicate response handling, and replay after creating a fresh gateway manager.

## Crash after commit, before ACK

```bash
cargo test --features crash-injection --test process_crash \
  -- --ignored --nocapture
```

The harness starts a gateway child process, lets Ursula commit an update, kills the child before the WebSocket ACK is sent, then starts a fresh child and verifies that the update is reconstructed from Ursula.

The `crash-injection` feature exists only for this test boundary.

## Three-voter Ursula tests

The cluster scripts expect an Ursula release binary at `/home/vik/ursula/target/release/ursula`. Set `URSULA_BIN` to use another path.

```bash
scripts/ursula-cluster-start.sh
scripts/ursula-cluster-stop.sh
scripts/ursula-cluster-clean.sh
```

Run all cluster tests serially:

```bash
cargo test --test cluster_failures \
  -- --ignored --nocapture --test-threads=1
```

The cluster tests cover:

### One voter and gateway fail

An update receives `Ack(Ok)`, one Ursula voter is killed, the gateway is killed, and a fresh gateway rebuilds the update from the two surviving voters.

```bash
cargo test --test cluster_failures \
  acknowledged_update_survives_one_voter_and_gateway_crash \
  -- --ignored --nocapture
```

### Leader fails around an append

The tests kill the leader before submission, after commit but before the HTTP response reaches the gateway, and after the client receives `Ack(Ok)`.

```bash
cargo test --test cluster_failures leader_failure \
  -- --ignored --nocapture --test-threads=1
```

The post-commit case is the most important one: the retry reaches the new leader, Ursula reports a duplicate, and the gateway verifies the stored frame before acknowledging it.

### Quorum is lost

After one voter fails, writes still commit through the remaining majority. After a second voter fails, the next write never receives `Ack(Ok)` and later writes stay blocked behind the unresolved sequence.

```bash
cargo test --test cluster_failures \
  one_voter_allows_writes_but_quorum_loss_never_acks \
  -- --ignored --nocapture
```

## What these tests do not prove

They do not prove recovery after two or three replicas are permanently lost, cold-tier disaster recovery, safe multi-gateway writing, authentication, retention, or production behavior under sustained concurrent load.
