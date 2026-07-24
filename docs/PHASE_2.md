# Phase 2: Multi-Node Ursula Validation

Phase 2 validates the existing gateway against a real three-voter Ursula
cluster. It does not add manifests, checkpoints, generation rotation,
retention, multi-gateway writing, or Ursula source changes.

## Cluster Topology

The local harness uses four Raft groups and three static voters. Each node uses
one shared client/Raft listener because Ursula currently builds HTTP leader
redirects from `raft.peers[].url`. A split Raft-only listener would therefore
produce a redirect target that does not serve stream HTTP routes.

| Node | Client and Raft | Admin | WAL | Snapshot store |
| --- | --- | --- | --- | --- |
| 1 | `127.0.0.1:18101` | `127.0.0.1:18201` | `target/phase2-cluster/data/node-1` | `target/phase2-cluster/snapshots/node-1` |
| 2 | `127.0.0.1:18102` | `127.0.0.1:18202` | `target/phase2-cluster/data/node-2` | `target/phase2-cluster/snapshots/node-2` |
| 3 | `127.0.0.1:18103` | `127.0.0.1:18203` | `target/phase2-cluster/data/node-3` | `target/phase2-cluster/snapshots/node-3` |

Generated configs, PIDs, logs, WALs, and snapshots remain under ignored
`target/phase2-cluster` and are never committed.

Build Ursula without changing its source:

```bash
(cd /home/vik/ursula && cargo build --release --locked --bin ursula)
```

Start, stop, restart, and clean the cluster:

```bash
scripts/phase2-cluster-start.sh
scripts/phase2-cluster-stop.sh
scripts/phase2-cluster-start.sh
scripts/phase2-cluster-clean.sh
```

`URSULA_BIN` can select another existing Ursula binary.
`PHASE2_CLUSTER_ROOT` can select an isolated root ending in `phase2-cluster`.
The start script launches nodes 2 and 3 before node 1, enables membership
initialization only for a fresh node-1 WAL, and waits until all nodes report all
four groups, voters `[1,2,3]`, and elected leaders.

## Verified Redirect Contract

Read-only source inspection at the Ursula revision in `/home/vik/ursula`
established this contract before gateway implementation:

- Writes sent to a follower return `307 Temporary Redirect`.
- The response includes an absolute `Location` and
  `x-ursula-raft-leader-id`.
- `Location` is the configured leader base plus the original path and query.
- A node that does not host the group may redirect to a configured voter, which
  may require another hop to reach the leader.
- Election/no-leader states return `503 Service Unavailable` with
  `Retry-After: 1`, not a successful response.
- Ordinary finite reads can be served locally by followers and may be stale.
  A follower without a leader can return `200` while omitting
  `stream-up-to-date`.

The gateway therefore must follow only marked Ursula `307` responses, retain
the method, exact append bytes and producer tuple, enforce an allowlist and hop
bound, reject loops/malformed targets, and never treat a redirect as commit
proof.

The implemented transport accepts only `307`, requires a numeric
`x-ursula-raft-leader-id`, requires an absolute `Location` whose origin appears
in the configured node list, and requires every hop to preserve the initial
path and query. It detects repeated target URLs and enforces
`--ursula-max-redirects`. Redirect failures and unreachable targets are
ambiguous outcomes, never commit proof. Append replay rebuilds every request
from the retained frame bytes and producer tuple; no redirect changes
`Producer-Seq`.

Run the gateway against any cluster node while allowlisting all possible
redirect targets:

```bash
cargo run -- \
  --ursula-url http://127.0.0.1:18102 \
  --ursula-peer-url http://127.0.0.1:18101 \
  --ursula-peer-url http://127.0.0.1:18102 \
  --ursula-peer-url http://127.0.0.1:18103 \
  --ursula-max-redirects 4
```

## Experiment Results

### One-voter failure durability

The real-process ignored test performs this sequence against disk-backed
Ursula voters:

1. Start three Ursula processes and a gateway process.
2. Join with the official Loro protocol and wait for `Ack(Ok)`.
3. SIGKILL voter 3.
4. SIGKILL the gateway.
5. Start a fresh gateway process with no local durable state against voter 2.
6. Rejoin and reconstruct the acknowledged Loro update from the two survivors.

This passed locally:

```bash
cargo test --test phase2_cluster \
  acknowledged_update_survives_one_voter_and_gateway_crash \
  -- --ignored --nocapture
```

This proves survival of one voter failure for an acknowledged update in the
tested three-voter, disk-WAL topology. It does not claim recovery from two or
three lost replicas.

### Leader failure

All leader-failure tests use actual Ursula and gateway processes:

- **Before the append reached the leader:** the stream leader was SIGKILLed
  immediately before submission. The observed result was `Ack(Unknown)`. The
  room entered `append_ambiguous`, and the next update returned `RateLimited`;
  no later `Producer-Seq` reached Ursula.
- **After replication, before the gateway received the HTTP response:** a
  test-only reverse proxy forwarded the append and waited for Ursula `200`,
  then held that response while the leader was SIGKILLed. It returned `503` to
  the gateway. The exact tuple/body retry reached the new leader, Ursula
  returned `204`, the gateway verified the exact stored frame range, and only
  then returned `Ack(Ok)`. The durable stream contained exactly one frame.
- **After the client received `Ack(Ok)`:** the current stream leader was
  SIGKILLed. A fresh gateway started against a survivor after re-election and
  reconstructed the acknowledged update.

Run the three scenarios serially because they use fixed isolated ports:

```bash
cargo test --test phase2_cluster leader_failure \
  -- --ignored --nocapture --test-threads=1
```

### Minority and quorum loss

After voter 3 was SIGKILLed, voters 1 and 2 elected leaders for all four groups,
and a subsequent update received `Ack(Ok)`. After voter 1 was also SIGKILLed:

- the next append returned `Ack(Unknown)`, never `Ack(Ok)`;
- the following update returned `RateLimited`, proving the sequence remained
  blocked behind the unresolved append;
- the sole surviving follower returned `200` for a finite read containing 518
  committed bytes in this test;
- that read omitted a true `stream-up-to-date` claim.

Run the experiment with:

```bash
cargo test --test phase2_cluster \
  one_voter_allows_writes_but_quorum_loss_never_acks \
  -- --ignored --nocapture
```

Ordinary finite reads can therefore remain available from a minority, but they
must be treated as potentially stale. Writes cannot commit without two voters.
No total-cluster-loss recovery claim is made.

### Ursula producer-state growth

`scripts/phase2-producer-state.py` compares two fresh three-voter clusters at
0, 100, 1,000, and 5,000 appends. The control sends the same 32-byte payloads
without producer headers. The producer workload assigns every append a distinct
68-byte gateway-shaped producer ID at epoch and sequence zero. All replicas are
allowed to apply through the committed index before measurement, and snapshots
are explicitly triggered at the first and last stages.

At 5,000 producers, the producer workload exceeded the control by:

| Measurement | Replica 1 | Replica 2 | Replica 3 |
| --- | ---: | ---: | ---: |
| WAL bytes | 390,367 | 389,795 | 389,539 |
| WAL bytes per producer | 78.0734 | 77.9590 | 77.9078 |
| Logical snapshot bytes | 447,938 | 447,938 | 447,938 |
| Logical snapshot bytes per producer | 89.5876 | 89.5876 | 89.5876 |

Physical snapshot directories are not used for the estimate because Ursula
retains cumulative local files without pruning. The Raft snapshot body metric
is the comparable per-build value. RSS deltas were 11,534,336, 5,144,576, and
-37,494,784 bytes. Those values are inconclusive: mimalloc released large
regions during sampling, so no in-memory bytes-per-producer estimate is claimed.

Every fresh gateway boot derives a new producer ID for each room it activates.
Consequently, repeated restarts add durable producer entries to that room's
Ursula stream even though the gateway keeps no local durable state. The measured
growth was approximately 90 logical-snapshot bytes and 78 current-WAL bytes per
retained producer on each replica for this workload; these are not general
Ursula format guarantees.

Run the measurement and inspect its raw samples with:

```bash
python scripts/phase2-producer-state.py
less results/phase2/producer-state.json
```

### Full replay scaling

The release-mode ignored benchmark generated 100, 1,000, and 10,000 sequential
single-character Loro updates, stored each exact update in one `QLGD` frame, and
loaded each complete encoded history into Ursula. It then measured production
`RoomActor` activation through `HttpUrsula` against the stream leader. Recovery
used 64 KiB finite read windows; larger windows produced intermittent Ursula
`500` responses at the largest history, so 64 KiB is also the gateway default.
History generation and loading are outside the reported recovery interval.

Each size ran three times against a three-voter disk-WAL/local-snapshot cluster
using the release profile. Reported durations are medians:

| Updates | Stored stream | Average Loro blob | Recovery total | Loro import | Activation wall | Maximum observed RSS delta |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 19,835 B | 85.3500 B | 6.260 ms | 1.389 ms | 6.433 ms | 409,600 B |
| 1,000 | 200,679 B | 87.6790 B | 35.626 ms | 17.621 ms | 35.846 ms | 458,752 B |
| 10,000 | 2,011,487 B | 88.1487 B | 173.928 ms | 75.231 ms | 174.112 ms | 6,971,392 B |

The process allocator retained memory between repetitions, so the RSS column is
the maximum observed process delta, not a per-room heap estimate. The timing
results apply to this local environment and synthetic update shape. They show
that full replay remains workable at 10,000 small updates, but do not remove its
O(history) cost or the active actor's retained raw history.

Run the benchmark with:

```bash
cargo test --release --test phase2_replay full_replay_benchmark \
  -- --ignored --nocapture
less results/phase2/full-replay.json
```

The raw results record Linux 6.19.11, Ursula revision
`3e13e3bfc6d985a0e867ab63a5ebc2d7d55f9b0c`, and gateway revision
`786d10f63bfb5e72cb2e02641642bc0768ac5500`.

### Checkpoint recommendation

Phase 2 deliberately leaves checkpointing unimplemented. Before production,
add a measured checkpoint/compaction design rather than relying on an unbounded
permanent delta stream. A conservative initial trigger for this update shape is
the earlier of 10,000 updates or 2 MiB of encoded stream history, where local
activation reached approximately 174 ms. That trigger is a starting point, not
a universal threshold: tune it against the deployment's recovery SLO, update
sizes, storage latency, and concurrent room load. Until then, configured history
limits must continue to fail closed.
