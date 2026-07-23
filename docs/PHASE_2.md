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

Quorum durability, leader-failure, minority/quorum-loss, producer-state, and
full-replay results will be recorded here only after their scripts and raw
results have run successfully.
