# Phase 2.5: Full-Replay Scale Decision

## Decision

**Recommendation: B. Begin Phase 3 checkpoint implementation, but only on the
explicit provisional SLO that p95 room activation after a gateway restart must
remain at or below one second.**

The measurements do not establish that checkpoints are universally required.
They establish that the current unbounded full-replay design violates that SLO
between 50,000 and 100,000 updates for the measured workload. If a deployment
does not adopt the one-second SLO and can tolerate approximately five seconds of
activation at 250,000 small updates, full replay remains functionally correct.

The previous 10,000-update / 2-MiB proposal should not be retained. For this
workload it is too conservative. A measured initial checkpoint trigger is the
earlier of approximately 40,000 updates or 8 MiB of encoded post-checkpoint
history. That estimate reserves 20% of the one-second p95 budget and must be
recalibrated for other update distributions and production hardware.

No checkpoint, manifest, rotation, retention, or Ursula change was implemented
in Phase 2.5.

## Raw Results

The complete machine-readable observations are committed at:

- `results/phase2_5/scale-replay.json`
- `results/phase2_5/restart-producers.json`

The replay command was:

```bash
PHASE2_REPLAY_COUNTS=10000,25000,50000,100000,250000 \
  cargo test --release --locked --test phase2_replay \
  full_replay_benchmark -- --ignored --nocapture --test-threads=1
```

Each size ran three times. Every repetition used a fresh three-voter Ursula
cluster and a fresh release-mode gateway worker process. Count order rotated by
one position between repetitions. Nearest-rank percentiles are used; with only
three repetitions, p95 is the maximum observation and should be treated as a
coarse bound rather than a production-grade percentile estimate.

Durations below are milliseconds. Peak RSS is Linux `VmHWM` for the isolated
gateway worker, measured immediately after activation and before state export.
The 1-ms `VmRSS` sampler is retained in the JSON but is a lower bound because it
missed short allocation spikes.

| Updates | Rep | Encoded bytes | Activation | Recovery total | Ursula read | Frame decode | Loro import | GETs | Peak RSS MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 1 | 2,031,487 | 126.055 | 125.946 | 7.742 | 6.303 | 64.371 | 31 | 15.164 |
| 10,000 | 2 | 2,031,487 | 237.315 | 236.739 | 30.330 | 17.178 | 81.415 | 31 | 14.918 |
| 10,000 | 3 | 2,031,487 | 214.205 | 213.999 | 26.274 | 16.835 | 82.360 | 31 | 14.789 |
| 25,000 | 1 | 5,108,719 | 341.329 | 341.157 | 15.119 | 12.681 | 187.409 | 78 | 24.789 |
| 25,000 | 2 | 5,108,719 | 402.176 | 402.077 | 14.466 | 21.184 | 216.127 | 78 | 24.797 |
| 25,000 | 3 | 5,108,719 | 532.991 | 532.776 | 62.689 | 38.023 | 229.479 | 78 | 24.789 |
| 50,000 | 1 | 10,258,719 | 657.209 | 657.097 | 32.275 | 26.354 | 384.028 | 157 | 42.246 |
| 50,000 | 2 | 10,258,719 | 896.258 | 896.012 | 113.869 | 83.325 | 410.507 | 157 | 42.543 |
| 50,000 | 3 | 10,258,719 | 911.786 | 911.575 | 74.110 | 54.879 | 468.695 | 157 | 42.328 |
| 100,000 | 1 | 20,558,719 | 1,450.196 | 1,450.086 | 48.728 | 54.167 | 841.621 | 314 | 77.152 |
| 100,000 | 2 | 20,558,719 | 1,903.519 | 1,903.272 | 231.715 | 113.534 | 970.373 | 314 | 77.152 |
| 100,000 | 3 | 20,558,719 | 1,817.666 | 1,817.470 | 227.753 | 103.024 | 950.373 | 314 | 76.926 |
| 250,000 | 1 | 51,458,719 | 3,403.553 | 3,403.438 | 133.639 | 132.696 | 2,036.687 | 786 | 178.578 |
| 250,000 | 2 | 51,458,719 | 4,654.695 | 4,654.472 | 607.081 | 221.534 | 2,365.874 | 786 | 178.676 |
| 250,000 | 3 | 51,458,719 | 4,816.762 | 4,816.618 | 570.601 | 163.311 | 2,652.240 | 786 | 178.805 |

The component durations do not sum to total recovery. The residual includes
stream creation checks, per-update Loro metadata validation, frame-to-history
flattening, allocations, and actor setup.

### Summary

| Updates | Stream MiB | Median activation | p95 activation | Median read | Median decode | Median import | Median GETs | p95 peak RSS MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 1.937 | 214.205 ms | 237.315 ms | 26.274 ms | 16.835 ms | 81.415 ms | 31 | 15.164 |
| 25,000 | 4.872 | 402.176 ms | 532.991 ms | 15.119 ms | 21.184 ms | 216.127 ms | 78 | 24.797 |
| 50,000 | 9.783 | 896.258 ms | 911.786 ms | 74.110 ms | 54.879 ms | 410.507 ms | 157 | 42.543 |
| 100,000 | 19.606 | 1,817.666 ms | 1,903.519 ms | 227.753 ms | 103.024 ms | 950.373 ms | 314 | 77.152 |
| 250,000 | 49.075 | 4,654.695 ms | 4,816.762 ms | 570.601 ms | 163.311 ms | 2,365.874 ms | 786 | 178.805 |

All three repetitions at every size reconstructed the expected text and agreed
on the canonical benchmark state hash:

| Updates | SHA-256 |
| ---: | --- |
| 10,000 | `b18199dfea741161a9c36bf267ae4608d4b47440762cad791ec5e8bdf13f21f8` |
| 25,000 | `b8336caf1a1063daa7ab061aa05d7259f567d14274d80af4f6fe09aa7d8f0abf` |
| 50,000 | `b8e15009dd7871e658eafd992e1bd2769e8a5d0ea2f244196225261c32b03c62` |
| 100,000 | `04e48322a9a8c361e1e76db13f2b223123ffda944c208f8c1306947958e8c707` |
| 250,000 | `6ffb2a46ce34aca7bba356cecf0d41f02bf550f83f18b46a0c5173191ae048f8` |

## Environment And Workload

The measured gateway revision was
`5ebeac8afec3453730d00f92cb8b808ec6706705`.

| Item | Value |
| --- | --- |
| CPU | 11th Gen Intel Core i5-1155G7 at 2.50 GHz |
| Logical CPUs | 8 |
| Memory | 16,530,407,424 bytes |
| Kernel | Linux 6.19.11-arch1-1 |
| Rust | 1.96.0, LLVM 22.1.2 |
| Build | Cargo release profile, locked dependencies |
| Ursula | Three voters, four Raft groups, one runtime core per node |
| Ursula persistence | Disk WAL, local snapshots, memory cold store |
| Ursula snapshot drive | Disabled (`0s`) |
| Read target | Current stream leader |
| Read window | 65,536 bytes |
| Loader chunks | Frame-aligned, at most 1 MiB target size |

The deterministic workload used one root text named `text`, Loro peer ID 700,
and one operation per update: append one ASCII `x` and commit. Each update was
stored in one `QLGD` frame. Loro blobs ranged from 84 to 91 bytes; their average
rose from 88.1487 bytes at 10,000 updates to 90.834876 bytes at 250,000 updates.
There was no random workload input.

History generation, Ursula loading, state export, and hash verification were
outside the activation interval. Reads used production `RoomActor` recovery and
`HttpUrsula`. GET counts include actual retry and redirect attempts; counts in
the final runs equal the required 64-KiB windows.

Linux page cache was not dropped. Each cluster and gateway process was fresh,
but the benchmark therefore represents normal warm-host behavior, not cold-disk
disaster recovery.

## Scaling Curves

Least-squares fits across the five measured points are:

| Metric | Fitted incremental cost | R-squared | Interpretation |
| --- | ---: | ---: | --- |
| Encoded stream | 205.974 bytes/update | 1.000000 | Linear |
| Median activation | 18.684 microseconds/update | 0.999608 | Approximately linear |
| p95 activation | 19.135 microseconds/update | 0.999491 | Approximately linear |
| Median Ursula read | 2.383 microseconds/update | 0.991970 | Approximately linear, high run variance |
| Median frame decode | 0.616 microseconds/update | 0.943248 | Roughly linear, timing noise visible |
| Median Loro import | 9.586 microseconds/update | 0.999351 | Approximately linear |
| p95 peak RSS high-water | 716.094 bytes/update | 0.999960 | Approximately linear plus process baseline |

The p95 activation fit is approximately:

```text
p95_activation_ms = 15.754 + 0.019135 * update_count
```

The stream-size fit is approximately:

```text
encoded_bytes = -36,469 + 205.974 * update_count
```

The negative fitted stream intercept is an artifact of update blobs growing
slightly with operation position. It must not be interpreted as a format
constant.

Relative to 10,000 updates, the observed curves are:

| Updates | Stream factor | p95 activation factor | p95 peak RSS factor |
| ---: | ---: | ---: | ---: |
| 10,000 | 1.00x | 1.00x | 1.00x |
| 25,000 | 2.51x | 2.25x | 1.64x |
| 50,000 | 5.05x | 3.84x | 2.81x |
| 100,000 | 10.12x | 8.02x | 5.09x |
| 250,000 | 25.33x | 20.30x | 11.79x |

Growth is therefore approximately linear over the measured range. This is not
evidence that it remains linear beyond 250,000 updates or for larger, dependent,
snapshot-heavy, or structurally different Loro updates.

## SLO Crossings

| p95 activation threshold | Last measured point at or below | First measured point above |
| --- | --- | --- |
| 250 ms | 10,000 updates, 1.937 MiB, 237.315 ms | 25,000 updates, 4.872 MiB, 532.991 ms |
| 500 ms | 10,000 updates, 1.937 MiB, 237.315 ms | 25,000 updates, 4.872 MiB, 532.991 ms |
| 1 second | 50,000 updates, 9.783 MiB, 911.786 ms | 100,000 updates, 19.606 MiB, 1,903.519 ms |

These are first measured crossings, not exact breakpoints. Three repetitions
are sufficient for the requested run count but not for a stable tail-latency
distribution.

## Restart Producer Growth

The restart experiment compared two fresh clusters with one long-lived stream
and the same 5,000 31-byte appends:

- The stable arm used one exact production-derived gateway producer ID and
  monotonically increasing sequences.
- The restart arm used 5,000 deterministic boot IDs, the exact production
  producer-ID derivation, and sequence zero once per boot session.

This direct-Ursula construction isolates producer metadata from gateway process
startup cost. Ursula does not expose producer-map cardinality, so producer count
is the number of distinct exact gateway IDs whose first append committed, not an
independent Ursula metric.

| Measurement after 5,000 appends | Replica 1 | Replica 2 | Replica 3 |
| --- | ---: | ---: | ---: |
| Stable retained producers | 1 | 1 | 1 |
| Restart retained producers | 5,000 | 5,000 | 5,000 |
| Extra logical snapshot bytes | 447,771 | 447,771 | 447,771 |
| Snapshot bytes per extra producer | 89.5721 | 89.5721 | 89.5721 |
| Current WAL difference | -14,870 | -14,772 | -14,777 |

The logical snapshot result reproduces the Phase 2 estimate and is linear in
retained producer cardinality. Across three replicas, 4,999 extra producers add
1,343,313 logical snapshot bytes. The current WAL footprint was slightly smaller
in the restart arm after snapshots; WAL compaction/layout noise dominates this
comparison, so no positive per-restart WAL cost is claimed.

Producer metadata is modest relative to document replay state and does not by
itself justify checkpoints. It also may not be removed by a checkpoint design
that continues writing to the same Ursula stream, so it must be treated as a
separate lifecycle concern in any later design.

## Checkpoint Recommendation

The explicit provisional recovery SLO is:

```text
p95 room activation after gateway restart <= 1 second
```

The 50,000-update point technically passes at 911.786 ms but leaves less than
9% margin. Reserving 20% for production variance gives an 800-ms trigger budget.
Solving the measured p95 fit for 800 ms yields approximately 40,985 updates and
8.02 MiB for this workload. The actionable initial trigger is therefore:

```text
checkpoint at the earlier of 40,000 post-checkpoint updates or 8 MiB of
post-checkpoint encoded history
```

Both dimensions are necessary. Encoded bytes capture large payloads, while
update count captures per-update validation and import overhead for many small
blobs. This trigger must be validated again under production hardware, storage
latency, concurrent activation, and representative Loro operation shapes.

The old 10,000-update / 2-MiB proposal should be **increased**, not retained or
removed without replacement. It used only 237.315 ms p95 in the isolated run and
therefore consumed less than one quarter of the stated one-second budget.

Checkpointing is justified only if the service adopts the one-second SLO and
allows room histories to grow beyond the measured operating envelope. Under
those conditions, permanent unbounded streams guarantee eventual SLO violation,
and the 100,000- and 250,000-update measurements demonstrate it directly.

Phase 2.5 stops here. It does not begin Phase 3 automatically.
