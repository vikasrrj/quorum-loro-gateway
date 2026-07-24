# Benchmarks

The current gateway rebuilds a room by reading and importing its full Ursula stream. These measurements show how that behaves as the number of small updates grows.

## Replay results

| Updates | Encoded history | Median activation | p95 activation | Peak RSS high-water |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 1.94 MiB | 214 ms | 237 ms | 15.2 MiB |
| 25,000 | 4.87 MiB | 402 ms | 533 ms | 24.8 MiB |
| 50,000 | 9.78 MiB | 896 ms | 912 ms | 42.5 MiB |
| 100,000 | 19.61 MiB | 1.82 s | 1.90 s | 77.2 MiB |
| 250,000 | 49.08 MiB | 4.65 s | 4.82 s | 178.8 MiB |

Every run reconstructed the expected document and matched the expected state hash.

Full replay stayed close to linear for this workload. It was still below one second at 50,000 updates, but reached about 1.9 seconds at 100,000 and 4.8 seconds at 250,000.

That makes checkpoints useful for larger rooms, but it does not mean every deployment needs the same cutoff.

## Test setup

- 11th Gen Intel Core i5-1155G7
- 8 logical CPUs
- about 16.5 GB memory
- Rust 1.96.0 release build
- three Ursula voters and four Raft groups
- disk WAL, local snapshots, memory cold store
- 64 KiB Ursula read windows
- fresh gateway and cluster processes for each repetition
- three repetitions per update count

The workload used one Loro text container. Each update appended one ASCII `x` and was stored in its own `QLGD` frame. History generation and loading were outside the measured activation interval.

The Linux page cache was not dropped, so these are normal warm-host results, not cold-disk disaster-recovery numbers. With only three repetitions, the reported p95 is the maximum run and should be read as a rough bound rather than a stable production percentile.

Raw observations are committed in:

- [`results/replay/scale.json`](../results/replay/scale.json)
- [`results/replay/initial.json`](../results/replay/initial.json)

## Recovery breakdown

At 250,000 updates, the median activation time was about 4.65 seconds. The main measured component was Loro import at about 2.37 seconds. Ursula reads were about 571 ms and frame decoding about 163 ms.

The remaining time includes stream checks, Loro metadata validation, flattening frame updates into history, allocations, and actor setup. Component timers are not expected to add up exactly to the end-to-end number.

## Checkpoint starting point

A provisional target for the next design is:

```text
checkpoint after 40,000 updates or 8 MiB of post-checkpoint history,
whichever comes first
```

That leaves some room under a one-second p95 activation goal for the measured workload. It is not a format guarantee or a universal production default. Payload shape, storage latency, concurrent room activation, and hardware can move the threshold significantly.

## Producer metadata

Ursula retains producer state per stream. A gateway restart creates a new producer ID for every room it activates, so repeated restarts grow this metadata even though the gateway itself is stateless.

The direct Ursula measurement compared 5,000 appends from one stable producer with 5,000 appends from distinct gateway-shaped producers.

| Measurement | Result per extra producer, per replica |
| --- | ---: |
| Logical snapshot bytes | about 89.6 bytes |
| Current WAL bytes | about 78 bytes in the controlled comparison |

A separate restart-shaped run reproduced the snapshot result. Its current WAL was slightly smaller after snapshotting, so no positive WAL cost was claimed from that run; compaction and layout noise dominated the comparison.

Producer metadata is much smaller than document replay state here, but it is a separate lifecycle issue. A checkpoint that continues writing to the same Ursula stream may not remove old producer entries.

Raw producer measurements are in:

- [`results/producer-state/direct.json`](../results/producer-state/direct.json)
- [`results/producer-state/restarts.json`](../results/producer-state/restarts.json)

## Reproducing the measurements

Full replay:

```bash
QLG_REPLAY_COUNTS=10000,25000,50000,100000,250000 \
  cargo test --release --locked --test replay_benchmark \
  full_replay_benchmark -- --ignored --nocapture --test-threads=1
```

Producer-state comparison:

```bash
python scripts/measure-producer-state.py
python scripts/measure-restart-producers.py
```
