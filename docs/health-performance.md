# Health routing benchmark baseline

Local x86_64 run on Rust 1.88.0, optimized benchmark profile. Criterion used
10 samples, 50 ms warm-up and 150 ms measurement per case. Other verification
work ran on the same host; these are exploratory measurements, not stable CI
performance thresholds. Times below are mean nanoseconds per operation.

Reproduce with:

```sh
cargo bench -p netplane --bench health -- --warm-up-time 0.05 --measurement-time 0.15 --sample-size 10
```

## Weighted selection

| Labels | Existing selector | All healthy | Half healthy | Concurrent observations | Health update |
| --- | --- | --- | --- | --- | --- |
| 2 | 5.7 | 13.6 | 61.6 | — | 92.2 |
| 8 | 10.8 | 28.9 | 197.8 | 349.1 | 93.4 |
| 32 | 14.6 | 90.9 | 599.9 | 934.0 | 93.6 |
| 128 | 15.9 | 297.1 | 1968.8 | 2378.0 | 86.7 |

The 128-label weighted fixture has 100 positive integer percentages and 28 zero
weights. The baseline cannot represent 128 positive percentages. The uniform
fixture below has all 128 targets selectable.

## Uniform selection

| Backends | Existing selector | All healthy | Half healthy |
| --- | --- | --- | --- |
| 2 | 6.0 | 9.2 | 50.6 |
| 8 | 6.3 | 22.6 | 136.3 |
| 32 | 5.4 | 42.1 | 556.3 |
| 128 | 6.6 | 162.4 | 2077.6 |

## Passive observations and interpretation

- `disabled`: 0.31 ns per completed operation.
- `single_worker`: 121.68 ns per completed operation.
- `four_workers_disabled`: 0.18 ns per completed operation.
- `four_workers`: 417.98 ns per completed operation.

The passive fixture executes a no-op, immediately ready service. Its raw baseline
is aggressively inlined; use the absolute wrapper cost rather than a ratio against
that baseline. Four-worker measurements divide elapsed wall time by total
operations and include worker startup, with all workers sharing one health record.
They measure contention in the observation mutex, not application throughput.

For example, 13 ns per isolated selection corresponds to roughly 77 million
selections per second on one worker; this is not an HTTP gateway throughput claim.
The five-percent small-set throughput aspiration is **not demonstrated** by these
microbenchmarks. Selection itself costs materially more than the existing selector.
An end-to-end workload must establish the effect relative to real backend work.

Healthy eligibility uses an atomic load. An unhealthy read takes the per-record
mutex to check cooldown, explaining the larger partially-unhealthy cost. Concurrent
observations exercise those same records; no registry-wide write lock is taken by
selection. This is a useful baseline before considering cached weighted tables.

Weighted selection and uniform selection through 128 targets use stack masks;
by inspection neither allocates in a steady routing decision. Uniform sets above
128 allocate one membership-sized Boolean mask per decision. Observation tokens
clone existing Arcs; state transitions allocate a new generation identity. These
allocation properties were inspected in source, not counted with an allocator
profiler. Criterion allocations and synchronization contention are not separately
instrumented; concurrent-observation and multiworker latency are contention proxies.

Health-disabled selectors retain their existing implementation. Checks use a
separate bounded scheduler and never reconstruct a graph during health updates.

## End-to-end Netplane execution

`health_execution` measures complete `Handle::execute` round trips through the
production graph builder, runtime admission, bounded workers, route selection,
and response delivery. All cases use the same deterministic uniform policy,
including 128 positive candidates, and an immediately ready integer service.
A four-worker Tokio runtime is constructed outside measurement. Each backend has
64 in-flight slots and 128 queue slots; global admission is 512. Buffered graphs
also have a 64/128 outer endpoint. Graph construction and warmup are untimed.

```sh
cargo bench -p netplane --bench health_execution -- --warm-up-time 0.05 --measurement-time 0.15 --sample-size 10
```

These exploratory measurements used the same x86_64 host and Rust 1.88.0 with
10 samples, 50 ms warmup and 150 ms measurement. Compilation and repository
verification also ran on the host. The sample is too short and noisy to establish
a five-percent regression threshold or attribute a difference to one lock.
Mean round-trip times are microseconds per completed request:

| Execution | Backends | Ordinary | Health only | With passive layer | Half unhealthy |
| --- | --- | --- | --- | --- | --- |
| backend_only | 2 | 8.93 | 11.29 | 12.38 | 10.03 |
| backend_only | 8 | 11.43 | 10.69 | 11.35 | 10.58 |
| backend_only | 32 | 11.78 | 12.41 | 18.12 | 17.25 |
| backend_only | 128 | 21.29 | 14.77 | 11.88 | 17.28 |
| buffered | 2 | 21.86 | 21.78 | 35.44 | 22.13 |
| buffered | 8 | 19.52 | 18.36 | 21.66 | 21.91 |
| buffered | 32 | 23.17 | 20.08 | 25.95 | 27.94 |
| buffered | 128 | 24.73 | 24.55 | 24.62 | 30.31 |

Health-only cases include selection but no passive observations. Half-unhealthy
cases omit the passive layer and keep every other backend excluded for a long
cooldown. All return the original service response. Sequential request keys
change each iteration. Healthy variants sometimes measure faster than the
ordinary baseline, demonstrating why these numbers should not be read as a
precise overhead percentage. The paired measurements establish the harness and
show absolute execution costs; longer isolated repeated runs are needed before
choosing a hot-path optimization.

### Concurrent execution and health changes

The concurrent group compares ordinary routing with passive health-aware routing
while a separate bounded worker repeatedly fails and recovers all but one
backend. Every timed iteration drives a batch of 16 accepted requests with
`join_all`; the admission budget and backend capacities are unchanged. One
backend remains healthy, so no routing failure or retry contributes to timings.
The health writer starts outside the timed iterations and stops after each case.
This deliberately exercises generation replacement, eligibility reads and
observation lock contention while Netplane requests execute.

Mean batch duration divided by 16 gives the amortized microseconds per request
below; the throughput columns are total completed requests per second in thousands.
These are throughput costs, not individual request latency percentiles.

| Execution | Backends | Ordinary µs/request | Changing µs/request | Ordinary Kreq/s | Changing Kreq/s |
| --- | --- | --- | --- | --- | --- |
| backend_only | 2 | 4.65 | 9.57 | 214.9 | 104.5 |
| backend_only | 8 | 6.68 | 12.01 | 149.7 | 83.3 |
| backend_only | 32 | 9.30 | 5.80 | 107.5 | 172.4 |
| backend_only | 128 | 6.99 | 9.73 | 143.2 | 102.7 |
| buffered | 2 | 3.78 | 4.72 | 264.2 | 211.9 |
| buffered | 8 | 3.04 | 13.32 | 328.4 | 75.1 |
| buffered | 32 | 3.57 | 4.35 | 280.0 | 230.1 |
| buffered | 128 | 3.32 | 4.53 | 300.8 | 220.5 |

The changing case includes passive observations plus deliberately aggressive
background health transitions, so its difference from ordinary routing is a
combined-system measurement rather than the cost of selection alone. No new
cache or clock optimization is justified solely by this short run. Repeat longer
runs on an otherwise idle host with application-representative service work
before establishing the end-to-end throughput target.

These measurements precede the final completion/deregistration synchronization
correction. They remain exploratory historical baselines; longer isolated reruns
are deferred to performance-focused work.

A future isolated run can use longer sampling, repeated independently:

```sh
cargo bench -p netplane --bench health_execution -- --warm-up-time 3 --measurement-time 10 --sample-size 100
```
