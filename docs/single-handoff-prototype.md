# Single-handoff router prototype (PR 1)

This experiment measures whether removing the outer routed `Endpoint` (the second
Tower buffer worker) closes most of the GraphBuilder latency gap versus bounded
Tower. **Production Netplane is unchanged.**

## Why

`GraphBuilder` currently wraps each backend in an `Endpoint`, then wraps the
router `service_fn` in another `Endpoint`. One routed request therefore crosses
two asynchronous buffer handoffs. An equivalent double-buffered Tower fixture is
already ~2× slower than single-buffered Tower on the recorded host; Netplane's
remaining gap over that fixture is much smaller. See [performance-baseline.md](performance-baseline.md).

## What this PR adds

| Artifact | Role |
| --- | --- |
| `benches/common::{tower_single_buffered_router, single_handoff_endpoints}` | Criterion fixtures for one-handoff topologies |
| `execution/serial/{tower_single_buffered_one_route, single_handoff_one_route}` | Serial microbenchmarks beside the existing paths |
| `examples/single_handoff_probe` | Multi-caller probe with latency percentiles, throughput, and CPU share |
| `scripts/bench-single-handoff.sh` | Paired matrix runner (≥5 repetitions) |
| `scripts/bench-single-handoff-report.py` | Raw JSON → `comparison.json` / `comparison.md` |

## Compared configurations

1. **Tower bounded** — one buffer + concurrency limit, no routing.
2. **Tower double-buffered** — outer + backend buffers (GraphBuilder topology without Netplane).
3. **GraphBuilder** — production Netplane routed path (admission + two handoffs).
4. **Single-handoff** — experimental: backend `Endpoint`s only; selector calls `Endpoint::execute` directly.
5. **GraphDraft** — production Netplane single-root path (admission + one handoff).

Workloads: `immediate` (ready `u64` increment) and `delayed` (100 µs sleep by default).
Caller counts: 1, 4, 8, 16, 32 independently scheduled tasks.

## Experimental capacity model

The single-handoff fixture **preserves**:

- per-backend `max_in_flight` and `buffer_capacity` via real `Endpoint`s
- a non-blocking global `max_outstanding` semaphore (same shape as the Tower fixtures in this probe)

It **does not** preserve GraphBuilder outer:

- `max_in_flight` across the routed version
- `buffer_capacity` queue depth, waiting, or saturation behavior

A semaphore is not a Tower `Buffer`. Waiting, fairness, and occupancy differ. Do
not treat a successful latency result as proof that production can drop the outer
endpoint without a replacement admission design.

## Reproduce

```bash
# Serial microbenchmarks (Criterion)
cargo bench -p netplane --bench execution -- \
  'execution/serial/(tower_bounded|tower_double_buffered_one_route|tower_single_buffered_one_route|single_handoff_one_route|graph_builder_one_route|graph_draft)' \
  --warm-up-time 3 --measurement-time 10 --sample-size 100 --noplot

# Paired probe matrix (output directory must be empty / new)
./scripts/bench-single-handoff.sh --runs 5 --output target/bench-single-handoff-$(date +%Y%m%d-%H%M%S)

# Faster local smoke (still ≥5 runs, fewer samples)
./scripts/bench-single-handoff.sh --quick --runs 5 --output target/bench-single-handoff-quick-$(date +%Y%m%d-%H%M%S)
```

The runner refuses a non-empty `--output` directory so old and new matrices cannot
mix. Each JSON run records an explicit `config.repetition`; the report pairs by
that ID and rejects missing, duplicated, or mismatched configs.

## Decision rule for PR 2

Proceed to `ExecutionGraph::Routed` when Criterion and the paired matrix show that
removing the outer handoff substantially closes the GraphBuilder gap toward the
single-buffer band, with `all_pairs_same_direction` mean-latency deltas on the
immediate workload. A flamegraph is not required before that decision when the
topology signal is already clear.

Do not fold admission, tracing, or boxing changes into PR 2.

## Measured findings (local quick matrix + Criterion spot check)

Raw JSON from the original quick matrix is under `target/bench-single-handoff/`
(gitignored). A checked-in summary lives in
[single-handoff-results.md](single-handoff-results.md).

**Criterion serial** (short sampling on this host; qualitative only):

| Path | Mean iteration time |
| --- | ---: |
| `tower_bounded` | 7.375 µs |
| `tower_single_buffered_one_route` | 6.931 µs |
| `single_handoff_one_route` | 7.032 µs |
| `graph_draft` | 7.967 µs |
| `tower_double_buffered_one_route` | 13.778 µs |
| `graph_builder_one_route` | 15.548 µs |

Removing the outer handoff collapses Tower and Endpoint-based routers onto the
single-buffer band. GraphBuilder remains ~2× that band, matching the double-buffer
cost plus a smaller Netplane-specific remainder.

**Paired probe, immediate service, 5 runs:** single-handoff mean latency was
~2.2–2.8× faster than GraphBuilder at 1–32 callers, with
`all_pairs_same_direction` true for mean latency in every cell. Remaining overhead
versus bounded Tower was about 1.3–1.9× depending on concurrency.

**Paired probe, delayed service (~100 µs requested sleep):** absolute gaps shrink to
a few percent of total latency (as expected when service time dominates). Single-handoff
still matched or slightly beat GraphBuilder; several higher-concurrency cells had
all paired deltas in the same direction.

**Do not treat the 2.2–2.8× probe speedup as the expected PR 2 production win.**
The probe `single_handoff` fixture skips `Handle` admission/version selection/observation.
Production `GraphDraft` already shows those costs can be large under concurrency. At
32 callers in the recorded matrix: single-handoff prototype 20.1 µs, GraphDraft 40.0 µs,
GraphBuilder 50.3 µs. PR 2 keeps the `Handle` path, so improvement should be closer to
removing the second buffer on top of GraphDraft-like fixed costs. Criterion's
topology-only comparison remains the cleaner justification for removing the outer worker.

`all_pairs_same_direction` means every paired delta shared a sign. With five pairs that
is exact two-sided sign-test p=0.0625, not significance at α=0.05.

**CPU utilization** from `/proc/self/schedstat` was near zero in this environment and
should not be trusted until re-checked on a host with normal process CPU accounting.
**Allocations per request** remain unmeasured (`unsafe_code = forbid`).

**Verdict:** removing the second buffer is still worthwhile. PR 2
(`ExecutionGraph::Routed`) should proceed with correctness and capacity semantics first,
expecting a smaller production speedup than the topology-only prototype.

## Backpressure semantics PR 2 must preserve

1. Outer `max_in_flight` for the routed version.
2. Outer `buffer_capacity` with explicit wait/reject behavior under saturation.
3. Global `max_outstanding`, including unpolled admitted futures.
4. Backend-local concurrency and buffer isolation.
5. Unpolled retention, queued cancellation, backend readiness ownership, deadlines,
   publication/rollback, and shutdown drain.
6. Measurable fairness when one backend saturates while another is idle.

## Out of scope for PR 1

- Production `GraphBuilder` / `Handle` changes
- Admission accounting, tracing fast-paths, or boxing reductions
- Allocation counting (workspace `unsafe_code = forbid` blocks an in-process
  counting allocator; use an external profiler if needed)
