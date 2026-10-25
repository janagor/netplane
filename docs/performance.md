# Performance measurement

The Criterion benchmarks in `crates/netplane/benches` use in-memory Tower services and no network transport. The default service adds one to a `u64` and returns an immediately ready future. Each benchmark builds a four-worker Tokio runtime outside timed iterations, warms request paths before measurement, and checks response values. No tracing subscriber is installed. Criterion reports wall-clock time per iteration; concurrent iterations contain 128 completed requests, so their `Elements` throughput is requests per second.

## Reproduce

```bash
cargo bench -p netplane --no-run
cargo bench -p netplane -- --save-baseline main
cargo bench -p netplane -- --baseline main
```

The controlled follow-up in [the baseline report](performance-baseline.md) uses `cargo bench -p netplane -- --warm-up-time 3 --measurement-time 10 --sample-size 100 --noplot`, followed by the `latency_probe` task mode at 8, 16, and 32 callers and both `resource_probe` modes at 1000 publications. The exact resolved dependency versions for that run are in [performance-dependencies.txt](performance-dependencies.txt). This workspace ignores `Cargo.lock`, so record a new dependency list for each later comparison.

Use one target or a name filter to narrow the run, for example `cargo bench -p netplane --bench execution -- execution/serial`. Keep CPU power mode, thermal conditions, compiler, target and background load stable. Criterion baseline files are local under `target/criterion`; commit the written report instead of generated sample data. The benchmark profile uses optimization level 3 and debug information. It inherits release settings, including fat LTO and one codegen unit.

For per-request latency percentiles, run `cargo run -p netplane --example latency_probe --release -- 8 4096 batched`, replacing `8` with 1, 2, 4, 16 or 32 callers as needed, and replace `batched` with `tasks` for independently scheduled caller tasks. Samples must be divisible by the caller count. The probe uses the same four-worker runtime, warm-up and immediate service, then records the elapsed time from each `Handle::execute` invocation through completion. It reports nearest-rank p50, p95, p99 and aggregate completed requests per second. Both modes clone one handle per caller before starting the throughput timer. Per-request latency samples exclude that clone and start immediately before `Handle::execute`; pushing sample values into result vectors still occurs during the throughput interval. This measurement mode is separate from Criterion's throughput groups. The `release` profile strips symbols and otherwise uses the same optimization and LTO settings as the benchmark profile.

`batched` drives concurrent in-flight requests with `join_all` inside one parent Tokio task. `tasks` spawns a fixed set of long-lived caller tasks outside the timed interval, releases them together, and has each task issue sequential requests. Use `batched` when comparing against Criterion's `concurrency/batched_inflight` group; use `tasks` when measuring multi-threaded entry into `Handle::execute`.

For resource retention, run `cargo run -p netplane --example resource_probe --release -- 1000 paced` or `... -- 1000 burst`. Both modes count live Tower service instances across repeated publications while displaced versions are temporarily held by unpolled requests. `paced` waits for asynchronous worker cleanup after each release and is the deterministic correctness check. `burst` publishes without waiting, periodically samples the live count, releases retained requests, then checks eventual cleanup. The printed peak and final counts are live service counts, not RSS or allocator measurements.

## Interpretation

| Benchmark | Timed work |
| --- | --- |
| `tower_direct` | A fresh `service_fn` and Tower `oneshot`; no queue or worker. |
| `tower_bounded` | Tower buffer and concurrency limit, with the same service. |
| `endpoint` | Netplane endpoint wrapper around a Tower buffer and limit. |
| `graph_draft` | Admission, version selection, observation bookkeeping, outer endpoint and service. |
| `graph_builder_one_route` | All graph costs plus one router lookup and bounded backend endpoint (two buffer handoffs). |
| `tower_double_buffered_one_route` | Manually composed Tower router with the same two buffer handoffs and no Netplane admission. |
| `graph_draft_yield_once` | `GraphDraft` path through a service that yields once before completing. |
| `admission/success_unpolled` | Acquire and release a global slot without polling the request. |
| `admission/rejected_at_capacity` | Synchronous rejection while one unpolled future holds the only slot. |
| `admission/accepted_waiting_capacity_two_completions` | Two concurrently accepted requests through a capacity-one endpoint with a 1 ms service delay; reports total time for both completions. |
| `concurrency/batched_inflight` | 128 completions in batches of 1–32 in-flight futures inside one parent task. |
| `concurrency/multi_task_callers` | 128 completions from independently scheduled caller tasks released after arming. |
| `routing/requests` | Netplane declared and fallback routes at 1–64 backends, allocated labels, and an equivalent Tower double-buffered router. |
| `routing/label_lookup` | Selector label resolution against a `BTreeMap` without endpoint execution. |
| `publication` | Validated candidate publication, controller history, retained old requests and one active caller; candidate construction is outside the timed portion. |
| `graph/compile_*` | Graph preparation that validates capacities, builds endpoints and starts Tower buffer workers. |
| `lifecycle` | Unpolled cancellation, cancellation during pending readiness or a full queue, expired deadline, construct-and-shutdown, preconstructed shutdown and drain after request drop. |

The direct Tower versus Netplane difference includes queueing and scheduling. Compare bounded Tower with `Endpoint`, then `Endpoint` with `GraphDraft`, then `GraphDraft` with `GraphBuilder`, and compare `GraphBuilder` with `tower_double_buffered_one_route` to separate the second worker handoff from Netplane-specific routing. The bounded Tower composition is similar but does not own or observe worker lifetime in the same way. Publication with a retained old request includes releasing that request after the swap; it is not a pure atomic-swap timing.

Criterion estimates are distributions of iteration time, not p50, p95 or p99 request latency. The latency and service-count probes provide separate views of request tails and Netplane-owned resources, but do not measure allocator counts or RSS. A subscriber would add tracing work; run a dedicated subscriber comparison before attributing that cost. Shared CI runners compile benchmarks and smoke-test them with `cargo test -p netplane --benches`, but do not enforce a performance threshold. For regression comparisons on a controlled host, prefer longer Criterion warm-up and measurement intervals than the exploratory 100 ms / 200 ms settings used in the first baseline.
