# First local performance baseline

This is an exploratory baseline from the working tree for PR 15, using the source parent commit `7fe6f82d4561f215c6baff09915dd9bffa1acf05` plus the benchmark changes. It is not a release benchmark or a tail-latency measurement. Record the exact PR commit SHA and `cargo tree -p netplane` once the branch is committed; this library workspace ignores `Cargo.lock`, so later runs can resolve newer compatible dependency versions.

| Environment | Value |
| --- | --- |
| CPU | 12th Gen Intel Core i5-12450H |
| Logical cores | 12 |
| RAM | 16,096,120 kB reported by `/proc/meminfo` |
| OS | NixOS Linux 6.18.52, x86_64 |
| Compiler | rustc 1.88.0, target `x86_64-unknown-linux-gnu` |
| Profile | Cargo `bench`: optimization 3, fat LTO, one codegen unit, debug info |
| Tokio | 1.53.3, multithread runtime, four worker threads |
| Tower | 0.5.3 |
| Tracing subscriber | None |
| Sampling | Criterion 0.5.1, 10 samples, 100 ms warm-up, 200 ms measurement |

Command: `cargo bench -p netplane --bench execution -- execution/serial --warm-up-time 0.1 --measurement-time 0.2 --sample-size 10 --noplot`.

| Serial path | Criterion point estimate | Approximate requests/s |
| --- | ---: | ---: |
| Direct Tower `service_fn` | 1.625 ns | 615,400,000 |
| Bounded Tower | 5.247 µs | 190,600 |
| Netplane `Endpoint` | 5.267 µs | 189,900 |
| `GraphDraft` + `Netplane` | 5.582 µs | 179,100 |
| `GraphBuilder` one route + `Netplane` | 10.530 µs | 95,000 |

The measured serial `GraphDraft` path is about 6% slower than bounded Tower in this run, and its estimated throughput is about 6% lower. `Endpoint` is within 1% of bounded Tower, `GraphDraft` is about 6% slower than `Endpoint`, and the one-route `GraphBuilder` path is about 89% slower than `GraphDraft`. These are comparisons of complete paths and include different worker ownership, queueing and routing work. Direct Tower is dramatically faster because it has no worker handoff; subtracting its time from Netplane would not isolate a Netplane mechanism. The direct result is close to a tiny CPU operation and should be treated as a floor, not a realistic service workload.

The one-route `GraphBuilder` path uses two Tower buffer-worker handoffs (outer router endpoint plus selected backend). Do not treat the ~5 µs gap versus `GraphDraft` as evidence that `BTreeMap` lookup is the bottleneck. An exploratory follow-up on the same host with the same short Criterion settings measured `tower_double_buffered_one_route` at about 9.8 µs versus about 11.6 µs for `graph_builder_one_route` and about 5.7 µs for `graph_draft`, which is consistent with most of the gap coming from the second worker handoff rather than route-table lookup. Re-measure with longer sampling, `routing/label_lookup` and `graph_draft_yield_once` before drawing stronger conclusions. Criterion's serial execution run did not record request p95 or p99 or resource retention; separate probes below cover those questions.

## Admission and publication spot checks

The same host and Criterion settings were used with `--bench execution -- admission` and `--bench lifecycle -- publication` filters. These timings are per operation, and the publication candidates were built outside the timed portion.

| Operation | Criterion point estimate |
| --- | ---: |
| Acquire and drop an unpolled admitted future | 443 ns |
| Reject synchronously at one held admission slot | 12.0 ns |
| Complete two accepted requests through a capacity-one endpoint with 1 ms service delay | 4.740 ms per pair |
| Direct publication with displaced graph drop | 2.503 µs |
| Controller publication, history 0 | 2.459 µs |
| Controller publication, history 2 | 2.464 µs |
| Controller publication, history 8 | 2.529 µs |
| Controller publication, history 32 | 2.518 µs |
| Direct publication with old request retained through the swap, then dropped | 0.839 µs |
| Direct publication with one concurrent caller | 3.023 µs |

Publication results include the cost of replacing and releasing publication wrappers, and may include worker cleanup. They do not isolate a bare atomic swap. The retained-request case drops that request inside the timed operation; its lower result is consistent with destruction being deferred or occurring asynchronously, so it is not a pure swap estimate either. The concurrent case runs one caller on a separate thread while publication is timed. The controller measurements include token lookup. Criterion's batching and short run length make small differences between history sizes inconclusive.

The accepted-capacity case includes two serialized service sleeps and Tokio timer scheduling. It demonstrates completion under bounded backpressure but does not isolate queue-wait overhead. Criterion extended its requested 200 ms collection target to about 258 ms to obtain 10 samples.

## Lifecycle spot checks

The `--bench lifecycle` filter with the same sampling settings produced these per-iteration point estimates. Names below match the current benchmark identifiers after the methodology rename; the measured work is unchanged unless noted.

| Operation | Criterion point estimate |
| --- | ---: |
| `graph/compile_single_root` (validate + endpoint/worker start) | 2.984 µs |
| `graph/compile_router_4_backends` | 21.408 µs |
| Drop an admitted, unpolled request | 432 ns |
| Cancel while its endpoint worker withholds readiness | 259 ns |
| Cancel while waiting for a full buffer | 248 ns |
| Observe an already expired deadline | 506 ns |
| `lifecycle/construct_and_shutdown_empty` | 3.561 µs |
| Close and drain after dropping one accepted request | 1.552 µs |

The cancellation cases time the caller future drop only; setup and worker polling occur outside the timed portion. `construct_and_shutdown_empty` includes construction and validation, so its value does not isolate draining; use `shutdown_preconstructed_empty` when comparing drain-only cost. Resource reclamation is asynchronous in some worker paths and requires separate live-resource checks.

## Request latency under concurrency

The `latency_probe` example was run in the release profile with 131,072 completed requests per caller count, four Tokio workers and no tracing subscriber. Each request starts its timer immediately before `Handle::execute` and stops it on completion. Percentiles use nearest rank. Runs were sequential on the same host. The table below is the original **batched** workload (`join_all` in one parent task). Re-run with trailing argument `tasks` before treating the figures as multi-threaded caller scalability.

| Callers | Throughput, requests/s | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 177,513 | 4.779 µs | 7.196 µs | 8.576 µs |
| 2 | 303,383 | 5.190 µs | 7.050 µs | 9.411 µs |
| 4 | 544,212 | 4.885 µs | 6.837 µs | 10.000 µs |
| 8 | 838,730 | 4.869 µs | 6.716 µs | 11.407 µs |
| 16 | 863,276 | 8.717 µs | 11.383 µs | 56.505 µs |
| 32 | 727,566 | 19.889 µs | 28.295 µs | 72.838 µs |

These are single runs, not confidence intervals. The 16 and 32 caller tail increase suggests contention or scheduling effects on this host; this measurement alone does not identify which mechanism dominates. Memory and live worker counts were not measured here.

Criterion's `concurrency/batched_inflight` group times 128 completions per iteration with the same caller matrix. Its point-estimate throughput was 142k, 259k, 506k, 748k, 910k and 793k requests/s at 1, 2, 4, 8, 16 and 32 callers respectively. The values differ from the latency probe because each mode uses different iteration boundaries and measurement overhead. Both runs peak near 16 callers on this host. The `concurrency/multi_task_callers` group and `latency_probe ... tasks` mode are the counterparts for independently scheduled caller tasks; collect those on a controlled host before comparing contention behavior.

## Service lifetime

`cargo run -p netplane --example resource_probe --release -- 1000 paced` reported `peak_live_services=2` and `final_live_services=0`. The paced probe held one old-version request through each publication, released it, and waited for worker cleanup before the next iteration. This demonstrates no accumulating service instances in that controlled ownership pattern; it does not measure RSS, allocator fragmentation or work independently retained by user services. Use `burst` mode to characterize peak retention during rapid publication without per-step waits; record peak and final live-service counts from that mode separately.

## Routing spot checks

The borrowed declared-label path was measured with `cargo bench -p netplane --bench routing -- routing/requests/declared_borrowed --warm-up-time 0.1 --measurement-time 0.2 --sample-size 10 --noplot`. Its selector returns static route labels without allocating.

| Backends | Per-request point estimate |
| ---: | ---: |
| 1 | 12.116 µs |
| 2 | 13.152 µs |
| 4 | 12.825 µs |
| 8 | 11.215 µs |
| 16 | 11.134 µs |
| 32 | 14.187 µs |
| 64 | 12.887 µs |

The values do not show a monotonic increase. Worker handoffs and run-to-run noise dominate any `BTreeMap` lookup trend at this sample duration. Compare against `routing/requests/tower_double_buffered` and `routing/label_lookup` before optimizing the route table. Fallback and allocated-label cases are implemented but were not included in this first spot check.
