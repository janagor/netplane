# Operational baseline

Schema-v2 matched Netplane versus Tower measurements for the in-memory operational harness. Criterion microbenchmark history remains in [performance-baseline.md](performance-baseline.md). Methodology is in [operational-benchmarks.md](operational-benchmarks.md).

## Standard baseline (schema v2)

Recorded 2026-10-26 on uncommitted harness work rooted at parent commit `704804a15f7d9610f132755fc1f3d6a128892c2a`. The parent SHA alone does not identify this harness; keep the raw JSON under `target/bench-pairs/` when reproducing locally.

### Environment and method

- Host: Linux `x86_64-unknown-linux-gnu`, Intel Core i5-12450H, CPU governor `powersave`.
- Compiler: `rustc 1.88.0 (6b00bc388 2025-06-23)`; release profile, four Tokio workers.
- Dependency fingerprint: `64c46bbaca327b47` from the locally resolved `Cargo.lock`.
- Workload: `four-balanced`, closed loop, **16** clients, **15** second measurement, **3** second concurrent warmup, seed 42, five pairs with Netplane/Tower order alternated.
- Capacities: global admission 128, outer concurrency 32 and buffer 64, backend concurrency 8 and buffer 16; backend 1 concurrency configured to 1 with immediate services.
- Fresh process and Tokio runtime per run. No tracing subscriber. HDR histograms (3 significant figures, 1 ns–60 s). Every run reported `measurement_complete`, zero runtime errors, zero drain cancellations, zero missing latency samples, and `latency_samples == completed`.

| Metric | Netplane median (range) | Matched Tower median (range) | Relative Netplane result |
| --- | ---: | ---: | ---: |
| Completed in-window requests/s | 671,386 (645,819–737,809) | 1,307,601 (1,283,590–1,318,742) | 0.513× throughput |
| p50 request latency | 21,647 ns (19,871–22,399) | 10,999 ns (10,943–11,223) | +96.8% |
| p95 request latency | 52,159 ns (46,335–55,071) | 24,399 ns (24,095–24,735) | +113.8% |
| p99 request latency | 67,455 ns (60,255–70,847) | 31,727 ns (31,343–32,463) | +112.6% |

These numbers describe this host and the complete matched fixtures (routing, admission, buffers, scheduling, and instrumentation). They are a useful baseline for further investigation, not a release performance target and not an isolated cost of graph-version selection. A follow-up performance PR should compare bounded `Endpoint`, `GraphDraft`, and routed `GraphBuilder` against Tower at the same concurrency before proposing optimizations.

## Exploratory precursor (schema v1 harness)

Earlier the same day, a 2-client / 1-second exploratory run produced approximately 0.658× throughput (Netplane 535k req/s vs Tower 814k req/s). That harness still used sequential warmup idle sleep, latency-window censoring, and unbounded sample vectors. **Do not compare it directly with the schema-v2 standard baseline above.** Raw files, when retained locally, live under `target/bench-local-baseline/`.
