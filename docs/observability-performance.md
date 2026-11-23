# Runtime counter performance check

Compared the observability changes against base commit `962e1f6` on Linux
x86_64, Intel Core i5-12450H, Rust 1.88.0, using the existing Criterion benchmarks.
Tracing subscribers were not installed. These are local measurements, not a
portable performance guarantee.

## Admission hot path

The `execution` benchmark used 30 samples, one second of warm-up, and two seconds
of measurement per case. Values below are Criterion point estimates.

| Case | Base | With counters | Time change |
| --- | --- | --- | --- |
| Successful unpolled admission and drop | 439.54 ns | 442.64 ns | +0.79% |
| Capacity rejection | 12.377 ns | 11.584 ns | -9.50% |

Criterion classified successful admission as within its noise threshold.
Neither case showed a material regression. The apparent rejection improvement
should not be attributed to the counter arithmetic.

## Concurrent request throughput

The `concurrency/multi_task_callers` benchmark uses a four-worker Tokio runtime
and independently scheduled callers, completing 128 requests per timed iteration.
Caller task setup occurs outside the timed interval. The comparison used 30
samples, one second of warm-up, and three seconds of measurement per case.
The base was extracted into a separate directory with the same lockfile.
Both base and modified core sources were forced to rebuild before their runs.

| Callers | Base throughput | With counters | Throughput change |
| --- | --- | --- | --- |
| 1 | 600.03 K requests/s | 682.59 K requests/s | +13.77% |
| 16 | 708.72 K requests/s | 800.33 K requests/s | +14.02% |

Neither case showed a regression in this run. The apparent throughput gains are
larger than expected from adding counters and should be treated as host or
scheduler variation, not evidence that counters improve performance. This short
local comparison does not rule out small regressions on other workloads.

## Reproduction

Run each command in a clean base checkout with `--save-baseline observability-base`,
then in the modified checkout with `--baseline observability-base`, using a shared
Criterion results directory and the same lockfile. Ensure Cargo rebuilds the
intended sources when sharing a target directory; source timestamps can otherwise
cause misleading reuse between extracted checkouts.

```sh
cargo bench --locked -p netplane --bench execution -- \
  'admission/(success_unpolled|rejected_at_capacity)' \
  --sample-size 30 --measurement-time 2 --warm-up-time 1 \
  --save-baseline observability-base

cargo bench --locked -p netplane --bench concurrency -- \
  'concurrency/multi_task_callers/(1|16)$' \
  --sample-size 30 --measurement-time 3 --warm-up-time 1 \
  --save-baseline observability-base
```

Keep other benchmark processes idle. These checks reuse the existing benchmarks
and introduce no benchmark dependency or runtime instrumentation.
