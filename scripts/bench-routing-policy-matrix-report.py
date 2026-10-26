#!/usr/bin/env python3
"""Summarize paired buffered and backend-only diagnostic probes."""

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

directory = Path(sys.argv[1])
cells = defaultdict(lambda: defaultdict(list))
for path in directory.glob("*.json"):
    report = json.loads(path.read_text())
    config = report["config"]
    key = (
        config.get("tokio_workers", 4),
        config["workload"],
        config.get("skew_percent", 0),
        "open" if config.get("offered_rps") else "closed",
    )
    policy = "base_1e06bd1" if path.name.startswith("base_") else config["implementation"]
    cells[key][policy].append(report["metrics"])

lines = [
    "# Routing policy diagnostic comparison",
    "",
    "At fixed offered rate, compare open-loop rows. Closed-loop rows use 32 callers.",
    "The schedstat counters can describe only the thread-group leader; interpret CPU and scheduling columns cautiously.",
    "",
    "| Workers | Workload | Skew % | Load | Policy | Throughput rps | Mean µs | p95 µs | p99 µs | Leader CPU share | Leader wait ms |",
    "| ---: | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
]
for key, policies in sorted(cells.items()):
    for policy in ("base_1e06bd1", "graph_builder", "backend_only"):
        runs = policies.get(policy, [])
        if not runs:
            continue

        def median(field):
            values = [field(run) for run in runs]
            values = [value for value in values if value is not None]
            return statistics.median(values) if values else None

        def fmt(value, divisor=1):
            return f"{value / divisor:.1f}" if value is not None else "n/a"

        workers, workload, skew, load = key
        lines.append(
            f"| {workers} | {workload} | {skew} | {load} | {policy} | "
            f"{fmt(median(lambda r: r['throughput_rps']))} | "
            f"{fmt(median(lambda r: r['latency']['mean']), 1000)} | "
            f"{fmt(median(lambda r: r['latency']['p95']), 1000)} | "
            f"{fmt(median(lambda r: r['latency']['p99']), 1000)} | "
            f"{fmt(median(lambda r: r['cpu_utilization']))} | "
            f"{fmt(median(lambda r: r.get('scheduler_wait_ns')), 1_000_000)} |"
        )

(directory / "summary.md").write_text("\n".join(lines) + "\n")
print(directory / "summary.md")
