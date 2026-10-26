#!/usr/bin/env python3
"""Summarize matched Netplane and Tower operational JSON runs."""
import json
import sys
from pathlib import Path


def main():
    directory = Path(sys.argv[1])
    rows = []
    for scenario in ("single", "two-balanced", "two-skewed", "four-balanced", "eight-balanced"):
        netplane = json.loads((directory / f"{scenario}-netplane.json").read_text())
        tower = json.loads((directory / f"{scenario}-tower.json").read_text())
        if netplane["config"] | {"implementation": None} != tower["config"] | {"implementation": None}:
            raise ValueError(f"mismatched configuration: {scenario}")
        a, b = netplane["metrics"], tower["metrics"]
        if a["runtime_errors"] or b["runtime_errors"]:
            raise ValueError(f"runtime errors in {scenario}")
        latency = {}
        for percentile in ("p50", "p95", "p99"):
            left, right = a["latency_ns"][percentile], b["latency_ns"][percentile]
            latency[percentile] = 100 * (left / right - 1) if right else None
        rows.append({"scenario": scenario,
                     "throughput_ratio": a["throughput_rps"] / b["throughput_rps"] if b["throughput_rps"] else None,
                     "latency_overhead_percent": latency,
                     "netplane_rejected": a["rejected_at_capacity"],
                     "tower_rejected": b["rejected_at_capacity"]})
    (directory / "summary.json").write_text(json.dumps({"schema_version": 1, "single_run_diagnostic": True, "comparisons": rows}, indent=2) + "\n")
    for row in rows:
        print(row)
    print("Single-run diagnostic only. Repeat paired runs before drawing performance conclusions.")


if __name__ == "__main__":
    main()
