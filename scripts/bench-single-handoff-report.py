#!/usr/bin/env python3
"""Summarize single-handoff probe JSON into paired comparison tables."""

from __future__ import annotations

import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

IMPLEMENTATIONS = (
    "tower_bounded",
    "tower_double_buffered",
    "graph_builder",
    "single_handoff",
    "graph_draft",
)
BASELINE = "tower_bounded"
REQUIRED_SCHEMA = 2
# Config fields that must match across implementations within one comparison cell.
CELL_CONFIG_KEYS = (
    "workload",
    "callers",
    "samples",
    "warmup",
    "work_us",
    "max_in_flight",
    "buffer_capacity",
    "backend_in_flight",
    "backend_buffer",
    "max_outstanding",
    "routes",
)


def load_runs(directory: Path) -> list[dict]:
    runs = []
    for path in sorted(directory.glob("*.json")):
        if path.name.startswith("warmup-") or path.name in {"comparison.json"}:
            continue
        report = json.loads(path.read_text())
        if report.get("experiment") != "single_handoff_router":
            continue
        if report.get("schema_version") != REQUIRED_SCHEMA:
            raise SystemExit(
                f"strict comparison requires schema_version {REQUIRED_SCHEMA}, got "
                f"{report.get('schema_version')}: {path}"
            )
        cfg = report.get("config") or {}
        if "repetition" not in cfg:
            raise SystemExit(f"missing config.repetition in {path}")
        report["_path"] = str(path)
        runs.append(report)
    if not runs:
        raise SystemExit(f"no probe results in {directory}")
    return runs


def cell_config(report: dict) -> tuple:
    cfg = report["config"]
    return tuple(cfg[key] for key in CELL_CONFIG_KEYS)


def all_pairs_same_direction(deltas: list[float]) -> bool:
    """True when every paired delta shares a sign.

    With five pairs this is an exact two-sided sign-test p-value of 0.0625, so it
    is reported as directional consistency rather than statistical significance.
    """
    if len(deltas) < 5:
        return False
    return all(d > 0 for d in deltas) or all(d < 0 for d in deltas)


def median(values: list[float]) -> float:
    return statistics.median(values)


def paired_deltas(base: list[float], other: list[float]) -> list[float]:
    if len(base) != len(other):
        raise ValueError("paired series lengths differ")
    return [other[i] - base[i] for i in range(len(base))]


def summarize_metric(values: list[float]) -> dict:
    if not values:
        return {"n": 0}
    return {
        "n": len(values),
        "median": median(values),
        "min": min(values),
        "max": max(values),
        "mean": statistics.fmean(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
    }


def index_runs(runs: list[dict]) -> dict[tuple, dict[str, dict[int, dict]]]:
    """Index as (workload, callers) -> implementation -> repetition -> report."""
    indexed: dict[tuple, dict[str, dict[int, dict]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    cell_configs: dict[tuple, tuple] = {}
    for report in runs:
        cfg = report["config"]
        cell = (cfg["workload"], cfg["callers"])
        impl = cfg["implementation"]
        repetition = int(cfg["repetition"])
        config = cell_config(report)
        if cell in cell_configs and cell_configs[cell] != config:
            raise SystemExit(
                f"incompatible configs mixed in cell {cell}: "
                f"{cell_configs[cell]} vs {config} ({report['_path']})"
            )
        cell_configs[cell] = config
        if impl not in IMPLEMENTATIONS:
            raise SystemExit(f"unknown implementation {impl} in {report['_path']}")
        if repetition in indexed[cell][impl]:
            raise SystemExit(
                f"duplicate repetition {repetition} for {impl} in cell {cell}: "
                f"{indexed[cell][impl][repetition]['_path']} and {report['_path']}"
            )
        indexed[cell][impl][repetition] = report
    return indexed


def pair_series(
    by_impl: dict[str, dict[int, dict]],
    expected_runs: int,
    cell: tuple,
) -> dict[str, list[dict]]:
    missing = [impl for impl in IMPLEMENTATIONS if impl not in by_impl]
    if missing:
        raise SystemExit(f"cell {cell} missing implementations: {missing}")
    repetition_sets = {
        impl: set(by_impl[impl].keys()) for impl in IMPLEMENTATIONS
    }
    expected = set(range(1, expected_runs + 1))
    for impl, reps in repetition_sets.items():
        if reps != expected:
            raise SystemExit(
                f"cell {cell} implementation {impl} has repetitions {sorted(reps)}; "
                f"expected {sorted(expected)}"
            )
    return {
        impl: [by_impl[impl][rep] for rep in sorted(expected)]
        for impl in IMPLEMENTATIONS
    }


def collect(runs: list[dict], expected_runs: int) -> dict:
    indexed = index_runs(runs)
    cells = []
    for cell in sorted(indexed):
        workload, n_callers = cell
        by_impl = pair_series(indexed[cell], expected_runs, cell)
        base = by_impl[BASELINE]
        base_tput = [r["metrics"]["throughput_rps"] for r in base]
        base_p50 = [r["metrics"]["latency"]["p50"] for r in base]
        base_mean = [r["metrics"]["latency"]["mean"] for r in base]
        shared = {
            key: base[0]["config"][key]
            for key in CELL_CONFIG_KEYS
            if key not in {"workload", "callers"}
        }
        out_cell = {
            "workload": workload,
            "callers": n_callers,
            "config": shared,
            "implementations": {},
        }
        for impl in IMPLEMENTATIONS:
            series = by_impl[impl]
            tput = [r["metrics"]["throughput_rps"] for r in series]
            mean_ns = [r["metrics"]["latency"]["mean"] for r in series]
            p50 = [r["metrics"]["latency"]["p50"] for r in series]
            p95 = [r["metrics"]["latency"]["p95"] for r in series]
            p99 = [r["metrics"]["latency"]["p99"] for r in series]
            cpu = [
                r["metrics"]["cpu_utilization"]
                for r in series
                if r["metrics"]["cpu_utilization"] is not None
            ]
            entry = {
                "runs": len(series),
                "repetitions": [r["config"]["repetition"] for r in series],
                "throughput_rps": summarize_metric(tput),
                "mean_ns": summarize_metric(mean_ns),
                "p50_ns": summarize_metric(p50),
                "p95_ns": summarize_metric(p95),
                "p99_ns": summarize_metric(p99),
                "cpu_utilization": summarize_metric(cpu) if cpu else {"n": 0},
                "allocations_per_request": None,
            }
            if impl != BASELINE:
                t_deltas = paired_deltas(base_tput, tput)
                m_deltas = paired_deltas(base_mean, mean_ns)
                p_deltas = paired_deltas(base_p50, p50)
                entry["vs_tower_bounded"] = {
                    "throughput_delta_rps": summarize_metric(t_deltas),
                    "mean_ns_delta": summarize_metric(m_deltas),
                    "p50_ns_delta": summarize_metric(p_deltas),
                    "throughput_all_pairs_same_direction": all_pairs_same_direction(
                        t_deltas
                    ),
                    "mean_latency_all_pairs_same_direction": all_pairs_same_direction(
                        m_deltas
                    ),
                    "mean_overhead_ratio": (
                        None if median(base_mean) == 0 else median(mean_ns) / median(base_mean)
                    ),
                }
            out_cell["implementations"][impl] = entry

        gb = by_impl["graph_builder"]
        sh = by_impl["single_handoff"]
        gb_mean = [r["metrics"]["latency"]["mean"] for r in gb]
        sh_mean = [r["metrics"]["latency"]["mean"] for r in sh]
        gb_tput = [r["metrics"]["throughput_rps"] for r in gb]
        sh_tput = [r["metrics"]["throughput_rps"] for r in sh]
        mean_deltas = paired_deltas(gb_mean, sh_mean)
        tput_deltas = paired_deltas(gb_tput, sh_tput)
        out_cell["single_handoff_vs_graph_builder"] = {
            "mean_ns_delta": summarize_metric(mean_deltas),
            "throughput_delta_rps": summarize_metric(tput_deltas),
            "mean_latency_all_pairs_same_direction": all_pairs_same_direction(
                mean_deltas
            ),
            "throughput_all_pairs_same_direction": all_pairs_same_direction(
                tput_deltas
            ),
            "median_speedup": (
                None if median(sh_mean) == 0 else median(gb_mean) / median(sh_mean)
            ),
        }
        cells.append(out_cell)
    return {
        "schema_version": 2,
        "baseline": BASELINE,
        "expected_runs": expected_runs,
        "cells": cells,
    }


def fmt_ns(value: float | None) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.3f} ms"
    if value >= 1_000:
        return f"{value / 1_000:.3f} µs"
    return f"{value:.1f} ns"


def fmt_rps(value: float | None) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    return f"{value:,.0f}"


def markdown(summary: dict) -> str:
    lines = [
        "# Single-handoff router experiment",
        "",
        "Paired comparison of five in-memory topologies. Production Netplane is unchanged.",
        f"Baseline for overhead ratios: `{summary['baseline']}`.",
        "",
        "## Capacity caveat",
        "",
        "The experimental `single_handoff` fixture preserves backend `Endpoint` bounds and a",
        "non-blocking `max_outstanding` semaphore. It does **not** enforce GraphBuilder outer",
        "`max_in_flight` or `buffer_capacity`. PR 2 must design an equivalent outer admission",
        "model before replacing the production double-handoff path.",
        "",
        "The probe `single_handoff` path also omits `Handle` admission bookkeeping beyond a",
        "semaphore, version selection, and observation. Do not treat its absolute speedup over",
        "`graph_builder` as the expected production improvement after PR 2.",
        "",
        "## Results",
        "",
    ]
    for cell in summary["cells"]:
        lines.append(f"### workload={cell['workload']} callers={cell['callers']}")
        lines.append("")
        lines.append(
            "| Implementation | runs | median rps | mean | p50 | p95 | p99 | CPU | vs bounded mean |"
        )
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for impl in IMPLEMENTATIONS:
            entry = cell["implementations"][impl]
            vs = entry.get("vs_tower_bounded", {})
            ratio = vs.get("mean_overhead_ratio")
            ratio_s = "baseline" if ratio is None else f"{ratio:.2f}×"
            if vs.get("mean_latency_all_pairs_same_direction"):
                ratio_s += " *"
            cpu = entry["cpu_utilization"]
            cpu_s = "n/a" if cpu.get("n", 0) == 0 else f"{cpu['median']:.2f}"
            lines.append(
                "| {impl} | {runs} | {rps} | {mean} | {p50} | {p95} | {p99} | {cpu} | {ratio} |".format(
                    impl=impl,
                    runs=entry["runs"],
                    rps=fmt_rps(entry["throughput_rps"]["median"]),
                    mean=fmt_ns(entry["mean_ns"]["median"]),
                    p50=fmt_ns(entry["p50_ns"]["median"]),
                    p95=fmt_ns(entry["p95_ns"]["median"]),
                    p99=fmt_ns(entry["p99_ns"]["median"]),
                    cpu=cpu_s,
                    ratio=ratio_s,
                )
            )
        lines.append("")
        lines.append(
            "\\* paired mean-latency deltas all share a sign across runs "
            "(`all_pairs_same_direction`; with five pairs this is sign-test p=0.0625, "
            "not significance at 0.05)."
        )
        lines.append("")
        cmp_ = cell["single_handoff_vs_graph_builder"]
        speedup = cmp_.get("median_speedup")
        speedup_s = "n/a" if speedup is None else f"{speedup:.2f}×"
        same = "yes" if cmp_["mean_latency_all_pairs_same_direction"] else "no"
        lines.append(
            f"Single-handoff vs GraphBuilder median latency speedup: **{speedup_s}** "
            f"(all_pairs_same_direction: {same}; "
            f"median Δmean {fmt_ns(cmp_['mean_ns_delta']['median'])})."
        )
        lines.append("")
    lines.extend(
        [
            "## PR 2 backpressure semantics to preserve",
            "",
            "1. Outer `max_in_flight` concurrency bound across the routed graph version.",
            "2. Outer `buffer_capacity` queue bound with Tower-buffer waiting/rejection behavior.",
            "3. Global `max_outstanding`, including unpolled admitted futures.",
            "4. Backend-local concurrency and buffer isolation.",
            "5. Unpolled request retention, cancellation while queued, readiness ownership on the",
            "   selected backend worker, publication/rollback, deadlines, and shutdown drain.",
            "6. Fairness under backend imbalance (outer slots must not permanently starve an idle backend).",
            "",
            "## Allocations",
            "",
            "Per-request heap allocation counts were not collected: the workspace forbids `unsafe`,",
            "which a process-wide counting allocator requires. Use an external allocator profiler",
            "if allocation share must be quantified before PR 4.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: bench-single-handoff-report.py DIR RUNS")
    directory = Path(sys.argv[1])
    runs_expected = int(sys.argv[2])
    if runs_expected < 5:
        raise SystemExit("RUNS must be >= 5")
    summary = collect(load_runs(directory), runs_expected)
    (directory / "comparison.json").write_text(json.dumps(summary, indent=2) + "\n")
    (directory / "comparison.md").write_text(markdown(summary))
    print(markdown(summary))


if __name__ == "__main__":
    main()
