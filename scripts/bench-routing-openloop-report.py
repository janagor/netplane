#!/usr/bin/env python3
"""Summarize open-loop multi-rate buffered vs backend-only with latency breakdown."""

from __future__ import annotations

import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

NAME_RE = re.compile(
    r"^(?P<impl>graph-builder|backend-only)_open_immediate_"
    r"r(?P<routes>\d+)_rate(?P<rate>\d+)_rep(?P<rep>\d+)\.json$"
)


def median(values: list[float]) -> float:
    return statistics.median(values)


def fmt_ns(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.3f} ms"
    if value >= 1_000:
        return f"{value / 1_000:.3f} µs"
    return f"{value:.1f} ns"


def fmt_rps(value: float) -> str:
    return f"{value:,.0f}"


def load(directory: Path):
    cells = defaultdict(lambda: defaultdict(dict))
    for path in sorted(directory.glob("*.json")):
        if path.name.startswith("comparison"):
            continue
        match = NAME_RE.match(path.name)
        if not match:
            continue
        report = json.loads(path.read_text())
        impl = match.group("impl").replace("-", "_")
        rate = int(match.group("rate"))
        rep = int(match.group("rep"))
        cells[rate][impl][rep] = report
    return cells


def series(bucket, impl, expected):
    reps = bucket.get(impl, {})
    want = set(range(1, expected + 1))
    if set(reps) != want:
        raise SystemExit(f"{impl} rate cell has reps {sorted(reps)}; expected {sorted(want)}")
    return [reps[i] for i in range(1, expected + 1)]


def metric(reports, *path):
    out = []
    for report in reports:
        value = report["metrics"]
        for part in path:
            if value is None:
                raise SystemExit(f"missing metrics path {path}")
            value = value[part]
        out.append(float(value))
    return out


def render(directory: Path, expected: int) -> None:
    cells = load(directory)
    lines = [
        "# Routing open-loop multi-rate (buffered vs backend-only)",
        "",
        "Latency breakdown separates generator lateness from service time.",
        f"Paired repetitions: {expected}. Skew=0, routes=4, Tokio workers=4.",
        "",
    ]
    for rate in sorted(cells):
        bucket = cells[rate]
        lines.append(f"### offered_rps={rate:,}")
        lines.append("")
        lines.append(
            "| impl | achieved rps | lateness p99 | service mean | service p99 | e2e p99 |"
        )
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
        for impl in ("graph_builder", "backend_only"):
            reports = series(bucket, impl, expected)
            for report in reports:
                if report["metrics"].get("open_loop") is None:
                    raise SystemExit(f"missing open_loop breakdown in rep report")
            lines.append(
                "| {impl} | {rps} | {late} | {svc} | {svc99} | {e2e} |".format(
                    impl=impl,
                    rps=fmt_rps(median(metric(reports, "throughput_rps"))),
                    late=fmt_ns(median(metric(reports, "open_loop", "arrival_lateness", "p99"))),
                    svc=fmt_ns(median(metric(reports, "open_loop", "service", "mean"))),
                    svc99=fmt_ns(median(metric(reports, "open_loop", "service", "p99"))),
                    e2e=fmt_ns(median(metric(reports, "open_loop", "end_to_end", "p99"))),
                )
            )
        lines.append("")
        gb = series(bucket, "graph_builder", expected)
        bo = series(bucket, "backend_only", expected)
        gb_svc = median(metric(gb, "open_loop", "service", "mean"))
        bo_svc = median(metric(bo, "open_loop", "service", "mean"))
        lines.append(
            f"- service mean speedup backend_only vs buffered: "
            f"{(gb_svc / bo_svc) if bo_svc else float('nan'):.2f}×"
        )
        lines.append("")

    text = "\n".join(lines)
    (directory / "comparison.md").write_text(text + "\n")
    print(text)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: bench-routing-openloop-report.py DIR RUNS")
    render(Path(sys.argv[1]), int(sys.argv[2]))


if __name__ == "__main__":
    main()
