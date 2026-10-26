#!/usr/bin/env python3
"""Compare long closed-loop base vs current buffered GraphBuilder default."""

from __future__ import annotations

import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

NAME_RE = re.compile(
    r"^(?P<side>base|head)_graph-builder_immediate_"
    r"r(?P<routes>\d+)_c(?P<callers>\d+)_w(?P<workers>\d+)_rep(?P<rep>\d+)\.json$"
)


def median(values: list[float]) -> float:
    return statistics.median(values)


def same_direction(deltas: list[float]) -> bool:
    return len(deltas) >= 5 and (all(d > 0 for d in deltas) or all(d < 0 for d in deltas))


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
        if path.name.startswith("warmup") or path.name.startswith("comparison"):
            continue
        match = NAME_RE.match(path.name)
        if not match:
            continue
        report = json.loads(path.read_text())
        side = match.group("side")
        callers = int(match.group("callers"))
        workers = int(match.group("workers"))
        rep = int(match.group("rep"))
        cells[(workers, callers)][side][rep] = report
    return cells


def series(bucket, side, expected):
    reps = bucket.get(side, {})
    want = set(range(1, expected + 1))
    if set(reps) != want:
        raise SystemExit(f"{side} has reps {sorted(reps)}; expected {sorted(want)}")
    return [reps[i] for i in range(1, expected + 1)]


def metric(reports, *path):
    out = []
    for report in reports:
        value = report["metrics"]
        for part in path:
            value = value[part]
        out.append(float(value))
    return out


def render(directory: Path, base_sha: str, expected: int) -> None:
    cells = load(directory)
    lines = [
        "# Buffered default vs `1e06bd1` (long closed-loop)",
        "",
        f"Base: `{base_sha}`. Head: current buffered `GraphBuilder` default.",
        "Identical routes/callers/workload; no skew. Paired repetitions: "
        f"{expected}. Target wall time ≈15s per cell.",
        "",
    ]
    ok = True
    notes = []
    for key in sorted(cells):
        workers, callers = key
        bucket = cells[key]
        base = series(bucket, "base", expected)
        head = series(bucket, "head", expected)
        base_mean = metric(base, "latency", "mean")
        head_mean = metric(head, "latency", "mean")
        base_p99 = metric(base, "latency", "p99")
        head_p99 = metric(head, "latency", "p99")
        base_rps = metric(base, "throughput_rps")
        head_rps = metric(head, "throughput_rps")
        elapsed = median([r["metrics"]["elapsed_ns"] for r in head]) / 1e9
        mean_deltas = [head_mean[i] - base_mean[i] for i in range(expected)]
        p99_deltas = [head_p99[i] - base_p99[i] for i in range(expected)]
        mean_sp = median(base_mean) / median(head_mean)
        lines.append(f"### workers={workers} callers={callers} (~{elapsed:.1f}s wall)")
        lines.append("")
        lines.append(
            f"| side | mean | p95 | p99 | rps | elapsed |"
        )
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
        for side, reports in (("base", base), ("head", head)):
            m = median(metric(reports, "latency", "mean"))
            p95 = median(metric(reports, "latency", "p95"))
            p99 = median(metric(reports, "latency", "p99"))
            rps = median(metric(reports, "throughput_rps"))
            wall = median([r["metrics"]["elapsed_ns"] for r in reports]) / 1e9
            lines.append(
                f"| {side} | {fmt_ns(m)} | {fmt_ns(p95)} | {fmt_ns(p99)} | "
                f"{fmt_rps(rps)} | {wall:.1f}s |"
            )
        lines.append("")
        lines.append(
            f"- mean speedup head/base inverse: **{mean_sp:.3f}×** "
            f"(>1 means head faster); same_direction={same_direction(mean_deltas)}"
        )
        lines.append(
            f"- p99 delta (head-base): {fmt_ns(median(p99_deltas))}; "
            f"same_direction={same_direction(p99_deltas)}"
        )
        lines.append(
            f"- rps delta (head-base): {fmt_rps(median(head_rps) - median(base_rps))}"
        )
        lines.append("")
        # Equivalence: within 5% mean and no consistent ≥15% p99 regression.
        if mean_sp < 0.95:
            ok = False
            notes.append(
                f"head mean regression at w{workers} c{callers}: {mean_sp:.3f}×"
            )
        if (
            median(head_p99) > median(base_p99) * 1.15
            and same_direction(p99_deltas)
            and median(p99_deltas) > 0
        ):
            ok = False
            notes.append(
                f"head p99 regression at w{workers} c{callers}: "
                f"{fmt_ns(median(base_p99))} → {fmt_ns(median(head_p99))}"
            )

    lines.extend(
        [
            "## Equivalence criteria",
            "",
            "- Head mean within ~5% of base (≥0.95×).",
            "- No consistent ≥15% p99 regression.",
            "",
        ]
    )
    if notes:
        lines.append("### Findings")
        lines.append("")
        for note in notes:
            lines.append(f"- {note}")
        lines.append("")
    lines.append(f"**equivalent={str(ok).lower()}**")
    lines.append("")
    text = "\n".join(lines)
    (directory / "comparison.md").write_text(text + "\n")
    (directory / "comparison.json").write_text(
        json.dumps({"equivalent": ok, "notes": notes, "base_sha": base_sha}, indent=2)
        + "\n"
    )
    print(text)
    if not ok:
        raise SystemExit(2)


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("usage: bench-buffered-baseline-report.py DIR BASE_SHA RUNS")
    render(Path(sys.argv[1]), sys.argv[2], int(sys.argv[3]))


if __name__ == "__main__":
    main()
