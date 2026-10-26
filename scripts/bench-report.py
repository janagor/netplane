#!/usr/bin/env python3
"""Summarize paired operational benchmark JSON with strict schema-v2 checks."""
import json
import statistics
import sys
from pathlib import Path

REQUIRED_SCHEMA = 2
CONFIG_KEYS = (
    "profile",
    "scenario",
    "load",
    "clients",
    "max_outstanding",
    "generator_capacity",
    "outer_in_flight",
    "outer_buffer",
    "backend_in_flight",
    "backend_buffer",
    "slow_in_flight",
    "slow_ms",
    "offered_rps",
    "warmup_secs",
    "duration_secs",
    "publications",
    "retention_mode",
    "seed",
)
ENV_KEYS = ("rustc", "target", "dependency_fingerprint", "tokio_workers")


def invariants(metrics, scenario):
    if metrics.get("runtime_errors", 0):
        raise ValueError("runtime_errors must be zero for comparison")
    if metrics.get("drain_cancelled", 0):
        raise ValueError("drain_cancelled must be zero for comparison")
    if metrics.get("admission_other_errors", 0):
        raise ValueError("admission_other_errors must be zero for comparison")
    if metrics.get("missing_latency_samples", 0):
        raise ValueError("missing_latency_samples must be zero for comparison")
    if metrics.get("measurement_complete") is not True:
        raise ValueError("measurement_complete must be true for comparison")
    if metrics.get("publications_after_window", 0):
        raise ValueError("publications_after_window must be zero for comparison")
    if scenario == "retention":
        if metrics.get("throughput_rps") is not None:
            raise ValueError("retention throughput_rps must be null")
        if metrics.get("cancelled_requests") != metrics.get("admitted"):
            raise ValueError("retention cancelled_requests must equal admitted")
        return
    if metrics["completed"] > metrics["admitted"]:
        raise ValueError("completed exceeds admitted")
    if metrics["completed"] + metrics.get("runtime_errors", 0) + metrics.get("drain_cancelled", 0) != metrics["admitted"]:
        raise ValueError("admitted must equal completed + runtime_errors + drain_cancelled")
    after = metrics.get("completed_after_window", 0)
    if metrics["completed_in_window"] + after != metrics["completed"]:
        raise ValueError("in-window and after-window completions must sum to completed")
    if metrics["attempted"] != metrics["admitted"] + metrics["rejected_at_capacity"] + metrics.get("admission_other_errors", 0):
        raise ValueError("attempt accounting invariant failed")
    samples = metrics.get("latency_samples")
    if samples is None or samples != metrics["completed"]:
        raise ValueError("latency_samples must equal completed for comparison")
    throughput = metrics.get("throughput_rps")
    if throughput is None or not isinstance(throughput, (int, float)) or throughput != throughput:
        raise ValueError("throughput_rps must be a finite number")


def normalize_config(config):
    return {key: config.get(key) for key in CONFIG_KEYS}


def normalize_environment(env):
    return {key: env.get(key) for key in ENV_KEYS}


def read_report(path, expected_sha=None):
    value = json.loads(path.read_text())
    schema = value.get("schema_version")
    if schema != REQUIRED_SCHEMA:
        raise ValueError(
            f"strict comparison requires schema_version {REQUIRED_SCHEMA}, got {schema}: {path}"
        )
    reported = (value.get("environment") or {}).get("git_sha")
    if not reported:
        raise ValueError(f"missing environment.git_sha: {path}")
    if expected_sha is not None and reported != expected_sha:
        raise ValueError(f"git sha mismatch in {path}: {reported} != {expected_sha}")
    invariants(value["metrics"], value.get("scenario"))
    return value


def assert_comparable(reports):
    first = reports[0]
    scenario = first["scenario"]
    config = normalize_config(first["config"])
    environment = normalize_environment(first["environment"])
    duration = first["config"].get("duration_secs")
    for report in reports[1:]:
        if report.get("schema_version") != REQUIRED_SCHEMA:
            raise ValueError("schema_version mismatch across compared runs")
        if report["scenario"] != scenario:
            raise ValueError("scenario mismatch across compared runs")
        if normalize_config(report["config"]) != config:
            raise ValueError("config mismatch across compared runs")
        if normalize_environment(report["environment"]) != environment:
            raise ValueError("environment mismatch across compared runs")
        if report["config"].get("duration_secs") != duration:
            raise ValueError("measured duration mismatch across compared runs")


def series(reports, field):
    values = []
    for report in reports:
        item = report["metrics"]
        for key in field:
            item = item[key]
        if item is None:
            raise ValueError(f"missing metric {field}")
        values.append(float(item))
    return values


def summarize(output, base_sha, head_sha, runs):
    base_reports = []
    head_reports = []
    for pair in range(1, runs + 1):
        base_reports.append(read_report(output / f"base-{pair}.json", base_sha))
        head_reports.append(read_report(output / f"head-{pair}.json", head_sha))
    assert_comparable(base_reports + head_reports)
    result = {
        "schema_version": REQUIRED_SCHEMA,
        "base": base_sha,
        "head": head_sha,
        "scenario": base_reports[0]["scenario"],
        "runs": runs,
        "metrics": {},
    }
    fields = {
        "throughput_rps": ("throughput_rps",),
        "p50_ns": ("latency_ns", "p50"),
        "p95_ns": ("latency_ns", "p95"),
        "p99_ns": ("latency_ns", "p99"),
    }
    for name, field in fields.items():
        base = series(base_reports, field)
        head = series(head_reports, field)
        base_med = statistics.median(base)
        head_med = statistics.median(head)
        paired = [100 * (h / b - 1) for b, h in zip(base, head) if b > 0]
        result["metrics"][name] = {
            "base_median": base_med,
            "head_median": head_med,
            "relative_change_percent": 100 * (head_med / base_med - 1) if base_med > 0 else None,
            "paired_change_range_percent": [min(paired), max(paired)] if paired else None,
            "base_range": [min(base), max(base)],
            "head_range": [min(head), max(head)],
        }
    return result


def main():
    output, base, head, runs = Path(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4])
    result = summarize(output, base, head, runs)
    (output / "comparison.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = [
        f"Base: {base}",
        f"Head: {head}",
        f"Scenario: {result['scenario']}",
        f"Paired runs: {runs}",
    ]
    for name, metric in result["metrics"].items():
        change = metric["relative_change_percent"]
        low, high = metric["paired_change_range_percent"]
        lines.append(f"{name}: {change:+.2f}% (paired range {low:+.2f}% to {high:+.2f}%)")
    lines.append(
        "Interpretation: inspect paired variation and host conditions; no automatic regression verdict."
    )
    report = "\n".join(lines) + "\n"
    (output / "comparison.txt").write_text(report)
    print(report, end="")


if __name__ == "__main__":
    main()
