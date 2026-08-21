import math
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np

matplotlib.use("Agg", force=True)
import matplotlib.pyplot as plt

plt.style.use(
    "seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default"
)
plt.rcParams["font.sans-serif"] = "DejaVu Sans"
plt.rcParams["axes.edgecolor"] = "#cccccc"
plt.rcParams["axes.linewidth"] = 0.8

SEMANTIC_KEYS = ("semantic_validation", "semantic_verification", "query_semantics")
READ_METRICS = (
    "traversal_1hop",
    "traversal_2hop",
    "traversal_3hop",
    "point_lookup",
    "filtered_lookup",
    "aggregation",
)
CHART_FILENAMES = (
    "ingestion_throughput.png",
    "traversal_latency.png",
    "concurrency_scaling.png",
    "concurrency_latency.png",
    "lookup_latency.png",
)


def _is_number(value: Any, *, minimum: float | None = None) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        return False
    return minimum is None or value >= minimum


def _is_count(value: Any, *, minimum: int = 0) -> bool:
    return _is_number(value, minimum=minimum) and float(value).is_integer()


def _pass_marker(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"pass", "passed", "ok", "true", "success", "successful"} or (
            normalized.startswith("pass (")
        )
    return False


def _check_passed(value: Any) -> bool:
    if _pass_marker(value):
        return True
    if not isinstance(value, dict):
        return False
    if "expected" in value and "actual" in value:
        return value["expected"] == value["actual"]
    explicit = next((value[key] for key in ("status", "passed", "result") if key in value), None)
    return explicit is not None and _pass_marker(explicit)


def _semantic_passed(data: dict[str, Any]) -> bool:
    validation = next((data[key] for key in SEMANTIC_KEYS if key in data), None)
    if isinstance(validation, (bool, str)):
        return _pass_marker(validation)
    if not isinstance(validation, dict) or not validation:
        return False

    explicit = next(
        (validation[key] for key in ("status", "passed", "result") if key in validation),
        None,
    )
    if explicit is not None and not _pass_marker(explicit):
        return False

    checks = validation.get("checks") or validation.get("results")
    if not isinstance(checks, dict):
        checks = {
            key: value
            for key, value in validation.items()
            if key not in {"status", "passed", "result", "notes"}
        }
    if checks:
        return all(_check_passed(value) for value in checks.values())
    return explicit is not None and _pass_marker(explicit)


def _valid_ingestion(data: dict[str, Any]) -> bool:
    ingestion = data.get("ingestion")
    if not isinstance(ingestion, dict):
        return False

    expected_nodes = ingestion.get("total_nodes")
    expected_edges = ingestion.get("total_edges")
    verified_nodes = ingestion.get("verified_nodes")
    verified_edges = ingestion.get("verified_edges")
    if not all(
        _is_count(value, minimum=1)
        for value in (expected_nodes, expected_edges, verified_nodes, verified_edges)
    ):
        return False
    if expected_nodes != verified_nodes or expected_edges != verified_edges:
        return False
    return all(
        _is_number(ingestion.get(key), minimum=0.0000001)
        for key in ("total_wall_clock_sec", "nodes_per_sec", "rels_per_sec")
    )


def _metric_counts(metric: dict[str, Any]) -> tuple[Any, Any, Any]:
    successful = metric.get("successful_count", metric.get("count"))
    errors = metric.get("error_count", metric.get("errors"))
    attempted = metric.get(
        "attempted_count", metric.get("attempted_operations", metric.get("attempted"))
    )
    if attempted is None and _is_count(successful) and _is_count(errors):
        attempted = successful + errors
    return attempted, successful, errors


def _valid_read_metric(metric: Any, expected_samples: int | None = None) -> bool:
    if not isinstance(metric, dict):
        return False
    attempted, successful, errors = _metric_counts(metric)
    if not _is_count(successful, minimum=1) or not _is_count(errors):
        return False
    if errors != 0 or not _is_count(attempted, minimum=1) or attempted != successful + errors:
        return False
    if expected_samples is not None and attempted != expected_samples:
        return False
    return all(
        _is_number(metric.get(key), minimum=0.0) for key in ("p50_ms", "p95_ms")
    ) and _is_number(metric.get("qps"), minimum=0.0000001)


def _configured_concurrency_levels(
    results: dict[str, Any], platforms: dict[str, dict[str, Any]]
) -> list[int]:
    metadata = results.get("_metadata")
    if isinstance(metadata, dict):
        configured = metadata.get("concurrency_levels")
        if isinstance(configured, list):
            levels = sorted(
                {
                    value
                    for value in configured
                    if isinstance(value, int) and not isinstance(value, bool) and value > 0
                }
            )
            if levels:
                return levels

    discovered: set[int] = set()
    for data in platforms.values():
        mixed = data.get("mixed_workload")
        if not isinstance(mixed, dict):
            continue
        for key in mixed:
            if not key.startswith("concurrency_"):
                continue
            try:
                clients = int(key.removeprefix("concurrency_"))
            except ValueError:
                continue
            if clients > 0:
                discovered.add(clients)
    return sorted(discovered)


def _valid_concurrency_metric(metric: Any, clients: int, ops_per_worker: int | None = None) -> bool:
    if not isinstance(metric, dict):
        return False
    attempted, successful, errors = _metric_counts(metric)
    if not _is_count(successful, minimum=1) or not _is_count(errors):
        return False
    if not _is_count(attempted, minimum=1) or attempted != successful + errors:
        return False
    if ops_per_worker is not None and attempted != clients * ops_per_worker:
        return False
    if not all(
        _is_number(metric.get(key), minimum=0.0) for key in ("p50_ms", "p95_ms")
    ) or not _is_number(metric.get("qps"), minimum=0.0000001):
        return False

    reads = metric.get("total_reads", metric.get("successful_reads"))
    writes = metric.get("total_writes", metric.get("successful_writes"))
    return (
        _is_count(reads)
        and _is_count(writes)
        and reads + writes == successful
        and reads + writes > 0
    )


def _publication_exclusion_reason(
    data: dict[str, Any], metadata: dict[str, Any], concurrency_levels: list[int]
) -> str | None:
    if str(data.get("status", "")).lower() != "complete":
        return f"status is {data.get('status', 'not reported')!r}"
    if not _semantic_passed(data):
        return "semantic validation is missing or did not pass"
    if not _valid_ingestion(data):
        return "ingestion/count-verification schema is invalid"

    expected_samples = metadata.get(
        "benchmark_iterations", metadata.get("benchmark_iterations_per_read_metric")
    )
    if not _is_count(expected_samples, minimum=1):
        expected_samples = None
    full_run = metadata.get("run_mode") == "full"
    for key in READ_METRICS:
        expected = expected_samples if key != "aggregation" or full_run else None
        if not _valid_read_metric(data.get(key), expected):
            return f"{key} samples or metrics are incomplete"

    if not concurrency_levels:
        return "no concurrency levels are recorded"
    mixed = data.get("mixed_workload")
    if not isinstance(mixed, dict):
        return "mixed workload results are missing"
    mixed_configuration = metadata.get("mixed_measurement", {})
    ops_per_worker = metadata.get("ops_per_worker")
    if ops_per_worker is None and isinstance(mixed_configuration, dict):
        ops_per_worker = mixed_configuration.get("ops_per_worker")
    if not _is_count(ops_per_worker, minimum=1):
        ops_per_worker = None
    for clients in concurrency_levels:
        if not _valid_concurrency_metric(
            mixed.get(f"concurrency_{clients}"), clients, ops_per_worker
        ):
            return f"concurrency_{clients} samples or metrics are incomplete"
    return None


def _clear_derived_charts(output_dir: Path) -> None:
    """Remove stale derived charts before attempting to publish a new result set."""
    for filename in CHART_FILENAMES:
        path = output_dir / filename
        if path.exists():
            path.unlink()


def generate_benchmark_charts(results: dict[str, Any], output_dir: Path):
    """Generate charts only from complete, verified, schema-valid platform results."""
    output_dir.mkdir(parents=True, exist_ok=True)
    _clear_derived_charts(output_dir)

    all_platforms = {
        name: data
        for name, data in results.items()
        if not name.startswith("_") and isinstance(data, dict)
    }
    metadata = results.get("_metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    clients = _configured_concurrency_levels(results, all_platforms)

    platforms = []
    for name, data in all_platforms.items():
        reason = _publication_exclusion_reason(data, metadata, clients)
        if reason is None:
            platforms.append(name)
        else:
            print(f"[Visualizer] Excluding {name}: {reason}.")

    if not platforms:
        print("[Visualizer] No complete, verified, schema-valid results to chart.")
        return

    colors = ["#2b5c8f", "#e06c53", "#3bb273", "#e4b343", "#8d5bbf", "#4a90e2"]

    # 1. Ingestion Throughput (Relationships/sec)
    fig = None
    try:
        fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
        rels_speed = [results[name]["ingestion"]["rels_per_sec"] for name in platforms]
        bars = ax.bar(
            platforms,
            rels_speed,
            color=colors[: len(platforms)],
            width=0.5,
            edgecolor="#333333",
            linewidth=0.8,
        )
        ax.set_title(
            "Data Ingestion Throughput (Relationships / Sec)",
            fontsize=14,
            fontweight="bold",
            pad=15,
        )
        ax.set_ylabel("Relationships / Sec (Higher is better)", fontsize=11)
        ax.grid(axis="y", linestyle="--", alpha=0.7)
        for bar in bars:
            height = bar.get_height()
            ax.annotate(
                f"{height:,.0f}",
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontweight="bold",
            )
        fig.tight_layout()
        chart_path = output_dir / "ingestion_throughput.png"
        fig.savefig(chart_path)
        print(f"[Visualizer] Saved {chart_path}")
    finally:
        if fig is not None:
            plt.close(fig)

    # 2. Traversal Latency (1-hop, 2-hop, 3-hop p95)
    fig = None
    try:
        fig, ax = plt.subplots(figsize=(12, 6), dpi=300)
        x = np.arange(len(platforms))
        width = 0.25
        p95_1hop = [results[name]["traversal_1hop"]["p95_ms"] for name in platforms]
        p95_2hop = [results[name]["traversal_2hop"]["p95_ms"] for name in platforms]
        p95_3hop = [results[name]["traversal_3hop"]["p95_ms"] for name in platforms]

        ax.bar(x - width, p95_1hop, width, label="1-Hop (p95)", color="#3b82f6")
        ax.bar(x, p95_2hop, width, label="2-Hop (p95)", color="#10b981")
        ax.bar(x + width, p95_3hop, width, label="3-Hop (p95)", color="#f59e0b")

        ax.set_ylabel("p95 Latency (ms) - Lower is better", fontsize=11)
        ax.set_title(
            "Graph Traversal Latency by Hop Depth (p95)",
            fontsize=14,
            fontweight="bold",
            pad=15,
        )
        ax.set_xticks(x)
        ax.set_xticklabels(platforms, fontsize=10, rotation=10 if len(platforms) > 3 else 0)
        ax.legend(frameon=True, facecolor="white")
        ax.set_yscale("log")
        ax.grid(axis="y", linestyle="--", alpha=0.7)
        fig.tight_layout()
        chart_path = output_dir / "traversal_latency.png"
        fig.savefig(chart_path)
        print(f"[Visualizer] Saved {chart_path}")
    finally:
        if fig is not None:
            plt.close(fig)

    # 3. Mixed Concurrency Throughput Scaling (QPS across clients)
    fig = None
    try:
        fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
        line_styles = ["-o", "-s", "-^", "-d", "-v", "-p"]
        for idx, name in enumerate(platforms):
            mixed = results[name].get("mixed_workload", {})
            qps_values = [mixed[f"concurrency_{c}"]["qps"] for c in clients]
            ax.plot(
                clients,
                qps_values,
                line_styles[idx % len(line_styles)],
                label=name,
                linewidth=2.5,
                markersize=8,
                color=colors[idx % len(colors)],
            )
        ax.set_xlabel("Concurrent Client Threads", fontsize=11)
        ax.set_ylabel("Sustained Throughput (QPS - Higher is better)", fontsize=11)
        ax.set_title(
            "Mixed Workload Throughput vs Concurrency Scaling (80% R / 20% W)",
            fontsize=14,
            fontweight="bold",
            pad=15,
        )
        ax.set_xticks(clients)
        ax.legend(frameon=True, facecolor="white")
        ax.grid(True, linestyle="--", alpha=0.7)
        fig.tight_layout()
        chart_path = output_dir / "concurrency_scaling.png"
        fig.savefig(chart_path)
        print(f"[Visualizer] Saved {chart_path}")
    finally:
        if fig is not None:
            plt.close(fig)

    # 4. Mixed Concurrency Latency Scaling (p95 latency across clients)
    fig = None
    try:
        fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
        line_styles = ["-o", "-s", "-^", "-d", "-v", "-p"]
        for idx, name in enumerate(platforms):
            mixed = results[name].get("mixed_workload", {})
            latencies = [mixed[f"concurrency_{c}"]["p95_ms"] for c in clients]
            ax.plot(
                clients,
                latencies,
                line_styles[idx % len(line_styles)],
                label=name,
                linewidth=2.5,
                markersize=8,
                color=colors[idx % len(colors)],
            )
        ax.set_xlabel("Concurrent Client Threads", fontsize=11)
        ax.set_ylabel("Mixed Workload p95 Latency (ms) - Lower is better", fontsize=11)
        ax.set_title(
            "Mixed Workload Latency vs Concurrency Scaling (80% R / 20% W)",
            fontsize=14,
            fontweight="bold",
            pad=15,
        )
        ax.set_xticks(clients)
        ax.legend(frameon=True, facecolor="white")
        ax.grid(True, linestyle="--", alpha=0.7)
        fig.tight_layout()
        chart_path = output_dir / "concurrency_latency.png"
        fig.savefig(chart_path)
        print(f"[Visualizer] Saved {chart_path}")
    finally:
        if fig is not None:
            plt.close(fig)

    # 4. Point vs Filtered Lookups (p95 latency)
    fig = None
    try:
        fig, ax = plt.subplots(figsize=(10, 5), dpi=300)
        x = np.arange(len(platforms))
        width = 0.35
        p95_point = [results[name]["point_lookup"]["p95_ms"] for name in platforms]
        p95_filtered = [results[name]["filtered_lookup"]["p95_ms"] for name in platforms]
        ax.bar(x - width / 2, p95_point, width, label="Point Lookup (p95)", color="#6366f1")
        ax.bar(
            x + width / 2,
            p95_filtered,
            width,
            label="Filtered Lookup (p95)",
            color="#ec4899",
        )
        ax.set_ylabel("p95 Latency (ms) - Lower is better", fontsize=11)
        ax.set_title(
            "Lookup Latency Comparison (Point vs Filtered p95)",
            fontsize=14,
            fontweight="bold",
            pad=15,
        )
        ax.set_xticks(x)
        ax.set_xticklabels(platforms, fontsize=10, rotation=10 if len(platforms) > 3 else 0)
        ax.legend(frameon=True, facecolor="white")
        ax.grid(axis="y", linestyle="--", alpha=0.7)
        fig.tight_layout()
        chart_path = output_dir / "lookup_latency.png"
        fig.savefig(chart_path)
        print(f"[Visualizer] Saved {chart_path}")
    finally:
        if fig is not None:
            plt.close(fig)


generate_publication_charts = generate_benchmark_charts
