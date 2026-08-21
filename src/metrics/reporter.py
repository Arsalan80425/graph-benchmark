import json
import math
from pathlib import Path
from typing import Any

from tabulate import tabulate

SEMANTIC_KEYS = ("semantic_validation", "semantic_verification", "query_semantics")
READ_WORKLOADS = (
    ("traversal_1hop", "Traversal - 1 hop"),
    ("traversal_2hop", "Traversal - 2 hop"),
    ("traversal_3hop", "Traversal - 3 hop"),
    ("point_lookup", "Point lookup"),
    ("filtered_lookup", "Indexed / filtered lookup"),
    ("aggregation", "Aggregation"),
)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _fmt(value: Any, decimals: int = 2, missing: str = "Not reported") -> str:
    if value is None or value == "":
        return missing
    if _is_number(value):
        if decimals == 0:
            return f"{int(value):,}"
        return f"{value:,.{decimals}f}"
    return str(value).replace("\n", " ").strip() or missing


def _status(data: dict[str, Any]) -> str:
    return _fmt(data.get("status"), missing="Not reported")


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


def _semantic_payload(data: dict[str, Any]) -> Any:
    for key in SEMANTIC_KEYS:
        if key in data:
            return data[key]
    return None


def _semantic_summary(data: dict[str, Any]) -> str:
    validation = _semantic_payload(data)
    if validation is None:
        return "Not recorded"
    if isinstance(validation, (bool, str)):
        return "PASS" if _pass_marker(validation) else f"FAIL ({_fmt(validation)})"
    if not isinstance(validation, dict) or not validation:
        return "FAIL (invalid validation record)"

    explicit = next(
        (validation[key] for key in ("status", "passed", "result") if key in validation),
        None,
    )
    checks = validation.get("checks") or validation.get("results")
    if not isinstance(checks, dict):
        checks = {
            key: value
            for key, value in validation.items()
            if key not in {"status", "passed", "result", "notes"}
        }

    if checks:
        passed = sum(1 for value in checks.values() if _check_passed(value))
        all_passed = passed == len(checks)
        if explicit is not None:
            all_passed = all_passed and _pass_marker(explicit)
        label = "PASS" if all_passed else "FAIL"
        return f"{label} ({passed}/{len(checks)} checks)"

    if explicit is not None:
        return "PASS" if _pass_marker(explicit) else f"FAIL ({_fmt(explicit)})"
    return "FAIL (no checks recorded)"


def _metric_counts(metric: Any) -> tuple[Any, Any, Any]:
    if not isinstance(metric, dict):
        return None, None, None

    successful = metric.get("successful_count", metric.get("count"))
    errors = metric.get("error_count", metric.get("errors"))
    attempted = metric.get(
        "attempted_count", metric.get("attempted_operations", metric.get("attempted"))
    )
    if attempted is None and _is_number(successful) and _is_number(errors):
        attempted = successful + errors
    return attempted, successful, errors


def _verification_summary(ingestion: Any) -> str:
    if not isinstance(ingestion, dict):
        return "Not recorded"
    expected_nodes = ingestion.get("total_nodes")
    expected_edges = ingestion.get("total_edges")
    verified_nodes = ingestion.get("verified_nodes")
    verified_edges = ingestion.get("verified_edges")
    values = (expected_nodes, expected_edges, verified_nodes, verified_edges)
    if not all(_is_number(value) for value in values):
        return "Not recorded"
    return (
        "PASS" if expected_nodes == verified_nodes and expected_edges == verified_edges else "FAIL"
    )


def _concurrency_levels(results: dict[str, Any], platforms: dict[str, Any]) -> list[int]:
    levels: set[int] = set()
    metadata = results.get("_metadata", {})
    if isinstance(metadata, dict):
        configured = metadata.get("concurrency_levels")
        if isinstance(configured, list):
            for value in configured:
                if isinstance(value, int) and not isinstance(value, bool) and value > 0:
                    levels.add(value)

    for data in platforms.values():
        if not isinstance(data, dict):
            continue
        mixed = data.get("mixed_workload")
        if not isinstance(mixed, dict):
            continue
        for key in mixed:
            if not key.startswith("concurrency_"):
                continue
            try:
                value = int(key.removeprefix("concurrency_"))
            except ValueError:
                continue
            if value > 0:
                levels.add(value)
    return sorted(levels)


def _realized_mix(metric: Any) -> str:
    if not isinstance(metric, dict):
        return "Not reported"
    empirical = metric.get("empirical_mix")
    if empirical:
        return _fmt(empirical)

    reads = metric.get("total_reads", metric.get("successful_reads"))
    writes = metric.get("total_writes", metric.get("successful_writes"))
    if _is_number(reads) and _is_number(writes) and reads + writes > 0:
        total = reads + writes
        return f"{reads / total * 100:.1f}% reads / {writes / total * 100:.1f}% writes"
    return "Not reported"


class BenchmarkReporter:
    """Formats and exports benchmark results without inventing missing evidence."""

    def __init__(self, results: dict[str, Any]):
        self.results = results

    def generate_markdown_report(self) -> str:
        md = [
            "# Graph Database Cloud Benchmark Results Matrix\n",
            "> Generated automatically by the Wexa AI Benchmarking Suite. "
            "Missing evidence is shown explicitly and is never treated as zero.\n\n",
        ]
        platforms = {
            key: value
            for key, value in self.results.items()
            if not key.startswith("_") and isinstance(value, dict)
        }

        md.append("## 1. Execution Status & Semantic Validation\n")
        status_rows = []
        for name, data in platforms.items():
            status_rows.append(
                [
                    name,
                    _status(data),
                    _semantic_summary(data),
                    _fmt(data.get("run_started_at", data.get("run_timestamp"))),
                    _fmt(data.get("error"), missing="None reported"),
                ]
            )
        md.append(
            tabulate(
                status_rows,
                headers=["Platform", "Status", "Semantic Validation", "Run Timestamp", "Error"],
                tablefmt="github",
            )
        )
        md.append("\n\n")

        md.append("## 2. Evaluated Platforms & Resource Specs\n")
        environment_rows = []
        for name, data in platforms.items():
            environment_rows.append(
                [
                    name,
                    _fmt(data.get("platform_type", data.get("type"))),
                    _fmt(data.get("hardware_spec")),
                    _fmt(data.get("storage_engine")),
                    _fmt(data.get("query_interface")),
                ]
            )
        md.append(
            tabulate(
                environment_rows,
                headers=[
                    "Platform",
                    "Type",
                    "Resource Limits",
                    "Storage Engine",
                    "Query Interface",
                ],
                tablefmt="github",
            )
        )
        md.append("\n\n")

        md.append("## 3. Data Loading, Throughput & Count Verification\n")
        ingestion_rows = []
        for name, data in platforms.items():
            ingestion = data.get("ingestion")
            ingestion = ingestion if isinstance(ingestion, dict) else {}
            ingestion_rows.append(
                [
                    name,
                    _status(data),
                    _fmt(ingestion.get("total_nodes"), decimals=0),
                    _fmt(ingestion.get("verified_nodes"), decimals=0),
                    _fmt(ingestion.get("total_edges"), decimals=0),
                    _fmt(ingestion.get("verified_edges"), decimals=0),
                    _verification_summary(ingestion),
                    _fmt(ingestion.get("total_wall_clock_sec")),
                    _fmt(ingestion.get("nodes_per_sec"), decimals=1),
                    _fmt(ingestion.get("rels_per_sec"), decimals=1),
                    _fmt(ingestion.get("method")),
                ]
            )
        md.append(
            tabulate(
                ingestion_rows,
                headers=[
                    "Platform",
                    "Status",
                    "Expected Nodes",
                    "Verified Nodes",
                    "Expected Edges",
                    "Verified Edges",
                    "Count Check",
                    "Wall Time (s)",
                    "Nodes/s",
                    "Rels/s",
                    "Load Method",
                ],
                tablefmt="github",
            )
        )
        md.append("\n\n")

        md.append("## 4. Read Latency, Samples & Errors\n")
        read_rows = []
        for name, data in platforms.items():
            for key, label in READ_WORKLOADS:
                metric = data.get(key)
                metric = metric if isinstance(metric, dict) else {}
                attempted, successful, errors = _metric_counts(metric)
                read_rows.append(
                    [
                        name,
                        _status(data),
                        label,
                        _fmt(attempted, decimals=0),
                        _fmt(successful, decimals=0),
                        _fmt(errors, decimals=0),
                        _fmt(metric.get("p50_ms")),
                        _fmt(metric.get("p95_ms")),
                    ]
                )
        md.append(
            tabulate(
                read_rows,
                headers=[
                    "Platform",
                    "Status",
                    "Workload",
                    "Attempted",
                    "Successful",
                    "Errors",
                    "p50 (ms)",
                    "p95 (ms)",
                ],
                tablefmt="github",
            )
        )
        md.append("\n\n")

        md.append("## 5. Concurrent Mixed Workload\n")
        concurrency_rows = []
        levels = _concurrency_levels(self.results, platforms)
        for name, data in platforms.items():
            mixed = data.get("mixed_workload")
            mixed = mixed if isinstance(mixed, dict) else {}
            for clients in levels:
                metric = mixed.get(f"concurrency_{clients}")
                metric = metric if isinstance(metric, dict) else {}
                attempted, successful, errors = _metric_counts(metric)
                concurrency_rows.append(
                    [
                        name,
                        _status(data),
                        clients,
                        _fmt(attempted, decimals=0),
                        _fmt(successful, decimals=0),
                        _fmt(errors, decimals=0),
                        _fmt(metric.get("total_reads", metric.get("successful_reads")), decimals=0),
                        _fmt(
                            metric.get("total_writes", metric.get("successful_writes")), decimals=0
                        ),
                        _realized_mix(metric),
                        _fmt(metric.get("qps"), decimals=1),
                        _fmt(metric.get("p50_ms")),
                        _fmt(metric.get("p95_ms")),
                    ]
                )

        if concurrency_rows:
            md.append(
                tabulate(
                    concurrency_rows,
                    headers=[
                        "Platform",
                        "Status",
                        "Clients",
                        "Attempted",
                        "Successful",
                        "Errors",
                        "Reads",
                        "Writes",
                        "Realized Mix",
                        "QPS",
                        "p50 (ms)",
                        "p95 (ms)",
                    ],
                    tablefmt="github",
                )
            )
        else:
            md.append("No concurrency measurements were recorded.")
        md.append("\n\n")

        md.append("## 6. Resource Footprint\n")
        footprint_rows = []
        for name, data in platforms.items():
            footprint_rows.append(
                [
                    name,
                    _status(data),
                    _fmt(
                        data.get("resource_footprint"),
                        missing="Not observed / not reported",
                    ),
                    _fmt(
                        data.get("footprint_notes"),
                        missing="No measurement notes reported",
                    ),
                ]
            )
        md.append(
            tabulate(
                footprint_rows,
                headers=["Platform", "Status", "Observed Footprint / Allocation", "Notes"],
                tablefmt="github",
            )
        )
        md.append("\n\n")

        # 7. Run Metadata & Provenance Audit
        metadata = self.results.get("_metadata", {})
        if isinstance(metadata, dict) and metadata:
            md.append("## 7. Run Metadata & Provenance Audit\n")
            client_env = metadata.get("client_environment", {})
            dataset_meta = metadata.get("dataset", {})
            audit_rows = [
                ["Git Commit", _fmt(metadata.get("git_commit"), missing="N/A")],
                ["Client Region", _fmt(client_env.get("client_region"), missing="not declared")],
                ["CognoDB Cloud Region", _fmt(client_env.get("cognodb_region"), missing="not declared")],
                ["Dataset Nodes CSV SHA-256", _fmt(dataset_meta.get("nodes_csv_sha256"), missing="N/A")],
                ["Dataset Edges CSV SHA-256", _fmt(dataset_meta.get("edges_csv_sha256"), missing="N/A")],
                ["Benchmark Harness SHA-256", _fmt(metadata.get("benchmark_harness_sha256"), missing="N/A")],
                ["Run Started At", _fmt(metadata.get("invocation_started_at"), missing="N/A")],
            ]
            md.append(
                tabulate(
                    audit_rows,
                    headers=["Audit Field", "Recorded Value"],
                    tablefmt="github",
                )
            )
            md.append("\n")

        return "".join(md)

    def save_json(self, filepath: Path):
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2)

    def save_markdown(self, filepath: Path):
        content = self.generate_markdown_report()
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

    def print_summary(self):
        print("\n" + "=" * 70)
        print("[+] BENCHMARK COMPLETE - RESULTS SUMMARY")
        print("=" * 70)
        print(self.generate_markdown_report())
