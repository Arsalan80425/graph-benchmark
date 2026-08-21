"""Reproducible multi-engine graph benchmark runner."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psutil

# Ensure readable Unicode output on Windows terminals.
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from data.download_dataset import parse_or_generate_graph
from src.adapters import (
    ArangoDBAdapter,
    BaseGraphAdapter,
    CognoDBAdapter,
    FalkorDBAdapter,
    MemgraphAdapter,
    Neo4jAdapter,
)
from src.config import (
    AGGREGATION_WARMUP_ITERATIONS,
    ARANGODB_DATABASE,
    ARANGODB_PASSWORD,
    ARANGODB_URL,
    ARANGODB_USER,
    BATCH_SIZE,
    BENCHMARK_ITERATIONS,
    CHARTS_DIR,
    COGNODB_PASSWORD,
    COGNODB_URI,
    COGNODB_USER,
    CONCURRENCY_LEVELS,
    CONNECTION_ATTEMPTS,
    CONNECTION_RETRY_DELAY_SECONDS,
    DATASET_STATS_FILE,
    EDGES_CSV,
    FALKORDB_HOST,
    FALKORDB_PASSWORD,
    FALKORDB_PORT,
    LOOKUP_WARMUP_ITERATIONS,
    MEMGRAPH_PASSWORD,
    MEMGRAPH_URI,
    MEMGRAPH_USER,
    MIXED_WORKLOAD_DURATION_SECONDS,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
    NODES_CSV,
    RESULTS_DIR,
    WARMUP_ITERATIONS,
)
from src.metrics.reporter import BenchmarkReporter
from src.visualizer.generate_charts import generate_publication_charts
from src.workloads.aggregations import run_aggregation_benchmark
from src.workloads.ingestion import run_ingestion_benchmark
from src.workloads.lookups import run_lookup_benchmark
from src.workloads.mixed_workload import run_mixed_workload_benchmark
from src.workloads.traversals import run_traversal_benchmark
from src.workloads.verification import (
    build_reference_expectations,
    validate_query_semantics,
)

READ_METRICS = (
    "traversal_1hop",
    "traversal_2hop",
    "traversal_3hop",
    "point_lookup",
    "filtered_lookup",
    "aggregation",
)
TARGETS = ("cognodb", "neo4j", "memgraph", "falkordb", "arangodb")
LOCAL_CONTAINER_NAMES = {
    "Neo4j 5 (Capped)": "benchmark-neo4j",
    "Memgraph 2.16 (In-Memory C++)": "benchmark-memgraph",
    "FalkorDB 4.2.1 (GraphBLAS)": "benchmark-falkordb",
    "ArangoDB 3.11": "benchmark-arangodb",
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def compute_file_sha256(filepath: Path) -> str:
    """Compute a SHA-256 checksum without loading a large file into memory."""
    sha256 = hashlib.sha256()
    with filepath.open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def compute_json_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def compute_harness_sha256(base_dir: Path) -> str:
    """Fingerprint every executable benchmark source file in a stable order."""
    paths = [base_dir / "run_benchmark.py", base_dir / "data" / "download_dataset.py"]
    paths.extend(sorted((base_dir / "src").rglob("*.py")))
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(base_dir).as_posix().encode("utf-8")
        digest.update(relative)
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def atomic_save_json(results: dict[str, Any], destination: Path) -> None:
    """Checkpoint results atomically so an interrupted later target loses no prior work."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(results, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    temporary.replace(destination)


def get_git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "not_available_not_a_git_checkout"


def inspect_container(container_name: str | None) -> dict[str, Any]:
    """Capture immutable image/resource evidence when a local container exists."""
    if not container_name:
        return {"observable": False, "reason": "managed cloud service"}
    try:
        result = subprocess.run(
            ["docker", "inspect", container_name],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        inspected = json.loads(result.stdout)[0]
        host_config = inspected.get("HostConfig", {})
        config = inspected.get("Config", {})
        state = inspected.get("State", {})
        return {
            "observable": True,
            "container_name": container_name,
            "configured_image": config.get("Image"),
            "image_id": inspected.get("Image"),
            "memory_limit_bytes": host_config.get("Memory"),
            "nano_cpus": host_config.get("NanoCpus"),
            "container_started_at": state.get("StartedAt"),
        }
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, IndexError) as exc:
        return {
            "observable": False,
            "container_name": container_name,
            "reason": f"docker inspect unavailable: {type(exc).__name__}",
        }


def get_adapter(target: str) -> BaseGraphAdapter:
    target = target.lower()
    if target == "cognodb":
        return CognoDBAdapter(uri=COGNODB_URI, user=COGNODB_USER, password=COGNODB_PASSWORD)
    if target == "neo4j":
        return Neo4jAdapter(uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD)
    if target == "memgraph":
        return MemgraphAdapter(uri=MEMGRAPH_URI, user=MEMGRAPH_USER, password=MEMGRAPH_PASSWORD)
    if target == "falkordb":
        return FalkorDBAdapter(host=FALKORDB_HOST, port=FALKORDB_PORT, password=FALKORDB_PASSWORD)
    if target == "arangodb":
        return ArangoDBAdapter(
            url=ARANGODB_URL,
            user=ARANGODB_USER,
            password=ARANGODB_PASSWORD,
            db_name=ARANGODB_DATABASE,
        )
    raise ValueError(f"Unknown target database: {target}")


def validate_target_configuration(targets: list[str]) -> None:
    """Reject missing/placeholding cloud settings before destructive work starts."""
    if "cognodb" not in targets:
        return
    if not COGNODB_URI or "<instance-id>" in COGNODB_URI:
        raise RuntimeError("COGNODB_URI is missing or still contains the example placeholder")
    if not COGNODB_PASSWORD or "your_cognodb_password" in COGNODB_PASSWORD.lower():
        raise RuntimeError("COGNODB_PASSWORD is missing or still contains the example placeholder")
    parsed = urlparse(COGNODB_URI)
    if parsed.scheme != "bolt+s" or not parsed.hostname:
        raise RuntimeError("COGNODB_URI must be a valid bolt+s:// managed-cloud endpoint")
    valid_suffixes = (".databases.cognodb.cloud", ".databases.cognodb.com")
    allow_nonstandard = os.getenv("COGNODB_ALLOW_NONSTANDARD_HOST") == "1"
    if (
        not any(parsed.hostname.lower().endswith(suffix) for suffix in valid_suffixes)
        and not allow_nonstandard
    ):
        raise RuntimeError(
            "The configured CognoDB host does not match the assessment's "
            "*.databases.cognodb.cloud or *.databases.cognodb.com endpoint. Correct it, or set "
            "COGNODB_ALLOW_NONSTANDARD_HOST=1 only after confirming a provider-issued legacy host."
        )


def _safe_footprint(adapter: BaseGraphAdapter) -> str:
    try:
        return adapter.get_resource_footprint()
    except Exception as exc:
        return f"not observable ({type(exc).__name__}: {_redact_error(exc)})"[:500]


def _redact_error(value: object) -> str:
    text = str(value)
    sensitive_values = (COGNODB_URI, COGNODB_PASSWORD)
    for sensitive in sensitive_values:
        if sensitive:
            text = text.replace(sensitive, "[REDACTED]")
    parsed = urlparse(COGNODB_URI)
    if parsed.hostname:
        text = text.replace(parsed.hostname, "[REDACTED_HOST]")
    return text[:2000]


def _connect_with_retry(adapter: BaseGraphAdapter) -> bool:
    for attempt in range(1, CONNECTION_ATTEMPTS + 1):
        if adapter.connect():
            return True
        if attempt < CONNECTION_ATTEMPTS:
            print(
                f"[{adapter.name}] Connection attempt {attempt}/{CONNECTION_ATTEMPTS} failed; "
                f"retrying in {CONNECTION_RETRY_DELAY_SECONDS:.1f}s..."
            )
            adapter.close()
            time.sleep(CONNECTION_RETRY_DELAY_SECONDS)
    return False


def _validate_read_metric(label: str, metric: Any, expected_count: int) -> None:
    if not isinstance(metric, dict):
        raise RuntimeError(f"Missing metric object: {label}")
    errors = int(metric.get("errors", -1))
    count = int(metric.get("count", -1))
    attempted = int(metric.get("attempted", -1))
    if errors != 0 or count != expected_count or attempted != expected_count:
        raise RuntimeError(
            f"Invalid {label} run: expected {expected_count} successes and zero errors; "
            f"received count={count}, attempted={attempted}, errors={errors}"
        )
    if metric.get("p50_ms") is None or metric.get("p95_ms") is None:
        raise RuntimeError(f"Invalid {label} run: required latency percentiles are missing")
    samples = metric.get("samples_ms")
    if not isinstance(samples, list) or len(samples) != expected_count:
        raise RuntimeError(f"Invalid {label} run: expected {expected_count} raw latency samples")


def validate_platform_completeness(
    platform_results: dict[str, Any],
    iterations: int,
    concurrency_levels: list[int],
    ops_per_worker: int | None,
    duration_sec: float | None,
) -> None:
    """Fail closed unless every required assessment metric is valid and complete."""
    ingestion = platform_results.get("ingestion")
    if not isinstance(ingestion, dict):
        raise RuntimeError("Ingestion metrics are missing")
    if ingestion.get("verified_nodes") != ingestion.get("total_nodes") or ingestion.get(
        "verified_edges"
    ) != ingestion.get("total_edges"):
        raise RuntimeError("Post-ingestion node/edge counts do not match the source CSVs")

    semantic = platform_results.get("semantic_validation")
    if not isinstance(semantic, dict) or semantic.get("status") != "passed":
        raise RuntimeError("Exact semantic validation evidence is missing or failed")

    for label in READ_METRICS:
        _validate_read_metric(label, platform_results.get(label), iterations)

    mixed = platform_results.get("mixed_workload")
    if not isinstance(mixed, dict):
        raise RuntimeError("Mixed-workload metrics are missing")
    for concurrency in concurrency_levels:
        label = f"concurrency_{concurrency}"
        metric = mixed.get(label)
        if not isinstance(metric, dict):
            raise RuntimeError(f"Missing mixed-workload level: {label}")
        attempted = int(metric.get("attempted_operations", -1))
        successes = int(metric.get("count", -1))
        errors = int(metric.get("errors", -1))
        operation_successes = int(metric.get("successful_reads", -1)) + int(
            metric.get("successful_writes", -1)
        )
        if (
            attempted <= 0
            or successes <= 0
            or (successes + errors) != attempted
            or operation_successes != successes
        ):
            raise RuntimeError(
                f"Invalid {label}: attempted={attempted}, successes={successes}, "
                f"operation_successes={operation_successes}, errors={errors}"
            )
        if int(metric.get("read_attempts", 0)) <= 0 or int(metric.get("write_attempts", 0)) <= 0:
            raise RuntimeError(f"Invalid {label}: both read and write operations are required")
        if metric.get("p50_ms") is None or metric.get("p95_ms") is None:
            raise RuntimeError(f"Invalid {label}: required latency percentiles are missing")
        if duration_sec is None:
            expected = concurrency * int(ops_per_worker or 0)
            if attempted != expected:
                raise RuntimeError(
                    f"Invalid {label}: expected {expected} fixed operations, got {attempted}"
                )
        elif float(metric.get("duration_sec", 0.0)) < duration_sec:
            raise RuntimeError(
                f"Invalid {label}: measured duration was shorter than {duration_sec:.1f}s"
            )


def run_single_benchmark(
    adapter: BaseGraphAdapter,
    sample_nodes: list[int],
    categories: list[str],
    semantic_expectations: dict[str, Any],
    run_id: str,
    dataset_provenance: dict[str, Any],
    benchmark_configuration: dict[str, Any] | None = None,
    quick_mode: bool = False,
    mixed_duration_sec: float = MIXED_WORKLOAD_DURATION_SECONDS,
) -> dict[str, Any]:
    """Run one isolated platform suite and return a fail-closed result record."""
    print(f"\n{'=' * 70}")
    print(f"[BENCHMARK] RUNNING SUITE: {adapter.name}")
    print(f"            Resource Spec: {adapter.hardware_spec}")
    print(f"{'=' * 70}\n")

    started_at = utc_now()
    started_perf = time.perf_counter()
    adapter_metadata = (
        adapter.get_benchmark_metadata() if hasattr(adapter, "get_benchmark_metadata") else {}
    )
    platform_results: dict[str, Any] = {
        "name": adapter.name,
        "hardware_spec": adapter.hardware_spec,
        "run_id": run_id,
        "run_started_at": started_at,
        "status": "running",
        "dataset_provenance": dataset_provenance,
        "benchmark_configuration": benchmark_configuration or {},
        **adapter_metadata,
    }
    container_name = platform_results.get("container_name") or LOCAL_CONTAINER_NAMES.get(
        adapter.name
    )
    platform_results["container_evidence"] = inspect_container(container_name)

    iterations = 15 if quick_mode else BENCHMARK_ITERATIONS
    concurrency_levels = [1, 5] if quick_mode else list(CONCURRENCY_LEVELS)
    ops_per_worker = 15 if quick_mode else None
    duration_sec = None if quick_mode else mixed_duration_sec

    connected = False
    try:
        connected = _connect_with_retry(adapter)
        if not connected:
            raise ConnectionError(
                f"Endpoint remained unreachable after {CONNECTION_ATTEMPTS} attempts"
            )

        ingestion = run_ingestion_benchmark(adapter, NODES_CSV, EDGES_CSV, batch_size=BATCH_SIZE)
        if hasattr(adapter, "get_retry_telemetry"):
            ingestion["retry_telemetry"] = adapter.get_retry_telemetry()
        platform_results["ingestion"] = ingestion
        platform_results["resource_footprint_after_ingestion"] = _safe_footprint(adapter)

        platform_results["semantic_validation"] = validate_query_semantics(
            adapter, semantic_expectations
        )

        traversal = run_traversal_benchmark(
            adapter,
            sample_nodes,
            warmup_iters=WARMUP_ITERATIONS,
            benchmark_iters=iterations,
        )
        platform_results.update(traversal)
        for label in ("traversal_1hop", "traversal_2hop", "traversal_3hop"):
            _validate_read_metric(label, platform_results[label], iterations)

        lookups = run_lookup_benchmark(
            adapter,
            sample_nodes,
            categories,
            warmup_iters=LOOKUP_WARMUP_ITERATIONS,
            benchmark_iters=iterations,
        )
        platform_results.update(lookups)
        for label in ("point_lookup", "filtered_lookup"):
            _validate_read_metric(label, platform_results[label], iterations)

        aggregation = run_aggregation_benchmark(
            adapter,
            warmup_iters=AGGREGATION_WARMUP_ITERATIONS,
            benchmark_iters=iterations,
        )
        platform_results.update(aggregation)
        _validate_read_metric("aggregation", platform_results["aggregation"], iterations)

        mixed = run_mixed_workload_benchmark(
            adapter,
            sample_nodes,
            concurrency_levels=concurrency_levels,
            ops_per_worker=ops_per_worker,
            duration_sec=duration_sec,
        )
        platform_results.update(mixed)
        platform_results["resource_footprint_after_workload"] = _safe_footprint(adapter)
        platform_results["resource_footprint"] = platform_results[
            "resource_footprint_after_workload"
        ]
        platform_results["footprint_notes"] = (
            "Two point-in-time observation attempts (after ingestion and after the workload); "
            "unavailable values are labelled not observed. This is not a peak-memory or "
            "stored-size measurement."
        )

        validate_platform_completeness(
            platform_results,
            iterations,
            concurrency_levels,
            ops_per_worker,
            duration_sec,
        )
        platform_results["status"] = "complete"
    except Exception as exc:
        print(f"[!] Benchmark failed closed for {adapter.name}: {type(exc).__name__}: {exc}")
        platform_results["status"] = "failed"
        platform_results["error_type"] = type(exc).__name__
        platform_results["error"] = _redact_error(exc)
        platform_results["resource_footprint"] = _safe_footprint(adapter)
        if hasattr(adapter, "get_retry_telemetry"):
            platform_results["retry_telemetry"] = adapter.get_retry_telemetry()
    finally:
        try:
            adapter.close()
        except Exception as close_exc:
            print(f"[Notice] Error during {adapter.name} close: {close_exc}")
        platform_results["run_finished_at"] = utc_now()
        platform_results["run_duration_sec"] = round(time.perf_counter() - started_perf, 3)

    return platform_results


def _load_compatible_existing_results(
    results_path: Path,
    nodes_hash: str,
    edges_hash: str,
    benchmark_configuration: dict[str, Any],
) -> dict[str, Any]:
    if not results_path.exists():
        return {}
    try:
        with results_path.open(encoding="utf-8") as stream:
            existing = json.load(stream)
    except (OSError, json.JSONDecodeError):
        return {}

    compatible: dict[str, Any] = {}
    for name, result in existing.items():
        if name == "_metadata" or not isinstance(result, dict):
            continue
        provenance = result.get("dataset_provenance", {})
        if (
            provenance.get("nodes_csv_sha256") == nodes_hash
            and provenance.get("edges_csv_sha256") == edges_hash
            and result.get("benchmark_configuration") == benchmark_configuration
        ):
            compatible[name] = result
        else:
            print(f"[Setup] Discarding incompatible stale result for {name}.")
    return compatible


def _measure_tcp_rtt(host: str, port: int) -> float | str:
    import socket

    try:
        t0 = time.perf_counter()
        with socket.create_connection((host, port), timeout=4.0):
            return round((time.perf_counter() - t0) * 1000, 2)
    except Exception:
        return "not_reachable"


def _client_environment() -> dict[str, Any]:
    from urllib.parse import urlparse
    cogno_host = urlparse(COGNODB_URI).hostname or ""
    return {
        "os": platform.platform(),
        "python_version": sys.version.split()[0],
        "processor": platform.processor() or "not_reported_by_os",
        "logical_cpu_count": os.cpu_count(),
        "physical_memory_bytes": psutil.virtual_memory().total,
        "client_region": os.getenv("BENCHMARK_CLIENT_REGION", "not_declared"),
        "cognodb_region": os.getenv("COGNODB_REGION", "not_declared"),
        "measured_cognodb_tcp_rtt_ms": _measure_tcp_rtt(cogno_host, 7687) if cogno_host else "N/A",
        "measured_loopback_tcp_rtt_ms": _measure_tcp_rtt("127.0.0.1", 7687),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Wexa AI Graph Database Cloud Benchmarking Suite")
    parser.add_argument("--all", action="store_true", help="Run all five databases")
    parser.add_argument(
        "--target", choices=("all", *TARGETS), default="all", help="Database to benchmark"
    )
    parser.add_argument("--quick", action="store_true", help="Run a short smoke benchmark")
    parser.add_argument(
        "--fresh", action="store_true", help="Do not merge compatible prior target records"
    )
    parser.add_argument(
        "--mixed-duration-seconds",
        type=float,
        default=MIXED_WORKLOAD_DURATION_SECONDS,
        help="Steady-state duration at each 1/10/40-client level in full mode",
    )
    parser.add_argument(
        "--generate-charts-only",
        action="store_true",
        help="Regenerate charts from the existing result JSON",
    )
    args = parser.parse_args()
    if args.all:
        args.target = "all"
    if args.mixed_duration_seconds <= 0:
        parser.error("--mixed-duration-seconds must be positive")

    results_json_path = RESULTS_DIR / "benchmark_results.json"
    results_md_path = RESULTS_DIR / "RESULTS_MATRIX.md"

    if args.generate_charts_only:
        if not results_json_path.exists():
            print("[-] No results file found to generate charts.")
            return
        with results_json_path.open(encoding="utf-8") as stream:
            existing_results = json.load(stream)
        generate_publication_charts(existing_results, CHARTS_DIR)
        print("[+] Charts generated successfully.")
        return

    targets_to_run = list(TARGETS) if args.target == "all" else [args.target]
    try:
        validate_target_configuration(targets_to_run)
    except RuntimeError as exc:
        parser.error(str(exc))

    regenerate_dataset = not (
        NODES_CSV.exists() and EDGES_CSV.exists() and DATASET_STATS_FILE.exists()
    )
    stats: dict[str, Any] = {}
    if not regenerate_dataset:
        with DATASET_STATS_FILE.open(encoding="utf-8") as stream:
            stats = json.load(stream)
        regenerate_dataset = stats.get("dataset_generator_version") != 3
    if regenerate_dataset:
        print("[Setup] Generating the standardized, provenance-labelled dataset...")
        stats = parse_or_generate_graph()

    sample_nodes = [int(value) for value in stats["traversal_sample_nodes"]]
    if len(sample_nodes) < BENCHMARK_ITERATIONS:
        raise RuntimeError(
            f"Dataset supplies only {len(sample_nodes)} traversal starts; "
            f"at least {BENCHMARK_ITERATIONS} are required"
        )
    categories = [str(value) for value in stats["categories"]]
    semantic_node = int(stats["semantic_validation_node"])
    semantic_expectations = build_reference_expectations(NODES_CSV, EDGES_CSV, semantic_node)

    nodes_hash = compute_file_sha256(NODES_CSV)
    edges_hash = compute_file_sha256(EDGES_CSV)
    requirements_path = Path(__file__).resolve().parent / "requirements.txt"
    dataset_provenance = {
        "dataset_name": stats.get("dataset_name"),
        "source_type": stats.get("source_type"),
        "source_url": stats.get("source_url"),
        "source_archive_sha256": stats.get("source_archive_sha256"),
        "nodes_csv_sha256": nodes_hash,
        "edges_csv_sha256": edges_hash,
        "total_nodes": stats.get("total_nodes"),
        "total_relationships": stats.get("total_relationships"),
        "edge_sampling": stats.get("edge_sampling"),
        "graph_semantics": stats.get("graph_semantics"),
        "traversal_sample_strategy": stats.get("traversal_sample_strategy"),
    }
    trace_definition = {
        "sample_nodes": sample_nodes,
        "categories": categories,
        "semantic_expectations": semantic_expectations,
        "lookup_seed": 42,
        "mixed_seed_base": 42,
        "mixed_read_ratio": 0.8,
    }
    dataset_provenance["query_trace_sha256"] = compute_json_sha256(trace_definition)

    run_id = uuid.uuid4().hex
    base_dir = Path(__file__).resolve().parent
    harness_hash = compute_harness_sha256(base_dir)
    benchmark_configuration = {
        "run_mode": "quick" if args.quick else "full",
        "iterations_per_read_metric": 15 if args.quick else BENCHMARK_ITERATIONS,
        "warmups": {
            "traversal_per_depth": WARMUP_ITERATIONS,
            "point_lookup": LOOKUP_WARMUP_ITERATIONS,
            "filtered_lookup": LOOKUP_WARMUP_ITERATIONS,
            "aggregation": AGGREGATION_WARMUP_ITERATIONS,
        },
        "requested_outer_batch_size": BATCH_SIZE,
        "concurrency_levels": [1, 5] if args.quick else list(CONCURRENCY_LEVELS),
        "mixed_measurement": {
            "mode": "fixed_operations" if args.quick else "duration",
            "ops_per_worker": 15 if args.quick else None,
            "duration_seconds_per_level": None if args.quick else args.mixed_duration_seconds,
            "intended_read_write_mix": "80% / 20%",
        },
        "benchmark_harness_sha256": harness_hash,
    }
    all_results: dict[str, Any]
    if args.fresh or args.target == "all":
        all_results = {}
    else:
        all_results = _load_compatible_existing_results(
            results_json_path, nodes_hash, edges_hash, benchmark_configuration
        )

    invocation_started_at = utc_now()
    all_results["_metadata"] = {
        "schema_version": 2,
        "last_invocation_run_id": run_id,
        "invocation_started_at": invocation_started_at,
        "command": [Path(sys.argv[0]).name, *sys.argv[1:]],
        "targets": targets_to_run,
        "run_mode": benchmark_configuration["run_mode"],
        "benchmark_iterations_per_read_metric": benchmark_configuration[
            "iterations_per_read_metric"
        ],
        "warmups": benchmark_configuration["warmups"],
        "requested_outer_batch_size": benchmark_configuration["requested_outer_batch_size"],
        "concurrency_levels": benchmark_configuration["concurrency_levels"],
        "mixed_measurement": benchmark_configuration["mixed_measurement"],
        "benchmark_harness_sha256": harness_hash,
        "dataset": dataset_provenance,
        "semantic_reference_sha256": compute_json_sha256(semantic_expectations),
        "client_environment": _client_environment(),
        "cognodb_nonstandard_host_override": os.getenv("COGNODB_ALLOW_NONSTANDARD_HOST") == "1",
        "git_commit": get_git_commit(),
        "requirements_sha256": compute_file_sha256(requirements_path),
    }
    atomic_save_json(all_results, results_json_path)

    executed_names: list[str] = []
    for target in targets_to_run:
        adapter = get_adapter(target)
        result = run_single_benchmark(
            adapter,
            sample_nodes,
            categories,
            semantic_expectations,
            run_id,
            dataset_provenance,
            benchmark_configuration,
            quick_mode=args.quick,
            mixed_duration_sec=args.mixed_duration_seconds,
        )
        all_results[adapter.name] = result
        executed_names.append(adapter.name)
        all_results["_metadata"]["last_checkpoint_at"] = utc_now()
        atomic_save_json(all_results, results_json_path)
        print(f"[Checkpoint] Saved {adapter.name} result to {results_json_path}.")

    all_results["_metadata"]["invocation_finished_at"] = utc_now()
    atomic_save_json(all_results, results_json_path)

    reporter = BenchmarkReporter(all_results)
    reporter.save_markdown(results_md_path)
    reporter.print_summary()

    try:
        generate_publication_charts(all_results, CHARTS_DIR)
        print(f"[+] Publication charts generated in: {CHARTS_DIR}")
    except Exception as exc:
        print(f"[!] Chart generation error: {type(exc).__name__}: {exc}")
        raise RuntimeError(f"Chart generation failed: {exc}") from exc

    failures = [name for name in executed_names if all_results[name].get("status") != "complete"]
    if failures:
        print(f"\n[!] Failed targets: {', '.join(failures)}")
        raise SystemExit(1)
    print(
        f"\n[OK] All {len(executed_names)} requested targets completed with strict, "
        "schema-validated metrics."
    )


if __name__ == "__main__":
    main()
