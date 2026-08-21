import random
import time
from typing import Any

from ..adapters.base import BaseGraphAdapter
from ..metrics.collector import LatencyCollector


def run_lookup_benchmark(
    adapter: BaseGraphAdapter,
    sample_nodes: list[int],
    categories: list[str],
    warmup_iters: int = 5,
    benchmark_iters: int = 100,
) -> dict[str, Any]:
    """
    Measures Point Lookup (by ID) and Indexed Filtered Lookup (category + year) latencies.
    Includes warm-up and deterministic seeded query parameters.
    """
    print(f"[{adapter.name}] --- Starting Lookup Benchmarks (Point & Filtered) ---")

    # Generate deterministic query parameters (seed 42)
    rng = random.Random(42)
    filtered_queries = [
        (rng.choice(categories), rng.randint(2012, 2020)) for _ in range(benchmark_iters)
    ]

    # Warm-up phase
    print(f"[{adapter.name}] Warming up lookup cache with {warmup_iters} queries...")
    for i in range(min(warmup_iters, len(sample_nodes))):
        try:
            adapter.point_lookup(sample_nodes[i])
        except Exception as exc:
            raise RuntimeError(
                f"[{adapter.name}] Point-lookup warm-up failed for node {sample_nodes[i]}: {exc}"
            ) from exc
    for i in range(min(warmup_iters, len(filtered_queries))):
        try:
            cat, yr = filtered_queries[i]
            adapter.filtered_lookup(cat, yr)
        except Exception as exc:
            raise RuntimeError(
                f"[{adapter.name}] Filtered-lookup warm-up failed for category={cat}, year={yr}: {exc}"
            ) from exc

    test_nodes = sample_nodes[:benchmark_iters]
    if len(test_nodes) < benchmark_iters:
        test_nodes = (sample_nodes * ((benchmark_iters // len(sample_nodes)) + 1))[:benchmark_iters]

    results = {}

    # 1. Point Lookup
    c_point = LatencyCollector(f"{adapter.name}_point_lookup")
    c_point.start_time = time.perf_counter()
    for nid in test_nodes:
        t0 = time.perf_counter()
        try:
            adapter.point_lookup(nid)
            c_point.record((time.perf_counter() - t0) * 1000.0)
        except Exception as exc:
            c_point.record_error(f"node={nid}: {type(exc).__name__}: {exc}")
    c_point.end_time = time.perf_counter()
    results["point_lookup"] = c_point.compute_statistics()
    print(
        f"[{adapter.name}] Point Lookup: p50={results['point_lookup']['p50_ms']}ms, p95={results['point_lookup']['p95_ms']}ms"
    )

    # 2. Filtered Lookup (Category + Min Year)
    c_filter = LatencyCollector(f"{adapter.name}_filtered_lookup")
    c_filter.start_time = time.perf_counter()
    for cat, min_year in filtered_queries:
        t0 = time.perf_counter()
        try:
            adapter.filtered_lookup(cat, min_year)
            c_filter.record((time.perf_counter() - t0) * 1000.0)
        except Exception as exc:
            c_filter.record_error(
                f"category={cat}, min_year={min_year}: {type(exc).__name__}: {exc}"
            )
    c_filter.end_time = time.perf_counter()
    results["filtered_lookup"] = c_filter.compute_statistics()
    print(
        f"[{adapter.name}] Filtered Lookup: p50={results['filtered_lookup']['p50_ms']}ms, p95={results['filtered_lookup']['p95_ms']}ms"
    )

    return results
