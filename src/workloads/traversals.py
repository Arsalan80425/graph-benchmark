import time
from typing import Any

from ..adapters.base import BaseGraphAdapter
from ..metrics.collector import LatencyCollector


def run_traversal_benchmark(
    adapter: BaseGraphAdapter,
    sample_nodes: list[int],
    warmup_iters: int = 10,
    benchmark_iters: int = 100,
) -> dict[str, Any]:
    """
    Measures 1-hop, 2-hop, and 3-hop traversal latencies across sampled nodes.
    Includes explicit warm-up and percentile calculations (p50, p90, p95, p99).
    """
    print(f"[{adapter.name}] --- Starting Traversal Benchmarks (1-hop, 2-hop, 3-hop) ---")

    # 1. Warm-up Phase
    print(f"[{adapter.name}] Warming up cache with {warmup_iters} queries (1-hop, 2-hop, 3-hop)...")
    for i in range(min(warmup_iters, len(sample_nodes))):
        try:
            adapter.traversal_1hop(sample_nodes[i])
            adapter.traversal_2hop(sample_nodes[i])
            adapter.traversal_3hop(sample_nodes[i])
        except Exception as exc:
            raise RuntimeError(
                f"[{adapter.name}] Traversal warm-up failed for node {sample_nodes[i]}: {exc}"
            ) from exc

    test_nodes = sample_nodes[:benchmark_iters]
    if len(test_nodes) < benchmark_iters:
        test_nodes = (sample_nodes * ((benchmark_iters // len(sample_nodes)) + 1))[:benchmark_iters]

    results = {}

    # 1-Hop Traversal
    c1 = LatencyCollector(f"{adapter.name}_1hop")
    c1.start_time = time.perf_counter()
    for nid in test_nodes:
        t0 = time.perf_counter()
        try:
            adapter.traversal_1hop(nid)
            c1.record((time.perf_counter() - t0) * 1000.0)
        except Exception as exc:
            c1.record_error(f"node={nid}: {type(exc).__name__}: {exc}")
    c1.end_time = time.perf_counter()
    results["traversal_1hop"] = c1.compute_statistics()
    print(
        f"[{adapter.name}] 1-Hop: p50={results['traversal_1hop']['p50_ms']}ms, p95={results['traversal_1hop']['p95_ms']}ms"
    )

    # 2-Hop Traversal
    c2 = LatencyCollector(f"{adapter.name}_2hop")
    c2.start_time = time.perf_counter()
    for nid in test_nodes:
        t0 = time.perf_counter()
        try:
            adapter.traversal_2hop(nid)
            c2.record((time.perf_counter() - t0) * 1000.0)
        except Exception as exc:
            c2.record_error(f"node={nid}: {type(exc).__name__}: {exc}")
    c2.end_time = time.perf_counter()
    results["traversal_2hop"] = c2.compute_statistics()
    print(
        f"[{adapter.name}] 2-Hop: p50={results['traversal_2hop']['p50_ms']}ms, p95={results['traversal_2hop']['p95_ms']}ms"
    )

    # 3-Hop Traversal
    c3 = LatencyCollector(f"{adapter.name}_3hop")
    c3.start_time = time.perf_counter()
    for nid in test_nodes:
        t0 = time.perf_counter()
        try:
            adapter.traversal_3hop(nid)
            c3.record((time.perf_counter() - t0) * 1000.0)
        except Exception as exc:
            c3.record_error(f"node={nid}: {type(exc).__name__}: {exc}")
    c3.end_time = time.perf_counter()
    results["traversal_3hop"] = c3.compute_statistics()
    print(
        f"[{adapter.name}] 3-Hop: p50={results['traversal_3hop']['p50_ms']}ms, p95={results['traversal_3hop']['p95_ms']}ms"
    )

    return results
