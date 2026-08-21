import time
from typing import Any

from ..adapters.base import BaseGraphAdapter
from ..metrics.collector import LatencyCollector


def run_aggregation_benchmark(
    adapter: BaseGraphAdapter, warmup_iters: int = 3, benchmark_iters: int = 50
) -> dict[str, Any]:
    """
    Measures Group-By / Count aggregation query performance with cache warm-up.
    """
    print(f"[{adapter.name}] --- Starting Aggregation Benchmark ---")

    # Warm-up phase
    print(f"[{adapter.name}] Warming up aggregation cache with {warmup_iters} queries...")
    for _ in range(warmup_iters):
        try:
            adapter.aggregation_category_counts()
        except Exception as exc:
            raise RuntimeError(f"[{adapter.name}] Aggregation warm-up failed: {exc}") from exc

    collector = LatencyCollector(f"{adapter.name}_aggregation")
    collector.start_time = time.perf_counter()

    for _ in range(benchmark_iters):
        t0 = time.perf_counter()
        try:
            adapter.aggregation_category_counts()
            collector.record((time.perf_counter() - t0) * 1000.0)
        except Exception as exc:
            collector.record_error(exc)

    collector.end_time = time.perf_counter()
    stats = collector.compute_statistics()
    print(f"[{adapter.name}] Aggregation: p50={stats['p50_ms']}ms, p95={stats['p95_ms']}ms")
    return {"aggregation": stats}
