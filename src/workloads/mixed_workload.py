import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from ..adapters.base import BaseGraphAdapter
from ..metrics.collector import LatencyCollector


def run_single_client_worker(
    adapter: BaseGraphAdapter,
    sample_nodes: list[int],
    operations_count: int | None = None,
    worker_id: int = 0,
    read_ratio: float = 0.8,
    barrier: threading.Barrier | None = None,
    start_event: threading.Event | None = None,
    duration_sec: float | None = None,
) -> dict[str, Any]:
    """Execute a deterministic mixed trace after every client is ready.

    Full runs use a duration-based steady-state window. Tests and quick runs may
    use a fixed operation count. The 80/20 mix is deterministic rather than a
    small probabilistic sample.
    """
    if not sample_nodes:
        raise ValueError("sample_nodes must not be empty")
    if duration_sec is None and (operations_count is None or operations_count <= 0):
        raise ValueError("operations_count must be positive when duration_sec is not set")
    if duration_sec is not None and duration_sec <= 0:
        raise ValueError("duration_sec must be positive")
    if not 0.0 < read_ratio < 1.0:
        raise ValueError("read_ratio must be between 0 and 1")

    rng = random.Random(42 + worker_id * 1000)
    latencies: list[float] = []
    error_details: list[str] = []
    read_attempts = 0
    write_attempts = 0
    successful_reads = 0
    successful_writes = 0

    if barrier is not None:
        barrier.wait(timeout=30.0)
    if start_event is not None and not start_event.wait(timeout=30.0):
        raise TimeoutError("Timed out waiting for the mixed-workload start signal")

    deadline = time.perf_counter() + duration_sec if duration_sec is not None else None
    operation_index = 0
    write_every = max(2, round(1.0 / (1.0 - read_ratio)))

    while True:
        if deadline is not None:
            if time.perf_counter() >= deadline:
                break
        elif operation_index >= int(operations_count or 0):
            break

        read_id = rng.choice(sample_nodes)
        write_id = rng.choice(sample_nodes)
        new_score = round(rng.uniform(1.0, 100.0), 2)
        is_read = (operation_index + worker_id) % write_every != 0
        operation_index += 1

        if is_read:
            read_attempts += 1
        else:
            write_attempts += 1

        started = time.perf_counter()
        try:
            if is_read:
                adapter.traversal_1hop(read_id)
                successful_reads += 1
            else:
                succeeded = adapter.mixed_read_write(read_id, write_id, new_score)
                if succeeded is not True:
                    raise RuntimeError("mixed_read_write returned a non-success value")
                successful_writes += 1
            latencies.append((time.perf_counter() - started) * 1000.0)
        except Exception as exc:
            if len(error_details) < 20:
                operation = "read" if is_read else "write"
                error_details.append(
                    f"worker={worker_id}, operation={operation}: {type(exc).__name__}: {exc}"[:500]
                )

    return {
        "latencies_ms": latencies,
        "error_details": error_details,
        "read_attempts": read_attempts,
        "write_attempts": write_attempts,
        "successful_reads": successful_reads,
        "successful_writes": successful_writes,
    }


def run_mixed_workload_benchmark(
    adapter: BaseGraphAdapter,
    sample_nodes: list[int],
    concurrency_levels: list[int] | None = None,
    ops_per_worker: int | None = 50,
    duration_sec: float | None = None,
) -> dict[str, Any]:
    """Measure synchronized concurrent read/write throughput.

    Set ``duration_sec`` for a steady-state window. When it is omitted,
    ``ops_per_worker`` provides a fast deterministic test mode.
    """
    if concurrency_levels is None:
        concurrency_levels = [1, 10, 40]
    if not concurrency_levels or any(level <= 0 for level in concurrency_levels):
        raise ValueError("concurrency_levels must contain positive integers")

    print(
        f"[{adapter.name}] --- Starting Mixed Workload Concurrency Sweeps "
        f"({', '.join(map(str, concurrency_levels))} clients) ---"
    )
    results: dict[str, Any] = {}

    for concurrency in concurrency_levels:
        collector = LatencyCollector(f"{adapter.name}_concurrency_{concurrency}")
        # The main thread participates so timing starts only after every worker is ready.
        barrier = threading.Barrier(concurrency + 1)
        start_event = threading.Event()
        totals = {
            "read_attempts": 0,
            "write_attempts": 0,
            "successful_reads": 0,
            "successful_writes": 0,
        }

        mode_description = (
            f"{duration_sec:.1f}s steady-state window"
            if duration_sec is not None
            else f"{ops_per_worker} ops/client"
        )
        print(
            f"[{adapter.name}] Running synchronized mixed workload with "
            f"{concurrency} concurrent clients ({mode_description})..."
        )

        # Warmup ramp to prime connection pools, sockets, and thread caches
        if duration_sec is not None and duration_sec > 5.0:
            try:
                with ThreadPoolExecutor(max_workers=concurrency) as warmup_exec:
                    warm_futures = [
                        warmup_exec.submit(
                            run_single_client_worker,
                            adapter,
                            sample_nodes,
                            operations_count=2,
                            worker_id=w_id,
                            read_ratio=0.8,
                            barrier=None,
                            start_event=None,
                            duration_sec=None,
                        )
                        for w_id in range(concurrency)
                    ]
                    for wf in as_completed(warm_futures):
                        try:
                            wf.result()
                        except Exception:
                            pass
            except Exception:
                pass

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(
                    run_single_client_worker,
                    adapter,
                    sample_nodes,
                    ops_per_worker,
                    worker_id,
                    0.8,
                    barrier,
                    start_event,
                    duration_sec,
                )
                for worker_id in range(concurrency)
            ]
            barrier.wait(timeout=30.0)
            collector.start_time = time.perf_counter()
            start_event.set()

            for future in as_completed(futures):
                try:
                    worker_result = future.result()
                except Exception as exc:
                    collector.record_error(exc)
                    continue

                for latency in worker_result["latencies_ms"]:
                    collector.record(latency)
                for detail in worker_result["error_details"]:
                    collector.record_error(detail)
                for key in totals:
                    totals[key] += int(worker_result[key])

        collector.end_time = time.perf_counter()
        stats = collector.compute_statistics()
        attempted = totals["read_attempts"] + totals["write_attempts"]
        read_pct = totals["read_attempts"] / attempted * 100 if attempted else 0.0
        write_pct = totals["write_attempts"] / attempted * 100 if attempted else 0.0

        stats.update(totals)
        stats["attempted_operations"] = attempted
        stats["empirical_mix"] = f"{read_pct:.1f}% Reads / {write_pct:.1f}% Writes"
        stats["error_count"] = stats["errors"]
        stats["measurement_mode"] = "duration" if duration_sec is not None else "fixed_operations"
        stats["requested_duration_sec"] = duration_sec
        stats["ops_per_worker"] = None if duration_sec is not None else ops_per_worker

        results[f"concurrency_{concurrency}"] = stats
        print(
            f"[{adapter.name}] Concurrency {concurrency:2d} clients: "
            f"Throughput={stats['qps']:,.1f} QPS "
            f"(p50={stats['p50_ms']}ms, p95={stats['p95_ms']}ms, "
            f"errors={stats['errors']}, mix={stats['empirical_mix']})"
        )

    return {"mixed_workload": results}
