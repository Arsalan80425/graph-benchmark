import re
import time
from collections.abc import Callable
from typing import Any

import numpy as np

URI_PATTERN = re.compile(r"(?:bolt(?:\+s|\+ssc)?|neo4j(?:\+s|\+ssc)?|https?)://[^\s,;]+")


class LatencyCollector:
    """
    Collects and calculates high-precision latency statistics and percentiles.
    """

    def __init__(self, name: str):
        self.name = name
        self.latencies_ms: list[float] = []
        self.errors: int = 0
        self.error_details: list[str] = []
        self.start_time: float = 0
        self.end_time: float = 0

    def record(self, latency_ms: float):
        self.latencies_ms.append(latency_ms)

    def record_error(self, error: BaseException | str | None = None):
        self.errors += 1
        if error is not None and len(self.error_details) < 20:
            if isinstance(error, BaseException):
                detail = f"{type(error).__name__}: {error}"
            else:
                detail = str(error)
            self.error_details.append(URI_PATTERN.sub("[REDACTED_URI]", detail)[:500])

    def compute_statistics(self) -> dict[str, Any]:
        duration_sec = (
            (self.end_time - self.start_time) if (self.end_time > self.start_time) else 0.0
        )
        attempted = len(self.latencies_ms) + self.errors

        if not self.latencies_ms:
            return {
                "name": self.name,
                "count": 0,
                "errors": self.errors,
                "attempted": attempted,
                "error_details": self.error_details,
                "p50_ms": None,
                "p90_ms": None,
                "p95_ms": None,
                "p99_ms": None,
                "mean_ms": None,
                "min_ms": None,
                "max_ms": None,
                "std_ms": None,
                "qps": 0.0,
                "attempt_qps": round(attempted / duration_sec, 2) if duration_sec > 0 else 0.0,
                "duration_sec": round(duration_sec, 4),
                "samples_ms": [],
            }

        arr = np.array(self.latencies_ms)
        if duration_sec <= 0:
            duration_sec = sum(self.latencies_ms) / 1000.0
        qps = (len(arr) / duration_sec) if duration_sec > 0 else 0.0

        return {
            "name": self.name,
            "count": len(arr),
            "errors": self.errors,
            "attempted": attempted,
            "error_details": self.error_details,
            "p50_ms": round(float(np.percentile(arr, 50)), 2),
            "p90_ms": round(float(np.percentile(arr, 90)), 2),
            "p95_ms": round(float(np.percentile(arr, 95)), 2),
            "p99_ms": round(float(np.percentile(arr, 99)), 2),
            "mean_ms": round(float(np.mean(arr)), 2),
            "min_ms": round(float(np.min(arr)), 2),
            "max_ms": round(float(np.max(arr)), 2),
            "std_ms": round(float(np.std(arr)), 2),
            "qps": round(qps, 2),
            "attempt_qps": round(attempted / duration_sec, 2) if duration_sec > 0 else 0.0,
            "duration_sec": round(duration_sec, 4),
            "samples_ms": [round(float(value), 4) for value in self.latencies_ms],
        }


def measure_execution_time(func: Callable, *args, **kwargs) -> tuple:
    """
    Measures wall-clock time of a function execution in milliseconds.
    """
    t0 = time.perf_counter()
    res = func(*args, **kwargs)
    t1 = time.perf_counter()
    return res, (t1 - t0) * 1000.0
