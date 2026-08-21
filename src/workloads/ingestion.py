import time
from pathlib import Path
from typing import Any

import pandas as pd

from ..adapters.base import BaseGraphAdapter


def run_ingestion_benchmark(
    adapter: BaseGraphAdapter, nodes_csv: Path, edges_csv: Path, batch_size: int = 1000
) -> dict[str, Any]:
    """
    Measures data loading performance:
    - Node ingestion throughput (nodes/sec)
    - Edge ingestion throughput (relationships/sec)
    - Total wall-clock time
    """
    print(f"[{adapter.name}] --- Starting Ingestion Benchmark ---")
    adapter.clear_database()

    # Assert database is 100% empty before ingestion begins
    pre_nodes = adapter.count_nodes()
    pre_edges = adapter.count_edges()
    if pre_nodes != 0 or pre_edges != 0:
        adapter.clear_database()
        pre_nodes = adapter.count_nodes()
        pre_edges = adapter.count_edges()
        if pre_nodes != 0 or pre_edges != 0:
            raise RuntimeError(
                f"[{adapter.name}] Database not clean prior to benchmark: found {pre_nodes} nodes, {pre_edges} edges"
            )
    print(f"[{adapter.name}] [OK] Database verified clean (0 nodes, 0 edges)")

    adapter.create_indexes()

    # Load CSVs
    df_nodes = pd.read_csv(nodes_csv)
    df_edges = pd.read_csv(edges_csv)

    total_nodes = len(df_nodes)
    total_edges = len(df_edges)

    # 1. Ingest Nodes
    print(f"[{adapter.name}] Ingesting {total_nodes:,} nodes in batches of {batch_size}...")
    nodes_records = df_nodes.to_dict(orient="records")

    t0_nodes = time.perf_counter()
    for i in range(0, total_nodes, batch_size):
        batch = nodes_records[i : i + batch_size]
        loaded = adapter.ingest_nodes_batch(batch)
        if loaded != len(batch):
            raise RuntimeError(
                f"[{adapter.name}] Node batch load failure: expected {len(batch)}, loaded {loaded}"
            )
    t1_nodes = time.perf_counter()

    nodes_duration_sec = max(t1_nodes - t0_nodes, 0.001)
    nodes_per_sec = total_nodes / nodes_duration_sec

    # Ensure indexes are active for edge resolution
    adapter.create_indexes()

    # 2. Ingest Edges
    print(f"[{adapter.name}] Ingesting {total_edges:,} relationships in batches of {batch_size}...")
    edges_records = df_edges.to_dict(orient="records")

    t0_edges = time.perf_counter()
    for i in range(0, total_edges, batch_size):
        batch = edges_records[i : i + batch_size]
        loaded = adapter.ingest_edges_batch(batch)
        if loaded != len(batch):
            raise RuntimeError(
                f"[{adapter.name}] Edge batch load failure: expected {len(batch)}, loaded {loaded}"
            )
    t1_edges = time.perf_counter()

    edges_duration_sec = max(t1_edges - t0_edges, 0.001)
    rels_per_sec = total_edges / edges_duration_sec

    # Includes the index-readiness step between node and relationship loading.
    total_wall_clock = max(t1_edges - t0_nodes, 0.001)

    # 3. Strict Post-load Verification
    verified_nodes = adapter.count_nodes()
    verified_edges = adapter.count_edges()
    if verified_nodes != total_nodes or verified_edges != total_edges:
        raise RuntimeError(
            f"[{adapter.name}] Strict count verification failed: "
            f"Expected ({total_nodes} nodes, {total_edges} edges), Found ({verified_nodes} nodes, {verified_edges} edges)"
        )
    print(
        f"[{adapter.name}] [OK] Ingestion verified: {verified_nodes:,} nodes, "
        f"{verified_edges:,} edges in database"
    )

    print(
        f"[{adapter.name}] Ingestion Complete: {nodes_per_sec:,.1f} nodes/sec, {rels_per_sec:,.1f} rels/sec. Total time: {total_wall_clock:.2f}s"
    )

    return {
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "verified_nodes": verified_nodes,
        "verified_edges": verified_edges,
        "nodes_duration_sec": round(nodes_duration_sec, 2),
        "edges_duration_sec": round(edges_duration_sec, 2),
        "total_wall_clock_sec": round(total_wall_clock, 2),
        "nodes_per_sec": round(nodes_per_sec, 1),
        "rels_per_sec": round(rels_per_sec, 1),
        "method": adapter.describe_ingestion(batch_size)
        if hasattr(adapter, "describe_ingestion")
        else f"Driver parameter batches (requested outer batch size {batch_size})",
    }
