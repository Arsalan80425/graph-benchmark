"""Dataset-derived semantic validation for every benchmark adapter."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from ..adapters.base import BaseGraphAdapter


def _count_undirected_trails(
    adjacency: dict[int, list[tuple[int, int]]], start: int, depth: int
) -> int:
    """Count fixed-length trails, matching Cypher/AQL relationship uniqueness."""

    def visit(node_id: int, remaining: int, used_edges: set[int]) -> int:
        if remaining == 0:
            return 1
        total = 0
        for neighbor, edge_id in adjacency.get(node_id, []):
            if edge_id in used_edges:
                continue
            used_edges.add(edge_id)
            total += visit(neighbor, remaining - 1, used_edges)
            used_edges.remove(edge_id)
        return total

    return visit(start, depth, set())


def build_reference_expectations(
    nodes_csv: Path,
    edges_csv: Path,
    sample_node_id: int,
    filter_category: str = "Physics",
    min_year: int = 2015,
) -> dict[str, Any]:
    """Build exact expected outputs from the immutable benchmark CSV files."""
    nodes = pd.read_csv(nodes_csv)
    edges = pd.read_csv(edges_csv)
    matching_node = nodes.loc[nodes["id"] == sample_node_id]
    if len(matching_node) != 1:
        raise ValueError(
            f"Semantic sample node {sample_node_id} must occur exactly once in {nodes_csv}"
        )

    node = matching_node.iloc[0]
    point_lookup = {
        "id": int(node["id"]),
        "name": str(node["name"]),
        "category": str(node["category"]),
        "year": int(node["year"]),
        "score": float(node["score"]),
    }

    adjacency: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for edge_id, edge in enumerate(edges.itertuples(index=False)):
        src = int(edge.src)
        dst = int(edge.dst)
        adjacency[src].append((dst, int(edge_id)))
        adjacency[dst].append((src, int(edge_id)))

    category_counts = Counter(str(category) for category in nodes["category"])
    filtered = nodes.loc[(nodes["category"] == filter_category) & (nodes["year"] >= min_year)]

    return {
        "reference": "Exact values derived from nodes.csv and edges.csv",
        "graph_semantics": "Undirected trails; a relationship cannot repeat within one path",
        "trace": {
            "sample_node_id": int(sample_node_id),
            "filter_category": filter_category,
            "filter_min_year": int(min_year),
        },
        "expected": {
            "point_lookup": point_lookup,
            "traversal_1hop": _count_undirected_trails(adjacency, sample_node_id, 1),
            "traversal_2hop": _count_undirected_trails(adjacency, sample_node_id, 2),
            "traversal_3hop": _count_undirected_trails(adjacency, sample_node_id, 3),
            "filtered_lookup": int(len(filtered)),
            "aggregation": dict(sorted(category_counts.items())),
        },
    }


def _assert_equal(label: str, actual: Any, expected: Any) -> None:
    if isinstance(expected, float):
        if not isinstance(actual, (int, float)) or not math.isclose(
            float(actual), expected, rel_tol=1e-9, abs_tol=1e-9
        ):
            raise AssertionError(f"{label}: expected {expected!r}, received {actual!r}")
    elif actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, received {actual!r}")


def validate_query_semantics(
    adapter: BaseGraphAdapter, expectations: dict[str, Any]
) -> dict[str, Any]:
    """Require exact logical equivalence before any timed query is recorded."""
    print(f"[{adapter.name}] --- Running exact semantic correctness validation ---")
    trace = expectations["trace"]
    expected = expectations["expected"]
    node_id = int(trace["sample_node_id"])

    actual_point = adapter.point_lookup(node_id)
    if not isinstance(actual_point, dict):
        raise AssertionError(
            f"point_lookup({node_id}): expected a property dictionary, received {actual_point!r}"
        )
    for key, expected_value in expected["point_lookup"].items():
        _assert_equal(f"point_lookup.{key}", actual_point.get(key), expected_value)

    actual_traversals = {
        "traversal_1hop": adapter.traversal_1hop(node_id),
        "traversal_2hop": adapter.traversal_2hop(node_id),
        "traversal_3hop": adapter.traversal_3hop(node_id),
    }
    for label, actual in actual_traversals.items():
        if not (isinstance(actual, int) and not isinstance(actual, bool)):
            raise AssertionError(
                f"{label}: expected integer result, received {type(actual).__name__}: {actual!r}"
            )
        _assert_equal(label, actual, expected[label])

    actual_filtered = adapter.filtered_lookup(
        str(trace["filter_category"]), int(trace["filter_min_year"])
    )
    if not (isinstance(actual_filtered, int) and not isinstance(actual_filtered, bool)):
        raise AssertionError(
            f"filtered_lookup: expected integer count, received {type(actual_filtered).__name__}: {actual_filtered!r}"
        )
    _assert_equal("filtered_lookup", actual_filtered, expected["filtered_lookup"])

    actual_aggregation = adapter.aggregation_category_counts()
    if not isinstance(actual_aggregation, dict):
        raise AssertionError(
            f"aggregation_category_counts: expected a dictionary, received {actual_aggregation!r}"
        )
    normalized_aggregation = {
        str(category): count for category, count in actual_aggregation.items()
    }
    for category, count in normalized_aggregation.items():
        if not (isinstance(count, int) and not isinstance(count, bool)):
            raise AssertionError(
                f"aggregation category {category}: expected integer count, received {type(count).__name__}: {count!r}"
            )
    _assert_equal("aggregation_category_counts", normalized_aggregation, expected["aggregation"])

    evidence = {
        "status": "passed",
        "reference": expectations["reference"],
        "graph_semantics": expectations["graph_semantics"],
        "trace": trace,
        "checks": {
            "point_lookup": {
                "expected": expected["point_lookup"],
                "actual": {key: actual_point.get(key) for key in expected["point_lookup"]},
            },
            **{
                label: {"expected": int(expected[label]), "actual": int(actual)}
                for label, actual in actual_traversals.items()
            },
            "filtered_lookup": {
                "expected": int(expected["filtered_lookup"]),
                "actual": int(actual_filtered),
            },
            "aggregation": {
                "expected": expected["aggregation"],
                "actual": normalized_aggregation,
            },
        },
    }
    print(f"[{adapter.name}] [OK] Exact semantic query validation passed.")
    return evidence
