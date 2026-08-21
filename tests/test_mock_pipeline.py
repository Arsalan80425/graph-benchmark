from typing import Any

import pandas as pd
import pytest

from run_benchmark import run_single_benchmark, validate_platform_completeness
from src.adapters.base import BaseGraphAdapter
from src.metrics.reporter import BenchmarkReporter
from src.visualizer.generate_charts import generate_publication_charts
from src.workloads.aggregations import run_aggregation_benchmark
from src.workloads.lookups import run_lookup_benchmark
from src.workloads.mixed_workload import run_mixed_workload_benchmark
from src.workloads.traversals import run_traversal_benchmark
from src.workloads.verification import build_reference_expectations, validate_query_semantics


class MockGraphAdapter(BaseGraphAdapter):
    """In-memory mock graph adapter for testing benchmark pipelines end-to-end."""

    def __init__(self, name: str = "MockGraph Engine"):
        super().__init__(name=name, hardware_spec="0.5 vCPU, 256MB RAM")
        self.nodes = {}
        self.edges = []
        self.connected = True

    def connect(self) -> bool:
        return self.connected

    def close(self):
        pass

    def clear_database(self):
        self.nodes.clear()
        self.edges.clear()

    def create_indexes(self):
        pass

    def count_nodes(self) -> int:
        return len(self.nodes)

    def count_edges(self) -> int:
        return len(self.edges)

    def ingest_nodes_batch(self, batch: list[dict[str, Any]]) -> int:
        for item in batch:
            self.nodes[item["id"]] = item
        return len(batch)

    def ingest_edges_batch(self, batch: list[dict[str, Any]]) -> int:
        self.edges.extend(batch)
        return len(batch)

    def traversal_1hop(self, node_id: int) -> int:
        return self._trail_count(node_id, 1)

    def traversal_2hop(self, node_id: int) -> int:
        return self._trail_count(node_id, 2)

    def traversal_3hop(self, node_id: int) -> int:
        return self._trail_count(node_id, 3)

    def _trail_count(self, node_id: int, depth: int) -> int:
        adjacency: dict[int, list[tuple[int, int]]] = {}
        for edge_id, edge in enumerate(self.edges):
            src = int(edge["src"])
            dst = int(edge["dst"])
            adjacency.setdefault(src, []).append((dst, edge_id))
            adjacency.setdefault(dst, []).append((src, edge_id))

        def visit(current: int, remaining: int, used: set[int]) -> int:
            if remaining == 0:
                return 1
            count = 0
            for neighbor, edge_id in adjacency.get(current, []):
                if edge_id in used:
                    continue
                used.add(edge_id)
                count += visit(neighbor, remaining - 1, used)
                used.remove(edge_id)
            return count

        return visit(node_id, depth, set())

    def point_lookup(self, node_id: int) -> dict[str, Any] | None:
        return self.nodes.get(node_id)

    def filtered_lookup(self, category: str, min_year: int) -> int:
        return sum(
            1
            for n in self.nodes.values()
            if n.get("category") == category and n.get("year", 0) >= min_year
        )

    def aggregation_category_counts(self) -> dict[str, int]:
        counts = {}
        for n in self.nodes.values():
            cat = n.get("category", "Unknown")
            counts[cat] = counts.get(cat, 0) + 1
        return counts

    def mixed_read_write(self, read_node_id: int, write_node_id: int, new_score: float) -> bool:
        _ = self.traversal_1hop(read_node_id)
        if write_node_id in self.nodes:
            self.nodes[write_node_id]["score"] = new_score
        return True

    def get_resource_footprint(self) -> str:
        return "Observed Mock Memory: 12.5 MB"


def test_cognodb_endpoint_guard(monkeypatch):
    import run_benchmark

    monkeypatch.setattr(run_benchmark, "COGNODB_URI", "bolt+s://fixture.databases.cognodb.cloud")
    monkeypatch.setattr(run_benchmark, "COGNODB_PASSWORD", "fixture-secret")
    run_benchmark.validate_target_configuration(["cognodb"])

    monkeypatch.setattr(run_benchmark, "COGNODB_URI", "bolt+s://fixture.example.invalid")
    monkeypatch.delenv("COGNODB_ALLOW_NONSTANDARD_HOST", raising=False)
    with pytest.raises(RuntimeError, match="does not match the assessment"):
        run_benchmark.validate_target_configuration(["cognodb"])


def test_mock_semantic_validation():
    adapter = MockGraphAdapter()
    adapter.nodes = {
        1: {"id": 1, "name": "Node_1", "category": "Physics", "year": 2018, "score": 4.5},
        2: {"id": 2, "name": "Node_2", "category": "Biology", "year": 2020, "score": 8.1},
    }
    adapter.edges = [{"src": 1, "dst": 2, "weight": 1.0, "type": "CITES"}]

    expectations = {
        "reference": "unit-test fixture",
        "graph_semantics": "Undirected trails; a relationship cannot repeat within one path",
        "trace": {
            "sample_node_id": 1,
            "filter_category": "Physics",
            "filter_min_year": 2015,
        },
        "expected": {
            "point_lookup": adapter.nodes[1],
            "traversal_1hop": 1,
            "traversal_2hop": 0,
            "traversal_3hop": 0,
            "filtered_lookup": 1,
            "aggregation": {"Biology": 1, "Physics": 1},
        },
    }
    result = validate_query_semantics(adapter, expectations)
    assert result["status"] == "passed"
    assert result["checks"]["traversal_1hop"]["actual"] == 1


def test_mock_workloads_and_reporter(tmp_path):
    adapter = MockGraphAdapter()
    for i in range(1, 101):
        adapter.nodes[i] = {
            "id": i,
            "name": f"Node_{i}",
            "category": "Physics" if i % 2 == 0 else "Biology",
            "year": 2010 + (i % 10),
            "score": float(i),
        }
        if i > 1:
            adapter.edges.append({"src": i - 1, "dst": i, "weight": 1.0, "type": "CITES"})

    sample_nodes = list(range(1, 20))
    categories = ["Physics", "Biology"]

    # 1. Traversals
    t_res = run_traversal_benchmark(adapter, sample_nodes, warmup_iters=2, benchmark_iters=10)
    assert "traversal_1hop" in t_res
    assert t_res["traversal_1hop"]["count"] == 10

    # 2. Lookups
    l_res = run_lookup_benchmark(
        adapter, sample_nodes, categories, warmup_iters=2, benchmark_iters=10
    )
    assert "point_lookup" in l_res
    assert "filtered_lookup" in l_res

    # 3. Aggregation
    a_res = run_aggregation_benchmark(adapter, warmup_iters=1, benchmark_iters=5)
    assert "aggregation" in a_res

    # 4. Mixed workload
    m_res = run_mixed_workload_benchmark(
        adapter, sample_nodes, concurrency_levels=[1, 2], ops_per_worker=10
    )
    assert "mixed_workload" in m_res

    # 5. Reporter
    mock_results = {
        "_metadata": {
            "run_mode": "quick",
            "benchmark_iterations_per_read_metric": 10,
            "concurrency_levels": [1, 2],
            "mixed_measurement": {"ops_per_worker": 10},
        },
        "Mock Engine": {
            "name": "Mock Engine",
            "hardware_spec": "0.5 vCPU, 256MB RAM",
            "type": "Resource-Capped Container",
            "resource_footprint": "Observed: 12.5 MB",
            "query_interface": "Neo4j Bolt / Cypher",
            "ingestion": {
                "total_nodes": 100,
                "total_edges": 99,
                "verified_nodes": 100,
                "verified_edges": 99,
                "nodes_duration_sec": 0.05,
                "edges_duration_sec": 0.05,
                "total_wall_clock_sec": 0.10,
                "nodes_per_sec": 2000.0,
                "rels_per_sec": 1980.0,
                "method": "Driver Batching",
            },
            **t_res,
            **l_res,
            **a_res,
            **m_res,
            "semantic_validation": {"status": "passed", "checks": {"fixture": "PASS"}},
            "status": "complete",
        },
    }

    reporter = BenchmarkReporter(mock_results)
    json_path = tmp_path / "results.json"
    md_path = tmp_path / "results.md"
    reporter.save_json(json_path)
    reporter.save_markdown(md_path)
    reporter.print_summary()

    assert json_path.exists()
    assert md_path.exists()

    # 6. Chart generation
    generate_publication_charts(mock_results, tmp_path / "charts")
    assert (tmp_path / "charts" / "ingestion_throughput.png").exists()
    assert (tmp_path / "charts" / "traversal_latency.png").exists()


def _write_tiny_dataset(tmp_path):
    nodes_path = tmp_path / "nodes.csv"
    edges_path = tmp_path / "edges.csv"
    nodes = [
        {
            "id": node_id,
            "name": f"Node_{node_id}",
            "category": "Physics" if node_id % 2 else "Biology",
            "year": 2014 + node_id,
            "score": float(node_id),
        }
        for node_id in range(1, 21)
    ]
    edges = [
        {"src": node_id, "dst": node_id + 1, "weight": 1.0, "type": "COLLABORATES"}
        for node_id in range(1, 20)
    ]
    pd.DataFrame(nodes).to_csv(nodes_path, index=False)
    pd.DataFrame(edges).to_csv(edges_path, index=False)
    return nodes_path, edges_path


def test_run_single_benchmark_quick_is_fail_closed(tmp_path, monkeypatch):
    import run_benchmark

    nodes_path, edges_path = _write_tiny_dataset(tmp_path)
    monkeypatch.setattr(run_benchmark, "NODES_CSV", nodes_path)
    monkeypatch.setattr(run_benchmark, "EDGES_CSV", edges_path)
    adapter = MockGraphAdapter()
    expectations = build_reference_expectations(nodes_path, edges_path, sample_node_id=10)

    result = run_single_benchmark(
        adapter,
        sample_nodes=list(range(1, 21)),
        categories=["Physics", "Biology"],
        semantic_expectations=expectations,
        run_id="test-run",
        dataset_provenance={"nodes_csv_sha256": "fixture", "edges_csv_sha256": "fixture"},
        quick_mode=True,
    )

    assert result["status"] == "complete"
    validate_platform_completeness(result, 15, [1, 5], 15, None)


def test_wrong_semantic_answer_prevents_completion(tmp_path, monkeypatch):
    import run_benchmark

    class WrongAnswerAdapter(MockGraphAdapter):
        def traversal_2hop(self, node_id: int) -> int:
            return super().traversal_2hop(node_id) + 1

    nodes_path, edges_path = _write_tiny_dataset(tmp_path)
    monkeypatch.setattr(run_benchmark, "NODES_CSV", nodes_path)
    monkeypatch.setattr(run_benchmark, "EDGES_CSV", edges_path)
    expectations = build_reference_expectations(nodes_path, edges_path, sample_node_id=10)

    result = run_single_benchmark(
        WrongAnswerAdapter(),
        sample_nodes=list(range(1, 21)),
        categories=["Physics", "Biology"],
        semantic_expectations=expectations,
        run_id="bad-semantic-run",
        dataset_provenance={},
        quick_mode=True,
    )

    assert result["status"] == "failed"
    assert result["error_type"] == "AssertionError"


def test_timed_query_error_prevents_completion(tmp_path, monkeypatch):
    import run_benchmark

    class TimedFailureAdapter(MockGraphAdapter):
        def __init__(self):
            super().__init__()
            self.one_hop_calls = 0

        def traversal_1hop(self, node_id: int) -> int:
            self.one_hop_calls += 1
            # One semantic call plus ten warm-ups precede the timed phase.
            if self.one_hop_calls == 12:
                raise RuntimeError("injected timed-query failure")
            return super().traversal_1hop(node_id)

    nodes_path, edges_path = _write_tiny_dataset(tmp_path)
    monkeypatch.setattr(run_benchmark, "NODES_CSV", nodes_path)
    monkeypatch.setattr(run_benchmark, "EDGES_CSV", edges_path)
    expectations = build_reference_expectations(nodes_path, edges_path, sample_node_id=10)

    result = run_single_benchmark(
        TimedFailureAdapter(),
        sample_nodes=list(range(1, 21)),
        categories=["Physics", "Biology"],
        semantic_expectations=expectations,
        run_id="timed-failure-run",
        dataset_provenance={},
        quick_mode=True,
    )

    assert result["status"] == "failed"
    assert "expected 15 successes and zero errors" in result["error"]
