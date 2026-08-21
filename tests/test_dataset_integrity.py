import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from data.download_dataset import SNAP_SHA256
from src.workloads.verification import build_reference_expectations

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"


def test_committed_dataset_is_canonical_and_nontrivial():
    nodes = pd.read_csv(DATA / "nodes.csv")
    edges = pd.read_csv(DATA / "edges.csv")
    stats = json.loads((DATA / "dataset_stats.json").read_text(encoding="utf-8"))

    assert stats["dataset_generator_version"] == 3
    assert stats["source_archive_sha256"] == SNAP_SHA256
    assert len(nodes) == stats["total_nodes"] == 17_441
    assert len(edges) == stats["total_relationships"] == 100_000
    assert (edges["src"] < edges["dst"]).all()
    assert not edges.duplicated(subset=["src", "dst"]).any()
    assert set(nodes["id"]) == set(edges["src"]) | set(edges["dst"])

    degrees = pd.concat([edges["src"], edges["dst"]]).value_counts().to_dict()
    starts = stats["traversal_sample_nodes"][:100]
    assert len(starts) == 100
    assert all(degrees.get(node_id, 0) > 0 for node_id in starts)

    expectations = build_reference_expectations(
        DATA / "nodes.csv", DATA / "edges.csv", stats["semantic_validation_node"]
    )
    assert expectations["expected"]["traversal_1hop"] > 0
    assert expectations["expected"]["traversal_2hop"] > 0
    assert expectations["expected"]["traversal_3hop"] > 0


def test_source_archive_matches_pinned_hash():
    archive = DATA / "ca-AstroPh.txt.gz"
    if not archive.exists():
        pytest.skip("Raw SNAP archive is intentionally not committed")
    archive_hash = hashlib.sha256(archive.read_bytes()).hexdigest()
    assert archive_hash == SNAP_SHA256
