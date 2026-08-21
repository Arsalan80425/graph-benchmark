import gzip
import hashlib
import json
import random
from pathlib import Path

import networkx as nx
import pandas as pd
import requests

DATA_DIR = Path(__file__).resolve().parent
NODES_CSV = DATA_DIR / "nodes.csv"
EDGES_CSV = DATA_DIR / "edges.csv"
STATS_JSON = DATA_DIR / "dataset_stats.json"

SNAP_URL = "https://snap.stanford.edu/data/ca-AstroPh.txt.gz"
SNAP_FILE = DATA_DIR / "ca-AstroPh.txt.gz"
SNAP_SHA256 = "51bf1e2cace269b884481a8502474efa67c0fd01d998ff7f5a154d7d3e527f27"

CATEGORIES = [
    "ComputerScience",
    "Physics",
    "Mathematics",
    "Biology",
    "Economics",
    "Engineering",
]
REL_TYPES = ["CITES", "COLLABORATES", "REFERENCES"]


def download_snap_dataset(url: str = SNAP_URL, dest: Path = SNAP_FILE) -> bool:
    """Download SNAP dataset if not present."""
    if dest.exists():
        observed_hash = hashlib.sha256(dest.read_bytes()).hexdigest()
        if observed_hash != SNAP_SHA256:
            raise RuntimeError(
                f"Existing SNAP archive failed SHA-256 verification: {observed_hash}"
            )
        print(f"[Dataset] Found existing archive at {dest}")
        return True
    print(f"[Dataset] Downloading SNAP dataset from {url}...")
    temporary = dest.with_suffix(dest.suffix + ".tmp")
    try:
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code == 200:
            with temporary.open("wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            observed_hash = hashlib.sha256(temporary.read_bytes()).hexdigest()
            if observed_hash != SNAP_SHA256:
                raise RuntimeError(
                    f"Downloaded SNAP archive failed SHA-256 verification: {observed_hash}"
                )
            temporary.replace(dest)
            print(f"[Dataset] Successfully downloaded to {dest}")
            return True
        else:
            print(f"[Dataset] Failed to download (HTTP {response.status_code}).")
            return False
    except Exception as e:
        print(f"[Dataset] Network error ({e}).")
        return False
    finally:
        if temporary.exists():
            temporary.unlink()


def parse_or_generate_graph(
    target_nodes: int = 20000,
    target_edges: int = 120000,
    allow_synthetic: bool = False,
):
    """
    Parses SNAP graph or generates a deterministic scale-free power-law graph.
    Ensures >= 100,000 relationships and fits cleanly in 256MB RAM free tier.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    edges: list[tuple[int, int]] = []
    raw_relationship_records = 0
    source_type = "snap"

    downloaded = download_snap_dataset()
    if downloaded and SNAP_FILE.exists():
        try:
            print(f"[Dataset] Parsing SNAP archive {SNAP_FILE}...")
            with gzip.open(SNAP_FILE, "rt", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("#"):
                        continue
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        u, v = int(parts[0]), int(parts[1])
                        if u != v:
                            raw_relationship_records += 1
                            edges.append((min(u, v), max(u, v)))
            # ca-AstroPh contains reciprocal rows for its undirected graph.
            # Canonicalize before sampling so one collaboration is loaded once.
            edges = sorted(set(edges))
            raw_nodes = {node_id for edge in edges for node_id in edge}
            print(
                f"[Dataset] Parsed SNAP graph: {len(raw_nodes)} nodes, "
                f"{raw_relationship_records} records, {len(edges)} unique collaborations"
            )
        except Exception as e:
            if not allow_synthetic:
                raise RuntimeError(f"Unable to parse required SNAP archive: {e}") from e
            print(f"[Dataset] Error parsing SNAP data: {e}. Using explicit synthetic fallback.")
            edges = []

    if len(edges) < 100000:
        if not allow_synthetic:
            raise RuntimeError(
                "The required SNAP dataset is unavailable or contains fewer than 100,000 edges. "
                "Re-run with allow_synthetic=True only for a clearly labelled development dataset."
            )
        print(
            f"[Dataset] Generating reproducible scale-free graph (~{target_nodes} nodes, ~{target_edges} edges)..."
        )
        source_type = "synthetic_barabasi_albert"
        random.seed(42)
        # Barabasi-Albert preferential attachment graph (m=6 -> ~120,000 edges)
        m = 6
        G = nx.barabasi_albert_graph(n=target_nodes, m=m, seed=42)
        edges = list(G.edges())
        raw_relationship_records = len(edges)
        print(f"[Dataset] Generated {G.number_of_nodes()} nodes, {len(edges)} edges.")

    raw_node_count = len({node_id for edge in edges for node_id in edge})
    raw_unique_relationship_count = len(edges)

    # Select the benchmark relationships before deriving its node set. This
    # prevents nodes that do not occur in the sampled graph from being loaded as
    # isolated records and removes source-file ordering bias.
    if len(edges) > 100000:
        sample_rng = random.Random(20240821)
        selected = sorted(sample_rng.sample(range(len(edges)), 100000))
        edges = [edges[index] for index in selected]

    nodes_set = {node_id for edge in edges for node_id in edge}

    # Standardize node IDs to 1..N
    id_map = {old_id: idx + 1 for idx, old_id in enumerate(sorted(nodes_set))}

    random.seed(42)
    nodes_data = []
    for _old_id, new_id in id_map.items():
        cat = random.choice(CATEGORIES)
        year = random.randint(2010, 2024)
        score = round(random.uniform(1.0, 100.0), 2)
        nodes_data.append(
            {
                "id": new_id,
                "name": f"Entity_{new_id}",
                "category": cat,
                "year": year,
                "score": score,
            }
        )

    edges_data = []
    for u, v in edges:
        src = id_map.get(u, u)
        dst = id_map.get(v, v)
        weight = round(random.uniform(0.1, 5.0), 3)
        rel_type = random.choice(REL_TYPES)
        edges_data.append({"src": src, "dst": dst, "weight": weight, "type": rel_type})

    # Save to CSV
    df_nodes = pd.DataFrame(nodes_data)
    df_edges = pd.DataFrame(edges_data)

    df_nodes.to_csv(NODES_CSV, index=False)
    df_edges.to_csv(EDGES_CSV, index=False)
    print(f"[Dataset] Saved {len(df_nodes)} nodes to {NODES_CSV}")
    print(f"[Dataset] Saved {len(df_edges)} edges to {EDGES_CSV}")

    # The source is an undirected collaboration graph stored once per unordered
    # pair. Select deterministic active starts across degree quartiles so the
    # benchmark does not mostly time empty traversals.
    undirected_degree: dict[int, int] = {}
    for edge in edges_data:
        undirected_degree[edge["src"]] = undirected_degree.get(edge["src"], 0) + 1
        undirected_degree[edge["dst"]] = undirected_degree.get(edge["dst"], 0) + 1

    degree_frame = pd.DataFrame(
        [{"id": node_id, "degree": degree} for node_id, degree in undirected_degree.items()]
    )
    degree_frame["quartile"] = pd.qcut(degree_frame["degree"].rank(method="first"), 4, labels=False)
    traversal_rng = random.Random(4242)
    sample_nodes: list[int] = []
    target_sample_count = min(200, len(degree_frame))
    per_quartile = target_sample_count // 4
    for quartile in range(4):
        candidates = degree_frame.loc[degree_frame["quartile"] == quartile, "id"].tolist()
        sample_nodes.extend(traversal_rng.sample(candidates, min(per_quartile, len(candidates))))
    if len(sample_nodes) < target_sample_count:
        selected_nodes = set(sample_nodes)
        remaining = [
            node_id for node_id in degree_frame["id"].tolist() if node_id not in selected_nodes
        ]
        sample_nodes.extend(
            traversal_rng.sample(
                remaining, min(target_sample_count - len(sample_nodes), len(remaining))
            )
        )
    traversal_rng.shuffle(sample_nodes)

    median_degree = float(degree_frame["degree"].median())
    semantic_validation_node = int(
        min(undirected_degree, key=lambda node_id: abs(undirected_degree[node_id] - median_degree))
    )

    archive_sha256 = (
        hashlib.sha256(SNAP_FILE.read_bytes()).hexdigest() if SNAP_FILE.exists() else "N/A"
    )

    stats = {
        "dataset_name": "SNAP Astrophysics Collaboration Network (Standardized Sample)",
        "source_type": source_type,
        "source_url": SNAP_URL,
        "source_archive_sha256": archive_sha256,
        "raw_nodes": raw_node_count,
        "raw_relationships": raw_relationship_records,
        "raw_relationship_records": raw_relationship_records,
        "raw_unique_relationships": raw_unique_relationship_count,
        "total_nodes": len(df_nodes),
        "total_relationships": len(df_edges),
        "graph_semantics": "Undirected collaboration graph; each unordered pair stored once",
        "edge_sampling": (
            "Canonicalize and deduplicate unordered pairs, then uniformly sample "
            "100,000 unique collaborations (seed 20240821)"
        ),
        "dataset_generator_version": 3,
        "synthetic_properties": {
            "seed": 42,
            "node_fields": ["name", "category", "year", "score"],
            "edge_fields": ["weight", "type"],
        },
        "categories": list(df_nodes["category"].value_counts().to_dict().keys()),
        "category_distribution": df_nodes["category"].value_counts().to_dict(),
        "year_min": int(df_nodes["year"].min()),
        "year_max": int(df_nodes["year"].max()),
        "traversal_sample_nodes": sample_nodes,
        "traversal_sample_strategy": "Degree-quartile-stratified active nodes (seed 4242)",
        "semantic_validation_node": semantic_validation_node,
        "sample_degree_min": int(min(undirected_degree[node_id] for node_id in sample_nodes)),
        "sample_degree_max": int(max(undirected_degree[node_id] for node_id in sample_nodes)),
    }

    with open(STATS_JSON, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)
    print(f"[Dataset] Saved metadata to {STATS_JSON}")

    return stats


if __name__ == "__main__":
    parse_or_generate_graph()
