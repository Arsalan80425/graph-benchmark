import os
from pathlib import Path

from dotenv import load_dotenv

# Explicit shell/CI variables take precedence over the local .env file.
load_dotenv(override=False)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CHARTS_DIR = BASE_DIR / "charts"
RESULTS_DIR = BASE_DIR / "results"

CHARTS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Datasets
NODES_CSV = DATA_DIR / "nodes.csv"
EDGES_CSV = DATA_DIR / "edges.csv"
STATS_JSON = DATA_DIR / "dataset_stats.json"
DATASET_STATS_FILE = STATS_JSON

# CognoDB Cloud
COGNODB_URI = os.getenv("COGNODB_URI") or os.getenv("Connection_URI", "")
COGNODB_USER = os.getenv("COGNODB_USER") or os.getenv("Username") or "cognodb"
if COGNODB_USER.lower() == "arsal":
    COGNODB_USER = "cognodb"
COGNODB_PASSWORD = os.getenv("COGNODB_PASSWORD") or os.getenv("Password", "")

# Neo4j (Local Docker or AuraDB)
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "benchmark_pass")

# Memgraph (Local Docker or Cloud)
MEMGRAPH_URI = os.getenv("MEMGRAPH_URI", "bolt://localhost:7688")
MEMGRAPH_USER = os.getenv("MEMGRAPH_USER", "")
MEMGRAPH_PASSWORD = os.getenv("MEMGRAPH_PASSWORD", "")

# FalkorDB (Local Docker or Cloud)
FALKORDB_HOST = os.getenv("FALKORDB_HOST", "localhost")
FALKORDB_PORT = int(os.getenv("FALKORDB_PORT", "6379"))
FALKORDB_PASSWORD = os.getenv("FALKORDB_PASSWORD", None)

# ArangoDB (Local Docker or Cloud)
ARANGODB_URL = os.getenv("ARANGODB_URL", "http://localhost:8529")
ARANGODB_USER = os.getenv("ARANGODB_USER", "root")
ARANGODB_PASSWORD = os.getenv("ARANGODB_PASSWORD", "benchmark_pass")
ARANGODB_DATABASE = os.getenv("ARANGODB_DATABASE", "benchmark_db")

# Benchmark Execution Parameters
WARMUP_ITERATIONS = 10
LOOKUP_WARMUP_ITERATIONS = 5
AGGREGATION_WARMUP_ITERATIONS = 3
BENCHMARK_ITERATIONS = 100
BATCH_SIZE = 1000
CONCURRENCY_LEVELS = [1, 10, 40]
MIXED_WORKLOAD_DURATION_SECONDS = 30.0
CONNECTION_ATTEMPTS = 10
CONNECTION_RETRY_DELAY_SECONDS = 3.0
