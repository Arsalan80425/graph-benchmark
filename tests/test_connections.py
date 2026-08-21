import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.adapters import (  # noqa: E402
    ArangoDBAdapter,
    CognoDBAdapter,
    FalkorDBAdapter,
    MemgraphAdapter,
    Neo4jAdapter,
)
from src.config import (  # noqa: E402
    ARANGODB_DATABASE,
    ARANGODB_PASSWORD,
    ARANGODB_URL,
    ARANGODB_USER,
    COGNODB_PASSWORD,
    COGNODB_URI,
    COGNODB_USER,
    FALKORDB_HOST,
    FALKORDB_PASSWORD,
    FALKORDB_PORT,
    MEMGRAPH_PASSWORD,
    MEMGRAPH_URI,
    MEMGRAPH_USER,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
)


def test_cognodb_connection():
    if not COGNODB_URI or not COGNODB_PASSWORD:
        pytest.skip("CognoDB credentials not configured in .env")
    adapter = CognoDBAdapter(COGNODB_URI, COGNODB_USER, COGNODB_PASSWORD)
    connected = adapter.connect()
    adapter.close()
    assert connected is True, f"Failed to connect to {adapter.name}"


def test_memgraph_connection():
    adapter = MemgraphAdapter(MEMGRAPH_URI, MEMGRAPH_USER, MEMGRAPH_PASSWORD)
    connected = adapter.connect()
    adapter.close()
    assert connected is True, f"Failed to connect to {adapter.name}"


def test_falkordb_connection():
    adapter = FalkorDBAdapter(FALKORDB_HOST, FALKORDB_PORT, FALKORDB_PASSWORD)
    connected = adapter.connect()
    adapter.close()
    assert connected is True, f"Failed to connect to {adapter.name}"


def test_arangodb_connection():
    adapter = ArangoDBAdapter(ARANGODB_URL, ARANGODB_USER, ARANGODB_PASSWORD, ARANGODB_DATABASE)
    connected = adapter.connect()
    adapter.close()
    assert connected is True, f"Failed to connect to {adapter.name}"


def test_neo4j_connection():
    adapter = Neo4jAdapter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    connected = adapter.connect()
    adapter.close()
    assert connected is True, f"Failed to connect to {adapter.name}"


def smoke_test_all() -> int:
    print("\n" + "=" * 60)
    print("Graph Database Connectivity Smoke Test")
    print("=" * 60 + "\n")

    adapters = [
        ("CognoDB Cloud", CognoDBAdapter(COGNODB_URI, COGNODB_USER, COGNODB_PASSWORD)),
        (
            "Memgraph (In-Memory)",
            MemgraphAdapter(MEMGRAPH_URI, MEMGRAPH_USER, MEMGRAPH_PASSWORD),
        ),
        (
            "FalkorDB (GraphBLAS)",
            FalkorDBAdapter(FALKORDB_HOST, FALKORDB_PORT, FALKORDB_PASSWORD),
        ),
        (
            "ArangoDB (Multi-Model)",
            ArangoDBAdapter(ARANGODB_URL, ARANGODB_USER, ARANGODB_PASSWORD, ARANGODB_DATABASE),
        ),
        ("Neo4j (Capped)", Neo4jAdapter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)),
    ]

    failed = 0
    for name, adapter in adapters:
        try:
            connected = adapter.connect()
            if connected:
                print(f"  [PASS] {name:25s} -> Connected successfully")
                adapter.close()
            else:
                print(f"  [FAIL] {name:25s} -> Connection failed")
                failed += 1
        except Exception as e:
            print(f"  [FAIL] {name:25s} -> Error: {e}")
            failed += 1

    print("\n" + "=" * 60)
    print(f"Summary: {len(adapters) - failed}/{len(adapters)} connected")
    print("=" * 60 + "\n")
    return failed


if __name__ == "__main__":
    exit_code = smoke_test_all()
    sys.exit(0 if exit_code == 0 else 1)
