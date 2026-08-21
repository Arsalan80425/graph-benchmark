from typing import Any

from arango import ArangoClient

from .base import BaseGraphAdapter


class ArangoDBAdapter(BaseGraphAdapter):
    """
    Adapter for ArangoDB (Multi-model Graph Database, AQL Traversal Engine).
    """

    def __init__(
        self,
        url: str = "http://localhost:8529",
        user: str = "root",
        password: str = "benchmark_pass",
        db_name: str = "benchmark_db",
    ):
        super().__init__(
            name="ArangoDB 3.11",
            hardware_spec="0.5 vCPU, 512MB container (RocksDB Engine)",
            platform_type="Resource-capped local container",
            query_interface="HTTP / AQL",
            storage_engine="ArangoDB RocksDB",
            index_strategy=(
                "Built-in primary _key index plus persistent indexes on nodes.category and nodes.year"
            ),
            index_readiness="Synchronous creation and verification through the collection index catalog",
            ingestion_method="HTTP collection.insert_many batches",
        )
        self.url = url
        self.user = user
        self.password = password
        self.db_name = db_name
        self.client: ArangoClient | None = None
        self.db = None

    def connect(self) -> bool:
        try:
            self.client = ArangoClient(hosts=self.url)
            sys_db = self.client.db("_system", username=self.user, password=self.password)
            if not sys_db.has_database(self.db_name):
                sys_db.create_database(self.db_name)
            self.db = self.client.db(self.db_name, username=self.user, password=self.password)
            return True
        except Exception as e:
            print(f"[{self.name}] Connection error: {e}")
            return False

    def close(self):
        pass

    def clear_database(self):
        if not self.db:
            return
        try:
            if self.db.has_graph("benchmark_graph"):
                self.db.delete_graph("benchmark_graph", drop_collections=True)
            if self.db.has_collection("nodes"):
                self.db.delete_collection("nodes")
            if self.db.has_collection("edges"):
                self.db.delete_collection("edges")
        except Exception:
            pass

        # Re-create collections
        self.db.create_collection("nodes")
        self.db.create_collection("edges", edge=True)
        if not self.db.has_graph("benchmark_graph"):
            self.db.create_graph(
                "benchmark_graph",
                edge_definitions=[
                    {
                        "edge_collection": "edges",
                        "from_vertex_collections": ["nodes"],
                        "to_vertex_collections": ["nodes"],
                    }
                ],
            )

    def create_indexes(self):
        if not self.db:
            raise RuntimeError(f"[{self.name}] Cannot create indexes before connecting")
        if not self.db.has_collection("nodes"):
            raise RuntimeError(
                f"[{self.name}] Cannot create indexes before nodes collection exists"
            )

        nodes_col = self.db.collection("nodes")
        expected = {"category": "node_category_idx", "year": "node_year_idx"}
        for prop, index_name in expected.items():
            nodes_col.add_persistent_index(
                fields=[prop],
                name=index_name,
                unique=False,
                sparse=False,
                in_background=False,
            )

        indexes = nodes_col.indexes()
        has_primary_key = any(
            details.get("type") == "primary" and details.get("fields") == ["_key"]
            for details in indexes
        )
        found_properties = {
            details["fields"][0]
            for details in indexes
            if details.get("type") == "persistent"
            and isinstance(details.get("fields"), list)
            and len(details["fields"]) == 1
        }
        missing = set(expected) - found_properties
        if not has_primary_key or missing:
            raise RuntimeError(
                f"[{self.name}] Required index verification failed; "
                f"primary_key={has_primary_key}, missing={sorted(missing)}"
            )

    def ingest_nodes_batch(self, batch: list[dict[str, Any]]) -> int:
        docs = []
        for row in batch:
            docs.append(
                {
                    "_key": str(row["id"]),
                    "id": row["id"],
                    "name": row["name"],
                    "category": row["category"],
                    "year": row["year"],
                    "score": row["score"],
                }
            )
        self.db.collection("nodes").insert_many(
            docs,
            sync=True,
            raise_on_document_error=True,
        )
        return len(batch)

    def ingest_edges_batch(self, batch: list[dict[str, Any]]) -> int:
        edges = []
        for row in batch:
            edges.append(
                {
                    "_from": f"nodes/{row['src']}",
                    "_to": f"nodes/{row['dst']}",
                    "weight": row["weight"],
                    "type": row["type"],
                }
            )
        self.db.collection("edges").insert_many(
            edges,
            sync=True,
            raise_on_document_error=True,
        )
        return len(batch)

    def traversal_1hop(self, node_id: int) -> int:
        query = """
        FOR v IN 1..1 ANY CONCAT('nodes/', @id) edges
        OPTIONS {uniqueVertices: "none", uniqueEdges: "path"}
        COLLECT WITH COUNT INTO cnt
        RETURN cnt
        """
        cursor = self.db.aql.execute(query, bind_vars={"id": str(node_id)})
        return cursor.next() if not cursor.empty() else 0

    def traversal_2hop(self, node_id: int) -> int:
        query = """
        FOR v IN 2..2 ANY CONCAT('nodes/', @id) edges
        OPTIONS {uniqueVertices: "none", uniqueEdges: "path"}
        COLLECT WITH COUNT INTO cnt
        RETURN cnt
        """
        cursor = self.db.aql.execute(query, bind_vars={"id": str(node_id)})
        return cursor.next() if not cursor.empty() else 0

    def traversal_3hop(self, node_id: int) -> int:
        query = """
        FOR v IN 3..3 ANY CONCAT('nodes/', @id) edges
        OPTIONS {uniqueVertices: "none", uniqueEdges: "path"}
        COLLECT WITH COUNT INTO cnt
        RETURN cnt
        """
        cursor = self.db.aql.execute(query, bind_vars={"id": str(node_id)})
        return cursor.next() if not cursor.empty() else 0

    def point_lookup(self, node_id: int) -> dict[str, Any] | None:
        query = "FOR doc IN nodes FILTER doc._key == @id RETURN doc"
        cursor = self.db.aql.execute(query, bind_vars={"id": str(node_id)})
        return cursor.next() if not cursor.empty() else None

    def filtered_lookup(self, category: str, min_year: int) -> int:
        query = """
        FOR doc IN nodes
        FILTER doc.category == @category AND doc.year >= @min_year
        COLLECT WITH COUNT INTO cnt
        RETURN cnt
        """
        cursor = self.db.aql.execute(query, bind_vars={"category": category, "min_year": min_year})
        return cursor.next() if not cursor.empty() else 0

    def aggregation_category_counts(self) -> dict[str, int]:
        query = """
        FOR doc IN nodes
        COLLECT cat = doc.category WITH COUNT INTO cnt
        RETURN {category: cat, count: cnt}
        """
        cursor = self.db.aql.execute(query)
        counts = {}
        for doc in cursor:
            counts[doc["category"]] = doc["count"]
        return counts

    def mixed_read_write(self, read_node_id: int, write_node_id: int, new_score: float) -> bool:
        query = """
        LET neighbors = (
            FOR v IN 1..1 ANY CONCAT('nodes/', @read_id) edges
            OPTIONS {uniqueVertices: "none", uniqueEdges: "path"}
            RETURN v
        )
        UPDATE {_key: @write_id, score: @new_score} IN nodes
        RETURN LENGTH(neighbors)
        """
        cursor = self.db.aql.execute(
            query,
            bind_vars={
                "read_id": str(read_node_id),
                "write_id": str(write_node_id),
                "new_score": new_score,
            },
        )
        return not cursor.empty()

    def count_nodes(self) -> int:
        if not self.db or not self.db.has_collection("nodes"):
            return 0
        try:
            cursor = self.db.aql.execute("RETURN LENGTH(nodes)")
            return cursor.next() if not cursor.empty() else 0
        except Exception:
            return 0

    def count_edges(self) -> int:
        if not self.db or not self.db.has_collection("edges"):
            return 0
        try:
            cursor = self.db.aql.execute("RETURN LENGTH(edges)")
            return cursor.next() if not cursor.empty() else 0
        except Exception:
            return 0

    def get_resource_footprint(self) -> str:
        import subprocess

        try:
            res = subprocess.run(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.MemUsage}}",
                    "benchmark-arangodb",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if res.returncode == 0 and res.stdout.strip():
                return f"Observed Container: {res.stdout.strip()} (RocksDB Engine)"
        except Exception:
            pass
        return "Not observed; configured allocation: 0.5 vCPU, 512MB container (RocksDB Engine)"
