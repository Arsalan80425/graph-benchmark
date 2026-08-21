from typing import Any

from neo4j import Driver, GraphDatabase

from .base import BaseGraphAdapter


class MemgraphAdapter(BaseGraphAdapter):
    """
    Adapter for Memgraph (In-memory C++ graph database, Bolt/Cypher compatible).
    """

    def __init__(self, uri: str, user: str = "", password: str = ""):
        super().__init__(
            name="Memgraph 2.16 (In-Memory C++)",
            hardware_spec="0.5 vCPU, 512MB container",
            platform_type="Resource-capped local container",
            query_interface="Bolt / Cypher",
            storage_engine="Memgraph in-memory transactional storage",
            index_strategy="Single-property skip-list indexes on Node.id, Node.category, and Node.year",
            index_readiness="Blocking creation in Memgraph 2.16; verified with SHOW INDEX INFO",
            ingestion_method="Bolt auto-commit UNWIND batches",
        )
        self.uri = uri
        self.user = user
        self.password = password
        self.driver: Driver | None = None

    def connect(self) -> bool:
        try:
            auth = (self.user, self.password) if self.user or self.password else None
            self.driver = GraphDatabase.driver(self.uri, auth=auth, max_connection_pool_size=50)
            self.driver.verify_connectivity()
            return True
        except Exception as e:
            print(f"[{self.name}] Connection error: {e}")
            return False

    def close(self):
        if self.driver:
            self.driver.close()

    def clear_database(self):
        if not self.driver:
            return
        with self.driver.session() as session:
            while True:
                try:
                    res = session.run(
                        "MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS cnt"
                    )
                    rec = res.single()
                    if not rec or rec["cnt"] == 0:
                        break
                except Exception:
                    try:
                        session.run("MATCH (n) DETACH DELETE n")
                    except Exception:
                        pass
                    break

    @staticmethod
    def _existing_node_property_indexes(session, required: set[str]) -> set[str]:
        """Read Memgraph's version-stable SHOW INDEX INFO result defensively."""
        found: set[str] = set()
        for record in session.run("SHOW INDEX INFO"):
            data = record.data()
            normalized = {str(key).lower().replace(" ", "_"): value for key, value in data.items()}
            label = normalized.get("label") or normalized.get("label_name")
            prop = normalized.get("property") or normalized.get("property_name")
            if str(label) == "Node" and str(prop) in required:
                found.add(str(prop))
                continue

            # Memgraph has changed presentation column names between releases.
            # Fall back to matching scalar values, but still require the Node label.
            scalar_values = {str(value) for value in data.values() if value is not None}
            if "Node" in scalar_values:
                found.update(required & scalar_values)
        return found

    def create_indexes(self):
        if not self.driver:
            raise RuntimeError(f"[{self.name}] Cannot create indexes before connecting")

        required = {"id", "category", "year"}
        with self.driver.session() as session:
            existing = self._existing_node_property_indexes(session, required)
            for prop in sorted(required - existing):
                try:
                    session.run(f"CREATE INDEX ON :Node({prop})").consume()
                except Exception as exc:
                    # CREATE INDEX has no IF NOT EXISTS in Memgraph 2.16. Treat a
                    # duplicate race as success only when SHOW INDEX INFO proves it.
                    if prop not in self._existing_node_property_indexes(session, required):
                        raise RuntimeError(
                            f"[{self.name}] Failed to create required Node.{prop} index"
                        ) from exc

            observed = self._existing_node_property_indexes(session, required)

        missing = required - observed
        if missing:
            raise RuntimeError(
                f"[{self.name}] Required index verification failed; missing: {sorted(missing)}"
            )

    def ingest_nodes_batch(self, batch: list[dict[str, Any]]) -> int:
        query = """
        UNWIND $batch AS row
        CREATE (n:Node {
            id: row.id,
            name: row.name,
            category: row.category,
            year: row.year,
            score: row.score
        })
        """
        with self.driver.session() as session:
            session.run(query, batch=batch).consume()
        return len(batch)

    def ingest_edges_batch(self, batch: list[dict[str, Any]]) -> int:
        query = """
        UNWIND $batch AS row
        MATCH (a:Node {id: row.src}), (b:Node {id: row.dst})
        CREATE (a)-[:RELATION {weight: row.weight, type: row.type}]->(b)
        """
        with self.driver.session() as session:
            session.run(query, batch=batch).consume()
        return len(batch)

    def traversal_1hop(self, node_id: int) -> int:
        query = "MATCH (n:Node {id: $id})-[:RELATION]-(m:Node) RETURN count(m) AS cnt"
        with self.driver.session() as session:
            result = session.run(query, id=node_id)
            rec = result.single()
            return rec["cnt"] if rec else 0

    def traversal_2hop(self, node_id: int) -> int:
        query = (
            "MATCH (n:Node {id: $id})-[:RELATION]-(:Node)-[:RELATION]-(m:Node) "
            "RETURN count(m) AS cnt"
        )
        with self.driver.session() as session:
            result = session.run(query, id=node_id)
            rec = result.single()
            return rec["cnt"] if rec else 0

    def traversal_3hop(self, node_id: int) -> int:
        query = (
            "MATCH (n:Node {id: $id})-[:RELATION]-(:Node)-[:RELATION]-(:Node)"
            "-[:RELATION]-(m:Node) RETURN count(m) AS cnt"
        )
        with self.driver.session() as session:
            result = session.run(query, id=node_id)
            rec = result.single()
            return rec["cnt"] if rec else 0

    def point_lookup(self, node_id: int) -> dict[str, Any] | None:
        query = "MATCH (n:Node {id: $id}) RETURN n.id AS id, n.name AS name, n.category AS category, n.year AS year, n.score AS score"
        with self.driver.session() as session:
            result = session.run(query, id=node_id)
            rec = result.single()
            return dict(rec) if rec else None

    def filtered_lookup(self, category: str, min_year: int) -> int:
        query = "MATCH (n:Node) WHERE n.category = $category AND n.year >= $min_year RETURN count(n) AS cnt"
        with self.driver.session() as session:
            result = session.run(query, category=category, min_year=min_year)
            rec = result.single()
            return rec["cnt"] if rec else 0

    def aggregation_category_counts(self) -> dict[str, int]:
        query = "MATCH (n:Node) RETURN n.category AS category, count(n) AS cnt"
        counts = {}
        with self.driver.session() as session:
            result = session.run(query)
            for rec in result:
                counts[rec["category"]] = rec["cnt"]
        return counts

    def mixed_read_write(self, read_node_id: int, write_node_id: int, new_score: float) -> bool:
        query = """
        MATCH (n:Node {id: $read_id})-[:RELATION]-(m:Node)
        WITH count(m) AS neighbor_count
        MATCH (w:Node {id: $write_id})
        SET w.score = $new_score
        RETURN neighbor_count
        """
        with self.driver.session() as session:
            result = session.run(
                query, read_id=read_node_id, write_id=write_node_id, new_score=new_score
            )
            return result.single() is not None

    def count_nodes(self) -> int:
        query = "MATCH (n:Node) RETURN count(n) AS cnt"
        with self.driver.session() as session:
            result = session.run(query)
            rec = result.single()
            return rec["cnt"] if rec else 0

    def count_edges(self) -> int:
        query = "MATCH ()-[r:RELATION]->() RETURN count(r) AS cnt"
        with self.driver.session() as session:
            result = session.run(query)
            rec = result.single()
            return rec["cnt"] if rec else 0

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
                    "benchmark-memgraph",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if res.returncode == 0 and res.stdout.strip():
                return f"Observed Container: {res.stdout.strip()} (In-Memory C++ Engine)"
        except Exception:
            pass
        return (
            "Not observed; configured allocation: 0.5 vCPU, 512MB container (In-Memory C++ Engine)"
        )
