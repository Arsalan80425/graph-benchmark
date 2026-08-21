from typing import Any

from neo4j import Driver, GraphDatabase

from .base import BaseGraphAdapter


class CognoDBAdapter(BaseGraphAdapter):
    """
    Adapter for CognoDB Cloud using the official Neo4j Bolt driver.
    """

    def __init__(self, uri: str, user: str, password: str):
        super().__init__(
            name="CognoDB Cloud",
            hardware_spec="0.5 vCPU (burstable), 512MB RAM, 1GB Disk (c0 free tier)",
            platform_type="Managed cloud service",
            query_interface="Bolt / Cypher",
            storage_engine="CognoDB managed graph engine (implementation not disclosed)",
            index_strategy=(
                "Unique constraint on Node.id (primary key hash index), plus single-property RANGE "
                "indexes on Node.category and Node.year"
            ),
            index_readiness=(
                "Creation acknowledged by managed service; no portable index-readiness catalog assumed"
            ),
            ingestion_method="Bolt auto-commit UNWIND with cloud-safe sub-batching",
            max_node_sub_batch_size=500,
            max_edge_sub_batch_size=50,
        )
        self.uri = uri
        self.user = user
        self.password = password
        self.driver: Driver | None = None
        self.retry_telemetry: dict[str, Any] = {
            "node_retries": 0,
            "edge_retries": 0,
            "retry_events": [],
        }

    def _redact(self, msg: Any) -> str:
        s = str(msg)
        for secret in (self.password, self.user, self.uri):
            if secret and len(secret) > 3:
                s = s.replace(secret, "[REDACTED]")
        return s[:200]

    def get_retry_telemetry(self) -> dict[str, Any]:
        return dict(self.retry_telemetry)

    def connect(self) -> bool:
        try:
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
                max_connection_pool_size=50,
                max_connection_lifetime=60,
                keep_alive=True,
                connection_timeout=15.0,
                connection_acquisition_timeout=15.0,
            )
            self.driver.verify_connectivity()
            return True
        except Exception as e:
            print(f"[{self.name}] Connection error: {self._redact(e)}")
            return False

    def close(self):
        if self.driver:
            try:
                self.driver.close()
            except Exception:
                pass

    def clear_database(self):
        self.retry_telemetry = {"node_retries": 0, "edge_retries": 0, "retry_events": []}
        if not self.driver:
            return
        with self.driver.session() as session:
            # Batch deletion loop to avoid cloud query timeout on large graphs
            while True:
                try:
                    res = session.run(
                        "MATCH (n) WITH n LIMIT 5000 DETACH DELETE n RETURN count(n) AS cnt",
                        timeout=30.0,
                    )
                    rec = res.single()
                    if not rec or rec["cnt"] == 0:
                        break
                except Exception:
                    # Fallback single sweep
                    try:
                        session.run("MATCH (n) DETACH DELETE n", timeout=30.0)
                    except Exception:
                        pass
                    break

    def create_indexes(self):
        if not self.driver:
            raise RuntimeError(f"[{self.name}] Cannot create indexes before connecting")
        with self.driver.session() as session:
            try:
                session.run(
                    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE",
                    timeout=20.0,
                ).consume()
            except Exception as constraint_error:
                try:
                    session.run("CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.id)", timeout=20.0).consume()
                except Exception as index_error:
                    raise RuntimeError(
                        f"[{self.name}] Could not create the required Node.id constraint/index; "
                        f"constraint error={self._redact(constraint_error)}; index error={self._redact(index_error)}"
                    ) from index_error

            for prop in ("category", "year"):
                try:
                    session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:Node) ON (n.{prop})", timeout=20.0).consume()
                except Exception as exc:
                    raise RuntimeError(
                        f"[{self.name}] Could not create required Node.{prop} index: {self._redact(exc)}"
                    ) from exc

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
        sub_batch_size = 500
        for i in range(0, len(batch), sub_batch_size):
            sub = batch[i : i + sub_batch_size]
            for attempt in range(5):
                try:
                    with self.driver.session() as session:
                        session.run(query, batch=sub, timeout=25.0).consume()
                    break
                except Exception as e:
                    self.retry_telemetry["node_retries"] += 1
                    self.retry_telemetry["retry_events"].append(
                        {"type": "node", "attempt": attempt + 1, "error": self._redact(e)}
                    )
                    print(f"[{self.name}] Transient node write retry ({attempt+1}/5): {self._redact(e)}")
                    if attempt == 4:
                        raise e
                    try:
                        self.driver.close()
                    except Exception:
                        pass
                    self.connect()
        return len(batch)

    def ingest_edges_batch(self, batch: list[dict[str, Any]]) -> int:
        import time

        query = """
        UNWIND $batch AS row
        MATCH (a:Node {id: row.src}), (b:Node {id: row.dst})
        CREATE (a)-[:RELATION {weight: row.weight, type: row.type}]->(b)
        """
        sub_batch_size = 50
        for i in range(0, len(batch), sub_batch_size):
            sub = batch[i : i + sub_batch_size]
            for attempt in range(5):
                try:
                    with self.driver.session() as session:
                        session.run(query, batch=sub, timeout=20.0).consume()
                    break
                except Exception as e:
                    self.retry_telemetry["edge_retries"] += 1
                    self.retry_telemetry["retry_events"].append(
                        {"type": "edge", "attempt": attempt + 1, "error": self._redact(e)}
                    )
                    print(f"[{self.name}] Transient edge write retry ({attempt+1}/5): {self._redact(e)}")
                    if attempt == 4:
                        raise e
                    try:
                        self.driver.close()
                    except Exception:
                        pass
                    self.connect()
                    time.sleep(0.5 * (attempt + 1))
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
        return (
            "Not observable via managed cloud interface "
            "(Console allocation: 0.5 burstable vCPU, 512MB RAM, 1GiB Storage, up to 500 IOPS)"
        )
