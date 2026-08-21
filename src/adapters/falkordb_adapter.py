import time
from typing import Any

from falkordb import FalkorDB

from .base import BaseGraphAdapter


class FalkorDBAdapter(BaseGraphAdapter):
    """
    Adapter for FalkorDB (RedisGraph successor, GraphBLAS / Sparse Matrix Cypher Engine).
    """

    def __init__(self, host: str = "localhost", port: int = 6379, password: str | None = None):
        super().__init__(
            name="FalkorDB 4.2.1 (GraphBLAS)",
            hardware_spec="0.5 vCPU, 512MB container (Redis In-Memory Sparse Matrices)",
            platform_type="Resource-capped local container",
            query_interface="Redis protocol / OpenCypher",
            storage_engine="FalkorDB GraphBLAS sparse matrices in Redis-compatible memory",
            index_strategy="Single-property RANGE indexes on Node.id, Node.category, and Node.year",
            index_readiness="Polled through db.indexes() until ready",
            ingestion_method="Redis GRAPH.QUERY parameterized UNWIND batches",
        )
        self.host = host
        self.port = port
        self.password = password
        self.client: FalkorDB | None = None
        self.graph = None
        self.graph_name = "benchmark_graph"

    def connect(self) -> bool:
        try:
            self.client = FalkorDB(host=self.host, port=self.port, password=self.password)
            self.graph = self.client.select_graph(self.graph_name)
            # Test connection
            self.graph.query("RETURN 1")
            return True
        except Exception as e:
            print(f"[{self.name}] Connection error: {e}")
            return False

    def close(self):
        # Redis connection pool cleans up automatically
        pass

    def clear_database(self):
        if not self.graph:
            return
        try:
            self.graph.delete()
        except Exception:
            pass
        self.graph = self.client.select_graph(self.graph_name)

    @staticmethod
    def _header_name(header: Any) -> str:
        if isinstance(header, (list, tuple)) and len(header) >= 2:
            return str(header[1])
        return str(header)

    @staticmethod
    def _property_names(value: Any) -> set[str]:
        if isinstance(value, (list, tuple, set)):
            return {str(item) for item in value}
        return {
            part.strip().strip("[]'\"")
            for part in str(value).split(",")
            if part.strip().strip("[]'\"")
        }

    def _required_index_statuses(self, required: set[str]) -> dict[str, str]:
        try:
            result = self.graph.query("CALL db.indexes()")
        except Exception:
            # A freshly GRAPH.DELETE'd database has no catalog until the first
            # schema/data command recreates it.
            return {}
        headers = [self._header_name(header).lower() for header in result.header]
        statuses: dict[str, str] = {}
        for values in result.result_set:
            row = dict(zip(headers, values, strict=False))
            if str(row.get("label", "")).lower() != "node":
                continue
            entity_type = str(row.get("entitytype", "NODE")).upper()
            if entity_type and entity_type not in {"NODE", "NODES"}:
                continue
            index_types = row.get("types", row.get("type", "RANGE"))
            if "RANGE" not in str(index_types).upper():
                continue
            properties = self._property_names(row.get("properties", []))
            status = str(row.get("status", "READY")).upper()
            for prop in required & properties:
                statuses[prop] = status
        return statuses

    def create_indexes(self):
        if not self.graph:
            raise RuntimeError(f"[{self.name}] Cannot create indexes before connecting")

        required = {"id", "category", "year"}
        observed = self._required_index_statuses(required)
        for prop in sorted(required - set(observed)):
            try:
                self.graph.query(f"CREATE INDEX FOR (n:Node) ON (n.{prop})")
            except Exception as exc:
                # A concurrent/duplicate create is acceptable only when the index
                # catalog proves that the required RANGE index now exists.
                if prop not in self._required_index_statuses(required):
                    raise RuntimeError(
                        f"[{self.name}] Failed to create required Node.{prop} RANGE index"
                    ) from exc

        deadline = time.monotonic() + 60.0
        ready_states = {"ACTIVE", "ONLINE", "OPERATIONAL", "READY"}
        while True:
            observed = self._required_index_statuses(required)
            missing = required - set(observed)
            failed = {
                prop: state
                for prop, state in observed.items()
                if "FAIL" in state or "ERROR" in state
            }
            if failed:
                raise RuntimeError(f"[{self.name}] Required index build failed: {failed}")
            pending = {prop: state for prop, state in observed.items() if state not in ready_states}
            if not missing and not pending:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"[{self.name}] Required indexes not ready after 60s; "
                    f"missing={sorted(missing)}, pending={pending}"
                )
            time.sleep(0.1)

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
        self.graph.query(query, {"batch": batch})
        return len(batch)

    def ingest_edges_batch(self, batch: list[dict[str, Any]]) -> int:
        query = """
        UNWIND $batch AS row
        MATCH (a:Node {id: row.src}), (b:Node {id: row.dst})
        CREATE (a)-[:RELATION {weight: row.weight, type: row.type}]->(b)
        """
        self.graph.query(query, {"batch": batch})
        return len(batch)

    def traversal_1hop(self, node_id: int) -> int:
        query = "MATCH (n:Node {id: $id})-[:RELATION]-(m:Node) RETURN count(m) AS cnt"
        res = self.graph.query(query, {"id": node_id})
        return res.result_set[0][0] if res.result_set else 0

    def traversal_2hop(self, node_id: int) -> int:
        query = (
            "MATCH (n:Node {id: $id})-[r1:RELATION]-(:Node)-[r2:RELATION]-(m:Node) "
            "WHERE ID(r1) <> ID(r2) RETURN count(m) AS cnt"
        )
        res = self.graph.query(query, {"id": node_id})
        return res.result_set[0][0] if res.result_set else 0

    def traversal_3hop(self, node_id: int) -> int:
        query = (
            "MATCH (n:Node {id: $id})-[r1:RELATION]-(:Node)-[r2:RELATION]-(:Node)"
            "-[r3:RELATION]-(m:Node) WHERE ID(r1) <> ID(r2) AND ID(r2) <> ID(r3) AND ID(r1) <> ID(r3) "
            "RETURN count(m) AS cnt"
        )
        res = self.graph.query(query, {"id": node_id})
        return res.result_set[0][0] if res.result_set else 0

    def point_lookup(self, node_id: int) -> dict[str, Any] | None:
        query = "MATCH (n:Node {id: $id}) RETURN n.id, n.name, n.category, n.year, n.score"
        res = self.graph.query(query, {"id": node_id})
        if res.result_set:
            row = res.result_set[0]
            return {
                "id": row[0],
                "name": row[1],
                "category": row[2],
                "year": row[3],
                "score": row[4],
            }
        return None

    def filtered_lookup(self, category: str, min_year: int) -> int:
        query = "MATCH (n:Node) WHERE n.category = $category AND n.year >= $min_year RETURN count(n) AS cnt"
        res = self.graph.query(query, {"category": category, "min_year": min_year})
        return res.result_set[0][0] if res.result_set else 0

    def aggregation_category_counts(self) -> dict[str, int]:
        query = "MATCH (n:Node) RETURN n.category AS category, count(n) AS cnt"
        res = self.graph.query(query)
        counts = {}
        for row in res.result_set:
            counts[row[0]] = row[1]
        return counts

    def mixed_read_write(self, read_node_id: int, write_node_id: int, new_score: float) -> bool:
        query = """
        MATCH (n:Node {id: $read_id})-[:RELATION]-(m:Node)
        WITH count(m) AS neighbor_count
        MATCH (w:Node {id: $write_id})
        SET w.score = $new_score
        RETURN neighbor_count
        """
        res = self.graph.query(
            query,
            {
                "read_id": read_node_id,
                "write_id": write_node_id,
                "new_score": new_score,
            },
        )
        return len(res.result_set) > 0

    def count_nodes(self) -> int:
        if not self.graph:
            return 0
        res = self.graph.query("MATCH (n:Node) RETURN count(n)")
        return res.result_set[0][0] if res.result_set else 0

    def count_edges(self) -> int:
        if not self.graph:
            return 0
        res = self.graph.query("MATCH ()-[r:RELATION]->() RETURN count(r)")
        return res.result_set[0][0] if res.result_set else 0

    def get_resource_footprint(self) -> str:
        import subprocess

        redis_mem = "N/A"
        try:
            info = self.client.info("memory")
            redis_mem = info.get("used_memory_human", "N/A")
        except Exception:
            pass

        container_mem = "N/A"
        try:
            res = subprocess.run(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.MemUsage}}",
                    "benchmark-falkordb",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if res.returncode == 0 and res.stdout.strip():
                container_mem = res.stdout.strip()
        except Exception:
            pass

        if redis_mem == "N/A" and container_mem == "N/A":
            return "Not observed; configured allocation: 0.5 vCPU, 512MB container (Redis GraphBLAS)"
        return f"Observed Redis: {redis_mem} (GraphBLAS RAM), Container: {container_mem}"
