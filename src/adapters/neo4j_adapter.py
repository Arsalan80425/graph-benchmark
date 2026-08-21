from typing import Any

from neo4j import Driver, GraphDatabase

from .base import BaseGraphAdapter


class Neo4jAdapter(BaseGraphAdapter):
    """
    Adapter for Neo4j (Community Edition in resource-capped Docker or AuraDB Cloud).
    """

    def __init__(self, uri: str, user: str, password: str):
        super().__init__(
            name="Neo4j 5 (Capped)",
            hardware_spec="0.5 vCPU, 512MB container (192MB Heap, 64MB PageCache)",
            platform_type="Resource-capped local container",
            query_interface="Bolt / Cypher",
            storage_engine="Neo4j native storage engine",
            index_strategy="Named single-property indexes on Node.id, Node.category, and Node.year",
            index_readiness="Awaited and verified ONLINE through Neo4j index catalog",
            ingestion_method="Bolt auto-commit UNWIND batches",
        )
        self.uri = uri
        self.user = user
        self.password = password
        self.driver: Driver | None = None

    def connect(self) -> bool:
        try:
            auth = (self.user, self.password) if (self.user and self.password) else None
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
                    res = session.run("MATCH ()-[r]->() WITH r LIMIT 2000 DELETE r RETURN count(r) AS cnt")
                    rec = res.single()
                    if not rec or rec["cnt"] == 0:
                        break
                except Exception:
                    break
            while True:
                try:
                    res = session.run("MATCH (n) WITH n LIMIT 2000 DELETE n RETURN count(n) AS cnt")
                    rec = res.single()
                    if not rec or rec["cnt"] == 0:
                        break
                except Exception:
                    break

    def create_indexes(self):
        if not self.driver:
            raise RuntimeError(f"[{self.name}] Cannot create indexes before connecting")

        expected = {
            "node_id_idx": "id",
            "node_cat_idx": "category",
            "node_yr_idx": "year",
        }
        with self.driver.session() as session:
            for index_name, prop in expected.items():
                session.run(
                    f"CREATE INDEX {index_name} IF NOT EXISTS FOR (n:Node) ON (n.{prop})"
                ).consume()

            # Neo4j index population is asynchronous. Do not benchmark until every
            # required index is ONLINE and its schema matches the expected property.
            session.run("CALL db.awaitIndexes(300)").consume()
            records = session.run(
                """
                SHOW INDEXES YIELD name, state, labelsOrTypes, properties
                WHERE name IN $names
                RETURN name, state, labelsOrTypes, properties
                """,
                names=list(expected),
            )
            observed = {record["name"]: record.data() for record in records}

        problems = []
        for index_name, prop in expected.items():
            details = observed.get(index_name)
            if details is None:
                problems.append(f"{index_name} is missing")
                continue
            if details["state"] != "ONLINE":
                problems.append(f"{index_name} state={details['state']}")
            if list(details["labelsOrTypes"]) != ["Node"] or list(details["properties"]) != [prop]:
                problems.append(
                    f"{index_name} has schema {details['labelsOrTypes']}/{details['properties']}"
                )
        if problems:
            raise RuntimeError(
                f"[{self.name}] Required index verification failed: {'; '.join(problems)}"
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
                ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", "benchmark-neo4j"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if res.returncode == 0 and res.stdout.strip():
                return f"Observed Container: {res.stdout.strip()} (JVM Heap 192MB, PageCache 64MB)"
        except Exception:
            pass
        return (
            "Not observed; configured allocation: 0.5 vCPU, 512MB container "
            "(JVM Heap 192MB, PageCache 64MB)"
        )
