from abc import ABC, abstractmethod
from typing import Any


class BaseGraphAdapter(ABC):
    """
    Abstract base class defining the standardized interface for all graph database adapters.
    Ensures identical logical operations, fair query execution, and consistent metric tracking.
    """

    def __init__(
        self,
        name: str,
        hardware_spec: str = "0.5 vCPU, 512MB container",
        *,
        platform_type: str = "Database platform",
        query_interface: str = "Not specified",
        storage_engine: str = "Not disclosed",
        index_strategy: str = "Not specified",
        index_readiness: str = "Not specified",
        ingestion_method: str = "Driver-parameterized batches",
        max_node_sub_batch_size: int | None = None,
        max_edge_sub_batch_size: int | None = None,
    ):
        self.name = name
        self.hardware_spec = hardware_spec
        self.platform_type = platform_type
        self.query_interface = query_interface
        self.storage_engine = storage_engine
        self.index_strategy = index_strategy
        self.index_readiness = index_readiness
        self.ingestion_method = ingestion_method
        self.max_node_sub_batch_size = max_node_sub_batch_size
        self.max_edge_sub_batch_size = max_edge_sub_batch_size
        self.driver = None

    def describe_ingestion(self, requested_batch_size: int) -> str:
        """Describe the actual driver and sub-batch sizes used by this adapter."""
        node_batch = min(requested_batch_size, self.max_node_sub_batch_size or requested_batch_size)
        edge_batch = min(requested_batch_size, self.max_edge_sub_batch_size or requested_batch_size)
        return (
            f"{self.ingestion_method}; requested outer batch={requested_batch_size:,}; "
            f"effective node batch<={node_batch:,}; effective edge batch<={edge_batch:,}"
        )

    def get_benchmark_metadata(self) -> dict[str, Any]:
        """Return adapter-owned facts for result provenance and reporting."""
        return {
            "platform_type": self.platform_type,
            "query_interface": self.query_interface,
            "storage_engine": self.storage_engine,
            "index_strategy": self.index_strategy,
            "index_readiness": self.index_readiness,
            "ingestion_method": self.ingestion_method,
            "max_node_sub_batch_size": self.max_node_sub_batch_size,
            "max_edge_sub_batch_size": self.max_edge_sub_batch_size,
        }

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the graph database."""

    @abstractmethod
    def close(self):
        """Close connection and clean up resources."""

    @abstractmethod
    def clear_database(self):
        """Wipe existing nodes and edges to ensure clean slate for benchmarking."""

    @abstractmethod
    def create_indexes(self):
        """Create primary and secondary indexes on node properties."""

    @abstractmethod
    def ingest_nodes_batch(self, batch: list[dict[str, Any]]) -> int:
        """Ingest a batch of nodes."""

    @abstractmethod
    def ingest_edges_batch(self, batch: list[dict[str, Any]]) -> int:
        """Ingest a batch of edges."""

    @abstractmethod
    def traversal_1hop(self, node_id: int) -> int:
        """Count undirected one-hop paths from node_id in the collaboration graph."""

    @abstractmethod
    def traversal_2hop(self, node_id: int) -> int:
        """Count undirected two-hop paths from node_id in the collaboration graph."""

    @abstractmethod
    def traversal_3hop(self, node_id: int) -> int:
        """Count undirected three-hop paths from node_id in the collaboration graph."""

    @abstractmethod
    def point_lookup(self, node_id: int) -> dict[str, Any] | None:
        """Point lookup for a specific node by ID."""

    @abstractmethod
    def filtered_lookup(self, category: str, min_year: int) -> int:
        """Filtered lookup on indexed attributes (category and year)."""

    @abstractmethod
    def aggregation_category_counts(self) -> dict[str, int]:
        """Aggregate node counts grouped by category."""

    @abstractmethod
    def mixed_read_write(self, read_node_id: int, write_node_id: int, new_score: float) -> bool:
        """Execute an undirected one-hop read plus a node property update."""

    @abstractmethod
    def count_nodes(self) -> int:
        """Count total nodes stored in the database."""

    @abstractmethod
    def count_edges(self) -> int:
        """Count total edges/relationships stored in the database."""

    @abstractmethod
    def get_resource_footprint(self) -> str:
        """Return observable memory/disk footprint or 'not observable'."""
