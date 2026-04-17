"""Module-level in-memory state. All state is lost on server restart."""
from typing import Any
import networkx as nx

# source_registry maps source_id -> SourceConfig dict
source_registry: dict[str, dict[str, Any]] = {}

# Merged lineage DAG across all registered sources
lineage_graph: nx.DiGraph = nx.DiGraph()

# Raw lineage DAG (before temp-view resolution) — used for path tracing
raw_graph: nx.DiGraph = nx.DiGraph()

# Parse warnings from the last refresh of each source
parse_warnings: list[dict[str, str]] = []
