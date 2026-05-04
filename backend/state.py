"""Module-level in-memory state. All state is lost on server restart."""
import networkx as nx
from api.models import SourceEntry, StoredWarning

# source_registry maps source_id -> SourceEntry
source_registry: dict[str, SourceEntry] = {}

# Merged lineage DAG across all registered sources
lineage_graph: nx.DiGraph = nx.DiGraph()

# Raw lineage DAG (before temp-view resolution) — used for path tracing
raw_graph: nx.DiGraph = nx.DiGraph()

# Parse warnings from the last refresh of each source
parse_warnings: list[StoredWarning] = []
