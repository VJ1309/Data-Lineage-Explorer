from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    import networkx as nx


@dataclass
class FileRecord:
    path: str
    content: str
    type: Literal["notebook", "python", "sql"]
    source_ref: str


@dataclass
class ColumnNode:
    id: str            # "{table}.{column}"
    table: str
    column: str
    dtype: str | None
    source_file: str
    source_cell: int | None
    source_line: int | None

    def __post_init__(self) -> None:
        if self.id != f"{self.table}.{self.column}":
            raise ValueError(
                f"ColumnNode.id must equal '{{table}}.{{column}}', "
                f"got {self.id!r} (table={self.table!r}, column={self.column!r})"
            )


@dataclass
class ColumnMeta:
    """Per-column metadata returned by engine.column_metadata().

    Carries the predecessor-aggregated source tables and expressions for one
    column node, plus the LineageEdge from preds[0] (None when the column has
    no predecessors). Routes shape this into the API dict.
    """
    node_id: str
    table: str
    column: str
    source_tables: list[str]
    expressions: list[str]
    edge_data: "LineageEdge | None"


@dataclass
class LineageEdge:
    source_col: str
    target_col: str
    transform_type: Literal[
        "passthrough", "aggregation", "expression",
        "join_key", "window", "cast", "filter"
    ]
    expression: str | None = None
    source_file: str = ""
    source_cell: int | None = None
    source_line: int | None = None
    confidence: Literal["certain", "approximate"] = "certain"
    qualified: bool = True


@dataclass
class SourceConfig:
    id: str
    source_type: Literal["git", "databricks", "upload"]
    url: str
    token: str | None = None


@dataclass
class ParseWarning:
    file: str
    error: str
    severity: Literal["info", "warn", "error"] = "warn"


@dataclass
class ParseResult:
    """Structured return type for all parsers."""
    edges: list[LineageEdge] = field(default_factory=list)
    raw_edges: list[LineageEdge] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class GraphResult:
    """Engine return type for build_graph_with_warnings.

    Carries both the deduped lineage DAG and the raw (pre-temp-view-resolution) DAG
    that path tracing needs. raw_graph is populated whenever graph is — preserving
    the dual-graph invariant documented in backend/AGENTS.md.

    file_stats and error_files are populated by the engine's per-file statistics
    helper so route handlers don't iterate edges themselves.
    """
    graph: "nx.DiGraph"
    raw_graph: "nx.DiGraph"
    warnings: list[ParseWarning] = field(default_factory=list)
    file_stats: dict[str, dict] = field(default_factory=dict)
    error_files: set[str] = field(default_factory=set)
