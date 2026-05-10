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
class TraceStepWrite:
    """One real-column write inside a Trace Step."""
    column_id: str
    expression: str | None
    transform_type: str
    source_line: int | None


@dataclass
class TraceStepPredicate:
    """One WHERE / HAVING / QUALIFY predicate inside a Trace Step."""
    kind: Literal["where", "having", "qualify"]
    expression: str | None
    source_columns: list[str]
    source_line: int | None


@dataclass
class TraceStepJoin:
    """One JOIN ON clause inside a Trace Step."""
    expression: str | None
    source_columns: list[str]
    source_line: int | None


@dataclass
class TraceStep:
    """One source-table-bounded step of a Lineage Trace.

    Returned by engine.lineage_trace(). Groups the writes that landed on the
    inspected column from a single source-table-and-file boundary, plus the
    filter / join predicates that constrained those writes — including
    predicates rolled up from collapsed temp views (named in via_temp_views).
    Routes reshape this into JSON via api/routes.py::_trace_step_to_dict; never
    via dataclasses.asdict (per the documented backend-parser-state-refactor
    pattern).
    """
    kind: Literal["sql", "pyspark"]
    source_file: str
    source_cell: int | None
    source_line: int | None
    target_table: str
    writes: list[TraceStepWrite] = field(default_factory=list)
    filters: list[TraceStepPredicate] = field(default_factory=list)
    joins: list[TraceStepJoin] = field(default_factory=list)
    via_temp_views: list[str] = field(default_factory=list)
    upstream_columns: list[str] = field(default_factory=list)


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
