from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal


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

    def __iter__(self):
        return iter(self.edges)

    def __len__(self):
        return len(self.edges)

    def __getitem__(self, key):
        return self.edges[key]
