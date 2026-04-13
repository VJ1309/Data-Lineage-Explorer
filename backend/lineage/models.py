from dataclasses import dataclass
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
