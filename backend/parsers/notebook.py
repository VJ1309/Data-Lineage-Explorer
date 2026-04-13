"""Notebook parser using nbformat. Routes cells to SQL or PySpark parsers."""
from __future__ import annotations
import nbformat
from parsers.sql import parse_sql
from parsers.pyspark import parse_pyspark
from lineage.models import LineageEdge


_SQL_MAGICS = ("%sql", "%%sql", "%spark.sql")


def _is_sql_cell(source: str) -> bool:
    stripped = source.strip()
    return any(stripped.startswith(magic) for magic in _SQL_MAGICS)


def _strip_sql_magic(source: str) -> str:
    stripped = source.strip()
    for magic in _SQL_MAGICS:
        if stripped.startswith(magic):
            return stripped[len(magic):].strip()
    return stripped


def parse_notebook(
    content: str,
    source_file: str,
) -> list[LineageEdge]:
    """Parse a Jupyter notebook JSON string and return all lineage edges."""
    try:
        nb = nbformat.reads(content, as_version=4)
    except Exception:
        return []

    edges: list[LineageEdge] = []

    for cell_idx, cell in enumerate(nb.cells):
        if cell.cell_type != "code":
            continue

        source = cell.source
        if not source.strip():
            continue

        lang = cell.get("metadata", {}).get("language", "")

        if _is_sql_cell(source) or lang == "sql":
            sql = _strip_sql_magic(source)
            cell_edges = parse_sql(
                sql,
                source_file=source_file,
                source_line=None,
                source_cell=cell_idx,
            )
        else:
            cell_edges = parse_pyspark(
                source,
                source_file=source_file,
                source_cell=cell_idx,
            )

        edges.extend(cell_edges)

    return edges
