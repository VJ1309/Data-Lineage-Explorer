from __future__ import annotations


def split_column_id(col_id: str) -> tuple[str, str]:
    """Split a column ID into (table, column) at the last dot.

    Always uses rsplit so "catalog.schema.table.col" yields
    ("catalog.schema.table", "col"), not ("catalog", "schema.table.col").
    """
    idx = col_id.rfind(".")
    if idx == -1:
        return (col_id, "")
    return (col_id[:idx], col_id[idx + 1:])
