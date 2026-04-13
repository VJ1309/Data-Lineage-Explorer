import json
from parsers.notebook import parse_notebook
from lineage.models import LineageEdge


def _make_notebook(cells: list[dict]) -> str:
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {"kernelspec": {"language": "python"}},
        "cells": cells,
    }
    return json.dumps(nb)


def _code_cell(source: str, language: str | None = None) -> dict:
    cell = {
        "cell_type": "code",
        "source": source,
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }
    if language:
        cell["metadata"]["language"] = language
    return cell


def test_sql_magic_cell_produces_edges():
    nb = _make_notebook([
        _code_cell("%sql SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id"),
    ])
    edges = parse_notebook(nb, source_file="nb.ipynb")
    assert len(edges) > 0
    agg = next((e for e in edges if e.transform_type == "aggregation"), None)
    assert agg is not None


def test_pyspark_cell_produces_edges():
    nb = _make_notebook([
        _code_cell(
            'df = spark.read.table("raw_orders")\n'
            'df2 = df.select("order_id")\n'
            'df2.write.saveAsTable("staging")\n'
        ),
    ])
    edges = parse_notebook(nb, source_file="nb.ipynb")
    assert any(e.target_col == "staging.order_id" for e in edges)


def test_cell_index_attached():
    nb = _make_notebook([
        _code_cell("x = 1"),  # cell 0 — no lineage
        _code_cell("%sql SELECT amount FROM raw_orders"),  # cell 1
    ])
    edges = parse_notebook(nb, source_file="nb.ipynb")
    for e in edges:
        assert e.source_cell == 1


def test_markdown_cells_skipped():
    nb = _make_notebook([
        {"cell_type": "markdown", "source": "# Title", "metadata": {}},
        _code_cell("%sql SELECT amount FROM raw_orders"),
    ])
    edges = parse_notebook(nb, source_file="nb.ipynb")
    assert len(edges) > 0  # markdown didn't crash anything


def test_bad_json_returns_empty():
    edges = parse_notebook("not json at all", source_file="bad.ipynb")
    assert edges == []
