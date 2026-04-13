from lineage.models import (
    FileRecord, ColumnNode, LineageEdge,
    SourceConfig, ParseWarning
)


def test_column_node_id_format():
    node = ColumnNode(
        id="orders.amount",
        table="orders",
        column="amount",
        dtype="double",
        source_file="pipeline.py",
        source_cell=None,
        source_line=10,
    )
    assert node.id == "orders.amount"
    assert node.table == "orders"


def test_lineage_edge_fields():
    edge = LineageEdge(
        source_col="orders.amount",
        target_col="revenue.total",
        transform_type="aggregation",
        expression="SUM(amount)",
        source_file="pipeline.py",
        source_cell=None,
        source_line=10,
    )
    assert edge.transform_type == "aggregation"
    assert edge.expression == "SUM(amount)"


def test_file_record_types():
    for t in ("notebook", "python", "sql"):
        r = FileRecord(path="f", content="c", type=t, source_ref="repo")
        assert r.type == t


def test_source_config_fields():
    cfg = SourceConfig(
        id="src-1",
        source_type="git",
        url="https://github.com/org/repo",
        token="ghp_test",
    )
    assert cfg.source_type == "git"


def test_parse_warning_fields():
    w = ParseWarning(file="bad.py", error="SyntaxError: invalid syntax")
    assert "SyntaxError" in w.error
