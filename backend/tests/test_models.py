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
    assert node.id == f"{node.table}.{node.column}"


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


def test_lineage_edge_has_qualified_field_defaulting_true():
    e = LineageEdge(source_col="a.b", target_col="c.d", transform_type="passthrough")
    assert e.qualified is True


def test_lineage_edge_qualified_can_be_set_false():
    e = LineageEdge(
        source_col="a.b", target_col="c.d",
        transform_type="passthrough", qualified=False,
    )
    assert e.qualified is False


def test_parse_warning_default_severity_is_warn():
    w = ParseWarning(file="x.sql", error="oops")
    assert w.severity == "warn"


def test_parse_warning_severity_can_be_error():
    w = ParseWarning(file="x.sql", error="boom", severity="error")
    assert w.severity == "error"
