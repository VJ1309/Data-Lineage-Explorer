import io
import zipfile
from ingestion.upload import ingest_zip


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def test_ingest_zip_sql_file():
    zip_bytes = _make_zip({"queries/agg.sql": "SELECT amount FROM raw_orders"})
    records = ingest_zip(zip_bytes, source_ref="upload")
    assert len(records) == 1
    assert records[0].type == "sql"
    assert records[0].path == "queries/agg.sql"


def test_ingest_zip_notebook():
    zip_bytes = _make_zip({"pipeline.ipynb": '{"nbformat":4,"cells":[],"metadata":{},"nbformat_minor":5}'})
    records = ingest_zip(zip_bytes, source_ref="upload")
    assert len(records) == 1
    assert records[0].type == "notebook"


def test_ingest_zip_python():
    zip_bytes = _make_zip({"etl/pipeline.py": "df = spark.read.table('orders')"})
    records = ingest_zip(zip_bytes, source_ref="upload")
    assert records[0].type == "python"


def test_ingest_zip_ignores_unknown_extensions():
    zip_bytes = _make_zip({
        "README.md": "# readme",
        "query.sql": "SELECT 1",
    })
    records = ingest_zip(zip_bytes, source_ref="upload")
    assert len(records) == 1
    assert records[0].path == "query.sql"


def test_ingest_zip_source_ref_set():
    zip_bytes = _make_zip({"q.sql": "SELECT 1"})
    records = ingest_zip(zip_bytes, source_ref="my-upload-42")
    assert records[0].source_ref == "my-upload-42"
