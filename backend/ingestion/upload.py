"""ZIP file ingestion — extract and classify files into FileRecords."""
from __future__ import annotations
import io
import zipfile
from lineage.models import FileRecord

_EXT_TYPE = {
    ".ipynb": "notebook",
    ".py": "python",
    ".sql": "sql",
}


def ingest_zip(zip_bytes: bytes, source_ref: str) -> list[FileRecord]:
    """Extract a ZIP archive and return one FileRecord per supported file."""
    records: list[FileRecord] = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in zf.namelist():
                ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
                file_type = _EXT_TYPE.get(ext)
                if file_type is None:
                    continue
                content = zf.read(name).decode("utf-8", errors="replace")
                records.append(FileRecord(
                    path=name,
                    content=content,
                    type=file_type,
                    source_ref=source_ref,
                ))
    except zipfile.BadZipFile:
        pass
    return records
