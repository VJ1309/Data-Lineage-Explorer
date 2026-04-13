"""Databricks Workspace ingestion using databricks-sdk."""
from __future__ import annotations
import base64
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectType
from lineage.models import FileRecord

_LANG_TYPE: dict[str, str | None] = {
    "PYTHON": "python",
    "SQL": "sql",
    "SCALA": None,
    "R": None,
}


def ingest_databricks(host: str, token: str, source_ref: str) -> list[FileRecord]:
    """Export all notebooks from a Databricks workspace and return FileRecords.

    Walks the entire workspace recursively starting from '/'.
    Skips SCALA and R notebooks.
    """
    client = WorkspaceClient(host=host, token=token)
    records: list[FileRecord] = []

    def _walk(path: str, depth: int = 0) -> None:
        if depth > 20:  # safety cap
            return
        try:
            items = list(client.workspace.list(path=path))
        except Exception as exc:
            if path == "/":
                raise RuntimeError(f"Failed to list Databricks workspace root: {exc}") from exc
            return  # sub-path failures are non-fatal
        for item in items:
            if item.path is None:
                continue
            if item.object_type == ObjectType.DIRECTORY:
                _walk(item.path, depth + 1)
            elif item.object_type == ObjectType.NOTEBOOK:
                if item.language is None:
                    continue
                lang = item.language.value
                file_type = _LANG_TYPE.get(lang)
                if file_type is None:
                    continue
                try:
                    export_resp = client.workspace.export(
                        path=item.path,
                        format=ExportFormat.SOURCE,
                    )
                    content = export_resp.content
                    if content is None:
                        continue
                    decoded = base64.b64decode(content).decode("utf-8", errors="replace")
                    records.append(FileRecord(
                        path=item.path,
                        content=decoded,
                        type=file_type,
                        source_ref=source_ref,
                    ))
                except Exception:
                    continue

    _walk("/")
    return records
