"""Git repository ingestion using GitPython."""
from __future__ import annotations
import tempfile
import os
from pathlib import Path
import git
from lineage.models import FileRecord

_EXT_TYPE = {
    ".ipynb": "notebook",
    ".py": "python",
    ".sql": "sql",
}


def ingest_git(url: str, token: str | None, source_ref: str) -> list[FileRecord]:
    """Clone a Git repo to a temp directory and return FileRecords for all supported files.

    For authenticated repos, embeds the token into the HTTPS URL:
    https://<token>@github.com/org/repo.git
    """
    if token:
        if url.startswith("https://"):
            auth_url = url.replace("https://", f"https://{token}@", 1)
        else:
            auth_url = url
    else:
        auth_url = url

    records: list[FileRecord] = []
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            git.Repo.clone_from(auth_url, tmpdir, depth=1)
        except git.GitCommandError as exc:
            raise RuntimeError(f"Git clone failed for {url} (credentials redacted)") from exc

        for root, _dirs, files in os.walk(tmpdir):
            if ".git" in Path(root).parts:
                continue
            for fname in files:
                ext = Path(fname).suffix.lower()
                file_type = _EXT_TYPE.get(ext)
                if file_type is None:
                    continue
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, tmpdir)
                try:
                    content = Path(full_path).read_text(encoding="utf-8", errors="replace")
                except OSError:
                    continue
                records.append(FileRecord(
                    path=rel_path.replace("\\", "/"),
                    content=content,
                    type=file_type,
                    source_ref=source_ref,
                ))

    return records
