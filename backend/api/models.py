from __future__ import annotations
from dataclasses import dataclass, field
from lineage.models import FileRecord, ParseWarning


@dataclass
class SourceEntry:
    """Source metadata. Use to_public_dict() for serialization — not dataclasses.asdict()."""
    id: str
    source_type: str
    url: str
    status: str = "registered"
    file_count: int = 0
    warning_count: int = 0
    # private fields — excluded from API responses via to_public_dict()
    token: str = ""
    records: list[FileRecord] = field(default_factory=list)
    parsed_files: set[str] = field(default_factory=set)
    file_stats: dict[str, dict] = field(default_factory=dict)
    error_files: set[str] = field(default_factory=set)

    def to_public_dict(self) -> dict:
        return {
            "id": self.id,
            "source_type": self.source_type,
            "url": self.url,
            "status": self.status,
            "file_count": self.file_count,
            "warning_count": self.warning_count,
        }


@dataclass
class StoredWarning:
    """ParseWarning + the source_id that produced it.

    Wraps the engine-layer ParseWarning so state.parse_warnings can be typed
    without leaking source-id awareness into the parser models. /warnings
    reshapes via to_public_dict() at the API boundary.
    """
    warning: ParseWarning
    source_id: str

    def to_public_dict(self) -> dict:
        return {
            "file": self.warning.file,
            "error": self.warning.error,
            "severity": self.warning.severity,
            "source_id": self.source_id,
        }
