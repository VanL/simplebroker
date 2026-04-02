"""Internal target resolution types for backend-aware CLI plumbing."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ResolvedTarget:
    """Resolved backend target used internally by CLI and connection plumbing."""

    backend_name: str
    target: str
    backend_options: dict[str, Any] = field(default_factory=dict)
    project_root: Path | None = None
    config_path: Path | None = None
    used_project_scope: bool = False
    legacy_sqlite_path_mode: bool = False

    @property
    def target_path(self) -> Path | None:
        """Return a filesystem path for sqlite targets, else ``None``."""
        if self.backend_name != "sqlite":
            return None
        return Path(self.target)

    @property
    def plugin(self):  # type: ignore[no-untyped-def]
        """Resolve the backend plugin lazily to keep targets data-only."""
        from ._backend_plugins import get_backend_plugin

        return get_backend_plugin(self.backend_name)


__all__ = ["ResolvedTarget"]
