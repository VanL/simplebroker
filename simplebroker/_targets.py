"""Internal target resolution types for backend-aware CLI plumbing."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

_CONNINFO_PASSWORD_RE = re.compile(
    r"(?i)(\bpassword\s*=\s*)(?:'[^']*'|\"[^\"]*\"|[^\s]+)"
)
_WHITESPACE_RE = re.compile(r"\s")


def _redact_parsed_url_password(target: str) -> str:
    parsed = urlsplit(target)
    if not parsed.scheme or not parsed.netloc or not parsed.password:
        return target

    userinfo, separator, hostport = parsed.netloc.rpartition("@")
    if not separator or ":" not in userinfo:
        return target

    username = userinfo.split(":", 1)[0]
    safe_netloc = f"{username}:***@{hostport}"
    return urlunsplit(
        (parsed.scheme, safe_netloc, parsed.path, parsed.query, parsed.fragment)
    )


def _redact_raw_url_userinfo(target: str) -> str:
    scheme_end = target.find("://")
    if scheme_end == -1:
        return target

    authority_start = scheme_end + 3
    whitespace = _WHITESPACE_RE.search(target, authority_start)
    authority_end = whitespace.start() if whitespace else len(target)
    authority = target[authority_start:authority_end]

    colon_index = authority.find(":")
    at_index = authority.rfind("@")
    if colon_index == -1 or at_index == -1 or colon_index > at_index:
        return target

    password_start = authority_start + colon_index + 1
    at_absolute = authority_start + at_index
    return f"{target[:password_start]}***{target[at_absolute:]}"


def redact_backend_target(target: str) -> str:
    """Return a display-safe backend target with password material redacted."""
    redacted = _redact_raw_url_userinfo(_redact_parsed_url_password(target))
    return _CONNINFO_PASSWORD_RE.sub(r"\1***", redacted)


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
    def display_target(self) -> str:
        """Return a display-safe target string for CLI output."""
        if self.backend_name == "sqlite":
            return self.target
        return redact_backend_target(self.target)

    @property
    def plugin(self):  # type: ignore[no-untyped-def]
        """Resolve the backend plugin lazily to keep targets data-only."""
        from ._backend_plugins import get_backend_plugin

        return get_backend_plugin(self.backend_name)


__all__ = ["ResolvedTarget", "redact_backend_target"]
