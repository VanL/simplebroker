"""Canonical resolution for queue operands that may carry the alias sigil.

One implementation shared by every surface that accepts a queue operand:
``BrokerConnection.canonicalize_queue`` on each backend, and the CLI command
layer. Keeping it here means the sigil rule cannot drift between backends or
between the CLI and the library.
"""

from __future__ import annotations

from collections.abc import Callable

from ._constants import ALIAS_PREFIX


def resolve_queue_operand(
    name: str,
    resolve_alias: Callable[[str], str | None],
) -> tuple[str, str | None]:
    """Resolve a queue operand to ``(canonical_queue, alias_used)``.

    A plain name is always the literal queue -- aliases resolve only behind the
    ``@`` sigil, so a queue and an alias of the same name never collide.

    Args:
        name: Queue operand, optionally ``@``-prefixed.
        resolve_alias: Maps an alias key to its target, or ``None`` if undefined.

    Returns:
        The canonical queue name, and the alias key used (``None`` for a
        literal name).

    Raises:
        ValueError: The sigil is present but the alias is empty or undefined.
    """
    if not name.startswith(ALIAS_PREFIX):
        return name, None

    alias_key = name[len(ALIAS_PREFIX) :]
    if not alias_key:
        raise ValueError("Alias name cannot be empty")

    target = resolve_alias(alias_key)
    if target is None:
        raise ValueError(f"Alias '{alias_key}' is not defined")
    return target, alias_key
