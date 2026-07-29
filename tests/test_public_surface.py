"""Public embedding-surface contract for ``simplebroker.commands``.

``simplebroker.commands`` is documented public embedding surface (the
programmatic CLI-equivalent operations). This pins the exact names in
``__all__`` so additions and removals are deliberate; ordering remains
formatter/linter-owned presentation. See the README "Command layer" subsection.
"""

from __future__ import annotations


def test_commands_all_exact_public_surface() -> None:
    """`simplebroker.commands.__all__` exposes exactly the supported names."""
    from simplebroker import commands

    expected = {
        "cmd_alias_add",
        "cmd_alias_list",
        "cmd_alias_remove",
        "cmd_broadcast",
        "cmd_delete",
        "cmd_dump",
        "cmd_exists",
        "cmd_init",
        "cmd_list",
        "cmd_load",
        "cmd_move",
        "cmd_peek",
        "cmd_read",
        "cmd_rename",
        "cmd_stats",
        "cmd_status",
        "cmd_vacuum",
        "cmd_watch",
        "cmd_write",
        "parse_exact_message_id",
    }

    assert set(commands.__all__) == expected

    # Every exported name is actually importable from the module.
    for name in expected:
        assert hasattr(commands, name), f"{name} missing from simplebroker.commands"
