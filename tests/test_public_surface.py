"""Public embedding-surface contract for ``simplebroker.commands``.

``simplebroker.commands`` is documented public embedding surface (the
programmatic CLI-equivalent operations). This pins its ``__all__`` as an exact
ordered list so additions/removals are deliberate. See the README "Command
layer" subsection.
"""

from __future__ import annotations


def test_commands_all_exact_ordered_list() -> None:
    """`simplebroker.commands.__all__` equals exactly this ordered 20-name list."""
    from simplebroker import commands

    expected = [
        "cmd_write",
        "cmd_read",
        "cmd_peek",
        "cmd_exists",
        "cmd_stats",
        "cmd_list",
        "cmd_delete",
        "cmd_move",
        "cmd_broadcast",
        "cmd_vacuum",
        "cmd_watch",
        "cmd_init",
        "cmd_status",
        "cmd_alias_list",
        "cmd_alias_add",
        "cmd_alias_remove",
        "cmd_rename",
        "cmd_dump",
        "cmd_load",
        "parse_exact_message_id",
    ]

    # LIST equality (order included), by contrast with the ext surface which is
    # compared as a set.
    assert list(commands.__all__) == expected

    # Every exported name is actually importable from the module.
    for name in expected:
        assert hasattr(commands, name), f"{name} missing from simplebroker.commands"
