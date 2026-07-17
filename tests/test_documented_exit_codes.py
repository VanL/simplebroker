"""Keep README exit-code enumeration synchronized with the CLI constants."""

import re
from pathlib import Path

from simplebroker._constants import EXIT_ERROR, EXIT_QUEUE_EMPTY, EXIT_SUCCESS


def test_readme_exit_code_section_is_complete() -> None:
    readme = (Path(__file__).parents[1] / "README.md").read_text(encoding="utf-8")
    section = readme.split("### Exit Codes", 1)[1].split("\n## ", 1)[0]
    documented = {int(code) for code in re.findall(r"^- `(\d+)`", section, re.M)}

    assert documented == {EXIT_SUCCESS, EXIT_ERROR, EXIT_QUEUE_EMPTY}


def test_command_layer_does_not_advertise_an_extra_exit_code() -> None:
    readme = (Path(__file__).parents[1] / "README.md").read_text(encoding="utf-8")
    section = readme.split("### Command layer", 1)[1].split("\n## ", 1)[0]

    assert "`124`" not in section
