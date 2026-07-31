"""Keep README exit-code enumeration synchronized with the CLI constants."""

import re
from pathlib import Path

from simplebroker._constants import EXIT_ERROR, EXIT_QUEUE_EMPTY, EXIT_SUCCESS

ROOT = Path(__file__).resolve().parents[1]
CLI_CONTRACT = ROOT / "docs" / "specs" / "10-cli.md"


def test_readme_exit_code_section_is_complete() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    section = readme.split("### Exit Codes", 1)[1].split("\n## ", 1)[0]
    documented = {
        int(code) for code in re.findall(r"^- `(\d+)`", section, re.MULTILINE)
    }

    assert documented == {EXIT_SUCCESS, EXIT_ERROR, EXIT_QUEUE_EMPTY}


def test_cli_contract_sb_cli_1_codes_match_constants() -> None:
    text = CLI_CONTRACT.read_text(encoding="utf-8")
    section = text.split("## Exit code set [SB-CLI-1]", 1)[1].split("## ", 1)[0]
    documented = {
        int(code) for code in re.findall(r"^\| `(\d+)` \|", section, re.MULTILINE)
    }
    assert documented == {EXIT_SUCCESS, EXIT_ERROR, EXIT_QUEUE_EMPTY}


def test_readme_exit_codes_link_cli_contract() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    section = readme.split("### Exit Codes", 1)[1].split("\n## ", 1)[0]
    assert "docs/specs/10-cli.md" in section


def test_command_layer_does_not_advertise_an_extra_exit_code() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    section = readme.split("### Command layer", 1)[1].split("\n## ", 1)[0]

    assert "`124`" not in section
