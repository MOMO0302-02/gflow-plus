"""`gflow plus` keeps its own command snapshot and docs, so upstream's stay untouched."""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

import gflow_cli.mcp.tools as upstream_tools
from gflow_cli._cli_helpers import cli_leaves, spends_credits_decision
from gflow_cli.cli import main
from gflow_cli.mcp.server import server
from gflow_cli.plus import mcp_plus

DOC = Path(__file__).resolve().parents[2] / "docs" / "plus" / "README.md"

EXPECTED_PLUS_TREE = {
    "plus health": False,
    "plus pool status": False,
    "plus pool pick": False,
    "plus pool jobs": False,
    "plus pool retry": True,
    "plus pool profile-control": False,
    "plus media": False,
    "plus upscale": True,
    "plus extend": True,
    "plus run-batch": True,
    "plus sync": False,
    "plus hide-taskbar": False,
}


def _plus_leaves() -> dict[str, bool | None]:
    return {
        " ".join(path): spends_credits_decision(cmd)
        for path, cmd, _ in cli_leaves(main)
        if path[0] == "plus"
    }


def test_plus_command_snapshot() -> None:
    assert _plus_leaves() == EXPECTED_PLUS_TREE


def test_every_plus_command_and_tool_is_documented() -> None:
    text = DOC.read_text(encoding="utf-8")
    missing = [
        f"gflow {name}"
        for name in _plus_leaves()
        if not re.search(re.escape(f"gflow {name}") + r"(?![\w-])", text)
    ]
    tools = [n for n in server._tool_manager._tools if n.startswith("gflow_plus_")]  # pyright: ignore[reportPrivateUsage]
    missing += [n for n in tools if f"`{n}`" not in text]
    assert tools
    assert not missing, f"Not documented in docs/plus/README.md: {missing}"


@pytest.fixture
def output_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    out = tmp_path / "out"
    out.mkdir()
    monkeypatch.setattr(
        upstream_tools, "get_settings", lambda: SimpleNamespace(output_dir=out, storage_uri=None)
    )
    return out


def test_write_paths_resolve_inside_the_output_dir(output_dir: Path) -> None:
    item = {"out_file": "batch1/shot_01.png", "out_dir": "batch1"}
    assert mcp_plus._confine_outputs(item) is None  # pyright: ignore[reportPrivateUsage]
    assert item == {
        "out_file": str((output_dir / "batch1" / "shot_01.png").resolve()),
        "out_dir": str((output_dir / "batch1").resolve()),
    }


@pytest.mark.parametrize("key", ["out_file", "output_file", "out_dir"])
def test_a_write_path_outside_the_output_dir_is_refused(output_dir: Path, key: str) -> None:
    outside = str(output_dir.parent / "elsewhere" / "x.png")
    refused = mcp_plus._confine_outputs({key: outside})  # pyright: ignore[reportPrivateUsage]
    assert refused is not None
    assert refused["status"] == "error"
    assert "outside the configured output directory" in refused["error"]["detail"]
