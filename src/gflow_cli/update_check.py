"""How this gflow-plus install was made: facts for telemetry and optional-extra hints.

gflow-plus is installed from its GitHub repository and has no self-update command or
update notice: an index "upgrade" would replace it with upstream gflow-cli from PyPI.
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

from gflow_cli import __version__

DIST_NAME = "gflow-plus"
REPO_URL = "https://github.com/MOMO0302-02/gflow-plus"


def _installed_from_index() -> bool:
    """True only for an index-installed wheel. PEP 610: index installs write no
    ``direct_url.json``; a GitHub, local-path or editable install always has one, and a
    pure source run has no distribution at all."""
    try:
        return distribution(DIST_NAME).read_text("direct_url.json") is None
    except PackageNotFoundError:
        return False


def in_ci() -> bool:
    return os.environ.get("CI", "").strip().lower() not in ("", "0", "false")


@dataclass(frozen=True)
class Installer:
    name: str  # "uv" | "pipx" | "pip"


_EXTRA_MARKER = re.compile(r"""extra\s*==\s*["']([^"']+)["']""")
_REQ_NAME = re.compile(r"[A-Za-z0-9._-]+")


def _installed_extras() -> tuple[str, ...]:
    """The extras whose every requirement is installed, so a hint keeps them."""
    try:
        requires = distribution(DIST_NAME).requires or []
    except PackageNotFoundError:
        return ()
    by_extra: dict[str, list[str]] = {}
    for req in requires:
        spec, _, marker = req.partition(";")
        extra, name = _EXTRA_MARKER.search(marker), _REQ_NAME.match(spec.strip())
        if extra and name:
            by_extra.setdefault(extra[1], []).append(name[0])

    def present(name: str) -> bool:
        try:
            distribution(name)
        except PackageNotFoundError:
            return False
        return True

    # `dev` is the contributor toolchain, never a user extra.
    return tuple(
        sorted(e for e, names in by_extra.items() if e != "dev" and all(map(present, names)))
    )


def installer() -> Installer | None:
    """Which package manager owns an index install, read off the venv root: ``uv tool``
    writes ``uv-receipt.toml`` and pipx writes ``pipx_metadata.json``. None for anything
    that is not an index install."""
    if not _installed_from_index():
        return None
    prefix = Path(sys.prefix)
    if (prefix / "uv-receipt.toml").is_file():
        return Installer("uv")
    if (prefix / "pipx_metadata.json").is_file():
        return Installer("pipx")
    return Installer("pip")


def extra_install_hint(extra: str) -> str:
    """The command that adds ``extra`` to this install, keeping the extras it already has."""
    extras = ",".join(sorted({*_installed_extras(), extra}))
    spec = f"{DIST_NAME}[{extras}] @ git+{REPO_URL}@v{__version__}"
    return (
        f'uv pip install --python "{sys.executable}" "{spec}", '
        f"or `uv sync --extra {extra}` in a source checkout"
    )
