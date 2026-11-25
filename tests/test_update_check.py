"""Install facts in :mod:`gflow_cli.update_check`, and the removed self-update surface.

gflow-plus installs from GitHub: there is no `gflow update` and no PyPI update notice,
because an index upgrade would replace it with upstream gflow-cli.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from gflow_cli import __version__
from gflow_cli import update_check as uc
from gflow_cli.cli import main


class TestInstalledFromIndex:
    def _with_direct_url(self, monkeypatch: pytest.MonkeyPatch, payload: str | None) -> bool:
        class _Dist:
            def read_text(self, name: str) -> str | None:
                assert name == "direct_url.json"
                return payload

        def _dist(name: str) -> _Dist:
            assert name == "gflow-plus"
            return _Dist()

        monkeypatch.setattr(uc, "distribution", _dist)
        return uc._installed_from_index()

    def test_no_direct_url_is_index_install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        assert self._with_direct_url(monkeypatch, None) is True

    @pytest.mark.parametrize(
        "payload",
        [
            json.dumps({"url": "https://github.com/MOMO0302-02/gflow-plus", "vcs_info": {}}),
            json.dumps({"url": "file:///dev/gflow-plus", "dir_info": {"editable": True}}),
        ],
    )
    def test_github_and_source_installs_are_not(
        self, monkeypatch: pytest.MonkeyPatch, payload: str
    ) -> None:
        assert self._with_direct_url(monkeypatch, payload) is False

    def test_missing_distribution_is_not(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _raise(name: str) -> object:
            raise uc.PackageNotFoundError(name)

        monkeypatch.setattr(uc, "distribution", _raise)
        assert uc._installed_from_index() is False


_REQUIRES = [
    "httpx>=0.27",
    'pytest>=8.0.0; extra == "dev"',
    'patchright==1.64.4; extra == "patchright"',
    "av>=12; extra == 'chain'",
    'pillow>=13.0.0; extra == "chain"',
    'universal_pathlib>=0.2.5; extra == "gcs"',
    'gcsfs>=2024.2.0; extra == "gcs"',
]


@pytest.fixture
def installed_dists(monkeypatch: pytest.MonkeyPatch) -> set[str]:
    """`distribution()` answers for gflow-plus (with `_REQUIRES`) and the names in the set."""
    present = {"gflow-plus"}

    class _Dist:
        requires = _REQUIRES

    def _dist(name: str) -> _Dist:
        if name not in present:
            raise uc.PackageNotFoundError(name)
        return _Dist()

    monkeypatch.setattr(uc, "distribution", _dist)
    return present


class TestInstalledExtras:
    def test_an_extra_counts_when_every_requirement_is_installed(
        self, installed_dists: set[str]
    ) -> None:
        installed_dists |= {"patchright", "av", "pillow", "universal_pathlib", "pytest"}
        # gcs lacks gcsfs, so a shared dependency alone does not claim it; dev never counts.
        assert uc._installed_extras() == ("chain", "patchright")

    def test_no_extras_installed(self, installed_dists: set[str]) -> None:
        assert uc._installed_extras() == ()


class TestInstaller:
    @pytest.fixture
    def index_install(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
        monkeypatch.setattr(uc, "_installed_from_index", lambda: True)
        monkeypatch.setattr(uc.sys, "prefix", str(tmp_path))
        return tmp_path

    def test_uv_tool_receipt(self, index_install: Path) -> None:
        (index_install / "uv-receipt.toml").write_text("[tool]\n", encoding="utf-8")
        assert uc.installer() == uc.Installer("uv")

    def test_pipx_metadata(self, index_install: Path) -> None:
        (index_install / "pipx_metadata.json").write_text("{}", encoding="utf-8")
        assert uc.installer() == uc.Installer("pipx")

    def test_plain_venv(self, index_install: Path) -> None:
        assert uc.installer() == uc.Installer("pip")

    def test_github_install_has_no_index_installer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(uc, "_installed_from_index", lambda: False)
        assert uc.installer() is None


def test_extra_hint_installs_from_this_release_on_github(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(uc, "_installed_extras", lambda: ("patchright",))
    hint = uc.extra_install_hint("tts")
    spec = (
        f"gflow-plus[patchright,tts] @ git+https://github.com/MOMO0302-02/gflow-plus@v{__version__}"
    )
    assert f'"{spec}"' in hint
    assert "uv sync --extra tts" in hint
    assert "gflow-cli[" not in hint  # never the upstream PyPI package


def test_there_is_no_update_command() -> None:
    res = CliRunner().invoke(main, ["update"])
    assert res.exit_code == 2
    assert "No such command" in res.output
