"""Tests for post-upgrade logged-in account sync across home directories and state DB."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from gflow_cli.cli import main
from gflow_cli.config import reset_settings
from gflow_cli.plus import mcp_plus, mcp_plus_ops
from gflow_cli.plus.config import PlusSettings, reset_plus_settings
from gflow_cli.plus.pool.browser_pool import WarmBrowserPool
from gflow_cli.plus.pool.manager import PoolJobManager, reset_pool_manager
from gflow_cli.plus.pool.scheduler import AccountPoolScheduler
from gflow_cli.plus.pool.state_store import PlusStateStore
from gflow_cli.plus.pool.sync import sync_accounts_into_pool
from gflow_cli.profile_lease import ProfileLease
from gflow_cli.profile_store import (
    ACCOUNT_FILE,
    CONFIG_FILENAME,
    PROFILE_DIR_PREFIX,
    get_default_profile,
    list_profiles,
)


def _create_profile(
    home: Path,
    name: str,
    *,
    with_cookies: bool = True,
    cookie_bytes: bytes = b"cookie-v1",
    cookie_mtime: float | None = None,
    email: str | None = None,
    extra_files: dict[str, bytes] | None = None,
) -> Path:
    pdir = home / f"{PROFILE_DIR_PREFIX}{name}"
    pdir.mkdir(parents=True, exist_ok=True)
    if with_cookies:
        cookies = pdir / "Default" / "Network" / "Cookies"
        cookies.parent.mkdir(parents=True, exist_ok=True)
        cookies.write_bytes(cookie_bytes)
        if cookie_mtime is not None:
            os.utime(cookies, (cookie_mtime, cookie_mtime))
    if email is not None:
        (pdir / ACCOUNT_FILE).write_text(f"{email}\n", encoding="utf-8")
    for rel, content in (extra_files or {}).items():
        fpath = pdir / rel
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_bytes(content)
    return pdir


def _configure_home(monkeypatch: pytest.MonkeyPatch, target_home: Path) -> None:
    monkeypatch.setenv("GFLOW_CLI_HOME", str(target_home))
    reset_settings()
    reset_plus_settings()
    reset_pool_manager()


def _make_scheduler(
    target_home: Path,
    *,
    excluded_profiles: str = "",
    credits_map: dict[str, int] | None = None,
    refresh_calls: list[tuple[str, bool]] | None = None,
    browser_pool: WarmBrowserPool | None = None,
) -> AccountPoolScheduler:
    async def fake_inspect(name: str, *, refresh: bool = False) -> dict[str, Any]:
        if refresh_calls is not None:
            refresh_calls.append((name, refresh))
        return {
            "profile": name,
            "authenticated": True,
            "credits": (credits_map or {}).get(name, 100),
        }

    settings = PlusSettings(
        state_db_path=target_home / "plus_state.db",
        excluded_profiles=excluded_profiles,
        warm_browser_ttl_s=0.0,
    )
    store = PlusStateStore(target_home / "plus_state.db")
    return AccountPoolScheduler(
        store=store,
        settings=settings,
        profile_lister=list_profiles,
        credits_inspector=fake_inspect,
        browser_pool=browser_pool or WarmBrowserPool(),
    )


@pytest.mark.asyncio
async def test_sync_auto_discovers_default_home_imports_accounts_and_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    default_home = tmp_path / "legacy_default_home"
    target_home = tmp_path / "target_home"
    target_home.mkdir()
    _configure_home(monkeypatch, target_home)
    monkeypatch.setattr("gflow_cli.plus.pool.sync.paths.default_home", lambda: default_home)

    _create_profile(
        default_home,
        "alpha",
        cookie_bytes=b"alpha-cookie",
        email="alpha@example.com",
        extra_files={
            "SingletonLock": b"stale-lock",
            "DevToolsActivePort": b"9222",
            "Default/LOCK": b"chrome-lock",
            "Default/Preferences": b"{}",
        },
    )
    _create_profile(default_home, "beta", cookie_bytes=b"beta-cookie", email="beta@example.com")
    _create_profile(default_home, "empty_shell", with_cookies=False)
    (default_home / CONFIG_FILENAME).write_text('default_profile = "beta"\n', encoding="utf-8")

    legacy_store = PlusStateStore(default_home / "plus_state.db")
    legacy_store.upsert_account_state(
        "alpha",
        credits=1234,
        credits_checked_at=500.0,
        cooldown_until=9999999999.0,
        cooldown_reason="transient_error",
        last_error="old crash",
    )
    legacy_store.upsert_account_state("beta", credits=5678, credits_checked_at=500.0, disabled=True)

    refresh_calls: list[tuple[str, bool]] = []
    scheduler = _make_scheduler(
        target_home,
        credits_map={"alpha": 1500, "beta": 6000},
        refresh_calls=refresh_calls,
    )

    result = await sync_accounts_into_pool(
        scheduler,
        target_home=target_home,
        refresh=True,
        renew=True,
    )

    assert result["status"] == "ok"
    assert [item["profile"] for item in result["imported"]] == ["alpha", "beta"]
    assert result["updated"] == []
    assert any(
        s["profile"] == "empty_shell" and s["reason"] == "source_no_cookies"
        for s in result["skipped"]
    )
    assert result["default_profile_synced"] == "beta"
    assert get_default_profile() == "beta"
    assert sorted(result["state_imported"]) == ["alpha", "beta"]
    assert result["refreshed"] is True
    assert sorted(refresh_calls) == [("alpha", True), ("beta", True)]

    alpha_dir = target_home / f"{PROFILE_DIR_PREFIX}alpha"
    assert (alpha_dir / "Default" / "Network" / "Cookies").read_bytes() == b"alpha-cookie"
    assert (alpha_dir / "Default" / "Preferences").exists()
    assert not (alpha_dir / "SingletonLock").exists()
    assert not (alpha_dir / "DevToolsActivePort").exists()

    # Cooldown and last_error from the old home were NOT carried over; disabled was preserved.
    alpha_state = scheduler.store.get_account_state("alpha")
    beta_state = scheduler.store.get_account_state("beta")
    assert alpha_state["credits"] == 1500
    assert alpha_state["cooldown_until"] is None
    assert alpha_state["last_error"] == ""
    assert beta_state["disabled"] == 1


@pytest.mark.asyncio
async def test_sync_conflict_policies_skip_existing_repair_empty_and_overwrite_newer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src_home = tmp_path / "src_home"
    target_home = tmp_path / "target_home"
    target_home.mkdir()
    _configure_home(monkeypatch, target_home)
    monkeypatch.setattr("gflow_cli.plus.pool.sync.paths.default_home", lambda: target_home)

    # Target has: keep_me (newer), replace_me (older), broken_shell (no cookies)
    _create_profile(target_home, "keep_me", cookie_bytes=b"target-newer", cookie_mtime=2000.0)
    _create_profile(target_home, "replace_me", cookie_bytes=b"target-older", cookie_mtime=1000.0)
    _create_profile(target_home, "broken_shell", with_cookies=False)

    # Source has matching profiles
    _create_profile(src_home, "keep_me", cookie_bytes=b"source-older", cookie_mtime=1500.0)
    _create_profile(src_home, "replace_me", cookie_bytes=b"source-newer", cookie_mtime=2500.0)
    _create_profile(src_home, "broken_shell", cookie_bytes=b"source-fixed", cookie_mtime=1800.0)

    scheduler = _make_scheduler(target_home)

    # 1. Default policy: never overwrite existing valid profiles, only repair empty shell.
    first = await sync_accounts_into_pool(
        scheduler,
        target_home=target_home,
        from_homes=[src_home],
        overwrite_newer=False,
        refresh=False,
    )
    assert [u["profile"] for u in first["updated"]] == ["broken_shell"]
    assert {s["profile"]: s["reason"] for s in first["skipped"]} == {
        "keep_me": "already_exists",
        "replace_me": "already_exists",
    }
    assert (
        target_home / f"{PROFILE_DIR_PREFIX}replace_me" / "Default" / "Network" / "Cookies"
    ).read_bytes() == b"target-older"
    assert (
        target_home / f"{PROFILE_DIR_PREFIX}broken_shell" / "Default" / "Network" / "Cookies"
    ).read_bytes() == b"source-fixed"

    # 2. overwrite_newer=True: replace_me updates, keep_me stays untouched.
    second = await sync_accounts_into_pool(
        scheduler,
        target_home=target_home,
        from_homes=[src_home],
        overwrite_newer=True,
        refresh=False,
    )
    assert [u["profile"] for u in second["updated"]] == ["replace_me"]
    assert (
        target_home / f"{PROFILE_DIR_PREFIX}replace_me" / "Default" / "Network" / "Cookies"
    ).read_bytes() == b"source-newer"
    assert (
        target_home / f"{PROFILE_DIR_PREFIX}keep_me" / "Default" / "Network" / "Cookies"
    ).read_bytes() == b"target-newer"
    assert any(
        s["profile"] == "keep_me" and s["reason"] == "target_is_newer_or_equal"
        for s in second["skipped"]
    )


@pytest.mark.asyncio
async def test_sync_refuses_to_overwrite_busy_or_leased_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src_home = tmp_path / "src_home"
    target_home = tmp_path / "target_home"
    target_home.mkdir()
    _configure_home(monkeypatch, target_home)
    monkeypatch.setattr("gflow_cli.plus.pool.sync.paths.default_home", lambda: target_home)

    _create_profile(target_home, "busy_acc", cookie_bytes=b"old-busy", cookie_mtime=1000.0)
    leased_dir = _create_profile(
        target_home, "leased_acc", cookie_bytes=b"old-leased", cookie_mtime=1000.0
    )
    _create_profile(src_home, "busy_acc", cookie_bytes=b"new-busy", cookie_mtime=3000.0)
    _create_profile(src_home, "leased_acc", cookie_bytes=b"new-leased", cookie_mtime=3000.0)

    scheduler = _make_scheduler(target_home)
    scheduler.mark_busy("busy_acc", True)

    with ProfileLease(leased_dir):
        res = await sync_accounts_into_pool(
            scheduler,
            target_home=target_home,
            from_homes=[src_home],
            overwrite_newer=True,
            refresh=False,
        )

    assert res["updated"] == []
    reasons = {s["profile"]: s["reason"] for s in res["skipped"]}
    assert reasons == {"busy_acc": "target_busy", "leased_acc": "target_busy"}
    assert (
        target_home / f"{PROFILE_DIR_PREFIX}busy_acc" / "Default" / "Network" / "Cookies"
    ).read_bytes() == b"old-busy"
    assert (leased_dir / "Default" / "Network" / "Cookies").read_bytes() == b"old-leased"


@pytest.mark.asyncio
async def test_sync_prunes_only_deleted_profiles_and_keeps_excluded_on_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_home = tmp_path / "target_home"
    target_home.mkdir()
    _configure_home(monkeypatch, target_home)
    monkeypatch.setattr("gflow_cli.plus.pool.sync.paths.default_home", lambda: target_home)

    _create_profile(target_home, "active_acc")
    _create_profile(target_home, "excluded_acc")

    scheduler = _make_scheduler(target_home, excluded_profiles="excluded_acc")
    scheduler.store.upsert_account_state("active_acc", credits=200)
    scheduler.store.upsert_account_state("excluded_acc", credits=300)
    scheduler.store.upsert_account_state("ghost_acc", credits=999)

    res = await sync_accounts_into_pool(
        scheduler,
        target_home=target_home,
        prune_state=True,
        refresh=False,
    )

    assert res["pruned_state_profiles"] == ["ghost_acc"]
    stored = scheduler.store.list_account_states()
    assert set(stored) == {"active_acc", "excluded_acc"}


@pytest.mark.asyncio
async def test_sync_dry_run_makes_no_disk_db_or_network_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src_home = tmp_path / "src_home"
    target_home = tmp_path / "target_home"
    target_home.mkdir()
    _configure_home(monkeypatch, target_home)
    monkeypatch.setattr("gflow_cli.plus.pool.sync.paths.default_home", lambda: src_home)

    _create_profile(src_home, "incoming", email="in@example.com")
    (src_home / CONFIG_FILENAME).write_text('default_profile = "incoming"\n', encoding="utf-8")

    refresh_calls: list[tuple[str, bool]] = []
    scheduler = _make_scheduler(target_home, refresh_calls=refresh_calls)
    scheduler.store.upsert_account_state("stale_ghost", credits=50)

    res = await sync_accounts_into_pool(
        scheduler,
        target_home=target_home,
        prune_state=True,
        refresh=True,
        dry_run=True,
    )

    assert res["dry_run"] is True
    assert res["refreshed"] is False
    assert [i["profile"] for i in res["imported"]] == ["incoming"]
    assert res["default_profile_synced"] == "incoming"
    assert res["pruned_state_profiles"] == ["stale_ghost"]
    assert not (target_home / f"{PROFILE_DIR_PREFIX}incoming").exists()
    assert not (target_home / CONFIG_FILENAME).exists()
    assert "stale_ghost" in scheduler.store.list_account_states()
    assert refresh_calls == []


@pytest.mark.asyncio
async def test_sync_skips_unsafe_profile_names_and_same_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src_home = tmp_path / "src_home"
    target_home = tmp_path / "target_home"
    target_home.mkdir()
    _configure_home(monkeypatch, target_home)
    monkeypatch.setattr("gflow_cli.plus.pool.sync.paths.default_home", lambda: target_home)

    _create_profile(src_home, "bad name with spaces", cookie_bytes=b"bad")
    scheduler = _make_scheduler(target_home)

    res = await sync_accounts_into_pool(
        scheduler,
        target_home=target_home,
        from_homes=[target_home, src_home],
        refresh=False,
    )

    assert res["imported"] == []
    assert any(
        s["profile"] == "bad name with spaces" and s["reason"] == "unsafe_name"
        for s in res["skipped"]
    )
    assert res["source_homes_scanned"] == [str(src_home.resolve())]


@pytest.mark.asyncio
async def test_mcp_sync_tool_handles_preview_sync_and_bad_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src_home = tmp_path / "src_home"
    target_home = tmp_path / "target_home"
    target_home.mkdir()
    _configure_home(monkeypatch, target_home)
    monkeypatch.setattr("gflow_cli.plus.pool.sync.paths.default_home", lambda: target_home)

    _create_profile(src_home, "mcp_acc", email="mcp@example.com")

    scheduler = _make_scheduler(target_home)
    manager = PoolJobManager(scheduler=scheduler)
    monkeypatch.setattr(mcp_plus, "_pool", lambda: manager)

    preview = await mcp_plus_ops.gflow_plus_sync_accounts(
        from_homes=[str(src_home)],
        refresh=False,
        dry_run=True,
    )
    assert preview["status"] == "ok"
    assert preview["dry_run"] is True
    assert [i["profile"] for i in preview["imported"]] == ["mcp_acc"]

    bad = await mcp_plus_ops.gflow_plus_sync_accounts(
        from_homes=[str(tmp_path / "missing_dir")],
        refresh=False,
    )
    assert bad["status"] == "error"
    assert "does not exist" in bad["error"]["detail"]


def test_cli_sync_command_imports_profiles_and_renders_json_and_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src_home = tmp_path / "src_home"
    target_home = tmp_path / "target_home"
    target_home.mkdir()
    _configure_home(monkeypatch, target_home)
    monkeypatch.setattr("gflow_cli.plus.pool.sync.paths.default_home", lambda: target_home)

    _create_profile(src_home, "cli_acc", email="cli@example.com")
    _create_profile(src_home, "second_acc", email="second@example.com")

    runner = CliRunner()
    cli_res = runner.invoke(
        main,
        ["plus", "sync", "--from-home", str(src_home), "--no-refresh", "--json"],
    )
    assert cli_res.exit_code == 0, cli_res.output
    assert "cli_acc" in cli_res.output
    assert "second_acc" in cli_res.output
    assert (target_home / f"{PROFILE_DIR_PREFIX}cli_acc").is_dir()
    assert (target_home / f"{PROFILE_DIR_PREFIX}second_acc").is_dir()

    table_res = runner.invoke(
        main,
        ["plus", "sync", "--from-home", str(src_home), "--no-refresh"],
    )
    assert table_res.exit_code == 0, table_res.output
    assert "Account sync complete" in table_res.output

    bad_res = runner.invoke(
        main,
        ["plus", "sync", "--from-home", str(tmp_path / "missing_dir"), "--no-refresh"],
    )
    assert bad_res.exit_code == 2
