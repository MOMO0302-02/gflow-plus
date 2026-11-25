"""Tests for operational MCP tools, cross-account media query, and sync-tool hiding."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from gflow_cli.config import reset_settings
from gflow_cli.data.store import DataStore
from gflow_cli.mcp import server as mcp_server_mod
from gflow_cli.plus import mcp_plus, mcp_plus_ops
from gflow_cli.plus.config import PlusSettings
from gflow_cli.plus.pool.browser_pool import WarmBrowserPool
from gflow_cli.plus.pool.manager import PoolJobManager
from gflow_cli.plus.pool.models import ExecutionOutcome
from gflow_cli.plus.pool.scheduler import AccountPoolScheduler
from gflow_cli.plus.pool.state_store import PlusStateStore
from gflow_cli.profile_store import ProfileMeta

_UUID_IMG = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_UUID_VID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
_UUID_PROJ = "cccccccc-cccc-cccc-cccc-cccccccccccc"


def _seed_catalog(db_path: Path, out_dir: Path) -> tuple[Path, Path]:
    img_file = out_dir / "hero.png"
    img_file.write_bytes(b"png")
    vid_file = out_dir / "clip.mp4"
    vid_file.write_bytes(b"mp4")

    store = DataStore.open(db_path)
    store.close()
    con = sqlite3.connect(db_path)
    try:
        with con:
            con.execute(
                "INSERT INTO projects "
                "(id, profile_name, flow_project_id, title, source, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("proj-a", "acc_a", _UUID_PROJ, "Proj A", "created", "2026-11-24T10:00:00Z"),
            )
            con.execute(
                "INSERT INTO projects "
                "(id, profile_name, flow_project_id, title, source, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("proj-b", "acc_b", _UUID_PROJ, "Proj B", "created", "2026-11-24T10:01:00Z"),
            )
            con.execute(
                "INSERT INTO assets (id, profile_name, flow_project_id, flow_media_id, kind, "
                "status, model, aspect_ratio, duration_seconds, metadata_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "asset-img",
                    "acc_a",
                    _UUID_PROJ,
                    _UUID_IMG,
                    "image",
                    "ready",
                    "nano2",
                    "9:16",
                    None,
                    "{}",
                    "2026-11-24T10:02:00Z",
                ),
            )
            con.execute(
                "INSERT INTO local_files (id, profile_name, asset_id, path, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ("lf-img", "acc_a", "asset-img", str(img_file), "2026-11-24T10:02:01Z"),
            )
            con.execute(
                "INSERT INTO operations (id, profile_name, mode, status, prompt, model, "
                "flow_project_id, started_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "op-img",
                    "acc_a",
                    "t2i",
                    "succeeded",
                    "golden sunset over mountains",
                    "nano2",
                    _UUID_PROJ,
                    "2026-11-24T10:02:00Z",
                ),
            )
            con.execute(
                "INSERT INTO operation_assets (operation_id, asset_id, role, position) "
                "VALUES (?, ?, ?, ?)",
                ("op-img", "asset-img", "output", 0),
            )
            con.execute(
                "INSERT INTO assets (id, profile_name, flow_project_id, flow_media_id, kind, "
                "status, model, aspect_ratio, duration_seconds, metadata_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "asset-vid",
                    "acc_b",
                    _UUID_PROJ,
                    _UUID_VID,
                    "video",
                    "ready",
                    "omni-1.2-flash",
                    "9:16",
                    8.0,
                    "{}",
                    "2026-11-24T10:03:00Z",
                ),
            )
            con.execute(
                "INSERT INTO local_files (id, profile_name, asset_id, path, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ("lf-vid", "acc_b", "asset-vid", str(vid_file), "2026-11-24T10:03:01Z"),
            )
            con.execute(
                "INSERT INTO operations (id, profile_name, mode, status, prompt, model, "
                "flow_project_id, started_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "op-vid",
                    "acc_b",
                    "t2v",
                    "succeeded",
                    "cyberpunk rain alley",
                    "omni-1.2-flash",
                    _UUID_PROJ,
                    "2026-11-24T10:03:00Z",
                ),
            )
            con.execute(
                "INSERT INTO operation_assets (operation_id, asset_id, role, position) "
                "VALUES (?, ?, ?, ?)",
                ("op-vid", "asset-vid", "output", 0),
            )
    finally:
        con.close()
    return img_file, vid_file


@pytest.mark.asyncio
async def test_list_media_queries_across_all_accounts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    db_path = tmp_path / "gflow.db"
    img_file, vid_file = _seed_catalog(db_path, out_dir)

    monkeypatch.setenv("GFLOW_CLI_HOME", str(tmp_path))
    monkeypatch.setenv("GFLOW_CLI_DB_PATH", str(db_path))
    monkeypatch.setenv("GFLOW_CLI_OUTPUT_DIR", str(out_dir))
    reset_settings()
    try:
        all_res = await mcp_plus_ops.gflow_plus_list_media()
        assert all_res["status"] == "ok"
        assert all_res["count"] == 2
        ids = {item["media_id"] for item in all_res["items"]}
        assert ids == {_UUID_IMG, _UUID_VID}
        by_id = {item["media_id"]: item for item in all_res["items"]}
        assert by_id[_UUID_IMG]["profile"] == "acc_a"
        assert by_id[_UUID_IMG]["file_exists"] is True
        assert by_id[_UUID_IMG]["local_path"] == str(img_file)
        assert by_id[_UUID_VID]["profile"] == "acc_b"
        assert by_id[_UUID_VID]["duration"] == 8.0
        assert by_id[_UUID_VID]["local_path"] == str(vid_file)

        filtered = await mcp_plus_ops.gflow_plus_list_media(query="cyberpunk")
        assert filtered["count"] == 1
        assert filtered["items"][0]["media_id"] == _UUID_VID

        img_only = await mcp_plus_ops.gflow_plus_list_media(kind="image", profile="acc_a")
        assert img_only["count"] == 1
        assert img_only["items"][0]["media_id"] == _UUID_IMG
    finally:
        reset_settings()


@pytest.mark.asyncio
async def test_profile_control_exclude_include_clear_cooldown_and_renew(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PlusStateStore(tmp_path / "plus_state.db")
    store.upsert_account_state(
        "acc_a", cooldown_until=9999999999.0, cooldown_reason="rate_limited", last_error="429"
    )
    settings = PlusSettings(state_db_path=tmp_path / "plus_state.db", stagger_seconds=0.0)

    async def fake_inspect(name: str, *, refresh: bool = False) -> dict[str, Any]:
        return {"profile": name, "authenticated": True, "credits": 777 if refresh else 500}

    profiles = [
        ProfileMeta(
            name="acc_a",
            profile_dir=tmp_path / "p_a",
            cookies_present=True,
            last_used_at=None,
            is_default=True,
            google_account="a@example.com",
        )
    ]
    scheduler = AccountPoolScheduler(
        store=store,
        settings=settings,
        profile_lister=lambda: profiles,
        credits_inspector=fake_inspect,
        browser_pool=WarmBrowserPool(settings=settings),
    )
    manager = PoolJobManager(
        scheduler=scheduler,
        executor=lambda p, _t, _pl: _ok_outcome(p),
        settings=settings,
        store=store,
    )
    monkeypatch.setattr(mcp_plus, "_pool", lambda: manager)

    # Clear cooldown makes acc_a healthy again
    cleared = await mcp_plus_ops.gflow_plus_profile_control("clear_cooldown", profile="acc_a")
    assert cleared["status"] == "ok"
    assert scheduler.snapshot_health()[0].healthy is True

    # Exclude disables acc_a
    excluded = await mcp_plus_ops.gflow_plus_profile_control("exclude", profile="acc_a")
    assert excluded["disabled"] is True
    snap = scheduler.snapshot_health()[0]
    assert snap.healthy is False
    assert snap.disabled is True

    # Include re-enables acc_a
    included = await mcp_plus_ops.gflow_plus_profile_control("include", profile="acc_a")
    assert included["disabled"] is False
    assert scheduler.snapshot_health()[0].healthy is True

    # Renew refreshes credits
    renewed = await mcp_plus_ops.gflow_plus_profile_control("renew", profile="acc_a")
    assert renewed["status"] == "ok"
    assert renewed["account"]["credits"] == 777

    # Missing profile rejected for exclude
    bad = await mcp_plus_ops.gflow_plus_profile_control("exclude")
    assert bad["status"] == "error"


@pytest.mark.asyncio
async def test_submit_upscale_and_extend_confine_paths_and_auto_route(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    db_path = tmp_path / "gflow.db"
    _seed_catalog(db_path, out_dir)

    monkeypatch.setenv("GFLOW_CLI_HOME", str(tmp_path))
    monkeypatch.setenv("GFLOW_CLI_DB_PATH", str(db_path))
    monkeypatch.setenv("GFLOW_CLI_OUTPUT_DIR", str(out_dir))
    reset_settings()

    seen: list[tuple[str, str, dict[str, Any]]] = []

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        seen.append((profile, task_type, dict(payload)))
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=str(payload["media_id"]),
            output_paths=(str(out_dir / f"{task_type}.out"),),
        )

    store = PlusStateStore(tmp_path / "plus_state.db")
    settings = PlusSettings(state_db_path=tmp_path / "plus_state.db", stagger_seconds=0.0)
    profiles = [
        ProfileMeta(
            name=name,
            profile_dir=tmp_path / f"p_{name}",
            cookies_present=True,
            last_used_at=None,
            is_default=(name == "acc_a"),
            google_account=f"{name}@example.com",
        )
        for name in ("acc_a", "acc_b")
    ]
    scheduler = AccountPoolScheduler(
        store=store,
        settings=settings,
        profile_lister=lambda: profiles,
        credits_inspector=lambda name, **_kw: _fake_credits(name),
        jitter_range=lambda _s: (0.0, 0.0),
        browser_pool=WarmBrowserPool(settings=settings),
    )
    manager = PoolJobManager(
        scheduler=scheduler, executor=fake_exec, settings=settings, store=store
    )
    monkeypatch.setattr(mcp_plus, "_pool", lambda: manager)
    try:
        # Escaping out_file is refused
        refused = await mcp_plus_ops.gflow_plus_submit_upscale(
            _UUID_IMG, out_file=str(tmp_path / "escape.png")
        )
        assert refused["status"] == "error"

        # Valid upscale auto-routes to acc_a from catalog
        up = await mcp_plus_ops.gflow_plus_submit_upscale(_UUID_IMG, media_kind="image", scale="2k")
        assert up["status"] == "ok"
        waited = await mcp_plus.gflow_plus_wait_jobs(
            job_ids=[up["job"]["job_id"]], timeout_seconds=5.0
        )
        assert waited["done"] is True
        assert seen[-1][0] == "acc_a"
        assert seen[-1][1] == "upscale_image"
        assert seen[-1][2]["project_id"] == _UUID_PROJ

        # Valid extend auto-routes to acc_b from catalog
        ext = await mcp_plus_ops.gflow_plus_submit_extend(
            _UUID_VID, "camera pans into neon street", max_credits=40
        )
        assert ext["status"] == "ok"
        waited2 = await mcp_plus.gflow_plus_wait_jobs(
            job_ids=[ext["job"]["job_id"]], timeout_seconds=5.0
        )
        assert waited2["done"] is True
        assert seen[-1][0] == "acc_b"
        assert seen[-1][1] == "extend_video"
    finally:
        reset_settings()


def test_hide_sync_tools_policy_removes_only_blocking_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mcp_server_mod._register_surfaces()  # pyright: ignore[reportPrivateUsage]
    mgr = mcp_server_mod.server._tool_manager  # pyright: ignore[reportPrivateUsage]
    saved = {
        name: mgr.get_tool(name)
        for name in mcp_server_mod._SYNC_GENERATION_TOOLS  # pyright: ignore[reportPrivateUsage]
    }
    try:
        monkeypatch.setenv("GFLOW_PLUS_HIDE_SYNC_TOOLS", "1")
        mcp_server_mod._apply_hide_sync_policy()  # pyright: ignore[reportPrivateUsage]
        names = set(mgr._tools.keys())  # pyright: ignore[reportPrivateUsage]
        for sync_tool in mcp_server_mod._SYNC_GENERATION_TOOLS:  # pyright: ignore[reportPrivateUsage]
            assert sync_tool not in names
        assert "gflow_plus_submit_image" in names
        assert "gflow_plus_submit_video" in names
        assert "gflow_plus_submit_upscale" in names
        assert "gflow_plus_submit_extend" in names
        assert "gflow_plus_list_media" in names
    finally:
        for name, tool in saved.items():
            if tool is not None:
                mgr._tools[name] = tool  # pyright: ignore[reportPrivateUsage]


async def _ok_outcome(profile: str) -> ExecutionOutcome:
    return ExecutionOutcome(status="succeeded", profile=profile)


async def _fake_credits(name: str) -> dict[str, Any]:
    return {"profile": name, "authenticated": True, "credits": 1000}
