"""Tests for WarmBrowserPool: reuse, eviction on errors, idle TTL reap, and refresh eviction."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from gflow_cli.errors import WafRejectionError
from gflow_cli.plus.config import PlusSettings
from gflow_cli.plus.pool.browser_pool import WarmBrowserPool
from gflow_cli.plus.pool.scheduler import AccountPoolScheduler
from gflow_cli.plus.pool.state_store import PlusStateStore
from gflow_cli.profile_store import ProfileMeta


class _FakeContext:
    def __init__(self) -> None:
        self.closed = False

    def is_closed(self) -> bool:
        return self.closed


class _FakeClient:
    def __init__(
        self,
        *,
        profile_dir: Path,
        headless: bool,
        transport: str,
        out_dir: Path,
        settings: Any = None,
    ) -> None:
        self.profile_dir = profile_dir
        self.headless = headless
        self.transport = transport
        self.out_dir = out_dir
        self.settings = settings
        self._context: _FakeContext | None = None
        self.enters = 0
        self.exits = 0

    async def __aenter__(self) -> _FakeClient:
        self.enters += 1
        self._context = _FakeContext()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        self.exits += 1
        if self._context is not None:
            self._context.closed = True


@pytest.mark.asyncio
async def test_warm_pool_reuses_live_client_and_updates_out_dir(tmp_path: Path) -> None:
    created: list[_FakeClient] = []

    def factory(**kwargs: Any) -> _FakeClient:
        client = _FakeClient(**kwargs)
        created.append(client)
        return client

    pool = WarmBrowserPool(settings=PlusSettings(warm_browser_ttl_s=60.0), client_factory=factory)
    try:
        async with pool.checkout(
            "acc_a",
            profile_dir=tmp_path / "p_a",
            headless=False,
            transport="ui_automation",
            out_dir=tmp_path / "out1",
        ) as c1:
            assert c1.enters == 1
            assert c1.out_dir == tmp_path / "out1"

        assert pool.warm_profiles() == ["acc_a"]

        async with pool.checkout(
            "acc_a",
            profile_dir=tmp_path / "p_a",
            headless=False,
            transport="ui_automation",
            out_dir=tmp_path / "out2",
        ) as c2:
            assert c2 is c1
            assert c2.enters == 1
            assert c2.out_dir == tmp_path / "out2"
        assert len(created) == 1
        assert created[0].exits == 0
    finally:
        closed = await pool.close_all()
        assert closed == 1
        assert created[0].exits == 1


@pytest.mark.asyncio
async def test_warm_pool_rebuilds_on_transport_mismatch_and_evicts_on_waf(tmp_path: Path) -> None:
    created: list[_FakeClient] = []
    pool = WarmBrowserPool(
        settings=PlusSettings(warm_browser_ttl_s=60.0),
        client_factory=lambda **kw: created.append(c := _FakeClient(**kw)) or c,
    )
    try:
        async with pool.checkout(
            "acc_a",
            profile_dir=tmp_path / "p_a",
            headless=False,
            transport="ui_automation",
            out_dir=tmp_path,
        ) as first:
            pass

        async with pool.checkout(
            "acc_a",
            profile_dir=tmp_path / "p_a",
            headless=True,
            transport="ui_automation",
            out_dir=tmp_path,
        ) as second:
            assert second is not first
            assert first.exits == 1

        with pytest.raises(WafRejectionError):
            async with pool.checkout(
                "acc_a",
                profile_dir=tmp_path / "p_a",
                headless=True,
                transport="ui_automation",
                out_dir=tmp_path,
            ):
                raise WafRejectionError("unusual activity")

        assert second.exits == 1
        assert pool.warm_profiles() == []
    finally:
        await pool.close_all()


@pytest.mark.asyncio
async def test_warm_pool_reaps_idle_browser_after_ttl(tmp_path: Path) -> None:
    created: list[_FakeClient] = []
    pool = WarmBrowserPool(
        settings=PlusSettings(warm_browser_ttl_s=0.05),
        client_factory=lambda **kw: created.append(c := _FakeClient(**kw)) or c,
    )
    try:
        async with pool.checkout(
            "acc_a",
            profile_dir=tmp_path / "p_a",
            headless=False,
            transport="ui_automation",
            out_dir=tmp_path,
        ):
            pass
        assert pool.warm_profiles() == ["acc_a"]
        await asyncio.sleep(0.15)
        assert pool.warm_profiles() == []
        assert created[0].exits == 1
    finally:
        await pool.close_all()


@pytest.mark.asyncio
async def test_warm_pool_disabled_when_ttl_zero(tmp_path: Path) -> None:
    created: list[_FakeClient] = []
    pool = WarmBrowserPool(
        settings=PlusSettings(warm_browser_ttl_s=0.0),
        client_factory=lambda **kw: created.append(c := _FakeClient(**kw)) or c,
    )
    async with pool.checkout(
        "acc_a",
        profile_dir=tmp_path / "p_a",
        headless=False,
        transport="ui_automation",
        out_dir=tmp_path,
    ):
        pass
    assert pool.warm_profiles() == []
    assert created[0].exits == 1


@pytest.mark.asyncio
async def test_scheduler_refresh_evicts_warm_browser_before_inspect(tmp_path: Path) -> None:
    events: list[str] = []
    pool = WarmBrowserPool(
        settings=PlusSettings(warm_browser_ttl_s=60.0),
        client_factory=lambda **kw: _FakeClient(**kw),
    )
    async with pool.checkout(
        "acc_a",
        profile_dir=tmp_path / "p_a",
        headless=False,
        transport="ui_automation",
        out_dir=tmp_path,
    ):
        pass
    assert pool.warm_profiles() == ["acc_a"]

    async def fake_inspect(name: str, *, refresh: bool = False) -> dict[str, Any]:
        del refresh
        assert name not in pool.warm_profiles(), "warm browser must be evicted before inspect"
        events.append(f"inspect:{name}")
        return {"profile": name, "authenticated": True, "credits": 500}

    scheduler = AccountPoolScheduler(
        store=PlusStateStore(tmp_path / "plus_state.db"),
        settings=PlusSettings(state_db_path=tmp_path / "plus_state.db", stagger_seconds=0.0),
        profile_lister=lambda: [
            ProfileMeta(
                name="acc_a",
                profile_dir=tmp_path / "p_a",
                cookies_present=True,
                last_used_at=None,
                is_default=True,
                google_account="a@example.com",
            )
        ],
        credits_inspector=fake_inspect,
        browser_pool=pool,
    )
    await scheduler.refresh_credits(force=True)
    assert events == ["inspect:acc_a"]
    assert pool.warm_profiles() == []
