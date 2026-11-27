"""Tests for per-profile proxies, egress-group isolation/pacing, and pre-submit validation."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from gflow_cli.api.image import GenerateImageRequest
from gflow_cli.api.transports.base import TransportSetup
from gflow_cli.api.transports.direct_http import DirectHttpTransport
from gflow_cli.config import reset_settings
from gflow_cli.errors import QueueSchemaError
from gflow_cli.plus import mcp_plus, mcp_plus_ops
from gflow_cli.plus.config import PlusSettings, parse_profile_proxies
from gflow_cli.plus.pool.browser_pool import WarmBrowserPool
from gflow_cli.plus.pool.executor import validate_payload
from gflow_cli.plus.pool.manager import PoolJobManager
from gflow_cli.plus.pool.models import ExecutionOutcome
from gflow_cli.plus.pool.scheduler import AccountPoolScheduler
from gflow_cli.plus.pool.state_store import PlusStateStore
from gflow_cli.plus.pool.workflows import parse_batch_item
from gflow_cli.profile_store import ProfileMeta


def _three_profiles(tmp_path: Path) -> list[ProfileMeta]:
    return [
        ProfileMeta(
            name=name,
            profile_dir=tmp_path / f"profile_{name}",
            cookies_present=True,
            last_used_at=None,
            is_default=(name == "acc_a"),
            google_account=f"{name}@example.com",
        )
        for name in ("acc_a", "acc_b", "acc_c")
    ]


def _build_pool(
    tmp_path: Path,
    executor: Any,
    *,
    profile_proxies: str = "",
    max_concurrent_per_egress: int = 1,
    egress_min_interval_s: float = 0.0,
    egress_waf_cooldown_s: float = 180.0,
    now_ref: list[float] | None = None,
    sleeps: list[float] | None = None,
) -> tuple[PoolJobManager, AccountPoolScheduler]:
    clock_box = now_ref if now_ref is not None else [1000.0]

    async def fake_sleep(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)
        clock_box[0] += seconds

    async def fake_inspect(name: str, *, refresh: bool = False) -> dict[str, Any]:
        del refresh
        return {"profile": name, "authenticated": True, "credits": 1000}

    settings = PlusSettings(
        state_db_path=tmp_path / "plus_state.db",
        stagger_seconds=0.0,
        profile_proxies=profile_proxies,
        max_concurrent_per_egress=max_concurrent_per_egress,
        egress_min_interval_s=egress_min_interval_s,
        egress_waf_cooldown_s=egress_waf_cooldown_s,
        waf_cooldown_s=600.0,
        max_failover_attempts=3,
    )
    store = PlusStateStore(tmp_path / "plus_state.db")
    scheduler = AccountPoolScheduler(
        store=store,
        settings=settings,
        profile_lister=lambda: _three_profiles(tmp_path),
        credits_inspector=fake_inspect,
        clock=lambda: clock_box[0],
        sleeper=fake_sleep if sleeps is not None else asyncio.sleep,
        jitter_range=lambda _spec: (0.0, 0.0),
        browser_pool=WarmBrowserPool(settings=settings),
    )
    manager = PoolJobManager(scheduler=scheduler, executor=executor, settings=settings, store=store)
    return manager, scheduler


def test_parse_profile_proxies_normalizes_and_rejects_credentials() -> None:
    parsed = parse_profile_proxies(
        "mmdzxy=HTTP://127.0.0.1:7891/, mmdzxy04=socks5://LocalHost:1080"
    )
    assert parsed == {
        "mmdzxy": "http://127.0.0.1:7891",
        "mmdzxy04": "socks5://localhost:1080",
    }
    with pytest.raises(ValueError, match="credentials") as exc_info:
        parse_profile_proxies("mmdzxy=http://secretuser:secretpass@127.0.0.1:7891")
    assert "secretpass" not in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_three_accounts_two_residential_ips_runs_two_concurrent_never_colliding_same_ip(
    tmp_path: Path,
) -> None:
    """3 accounts sharing 2 residential IPs (2+1): max 2 concurrent, same IP strictly serialized."""
    active_by_egress: dict[str, int] = {}
    max_total = 0
    current_total = 0
    egress_map = {
        "acc_a": "http://127.0.0.1:7891",
        "acc_b": "http://127.0.0.1:7891",
        "acc_c": "http://127.0.0.1:7892",
    }

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        nonlocal max_total, current_total
        del task_type, payload
        egress = egress_map[profile]
        assert active_by_egress.get(egress, 0) == 0, f"Same-IP collision on {egress} ({profile})"
        active_by_egress[egress] = active_by_egress.get(egress, 0) + 1
        current_total += 1
        max_total = max(max_total, current_total)
        await asyncio.sleep(0.08)
        current_total -= 1
        active_by_egress[egress] -= 1
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=f"m-{profile}",
            output_paths=(str(tmp_path / f"{profile}.png"),),
        )

    manager, scheduler = _build_pool(
        tmp_path,
        fake_exec,
        profile_proxies=(
            "acc_a=http://127.0.0.1:7891,"
            "acc_b=http://127.0.0.1:7891,"
            "acc_c=http://127.0.0.1:7892"
        ),
        max_concurrent_per_egress=1,
        egress_min_interval_s=0.0,
    )
    report = scheduler.health_report()
    assert len(report["egress_groups"]) == 2

    submitted = await manager.submit_batch(
        [
            {"kind": "image", "prompt": "shot 1", "label": "s1"},
            {"kind": "image", "prompt": "shot 2", "label": "s2"},
            {"kind": "image", "prompt": "shot 3", "label": "s3"},
            {"kind": "image", "prompt": "shot 4", "label": "s4"},
        ]
    )
    waited = await manager.wait_jobs(batch_id=submitted["batch_id"], timeout_s=5.0)
    assert waited["done"] is True
    assert waited["counts"]["succeeded"] == 4
    assert max_total == 2


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_three_accounts_shared_single_ip_serializes_automatically(tmp_path: Path) -> None:
    """With no per-profile proxies configured, 3 accounts share 'direct' and serialize (max=1)."""
    max_total = 0
    current = 0

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        nonlocal max_total, current
        del task_type, payload
        current += 1
        max_total = max(max_total, current)
        await asyncio.sleep(0.05)
        current -= 1
        return ExecutionOutcome(status="succeeded", profile=profile)

    manager, _ = _build_pool(
        tmp_path,
        fake_exec,
        profile_proxies="",
        max_concurrent_per_egress=1,
        egress_min_interval_s=0.0,
    )
    submitted = await manager.submit_batch(
        [
            {"kind": "image", "prompt": "p1"},
            {"kind": "image", "prompt": "p2"},
            {"kind": "image", "prompt": "p3"},
        ]
    )
    waited = await manager.wait_jobs(batch_id=submitted["batch_id"], timeout_s=5.0)
    assert waited["done"] is True
    assert max_total == 1


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_waf_on_shared_egress_protects_sibling_and_fails_over_to_second_ip(
    tmp_path: Path,
) -> None:
    """WAF on acc_a cools egress #1 (shielding sibling acc_b) and fails over to acc_c on IP #2."""
    attempts: list[str] = []

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        del task_type, payload
        attempts.append(profile)
        if profile == "acc_a":
            return ExecutionOutcome(
                status="failed",
                profile=profile,
                error_type="waf-rejection",
                error_detail="PUBLIC_ERROR_UNUSUAL_ACTIVITY",
                charged="no",
            )
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id="ok-ip2",
            output_paths=(str(tmp_path / "ok.png"),),
        )

    manager, scheduler = _build_pool(
        tmp_path,
        fake_exec,
        profile_proxies=(
            "acc_a=http://127.0.0.1:7891,"
            "acc_b=http://127.0.0.1:7891,"
            "acc_c=http://127.0.0.1:7892"
        ),
        egress_waf_cooldown_s=180.0,
        egress_min_interval_s=0.0,
    )
    job = await manager.submit_job(
        kind="image",
        task_type="t2i",
        payload={"prompt": "test waf isolation"},
        preferred_profile="acc_a",
    )
    waited = await manager.wait_jobs(job_ids=[job.job_id], timeout_s=5.0)
    assert waited["done"] is True
    assert waited["jobs"][0]["status"] == "succeeded"
    # acc_b shares IP #1 with acc_a so it was skipped; acc_c on IP #2 took over cleanly!
    assert attempts == ["acc_a", "acc_c"]

    health = {p["profile"]: p for p in scheduler.health_report()["profiles"]}
    assert health["acc_a"]["healthy"] is False
    assert health["acc_b"]["healthy"] is False
    assert health["acc_b"]["egress_cooldown_remaining_s"] > 0
    assert health["acc_c"]["healthy"] is True


@pytest.mark.asyncio
async def test_egress_pacing_spaces_cross_account_switch_on_same_ip(tmp_path: Path) -> None:
    now = [2000.0]
    sleeps: list[float] = []
    _, scheduler = _build_pool(
        tmp_path,
        lambda p, _t, _pl: asyncio.sleep(0),
        profile_proxies="acc_a=http://127.0.0.1:7891,acc_b=http://127.0.0.1:7891",
        egress_min_interval_s=15.0,
        now_ref=now,
        sleeps=sleeps,
    )
    w1 = await scheduler.wait_egress_pacing("http://127.0.0.1:7891", "acc_a")
    assert w1 == 0.0
    now[0] += 5.0
    scheduler.note_egress_finished("http://127.0.0.1:7891", "acc_a")
    # Switching to sibling acc_b immediately after acc_a finishes waits the full 15s gap
    w2 = await scheduler.wait_egress_pacing("http://127.0.0.1:7891", "acc_b")
    assert pytest.approx(15.0) == w2
    assert sleeps == [15.0]
    # Another independent IP does not wait at all
    w3 = await scheduler.wait_egress_pacing("http://127.0.0.1:7892", "acc_c")
    assert w3 == 0.0


@pytest.mark.asyncio
async def test_profile_control_set_proxy_and_clear_proxy_evict_warm_browser(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    closed: list[str] = []

    class FakeClient:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.out_dir = kwargs.get("out_dir")
            self.settings = kwargs.get("settings")
            self._context = object()
            self._page = object()

        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *exc: object) -> None:
            closed.append(str(self.kwargs["profile_dir"]))

    settings = PlusSettings(
        state_db_path=tmp_path / "plus_state.db",
        stagger_seconds=0.0,
        egress_min_interval_s=0.0,
        warm_browser_ttl_s=60.0,
    )
    pool = WarmBrowserPool(settings=settings, client_factory=FakeClient)
    store = PlusStateStore(tmp_path / "plus_state.db")
    scheduler = AccountPoolScheduler(
        store=store,
        settings=settings,
        profile_lister=lambda: _three_profiles(tmp_path),
        browser_pool=pool,
    )
    async def noop_exec(
        profile: str, _task_type: str, _payload: dict[str, Any]
    ) -> ExecutionOutcome:
        return ExecutionOutcome(status="succeeded", profile=profile)

    manager = PoolJobManager(
        scheduler=scheduler,
        executor=noop_exec,
        settings=settings,
        store=store,
    )
    monkeypatch.setattr(mcp_plus, "_pool", lambda: manager)

    # Open a warm browser for acc_a
    async with pool.checkout(
        "acc_a",
        profile_dir=tmp_path / "profile_acc_a",
        headless=False,
        transport=None,
        out_dir=tmp_path,
    ):
        pass
    assert pool.warm_profiles() == ["acc_a"]

    # Runtime set_proxy persists binding and evicts warm browser on old route
    res = await mcp_plus_ops.gflow_plus_profile_control(
        "set_proxy", profile="acc_a", proxy="http://127.0.0.1:7891"
    )
    assert res["status"] == "ok"
    assert res["proxy"] == "http://127.0.0.1:7891"
    assert pool.warm_profiles() == []
    assert len(closed) == 1
    assert scheduler.resolve_profile_proxy("acc_a") == "http://127.0.0.1:7891"

    # Rejects credentialed proxy without echoing secret
    bad = await mcp_plus_ops.gflow_plus_profile_control(
        "set_proxy", profile="acc_a", proxy="http://u:supersecret@127.0.0.1:7891"
    )
    assert bad["status"] == "error"
    assert "supersecret" not in str(bad)

    # clear_proxy removes binding
    cleared = await mcp_plus_ops.gflow_plus_profile_control("clear_proxy", profile="acc_a")
    assert cleared["status"] == "ok"
    assert scheduler.resolve_profile_proxy("acc_a") is None


def test_direct_http_transport_skips_direct_post_when_per_profile_proxy_is_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("GFLOW_CLI_BROWSER_PROXY", raising=False)
    reset_settings()
    try:
        transport = DirectHttpTransport()
        transport._profile_dir = tmp_path  # noqa: SLF001
        transport.apply_setup(TransportSetup(browser_proxy="http://127.0.0.1:7891"))
        assert transport._why_not_direct_upload() == "proxy_configured"  # noqa: SLF001
        req = GenerateImageRequest(prompt="test")
        assert transport._why_not_direct(req, "proj-1") == "proxy_configured"  # noqa: SLF001
    finally:
        reset_settings()


def test_pre_submit_validation_rejects_typos_missing_files_and_bad_modes(tmp_path: Path) -> None:
    # 1. Batch misspelled key fails fast with close-match hint instead of silently dropping
    with pytest.raises(ValueError, match="start_image"):
        parse_batch_item(
            {"kind": "video", "prompt": "pan right", "start_imag": str(tmp_path / "a.png")}
        )

    # 2. end_image without start_image rejected
    valid_img = tmp_path / "kf.png"
    valid_img.write_bytes(b"\x89PNG\r\n\x1a\n1234")
    with pytest.raises(QueueSchemaError, match="end_image requires start_image"):
        validate_payload(
            "i2v",
            {"prompt": "move", "mode": "i2v", "end_image": str(valid_img)},
        )

    # 3. Missing local start_image rejected before Chrome launch
    with pytest.raises(QueueSchemaError, match="not found"):
        validate_payload(
            "i2v",
            {"prompt": "move", "mode": "i2v", "start_image": str(tmp_path / "missing.png")},
        )

    # 4. Empty local image file rejected
    empty_img = tmp_path / "empty.png"
    empty_img.write_bytes(b"")
    with pytest.raises(QueueSchemaError, match="empty"):
        validate_payload(
            "i2i",
            {"prompt": "edit", "ref_paths": [str(empty_img)]},
        )

    # 5. Mismatched entity ids and names rejected
    with pytest.raises(QueueSchemaError, match="same length"):
        validate_payload(
            "t2i",
            {
                "prompt": "scene",
                "reference_entities": ["id1", "id2"],
                "reference_entity_names": ["Alice"],
            },
        )

    # 6. Valid local start_image + end_image passes cleanly
    end_img = tmp_path / "kf_end.png"
    end_img.write_bytes(b"\x89PNG\r\n\x1a\n5678")
    encoded = validate_payload(
        "i2v",
        {
            "prompt": "smooth move",
            "mode": "i2v",
            "model": "omni-1.2-flash",
            "start_image": str(valid_img),
            "end_image": str(end_img),
        },
    )
    assert encoded["mode"] == "i2v"
