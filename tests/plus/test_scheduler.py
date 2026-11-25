"""Unit tests for multi-account pool scheduling, stagger, and failover classification."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from gflow_cli.errors import AuthExpiredError, ProfileLockedError
from gflow_cli.plus.config import PlusSettings
from gflow_cli.plus.pool.models import ExecutionOutcome
from gflow_cli.plus.pool.scheduler import AccountPoolScheduler, classify_failover
from gflow_cli.plus.pool.state_store import PlusStateStore
from gflow_cli.profile_store import ProfileMeta


def _meta(name: str, *, cookies: bool = True, default: bool = False) -> ProfileMeta:
    return ProfileMeta(
        name=name,
        profile_dir=Path(f"tmp/fake_profile_{name}"),
        cookies_present=cookies,
        last_used_at=None,
        is_default=default,
        google_account=f"{name}@example.com",
    )


def _make_scheduler(
    tmp_path: Path,
    *,
    profiles: list[ProfileMeta],
    now_ref: list[float],
    credits_reply: dict[str, Any] | None = None,
    stagger_seconds: float = 2.5,
) -> tuple[AccountPoolScheduler, list[float]]:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now_ref[0] += seconds

    async def fake_inspect(name: str, *, refresh: bool = False) -> dict[str, Any]:
        del refresh
        for item in (credits_reply or {}).get("profiles", []):
            if item["profile"] == name:
                return item
        raise AuthExpiredError(f"{name} signed out")

    settings = PlusSettings(
        state_db_path=tmp_path / "plus_state.db",
        min_video_credits=20,
        stagger_seconds=stagger_seconds,
        rate_limit_cooldown_s=120.0,
        auth_error_cooldown_s=600.0,
        transient_cooldown_s=30.0,
    )
    store = PlusStateStore(tmp_path / "plus_state.db")
    scheduler = AccountPoolScheduler(
        store=store,
        settings=settings,
        profile_lister=lambda: profiles,
        credits_inspector=fake_inspect,
        clock=lambda: now_ref[0],
        sleeper=fake_sleep,
        jitter_range=lambda _spec: (2.0, 2.0),
    )
    return scheduler, sleeps


@pytest.mark.asyncio
@pytest.mark.sharpit(
    allow_n_plus_one=True
)  # one pool-state read per pick, several picks on purpose
async def test_video_picks_highest_credit_idle_account_and_skips_low_balance(
    tmp_path: Path,
) -> None:
    now = [1000.0]
    profiles = [_meta("acc_a", default=True), _meta("acc_b"), _meta("acc_c")]
    reply = {
        "status": "ok",
        "profiles": [
            {"profile": "acc_a", "authenticated": True, "credits": 10},
            {"profile": "acc_b", "authenticated": True, "credits": 500},
            {"profile": "acc_c", "authenticated": True, "credits": 2500},
        ],
    }
    sched, _ = _make_scheduler(tmp_path, profiles=profiles, now_ref=now, credits_reply=reply)
    report = await sched.refresh_credits(force=True)

    assert report["total_credits"] == 3010
    assert sched.pick_profile("video") == "acc_c"

    # Once acc_c is busy, video routes to acc_b (acc_a has only 10 < min 20).
    sched.mark_busy("acc_c", True)
    assert sched.pick_profile("video") == "acc_b"

    sched.mark_busy("acc_b", True)
    assert sched.pick_profile("video") is None


@pytest.mark.asyncio
async def test_image_picks_idle_accounts_by_lru(tmp_path: Path) -> None:
    now = [1000.0]
    profiles = [_meta("acc_a"), _meta("acc_b"), _meta("acc_c")]
    sched, _ = _make_scheduler(tmp_path, profiles=profiles, now_ref=now)

    first = sched.pick_profile("image")
    assert first == "acc_a"
    sched.store.upsert_account_state("acc_a", last_used_at=1001.0)
    assert sched.pick_profile("image") == "acc_b"


@pytest.mark.asyncio
async def test_stagger_spaces_concurrent_launches(tmp_path: Path) -> None:
    now = [1000.0]
    sched, sleeps = _make_scheduler(
        tmp_path,
        profiles=[_meta("acc_a"), _meta("acc_b"), _meta("acc_c")],
        now_ref=now,
        stagger_seconds=2.5,
    )
    waits = await asyncio.gather(
        sched.wait_stagger(),
        sched.wait_stagger(),
        sched.wait_stagger(),
    )
    assert waits.count(0.0) == 1
    assert len(sleeps) == 2
    assert all(pytest.approx(2.5) == s for s in sleeps)


def test_failover_classifier_never_retries_charged_failures() -> None:
    settings = PlusSettings()

    # Non-charged 429 -> cooldown and failover
    d_429 = classify_failover(
        ExecutionOutcome(
            status="failed",
            profile="acc_a",
            error_type="https://gflow-cli.dev/errors/rate-limit",
            charged="no",
        ),
        settings,
    )
    assert d_429.retry_other_profile is True
    assert d_429.cooldown_s == settings.rate_limit_cooldown_s

    # Non-charged pre-submit UI drift (e.g. file chooser timeout) -> short cooldown and failover
    d_ui = classify_failover(
        ExecutionOutcome(
            status="failed",
            profile="acc_a",
            error_type="https://gflow-cli.dev/errors/ui-selector-drift",
            charged="no",
        ),
        settings,
    )
    assert d_ui.retry_other_profile is True
    assert d_ui.cooldown_s == settings.transient_cooldown_s

    # Profile lock -> immediate failover with zero cooldown
    d_lock = classify_failover(
        ExecutionOutcome(
            status="failed",
            profile="acc_a",
            error_type="https://gflow-cli.dev/errors/profile-locked",
            charged="no",
        ),
        settings,
    )
    assert d_lock.retry_other_profile is True
    assert d_lock.cooldown_s == 0.0

    # Charged=maybe or yes -> HARD STOP, never retry on another account
    for charged_state in ("maybe", "yes"):
        d_charged = classify_failover(
            ExecutionOutcome(
                status="failed",
                profile="acc_a",
                error_type="https://gflow-cli.dev/errors/rate-limit",
                charged=charged_state,
            ),
            settings,
        )
        assert d_charged.retry_other_profile is False


@pytest.mark.asyncio
async def test_refresh_never_cools_down_a_busy_or_locked_account(tmp_path: Path) -> None:
    # Live 2026-11-24: a refresh during a batch put both busy accounts in a 30-min
    # auth cooldown (ProfileLockedError), and took the idle one's lease from its job.
    now = [1000.0]
    profiles = [_meta("busy"), _meta("locked"), _meta("signed_out"), _meta("ok")]
    asked: list[str] = []

    async def inspect(name: str, *, refresh: bool = False) -> dict[str, Any]:
        del refresh
        asked.append(name)
        assert sched.is_busy(name), "a read must hold the profile's lane"
        if name == "locked":
            raise ProfileLockedError("held by another gflow process")
        if name == "signed_out":
            raise AuthExpiredError("cookies expired")
        return {"profile": name, "authenticated": True, "credits": 900}

    sched, _ = _make_scheduler(tmp_path, profiles=profiles, now_ref=now)
    sched._inspect = inspect  # pyright: ignore[reportPrivateUsage]
    sched.store.upsert_account_state("busy", credits=500, credits_checked_at=900.0)
    sched.mark_busy("busy", True)

    report = await sched.refresh_credits(force=True)
    by_name = {p["profile"]: p for p in report["profiles"]}

    assert sorted(asked) == ["locked", "ok", "signed_out"]
    assert by_name["busy"]["credits"] == 500
    assert by_name["locked"]["cooldown_remaining_s"] == 0
    assert by_name["ok"]["credits"] == 900
    assert by_name["signed_out"]["healthy"] is False
    assert by_name["signed_out"]["cooldown_reason"] == "auth_required"
    assert not sched.is_busy("ok")


@pytest.mark.asyncio
async def test_a_balance_read_keeps_a_waf_cooldown_but_clears_an_auth_one(tmp_path: Path) -> None:
    now = [1000.0]
    reply = {
        "profiles": [
            {"profile": "hot", "authenticated": True, "credits": 50},
            {"profile": "renewed", "authenticated": True, "credits": 60},
        ]
    }
    sched, _ = _make_scheduler(
        tmp_path, profiles=[_meta("hot"), _meta("renewed")], now_ref=now, credits_reply=reply
    )
    sched.store.upsert_account_state(
        "hot", cooldown_until=4600.0, cooldown_reason="waf_unusual_activity"
    )
    sched.store.upsert_account_state(
        "renewed", cooldown_until=4600.0, cooldown_reason="auth_required"
    )

    report = await sched.refresh_credits(force=True)
    by_name = {p["profile"]: p for p in report["profiles"]}

    assert by_name["hot"]["healthy"] is False
    assert by_name["hot"]["credits"] == 50
    assert by_name["renewed"]["healthy"] is True


def test_waf_refusal_cools_the_profile_down_without_a_charged_retry() -> None:
    # Live 2026-11-24: "Flow refused the submit: PUBLIC_ERROR_UNUSUAL_ACTIVITY", charged=maybe.
    settings = PlusSettings()
    decision = classify_failover(
        ExecutionOutcome(
            status="failed",
            profile="acc_a",
            error_type="https://gflow-cli.dev/errors/waf-rejection",
            error_detail="Flow refused the submit: PUBLIC_ERROR_UNUSUAL_ACTIVITY (gRPC 7)",
            charged="maybe",
        ),
        settings,
    )
    assert decision.retry_other_profile is False
    assert decision.cooldown_s == settings.waf_cooldown_s
    assert decision.cooldown_reason == "waf_unusual_activity"


@pytest.mark.asyncio
async def test_lane_jitter_spaces_back_to_back_submits_on_one_profile(tmp_path: Path) -> None:
    now = [1000.0]
    sched, sleeps = _make_scheduler(tmp_path, profiles=[_meta("acc_a")], now_ref=now)

    assert await sched.wait_lane_jitter("acc_a") == 0.0
    assert await sched.wait_lane_jitter("acc_b") == 0.0
    assert await sched.wait_lane_jitter("acc_a") == 2.0
    assert sleeps == [2.0]
