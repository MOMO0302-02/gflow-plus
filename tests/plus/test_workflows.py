"""Tests for batch dependency chaining, upscale/extend pinning, and safe job retry."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from gflow_cli.plus.config import PlusSettings
from gflow_cli.plus.pool.browser_pool import WarmBrowserPool
from gflow_cli.plus.pool.manager import PoolJobManager
from gflow_cli.plus.pool.models import ExecutionOutcome
from gflow_cli.plus.pool.scheduler import AccountPoolScheduler
from gflow_cli.plus.pool.state_store import PlusStateStore
from gflow_cli.profile_store import ProfileMeta

_UUID_1 = "11111111-1111-1111-1111-111111111111"
_UUID_2 = "22222222-2222-2222-2222-222222222222"


def _make_manager(tmp_path: Path, executor: Any) -> PoolJobManager:
    settings = PlusSettings(
        state_db_path=tmp_path / "plus_state.db",
        stagger_seconds=0.0,
        egress_min_interval_s=0.0,
        image_egress_min_interval_s=0.0,
        egress_jitter_ratio=0.0,
        egress_breather_every=0,
        max_concurrent_per_egress=0,
        rate_limit_cooldown_s=60.0,
        transient_cooldown_s=15.0,
        max_failover_attempts=3,
    )
    store = PlusStateStore(tmp_path / "plus_state.db")

    async def fake_inspect(name: str, *, refresh: bool = False) -> dict[str, Any]:
        del refresh
        return {"profile": name, "authenticated": True, "credits": 1000}

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
        credits_inspector=fake_inspect,
        jitter_range=lambda _spec: (0.0, 0.0),
        browser_pool=WarmBrowserPool(settings=settings),
    )
    return PoolJobManager(scheduler=scheduler, executor=executor, settings=settings, store=store)


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_batch_chains_image_to_upscale_and_i2v_video(tmp_path: Path) -> None:
    kf_path = tmp_path / "kf01.png"
    calls: list[tuple[str, str, dict[str, Any]]] = []

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        calls.append((profile, task_type, dict(payload)))
        if task_type == "t2i":
            kf_path.write_bytes(b"\x89PNG\r\n\x1a\nfake")
            return ExecutionOutcome(
                status="succeeded",
                profile=profile,
                media_id=_UUID_1,
                output_paths=(str(kf_path),),
                model="nano2",
                aspect="9:16",
            )
        if task_type == "upscale_image":
            out = tmp_path / "kf01_2k.png"
            out.write_bytes(b"\x89PNG\r\n\x1a\n2k")
            return ExecutionOutcome(
                status="succeeded",
                profile=profile,
                media_id=str(payload["media_id"]),
                output_paths=(str(out),),
                model="upscale:2k",
            )
        if task_type == "i2v":
            out = tmp_path / "vid01.mp4"
            out.write_bytes(b"mp4")
            return ExecutionOutcome(
                status="succeeded",
                profile=profile,
                media_id=_UUID_2,
                output_paths=(str(out),),
                model="omni-1.2-flash",
            )
        raise AssertionError(f"Unexpected task_type {task_type}")

    manager = _make_manager(tmp_path, fake_exec)
    submitted = await manager.submit_batch(
        [
            {"kind": "image", "label": "kf1", "prompt": "a glowing fox", "profile": "acc_b"},
            {"kind": "upscale", "label": "kf1_2k", "media_id": "job:kf1", "scale": "2k"},
            {
                "kind": "video",
                "label": "shot1",
                "prompt": "the fox leaps",
                "start_image": "job:kf1",
            },
        ]
    )
    waited = await manager.wait_jobs(batch_id=submitted["batch_id"], timeout_s=5.0)
    assert waited["done"] is True
    assert waited["counts"]["succeeded"] == 3

    by_type = {task_type: (prof, payload) for prof, task_type, payload in calls}
    # Upscale inherited media_id AND pinned to kf1's owning profile acc_b
    assert by_type["upscale_image"][0] == "acc_b"
    assert by_type["upscale_image"][1]["media_id"] == _UUID_1
    # Video i2v received the resolved local path of kf1
    assert by_type["i2v"][1]["start_image"] == str(kf_path)


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_upstream_failure_propagates_to_dependent_jobs(tmp_path: Path) -> None:
    executed: list[str] = []

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        del payload
        executed.append(task_type)
        return ExecutionOutcome(
            status="failed",
            profile=profile,
            error_type="policy-refusal",
            error_detail="blocked prompt",
            charged="no",
        )

    manager = _make_manager(tmp_path, fake_exec)
    submitted = await manager.submit_batch(
        [
            {"kind": "image", "label": "kf1", "prompt": "bad prompt"},
            {"kind": "video", "label": "v1", "prompt": "animate", "start_image": "job:kf1"},
        ]
    )
    waited = await manager.wait_jobs(batch_id=submitted["batch_id"], timeout_s=5.0)
    assert waited["done"] is True
    assert executed == ["t2i"]  # downstream never ran
    v1 = next(j for j in waited["jobs"] if j["label"] == "v1")
    assert v1["status"] == "failed"
    assert v1["error"]["type"] == "dependency-failed"
    assert v1["may_have_spent"] is False


@pytest.mark.asyncio
async def test_batch_rejects_unknown_ref_and_cycle(tmp_path: Path) -> None:
    async def noop_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        del task_type, payload
        return ExecutionOutcome(status="succeeded", profile=profile)

    manager = _make_manager(tmp_path, noop_exec)
    with pytest.raises(ValueError, match="Unknown upstream job reference"):
        await manager.submit_batch(
            [{"kind": "video", "label": "v1", "prompt": "x", "start_image": "job:missing"}]
        )

    with pytest.raises(ValueError, match="Cyclic dependency"):
        await manager.submit_batch(
            [
                {"kind": "image", "label": "a", "prompt": "x", "depends_on": ["b"]},
                {"kind": "image", "label": "b", "prompt": "y", "depends_on": ["a"]},
            ]
        )


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_retry_jobs_respects_may_have_spent_safety_guard(tmp_path: Path) -> None:
    attempt = 0

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        nonlocal attempt
        del payload
        attempt += 1
        if task_type == "t2v" and attempt == 1:
            return ExecutionOutcome(
                status="indeterminate",
                profile=profile,
                error_type="timeout",
                error_detail="lost connection after submit",
                charged="maybe",
            )
        if task_type == "t2i" and attempt == 2:
            return ExecutionOutcome(
                status="failed",
                profile=profile,
                error_type="policy-refusal",
                error_detail="blocked",
                charged="no",
            )
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=_UUID_1,
            output_paths=(str(tmp_path / "ok.png"),),
        )

    manager = _make_manager(tmp_path, fake_exec)
    vid = await manager.submit_job(kind="video", task_type="t2v", payload={"prompt": "v"})
    await manager.wait_jobs(job_ids=[vid.job_id], timeout_s=5.0)
    img = await manager.submit_job(kind="image", task_type="t2i", payload={"prompt": "i"})
    await manager.wait_jobs(job_ids=[img.job_id], timeout_s=5.0)

    # Default retry refuses indeterminate / may_have_spent video job
    refused = await manager.retry_jobs(job_id=vid.job_id)
    assert refused["retried_count"] == 0
    assert refused["skipped_count"] == 1
    assert "may_have_spent" in refused["skipped"][0]["reason"]

    # Safe uncharged image failure retries cleanly
    ok_retry = await manager.retry_jobs(job_id=img.job_id)
    assert ok_retry["retried_count"] == 1
    new_id = ok_retry["retried"][0]["new_job"]["job_id"]
    done = await manager.wait_jobs(job_ids=[new_id], timeout_s=5.0)
    assert done["counts"]["succeeded"] == 1

    # Explicit force_spent=True unlocks video retry
    forced = await manager.retry_jobs(job_id=vid.job_id, force_spent=True)
    assert forced["retried_count"] == 1
