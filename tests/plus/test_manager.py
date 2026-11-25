"""Unit tests for PoolJobManager: 3-lane concurrency, failover, skip-existing, bounded wait."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from gflow_cli.plus.config import PlusSettings
from gflow_cli.plus.pool import manager as manager_module
from gflow_cli.plus.pool.executor import _resolve_output_paths, check_existing_output
from gflow_cli.plus.pool.manager import PoolJobManager
from gflow_cli.plus.pool.models import ExecutionOutcome, PoolJob
from gflow_cli.plus.pool.owner import OwnerRegistry
from gflow_cli.plus.pool.scheduler import AccountPoolScheduler
from gflow_cli.plus.pool.state_store import PlusStateStore
from gflow_cli.profile_store import ProfileMeta


def _profiles() -> list[ProfileMeta]:
    return [
        ProfileMeta(
            name=name,
            profile_dir=Path(f"tmp/p_{name}"),
            cookies_present=True,
            last_used_at=None,
            is_default=(name == "acc_a"),
            google_account=f"{name}@example.com",
        )
        for name in ("acc_a", "acc_b", "acc_c")
    ]


def _make_manager(tmp_path: Path, executor: Any) -> PoolJobManager:
    settings = PlusSettings(
        state_db_path=tmp_path / "plus_state.db",
        stagger_seconds=0.0,
        rate_limit_cooldown_s=60.0,
        transient_cooldown_s=15.0,
        max_failover_attempts=3,
    )
    store = PlusStateStore(tmp_path / "plus_state.db")

    async def fake_inspect(name: str, *, refresh: bool = False) -> dict[str, Any]:
        del refresh
        return {"profile": name, "authenticated": True, "credits": 1000}

    scheduler = AccountPoolScheduler(
        store=store,
        settings=settings,
        profile_lister=_profiles,
        credits_inspector=fake_inspect,
        jitter_range=lambda _spec: (0.0, 0.0),
    )
    return PoolJobManager(scheduler=scheduler, executor=executor, settings=settings, store=store)


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)  # one job-row read per job across three jobs
async def test_three_lanes_run_in_parallel_without_same_profile_collision(tmp_path: Path) -> None:
    active_per_profile: dict[str, int] = {}
    max_concurrent = 0
    current_total = 0

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        nonlocal max_concurrent, current_total
        assert active_per_profile.get(profile, 0) == 0, f"Concurrent collision on {profile}"
        active_per_profile[profile] = active_per_profile.get(profile, 0) + 1
        current_total += 1
        max_concurrent = max(max_concurrent, current_total)
        await asyncio.sleep(0.08)
        current_total -= 1
        active_per_profile[profile] -= 1
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=f"media-{profile}",
            output_paths=(str(tmp_path / f"{profile}.png"),),
            model=payload.get("model"),
            aspect=payload.get("aspect"),
        )

    manager = _make_manager(tmp_path, fake_exec)
    submitted = await manager.submit_batch(
        [
            {"kind": "image", "prompt": "shot 1", "label": "kf01"},
            {"kind": "image", "prompt": "shot 2", "label": "kf02"},
            {"kind": "image", "prompt": "shot 3", "label": "kf03"},
        ]
    )
    waited = await manager.wait_jobs(batch_id=submitted["batch_id"], timeout_s=5.0)

    assert waited["done"] is True
    assert max_concurrent == 3
    used_profiles = {j["profile"] for j in waited["jobs"]}
    assert used_profiles == {"acc_a", "acc_b", "acc_c"}


@pytest.mark.asyncio
@pytest.mark.sharpit(
    allow_n_plus_one=True
)  # one pool-state read per scheduling decision, once per attempt
async def test_non_charged_ui_failure_fails_over_to_second_account(tmp_path: Path) -> None:
    calls: list[str] = []

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        calls.append(profile)
        if len(calls) == 1:
            return ExecutionOutcome(
                status="failed",
                profile=profile,
                error_type="https://gflow-cli.dev/errors/ui-selector-drift",
                error_detail="upload entry opened no file chooser within 8s",
                charged="no",
                exit_code=23,
            )
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id="vid-001",
            output_paths=(str(tmp_path / "shot1.mp4"),),
            model=payload.get("model"),
            aspect=payload.get("aspect"),
        )

    manager = _make_manager(tmp_path, fake_exec)
    job = await manager.submit_job(
        kind="video",
        task_type="t2v",
        payload={"prompt": "test clip", "mode": "t2v", "aspect": "9:16", "model": "omni-1.2-flash"},
    )
    waited = await manager.wait_jobs(job_ids=[job.job_id], timeout_s=5.0)
    final = waited["jobs"][0]

    assert final["status"] == "succeeded"
    assert len(calls) == 2
    assert calls[0] != calls[1]
    full = manager.get_job(job.job_id)
    assert full is not None
    assert len(full.attempts) == 2
    assert final["result"]["media_id"] == "vid-001"


@pytest.mark.asyncio
@pytest.mark.sharpit(
    allow_n_plus_one=True
)  # one pool-state read per scheduling decision, once per attempt
async def test_maybe_charged_failure_never_retries_another_account(tmp_path: Path) -> None:
    calls: list[str] = []

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        calls.append(profile)
        return ExecutionOutcome(
            status="indeterminate",
            profile=profile,
            error_type="indeterminate-outcome",
            error_detail="interrupted after submit",
            charged="maybe",
        )

    manager = _make_manager(tmp_path, fake_exec)
    job = await manager.submit_job(
        kind="video",
        task_type="t2v",
        payload={"prompt": "paid clip", "mode": "t2v", "aspect": "9:16", "model": "omni-1.2-flash"},
    )
    waited = await manager.wait_jobs(job_ids=[job.job_id], timeout_s=5.0)
    final = waited["jobs"][0]

    assert len(calls) == 1
    assert final["status"] == "indeterminate"
    assert final["may_have_spent"] is True


@pytest.mark.asyncio
async def test_skip_if_exists_skips_completed_output_without_running_executor(
    tmp_path: Path,
) -> None:
    existing = tmp_path / "kf01.png"
    existing.write_bytes(b"png-bytes")
    called = False

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        nonlocal called
        called = True
        return ExecutionOutcome(status="succeeded", profile=profile)

    manager = _make_manager(tmp_path, fake_exec)
    job = await manager.submit_job(
        kind="image",
        task_type="t2i",
        payload={"prompt": "skip me", "output_file": str(existing)},
        skip_if_exists=True,
    )
    assert called is False
    assert job.status == "succeeded"
    assert job.result is not None
    assert job.result["skipped_existing"] is True


@pytest.mark.asyncio
async def test_wait_jobs_respects_short_timeout_without_cancelling_background_job(
    tmp_path: Path,
) -> None:
    gate = asyncio.Event()

    async def slow_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        await gate.wait()
        return ExecutionOutcome(status="succeeded", profile=profile, media_id="done-later")

    manager = _make_manager(tmp_path, slow_exec)
    job = await manager.submit_job(
        kind="image",
        task_type="t2i",
        payload={"prompt": "slow shot"},
    )
    first = await manager.wait_jobs(job_ids=[job.job_id], timeout_s=0.08)
    assert first["done"] is False
    assert first["timed_out"] is True

    gate.set()
    second = await manager.wait_jobs(job_ids=[job.job_id], timeout_s=5.0)
    assert second["done"] is True
    assert second["jobs"][0]["status"] == "succeeded"
    assert second["jobs"][0]["result"]["media_id"] == "done-later"


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)  # polls the pool every 1 ms until a lane frees
async def test_jobs_beyond_lane_count_queue_until_a_lane_frees(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Live 2026-11-24: shots 5 and 6 of a 6-shot batch failed "no-healthy-profile"
    # after a bounded ~30 s wait, while every lane was busy on a ~35 s image.
    monkeypatch.setattr(manager_module, "_LANE_POLL_S", 0.001)
    gate = asyncio.Event()

    async def gated_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        await gate.wait()
        return ExecutionOutcome(status="succeeded", profile=profile, media_id=f"m-{profile}")

    manager = _make_manager(tmp_path, gated_exec)
    submitted = await manager.submit_batch(
        [{"kind": "image", "prompt": f"shot {i}"} for i in range(1, 7)]
    )
    await asyncio.sleep(1.0)  # far more polls than any fixed retry budget
    queued = await manager.wait_jobs(batch_id=submitted["batch_id"], timeout_s=0.05)
    assert sorted(j["status"] for j in queued["jobs"]) == ["queued"] * 3 + ["running"] * 3

    gate.set()
    waited = await manager.wait_jobs(batch_id=submitted["batch_id"], timeout_s=5.0)
    assert waited["done"] is True
    assert [j["status"] for j in waited["jobs"]] == ["succeeded"] * 6


def test_existing_output_matches_the_corrected_image_extension(tmp_path: Path) -> None:
    # Flow serves JPEG bytes for a .png request; gflow renames the file to .jpg.
    (tmp_path / "shot_01.jpg").write_bytes(b"jpeg-bytes")
    found = check_existing_output({"output_file": str(tmp_path / "shot_01.png")})
    assert found == tmp_path / "shot_01.jpg"
    assert check_existing_output({"output_file": str(tmp_path / "shot_02.png")}) is None


def test_output_paths_resolve_from_the_catalog_by_media_id(tmp_path: Path) -> None:
    db = tmp_path / "gflow.db"
    con = sqlite3.connect(db)
    con.executescript(
        """
        CREATE TABLE assets (id INTEGER PRIMARY KEY, flow_media_id TEXT);
        CREATE TABLE local_files (
            id INTEGER PRIMARY KEY, asset_id INTEGER, path TEXT, superseded_at TEXT
        );
        INSERT INTO assets VALUES (1, 'media-1');
        INSERT INTO local_files VALUES (1, 1, 'D:/old.jpg', '2026-11-01');
        INSERT INTO local_files VALUES (2, 1, 'D:/out/a.jpg', NULL);
        """
    )
    con.commit()
    con.close()
    task = SimpleNamespace(payload={}, flow_media_id="media-1")
    assert _resolve_output_paths(task, db_path=db) == ("D:/out/a.jpg",)  # type: ignore[arg-type]


def _orphan(store: PlusStateStore, job_id: str, kind: str, status: str, owner: str | None) -> None:
    store.create_job(
        PoolJob(
            job_id=job_id,
            kind="video" if kind == "video" else "image",
            task_type="t2v" if kind == "video" else "t2i",
            status="running" if status == "running" else "queued",
            payload={"prompt": job_id, "mode": "t2v"} if kind == "video" else {"prompt": job_id},
            assigned_profile="acc_a" if status == "running" else None,
            owner=owner,
        )
    )


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)  # one job-row read per recovered job
async def test_restarted_process_takes_over_orphans_but_never_reruns_a_video(
    tmp_path: Path,
) -> None:
    ran: list[str] = []

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        ran.append(payload["prompt"])
        return ExecutionOutcome(status="succeeded", profile=profile, media_id="m")

    manager = _make_manager(tmp_path, fake_exec)
    live = OwnerRegistry(tmp_path / "plus_owners")  # stands in for another running gflow
    _orphan(manager.store, "queued-img", "image", "queued", "gone-owner")
    _orphan(manager.store, "running-img", "image", "running", None)
    _orphan(manager.store, "running-vid", "video", "running", "gone-owner")
    _orphan(manager.store, "live-img", "image", "queued", live.owner_id)

    recovered = manager.recover_orphans()
    assert sorted(recovered["restarted"]) == ["queued-img", "running-img"]
    assert recovered["abandoned"] == ["running-vid"]
    assert manager.recover_orphans() == {"restarted": [], "abandoned": []}

    waited = await manager.wait_jobs(job_ids=["queued-img", "running-img"], timeout_s=5.0)
    assert waited["counts"]["succeeded"] == 2
    assert sorted(ran) == ["queued-img", "running-img"]
    video = manager.get_job("running-vid")
    assert video is not None
    assert video.status == "indeterminate"
    assert video.may_have_spent is True
    still_theirs = manager.get_job("live-img")
    assert still_theirs is not None
    assert still_theirs.status == "queued"


@pytest.mark.asyncio
async def test_wait_returns_brief_jobs_with_counts(tmp_path: Path) -> None:
    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        return ExecutionOutcome(status="succeeded", profile=profile, media_id="m1")

    manager = _make_manager(tmp_path, fake_exec)
    job = await manager.submit_job(kind="image", task_type="t2i", payload={"prompt": "a"})
    waited = await manager.wait_jobs(job_ids=[job.job_id], timeout_s=5.0)

    assert waited["counts"]["succeeded"] == 1
    assert set(waited["jobs"][0]) == {
        "job_id",
        "label",
        "status",
        "profile",
        "result",
        "error",
        "may_have_spent",
    }


@pytest.mark.asyncio
async def test_a_crashed_runner_fails_its_job_instead_of_leaving_it_queued(
    tmp_path: Path,
) -> None:
    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        return ExecutionOutcome(status="succeeded", profile=profile)

    manager = _make_manager(tmp_path, fake_exec)

    async def broken_refresh(*, force: bool = False, renew: bool = False) -> dict[str, Any]:
        raise RuntimeError("credits store unreadable")

    manager.scheduler.refresh_credits = broken_refresh  # type: ignore[method-assign]
    job = await manager.submit_job(
        kind="video", task_type="t2v", payload={"prompt": "clip", "mode": "t2v"}
    )
    waited = await manager.wait_jobs(job_ids=[job.job_id], timeout_s=5.0)

    final = waited["jobs"][0]
    assert final["status"] == "failed"
    assert final["error"]["type"] == "RuntimeError"
    assert final["may_have_spent"] is False
