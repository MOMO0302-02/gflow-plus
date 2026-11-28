"""Tests for single-IP optimization upgrades (v1.4.0):

1. Sticky primary profile per egress group with seamless failover on cooldown/exhaustion.
2. Automatic sticky Flow project reuse per batch & profile, plus clear_sticky_project.
3. Zero-reupload in-project job:<label> chaining for i2v & i2i, failover downgrade,
   and MigratedComposer existing-frame selection with local upload fallback.
4. Kind-aware egress pacing (image vs video), jitter, periodic breathers, and AIMD backoff.
5. Local credit deduction on video completion and ref-counted WarmBrowserPool sharing.
6. Decoupled video submit-gate vs render-wait pipelining on one profile without
   cross-account same-IP concurrency.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from gflow_cli.api.transports._mc_common import _unported_form  # pyright: ignore[reportPrivateUsage]
from gflow_cli.api.transports._mc_upload_frame import UploadFrameMixin
from gflow_cli.api.transports.ui_automation import UiAutomationTransport
from gflow_cli.api.video import GenerateVideoRequest, Mode, VideoResult, VideoStatus
from gflow_cli.errors import ReferenceNotFoundError
from gflow_cli.plus import mcp_plus, mcp_plus_ops
from gflow_cli.plus.config import PlusSettings
from gflow_cli.plus.pool.browser_pool import WarmBrowserPool
from gflow_cli.plus.pool.manager import PoolJobManager
from gflow_cli.plus.pool.models import ExecutionOutcome
from gflow_cli.plus.pool.scheduler import AccountPoolScheduler
from gflow_cli.plus.pool.state_store import PlusStateStore
from gflow_cli.profile_store import ProfileMeta

_UUID_IMG = "11111111-1111-4111-8111-111111111111"
_UUID_IMG2 = "22222222-2222-4222-8222-222222222222"
_UUID_PROJ = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_UUID_VID = "33333333-3333-4333-8333-333333333333"


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


def _build_single_ip_pool(
    tmp_path: Path,
    executor: Any,
    *,
    now_ref: list[float] | None = None,
    sleeps: list[float] | None = None,
    settings_overrides: dict[str, Any] | None = None,
) -> tuple[PoolJobManager, AccountPoolScheduler]:
    clock_box = now_ref if now_ref is not None else [1000.0]

    async def fake_sleep(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)
        clock_box[0] += seconds

    async def fake_inspect(name: str, *, refresh: bool = False) -> dict[str, Any]:
        del refresh
        balances = {"acc_a": 500, "acc_b": 1500, "acc_c": 2500}
        return {"profile": name, "authenticated": True, "credits": balances.get(name, 1000)}

    defaults: dict[str, Any] = {
        "state_db_path": tmp_path / "plus_state.db",
        "stagger_seconds": 0.0,
        "egress_min_interval_s": 0.0,
        "image_egress_min_interval_s": 0.0,
        "egress_jitter_ratio": 0.0,
        "egress_breather_every": 0,
        "max_concurrent_per_egress": 1,
        "sticky_profile_per_egress": True,
        "sticky_project": True,
        "max_inflight_videos_per_profile": 2,
        "waf_cooldown_s": 600.0,
        "egress_waf_cooldown_s": 180.0,
    }
    if settings_overrides:
        defaults.update(settings_overrides)
    settings = PlusSettings(**defaults)
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


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_sticky_profile_stays_on_primary_account_and_fails_over_on_cooldown(
    tmp_path: Path,
) -> None:
    """On one shared IP, all jobs stick to one primary profile instead of hopping A->B->C."""
    used: list[str] = []

    async def fake_exec(
        profile: str, _task_type: str, _payload: dict[str, Any]
    ) -> ExecutionOutcome:
        used.append(profile)
        return ExecutionOutcome(status="succeeded", profile=profile, project_id=_UUID_PROJ)

    manager, scheduler = _build_single_ip_pool(tmp_path, fake_exec)
    await scheduler.refresh_credits(force=True)

    # First video picks highest-credit acc_c; subsequent images & videos all stick to acc_c
    j1 = await manager.submit_job(kind="video", task_type="t2v", payload={"prompt": "v1"})
    await manager.wait_jobs(job_ids=[j1.job_id], timeout_s=5.0)
    j2 = await manager.submit_job(kind="image", task_type="t2i", payload={"prompt": "i1"})
    await manager.wait_jobs(job_ids=[j2.job_id], timeout_s=5.0)
    j3 = await manager.submit_job(kind="image", task_type="t2i", payload={"prompt": "i2"})
    await manager.wait_jobs(job_ids=[j3.job_id], timeout_s=5.0)

    assert used == ["acc_c", "acc_c", "acc_c"]
    report = scheduler.health_report()
    assert report["egress_groups"][0]["sticky_profile"] == "acc_c"

    # Put acc_c on rate-limit cooldown -> seamlessly fails over to acc_b and sticks there
    scheduler.record_attempt(
        "acc_c",
        ExecutionOutcome(
            status="failed",
            profile="acc_c",
            error_type="rate-limit",
            error_detail="HTTP 429",
            charged="no",
        ),
        kind="video",
    )
    j4 = await manager.submit_job(kind="video", task_type="t2v", payload={"prompt": "v2"})
    await manager.wait_jobs(job_ids=[j4.job_id], timeout_s=5.0)
    j5 = await manager.submit_job(kind="image", task_type="t2i", payload={"prompt": "i3"})
    await manager.wait_jobs(job_ids=[j5.job_id], timeout_s=5.0)

    assert used[3:] == ["acc_b", "acc_b"]
    assert scheduler.health_report()["egress_groups"][0]["sticky_profile"] == "acc_b"


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_sticky_project_auto_reused_and_clearable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen_projects: list[str | None] = []

    async def fake_exec(profile: str, _task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        seen_projects.append(payload.get("project_id"))
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=_UUID_IMG,
            project_id=str(payload.get("project_id") or _UUID_PROJ),
        )

    manager, scheduler = _build_single_ip_pool(tmp_path, fake_exec)
    monkeypatch.setattr(mcp_plus, "_pool", lambda: manager)

    # First job creates _UUID_PROJ; second job inherits it automatically
    j1 = await manager.submit_job(kind="image", task_type="t2i", payload={"prompt": "shot 1"})
    await manager.wait_jobs(job_ids=[j1.job_id], timeout_s=5.0)
    j2 = await manager.submit_job(kind="image", task_type="t2i", payload={"prompt": "shot 2"})
    await manager.wait_jobs(job_ids=[j2.job_id], timeout_s=5.0)
    # Explicit project_name is never overwritten by sticky project
    j3 = await manager.submit_job(
        kind="image",
        task_type="t2i",
        payload={"prompt": "shot 3", "project_name": "Fresh Campaign"},
    )
    await manager.wait_jobs(job_ids=[j3.job_id], timeout_s=5.0)

    assert seen_projects == [None, _UUID_PROJ, None]
    assert scheduler.get_sticky_project("acc_a") == _UUID_PROJ

    # Runtime clear_sticky_project resets it so the next submit starts fresh
    res = await mcp_plus_ops.gflow_plus_profile_control("clear_sticky_project", profile="acc_a")
    assert res["status"] == "ok"
    assert scheduler.get_sticky_project("acc_a") is None


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_zero_reupload_in_project_chaining_for_i2v_and_i2i_with_failover_downgrade(
    tmp_path: Path,
) -> None:
    kf_path = tmp_path / "kf1.png"
    kf_path.write_bytes(b"\x89PNG\r\n\x1a\nkeyframe-bytes")
    seen: list[tuple[str, str, dict[str, Any]]] = []

    async def fake_exec(profile: str, task_type: str, payload: dict[str, Any]) -> ExecutionOutcome:
        seen.append((profile, task_type, dict(payload)))
        if task_type == "t2i":
            return ExecutionOutcome(
                status="succeeded",
                profile=profile,
                media_id=_UUID_IMG,
                project_id=_UUID_PROJ,
                display_name="hero-kf-01.png",
                output_paths=(str(kf_path),),
            )
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=_UUID_VID,
            project_id=str(payload.get("project_id") or _UUID_PROJ),
            output_paths=(str(tmp_path / f"{task_type}.out"),),
        )

    manager, _ = _build_single_ip_pool(tmp_path, fake_exec)
    submitted = await manager.submit_batch(
        [
            {"kind": "image", "label": "kf1", "prompt": "hero frame"},
            {"kind": "image", "label": "kf2", "prompt": "variation", "ref_paths": ["job:kf1"]},
            {"kind": "video", "label": "vid1", "prompt": "animate", "start_image": "job:kf1"},
        ]
    )
    waited = await manager.wait_jobs(batch_id=submitted["batch_id"], timeout_s=5.0)
    assert waited["done"] is True
    assert waited["counts"]["succeeded"] == 3

    by_type = {tt: (prof, pl) for prof, tt, pl in seen}
    # Downstream i2i bound in-project ref without ref_paths re-upload
    i2i_prof, i2i_payload = by_type["i2i"]
    assert i2i_prof == by_type["t2i"][0]
    assert i2i_payload["refs"] == [_UUID_IMG]
    assert "ref_paths" not in i2i_payload
    assert i2i_payload["project_id"] == _UUID_PROJ
    assert i2i_payload["ref_meta"][_UUID_IMG]["display_name"] == "hero-kf-01.png"

    # Downstream i2v bound in-project start_image_ref + display_name + local fallback + sha256
    i2v_prof, i2v_payload = by_type["i2v"]
    assert i2v_prof == by_type["t2i"][0]
    assert i2v_payload["start_image_ref"] == _UUID_IMG
    assert i2v_payload["start_image_ref_display_name"] == "hero-kf-01.png"
    assert i2v_payload["start_image_ref_local_path"] == str(kf_path)
    assert len(i2v_payload["start_image_ref_local_sha256"]) == 64
    assert "start_image" not in i2v_payload
    assert i2v_payload["project_id"] == _UUID_PROJ

    # Cross-account failover strips in-project refs and cleanly falls back to local path
    vid_job = next(
        j for j in manager.list_jobs(batch_id=submitted["batch_id"]) if j.task_type == "i2v"
    )
    downgraded = manager._prepare_execution_payload(vid_job, "acc_b")  # noqa: SLF001
    assert downgraded["start_image"] == str(kf_path)
    assert "start_image_ref" not in downgraded
    assert "project_id" not in downgraded


@pytest.mark.asyncio
async def test_migrated_existing_frame_attach_reuses_and_falls_back_on_miss(tmp_path: Path) -> None:
    """_unported_form allows named in-project refs, and UploadFrameMixin falls back on miss."""
    frame_file = tmp_path / "start.png"
    frame_file.write_bytes(b"\x89PNG\r\n\x1a\n1234")
    sha = "a" * 64

    # Named in-project ref is accepted; bare UUID without display_name stays refused
    named_req = GenerateVideoRequest(
        prompt="move",
        mode=Mode.I2V,
        start_image_ref_id=_UUID_IMG,
        start_image_ref_display_name="hero-kf-01.png",
        start_image_ref_local_path=frame_file,
        start_image_ref_local_sha256=sha,
    )
    assert _unported_form(named_req) is None

    bare_uuid_req = GenerateVideoRequest(
        prompt="move",
        mode=Mode.I2V,
        start_image_ref_id=_UUID_IMG,
    )
    assert _unported_form(bare_uuid_req) == "a frame given by Flow media UUID"

    class _FakeMixin(UploadFrameMixin):
        def __init__(self, *, miss: bool) -> None:
            self.miss = miss
            self.uploaded = False

        async def _pick_frame_by_name(
            self,
            page: Any,
            name: str,
            media_id: str,
            *,
            chip_label: str = "Start",
            expect_bound_chips: int = 1,
        ) -> None:
            del page, name, media_id, chip_label, expect_bound_chips
            if self.miss:
                raise ReferenceNotFoundError(detail="not in picker")

        async def attach_start_frame(self, page: Any, project_id: str, image_path: Path) -> str:
            del page, project_id, image_path
            self.uploaded = True
            return _UUID_IMG2

    # Hit path: zero upload
    hit = _FakeMixin(miss=False)
    mid = await hit.attach_existing_or_upload_start_frame(
        page=object(),  # type: ignore[arg-type]
        project_id=_UUID_PROJ,
        media_id=_UUID_IMG,
        display_name="hero-kf-01.png",
        fallback_path=frame_file,
    )
    assert mid == _UUID_IMG
    assert hit.uploaded is False

    # Miss path: automatic fallback to local upload
    miss = _FakeMixin(miss=True)
    mid2 = await miss.attach_existing_or_upload_start_frame(
        page=object(),  # type: ignore[arg-type]
        project_id=_UUID_PROJ,
        media_id=_UUID_IMG,
        display_name="stale-caption.png",
        fallback_path=frame_file,
    )
    assert mid2 == _UUID_IMG2
    assert miss.uploaded is True


@pytest.mark.asyncio
async def test_kind_aware_pacing_aimd_backoff_breather_and_local_credit_deduction(
    tmp_path: Path,
) -> None:
    now = [2000.0]
    sleeps: list[float] = []

    async def noop_exec(profile: str, _t: str, _pl: dict[str, Any]) -> ExecutionOutcome:
        return ExecutionOutcome(status="succeeded", profile=profile)

    _, scheduler = _build_single_ip_pool(
        tmp_path,
        noop_exec,
        now_ref=now,
        sleeps=sleeps,
        settings_overrides={
            "egress_min_interval_s": 15.0,
            "image_egress_min_interval_s": 4.0,
            "egress_jitter_ratio": 0.0,
            "egress_breather_every": 2,
            "egress_breather_s": 40.0,
        },
    )
    await scheduler.refresh_credits(force=True)

    # 1. Image spacing uses 4s, not 15s
    assert await scheduler.wait_egress_pacing("direct", "acc_c", kind="image") == 0.0
    assert await scheduler.wait_egress_pacing("direct", "acc_c", kind="image") == pytest.approx(4.0)

    # 2. Video spacing uses 15s
    w_vid = await scheduler.wait_egress_pacing("direct", "acc_c", kind="video")
    assert w_vid == pytest.approx(15.0)

    # 3. Video completion deducts local credits and increments breather counter
    scheduler.record_attempt(
        "acc_c",
        ExecutionOutcome(status="succeeded", profile="acc_c", quoted_credits=25),
        kind="video",
    )
    scheduler.record_attempt(
        "acc_c",
        ExecutionOutcome(status="succeeded", profile="acc_c", quoted_credits=25),
        kind="video",
    )
    snap = {a.profile: a for a in scheduler.snapshot_health()}
    assert snap["acc_c"].credits == 2450  # 2500 - 25 - 25

    # 4. After 2 consecutive videos (egress_breather_every=2), next video waits 40s breather
    w_breather = await scheduler.wait_egress_pacing("direct", "acc_c", kind="video")
    assert w_breather == pytest.approx(40.0)

    # 5. AIMD backoff on WAF raises multiplier 1.0 -> 1.8, then 5 successes decay 1.8 -> 1.44
    scheduler.record_attempt(
        "acc_c",
        ExecutionOutcome(
            status="failed",
            profile="acc_c",
            error_type="waf-rejection",
            error_detail="PUBLIC_ERROR_UNUSUAL_ACTIVITY",
            charged="no",
        ),
        kind="video",
    )
    eg_after_waf = scheduler.store.get_egress_state("direct")
    assert eg_after_waf["backoff_multiplier"] == pytest.approx(1.8)

    for _ in range(5):
        scheduler.record_attempt(
            "acc_c", ExecutionOutcome(status="succeeded", profile="acc_c"), kind="image"
        )
    eg_decayed = scheduler.store.get_egress_state("direct")
    assert eg_decayed["backoff_multiplier"] == pytest.approx(1.44)


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_single_profile_video_pipeline_runs_two_renders_without_ip_mix(
    tmp_path: Path,
) -> None:
    """Third-tier optimization: on a single IP, one primary profile pipelines 2 concurrent
    video renders while strictly serializing submit phases and NEVER mixing a second account."""
    submit_active = 0
    max_submit_active = 0
    render_active = 0
    max_render_active = 0
    profiles_used: list[str] = []
    two_rendering = asyncio.Event()
    release_renders = asyncio.Event()

    async def pipelined_exec(
        profile: str,
        _task_type: str,
        _payload: dict[str, Any],
        *,
        on_submit_done: Callable[[], None] | None = None,
    ) -> ExecutionOutcome:
        nonlocal submit_active, max_submit_active, render_active, max_render_active
        profiles_used.append(profile)
        submit_active += 1
        max_submit_active = max(max_submit_active, submit_active)
        await asyncio.sleep(0.03)  # simulate submit DOM interaction
        submit_active -= 1
        if on_submit_done is not None:
            on_submit_done()

        render_active += 1
        max_render_active = max(max_render_active, render_active)
        if render_active >= 2:
            two_rendering.set()
        await asyncio.wait_for(release_renders.wait(), timeout=5.0)
        render_active -= 1
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=_UUID_VID,
            project_id=_UUID_PROJ,
            output_paths=(str(tmp_path / "clip.mp4"),),
        )

    manager, scheduler = _build_single_ip_pool(
        tmp_path,
        pipelined_exec,
        settings_overrides={
            "max_concurrent_per_egress": 1,
            "max_inflight_videos_per_profile": 2,
        },
    )
    await scheduler.refresh_credits(force=True)

    submitted = await manager.submit_batch(
        [
            {"kind": "video", "prompt": "clip 1", "label": "v1"},
            {"kind": "video", "prompt": "clip 2", "label": "v2"},
            {"kind": "video", "prompt": "clip 3", "label": "v3"},
        ]
    )

    # Wait until 2 videos are rendering simultaneously on the same profile
    await asyncio.wait_for(two_rendering.wait(), timeout=3.0)
    assert max_submit_active == 1
    assert max_render_active == 2
    assert set(profiles_used) == {"acc_c"}  # strictly one primary account on the shared IP

    # Release renders so all 3 videos finish cleanly (v3 enters once a render slot frees)
    release_renders.set()
    waited = await manager.wait_jobs(batch_id=submitted["batch_id"], timeout_s=5.0)
    assert waited["done"] is True
    assert waited["counts"]["succeeded"] == 3
    assert profiles_used == ["acc_c", "acc_c", "acc_c"]
    assert scheduler.inflight_videos("acc_c") == 0


@pytest.mark.asyncio
async def test_warm_browser_pool_refcounts_concurrent_leases_and_transport_releases_lock_early(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """WarmBrowserPool shares one live client across concurrent leases; UiAutomationTransport
    releases _generate_lock and calls on_submit_done before rendering completes."""
    created = 0
    closed = 0

    class _FakeClient:
        def __init__(self, **_kw: Any) -> None:
            self._context = SimpleNamespace(is_closed=lambda: False)
            self._page = SimpleNamespace(is_closed=lambda: False)
            self.out_dir = tmp_path
            self.settings = None

        async def __aenter__(self) -> _FakeClient:
            nonlocal created
            created += 1
            return self

        async def __aexit__(self, *_exc: Any) -> None:
            nonlocal closed
            closed += 1

    settings = PlusSettings(state_db_path=tmp_path / "s.db", warm_browser_ttl_s=600.0)
    pool = WarmBrowserPool(settings=settings, client_factory=_FakeClient)

    async with (
        pool.checkout(
            "acc_c",
            profile_dir=tmp_path,
            headless=False,
            transport=None,
            out_dir=tmp_path,
        ) as c1,
        pool.checkout(
            "acc_c",
            profile_dir=tmp_path,
            headless=False,
            transport=None,
            out_dir=tmp_path,
        ) as c2,
    ):
        assert c1 is c2
        assert created == 1
        assert closed == 0
    assert await pool.close_all() == 1
    assert closed == 1

    # Transport dedicated-tab pipelining releases _generate_lock before render wait finishes
    transport = UiAutomationTransport()
    tabs_closed = 0

    class _FakeTab:
        url = "https://flow.google.com/project/p1"

        async def close(self) -> None:
            nonlocal tabs_closed
            tabs_closed += 1

    class _FakeContext:
        async def new_page(self) -> _FakeTab:
            return _FakeTab()

    transport._page = SimpleNamespace(  # type: ignore[assignment]  # noqa: SLF001
        url="https://flow.google.com/project/p1",
        context=_FakeContext(),
    )
    transport._setup_done = True  # noqa: SLF001
    transport.park_deferred_page = AsyncMock()  # type: ignore[method-assign]

    submit_done_called = False
    lock_free_during_render = False

    async def fake_run_video(
        tab: Any,
        request: GenerateVideoRequest,
        *,
        project_id: str | None,
        save: Any,
        poll_timeout_s: float,
        download: bool,
        on_started: Any,
        on_submit_done: Callable[[], None] | None = None,
    ) -> VideoResult:
        nonlocal lock_free_during_render
        del tab, request, save, poll_timeout_s, download, on_started
        assert transport._generate_lock.locked() is True  # noqa: SLF001
        assert on_submit_done is not None
        on_submit_done()
        # Lock must already be free while render polling continues!
        lock_free_during_render = not transport._generate_lock.locked()  # noqa: SLF001
        return VideoResult(
            status=VideoStatus(
                media_id=_UUID_VID,
                status="MEDIA_GENERATION_STATUS_SUCCESSFUL",
            ),
            local_path=tmp_path / "v.mp4",
            project_id=project_id or _UUID_PROJ,
        )

    from gflow_cli.api.transports import migrated_composer

    monkeypatch.setattr(migrated_composer, "run_video", fake_run_video)
    monkeypatch.setattr(migrated_composer, "migrated_can_serve", lambda _r, _p: True)

    def _on_submit_done() -> None:
        nonlocal submit_done_called
        submit_done_called = True

    res = await transport.generate_video(
        request=GenerateVideoRequest(prompt="test clip", mode=Mode.T2V),
        project_id=_UUID_PROJ,
        out_dir=tmp_path,
        on_submit_done=_on_submit_done,
    )
    assert res.status.succeeded is True
    assert submit_done_called is True
    assert lock_free_during_render is True
    assert tabs_closed == 1


@pytest.mark.asyncio
@pytest.mark.sharpit(allow_n_plus_one=True)
async def test_sticky_project_rotates_at_max_uses_and_quiet_time_decays_backoff(
    tmp_path: Path,
) -> None:
    """Sticky projects rotate once sticky_project_max_uses is reached so projects never
    overflow Flow's first-screen grid; stale AIMD backoff decays after quiet windows."""
    now = [1000.0]
    sleeps: list[float] = []
    seen_projects: list[str | None] = []
    proj_counter = 0

    async def fake_exec(
        profile: str, _task_type: str, payload: dict[str, Any]
    ) -> ExecutionOutcome:
        nonlocal proj_counter
        pid = payload.get("project_id")
        seen_projects.append(pid)
        if not pid:
            proj_counter += 1
            pid = f"aaaaaaaa-aaaa-4aaa-8aaa-{proj_counter:012d}"
        return ExecutionOutcome(status="succeeded", profile=profile, project_id=pid)

    manager, scheduler = _build_single_ip_pool(
        tmp_path,
        fake_exec,
        now_ref=now,
        sleeps=sleeps,
        settings_overrides={
            "sticky_project": True,
            "sticky_project_max_uses": 2,
            "egress_waf_cooldown_s": 180.0,
            "egress_min_interval_s": 15.0,
            "image_egress_min_interval_s": 4.0,
        },
    )
    await scheduler.refresh_credits(force=True)

    # First 2 jobs share project #1; 3rd job rotates to a fresh project #2.
    for i in range(3):
        job = await manager.submit_job(
            kind="image", task_type="t2i", payload={"prompt": f"frame {i}"}
        )
        await manager.wait_jobs(job_ids=[job.job_id], timeout_s=5.0)

    assert seen_projects[0] is None
    assert seen_projects[1] == "aaaaaaaa-aaaa-4aaa-8aaa-000000000001"
    assert seen_projects[2] is None
    report = scheduler.health_report()
    assert report["sticky_project_max_uses"] == 2
    assert "scheduling_hint" in report

    # Simulate stale backoff from an earlier WAF, then advance 3 quiet cooldown windows.
    scheduler.store.upsert_egress_state(
        "direct",
        last_used_at=now[0],
        backoff_multiplier=3.24,
        consecutive_successes=0,
    )
    now[0] += 600.0  # > 3 * 180s quiet
    await scheduler.wait_egress_pacing("direct", "acc_c", kind="image")
    eg = scheduler.store.get_egress_state("direct")
    assert eg["backoff_multiplier"] == pytest.approx(1.0)


def test_outcome_from_task_marks_free_image_and_explicit_waf_refusal_uncharged(
    tmp_path: Path,
) -> None:
    """0-credit image failures and explicit wire WAF refusals without media_id must not
    set charged='maybe' / may_have_spent=True."""
    from gflow_cli.plus.pool.executor import _outcome_from_task  # noqa: PLC2701
    from gflow_cli.worker.queue import QueueTask

    img_task = QueueTask(
        task_id="t1",
        profile_name="acc_a",
        task_type="i2i",
        payload={"prompt": "test"},
        status="failed",
        error={
            "type": "https://gflow-cli.dev/errors/waf-rejection",
            "detail": "Flow refused the submit: PUBLIC_ERROR_UNUSUAL_ACTIVITY (gRPC 7)",
            "charged": "maybe",
            "exit_code": 10,
        },
        flow_media_id=None,
    )
    img_outcome = _outcome_from_task(img_task, profile="acc_a", db_path=tmp_path / "none.db")
    assert img_outcome.charged == "no"

    vid_task = QueueTask(
        task_id="t2",
        profile_name="acc_a",
        task_type="i2v",
        payload={"prompt": "test"},
        status="failed",
        error={
            "type": "https://gflow-cli.dev/errors/waf-rejection",
            "detail": "Flow refused the submit: PUBLIC_ERROR_UNUSUAL_ACTIVITY (gRPC 7)",
            "charged": "maybe",
            "exit_code": 10,
        },
        flow_media_id=None,
    )
    vid_outcome = _outcome_from_task(vid_task, profile="acc_a", db_path=tmp_path / "none.db")
    assert vid_outcome.charged == "no"

    # A timeout after submit (not an explicit refusal) must stay charged='maybe'
    unknown_vid = QueueTask(
        task_id="t3",
        profile_name="acc_a",
        task_type="i2v",
        payload={"prompt": "test"},
        status="failed",
        error={
            "type": "https://gflow-cli.dev/errors/transport-timeout",
            "detail": "Submit was already clicked",
            "charged": "maybe",
            "exit_code": 9,
        },
        flow_media_id=None,
    )
    unknown_outcome = _outcome_from_task(
        unknown_vid, profile="acc_a", db_path=tmp_path / "none.db"
    )
    assert unknown_outcome.charged == "maybe"


def test_unique_display_name_caps_long_stems(tmp_path: Path) -> None:
    """Long production filenames are capped to stay within Flow's @ picker debounce window."""
    from gflow_cli.api.transports.migrated_composer import _unique_display_name

    long_path = tmp_path / "ref_01_grey_3pc_stacked_1011741626174_extra_long_suffix.jpg"
    name = _unique_display_name(long_path)
    assert len(name) <= 37  # 24 stem + 1 hyphen + 8 hex + 4 ext
    assert name.endswith(".jpg")
    assert name.startswith("ref_01_grey_3pc_stacked-")
