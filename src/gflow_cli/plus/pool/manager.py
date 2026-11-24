"""Non-blocking multi-account job manager — eliminates 50s MCP sync timeouts.

Jobs are persisted immediately and executed in background asyncio tasks across
all healthy accounts (1 lane per profile, staggered cold starts, automatic
failover on non-charged errors). Callers poll status or call ``wait_jobs`` with
a bounded timeout (<= 35s).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

import structlog

from gflow_cli.plus.config import PlusSettings, get_plus_settings
from gflow_cli.plus.pool.executor import (
    check_existing_output,
    execute_via_worker,
    ledger_duration,
    validate_payload,
)
from gflow_cli.plus.pool.models import AttemptRecord, ExecutionOutcome, JobKind, JobStatus, PoolJob
from gflow_cli.plus.pool.owner import OwnerRegistry
from gflow_cli.plus.pool.scheduler import AccountPoolScheduler
from gflow_cli.plus.pool.state_store import PlusStateStore, utc_now_iso

log = structlog.get_logger(__name__)

JobExecutor = Callable[[str, str, dict[str, Any]], Awaitable[ExecutionOutcome]]

_TERMINAL_STATUSES: frozenset[JobStatus] = frozenset(
    {"succeeded", "failed", "indeterminate", "cancelled"}
)
_MAX_SAFE_WAIT_S = 35.0
_LANE_POLL_S = 0.5
_JOB_POLL_S = 0.5


class PoolJobManager:
    """Async multi-lane job dispatcher backed by ``AccountPoolScheduler``."""

    def __init__(
        self,
        *,
        scheduler: AccountPoolScheduler | None = None,
        executor: JobExecutor | None = None,
        settings: PlusSettings | None = None,
        store: PlusStateStore | None = None,
    ) -> None:
        self._settings = settings or (scheduler.settings if scheduler else get_plus_settings())
        default_store = (
            scheduler.store
            if scheduler
            else PlusStateStore(self._settings.resolved_state_db_path())
        )
        self._store = store or default_store
        self._scheduler = scheduler or AccountPoolScheduler(
            store=self._store,
            settings=self._settings,
        )
        self._executor = executor or execute_via_worker
        self._owners = OwnerRegistry(self._store.db_path.parent / "plus_owners")
        self._tasks: dict[str, asyncio.Task[None]] = {}

    @property
    def scheduler(self) -> AccountPoolScheduler:
        return self._scheduler

    @property
    def store(self) -> PlusStateStore:
        return self._store

    async def submit_job(
        self,
        *,
        kind: JobKind,
        task_type: str,
        payload: dict[str, Any],
        batch_id: str | None = None,
        label: str | None = None,
        preferred_profile: str | None = None,
        skip_if_exists: bool = True,
        start_background: bool = True,
    ) -> PoolJob:
        """Validate, persist, and immediately launch one background generation job."""
        normalized = validate_payload(task_type, payload)
        job_id = f"job-{uuid.uuid4().hex[:10]}"

        existing = check_existing_output(normalized) if skip_if_exists else None
        if existing is not None:
            profile = preferred_profile or "cached"
            job = PoolJob(
                job_id=job_id,
                batch_id=batch_id,
                label=label,
                kind=kind,
                task_type=task_type,
                status="succeeded",
                preferred_profile=preferred_profile,
                assigned_profile=profile,
                payload=normalized,
                result=_skipped_ledger(normalized, profile, existing),
                owner=self._owners.owner_id,
            )
            self._store.create_job(job)
            return job

        job = PoolJob(
            job_id=job_id,
            batch_id=batch_id,
            label=label,
            kind=kind,
            task_type=task_type,
            status="queued",
            preferred_profile=preferred_profile,
            payload=normalized,
            owner=self._owners.owner_id,
        )
        self._store.create_job(job)

        if start_background:
            self._start(job_id)
        return job

    def _start(self, job_id: str) -> None:
        task = asyncio.create_task(self._run_job(job_id), name=f"gflow-plus-{job_id}")
        self._tasks[job_id] = task
        task.add_done_callback(lambda t, jid=job_id: self._finished(jid, t))

    def _finished(self, job_id: str, task: asyncio.Task[None]) -> None:
        """Close out a job whose runner crashed, so no poller waits on it forever."""
        self._tasks.pop(job_id, None)
        if task.cancelled() or (exc := task.exception()) is None:
            return
        log.error("plus.pool.job_crashed", job_id=job_id, exc_info=exc)
        job = self._store.get_job(job_id)
        if job is None or job.status in _TERMINAL_STATUSES:
            return
        spent = job.status == "running" and job.kind == "video"
        job.status = "indeterminate" if spent else "failed"
        job.may_have_spent = spent
        job.error = {
            "type": type(exc).__name__,
            "detail": str(exc),
            "charged": "maybe" if spent else "no",
        }
        self._store.save_job(job)

    def recover_orphans(self) -> dict[str, list[str]]:
        """Take over unfinished jobs whose owning process has exited.

        A queued job never reached Flow, so it is started here. A running image is
        free and started again unless its output already exists. A running video
        may have been submitted and charged, so it is never re-run: it is marked
        indeterminate for a person to check Flow before spending again.
        """
        restarted: list[str] = []
        abandoned: list[str] = []
        for job in self._store.list_unfinished_jobs():
            if self._owners.is_alive(job.owner):
                continue
            job.owner = self._owners.owner_id
            if job.status == "running" and job.kind == "video":
                job.status = "indeterminate"
                job.may_have_spent = True
                job.error = {
                    "type": "owner-exited",
                    "detail": (
                        "The gflow process running this video exited before it finished. "
                        "Flow may have generated and charged it: check gflow data or Flow "
                        "before submitting it again."
                    ),
                    "charged": "maybe",
                }
                self._store.save_job(job)
                abandoned.append(job.job_id)
                continue
            existing = check_existing_output(job.payload)
            if existing is not None:
                job.status = "succeeded"
                job.result = _skipped_ledger(
                    job.payload, job.assigned_profile or "cached", existing
                )
                self._store.save_job(job)
                continue
            job.status = "queued"
            self._store.save_job(job)
            self._start(job.job_id)
            restarted.append(job.job_id)
        if restarted or abandoned:
            log.info("plus.pool.recovered", restarted=restarted, abandoned=abandoned)
        return {"restarted": restarted, "abandoned": abandoned}

    async def submit_batch(
        self,
        items: Sequence[dict[str, Any]],
        *,
        batch_id: str | None = None,
        skip_if_exists: bool = True,
    ) -> dict[str, Any]:
        """Submit a batch of image/video items to run in parallel across all healthy lanes."""
        resolved_batch = batch_id or f"batch-{uuid.uuid4().hex[:8]}"
        jobs: list[PoolJob] = []
        for idx, item in enumerate(items, start=1):
            kind, task_type, payload = _parse_batch_item(item)
            job = await self.submit_job(
                kind=kind,
                task_type=task_type,
                payload=payload,
                batch_id=resolved_batch,
                label=str(item.get("label") or f"item-{idx:02d}"),
                preferred_profile=item.get("profile"),
                skip_if_exists=bool(item.get("skip_if_exists", skip_if_exists)),
            )
            jobs.append(job)
        return {
            "batch_id": resolved_batch,
            "total": len(jobs),
            "skipped_existing": sum(1 for j in jobs if j.status == "succeeded"),
            "queued": sum(1 for j in jobs if j.status == "queued"),
            "jobs": [j.to_public_dict() for j in jobs],
        }

    def get_job(self, job_id: str) -> PoolJob | None:
        return self._store.get_job(job_id)

    def list_jobs(
        self,
        *,
        batch_id: str | None = None,
        status: JobStatus | None = None,
        limit: int = 50,
    ) -> list[PoolJob]:
        return self._store.list_jobs(batch_id=batch_id, status=status, limit=limit)

    def batch_summary(self, batch_id: str) -> dict[str, Any]:
        jobs = self._store.list_jobs(batch_id=batch_id, limit=500)
        counts = _count_statuses(jobs)
        return {
            "batch_id": batch_id,
            "done": (counts["queued"] + counts["running"]) == 0,
            "total": len(jobs),
            "counts": counts,
            "jobs": [j.to_public_dict() for j in jobs],
        }

    async def wait_jobs(
        self,
        *,
        job_ids: Sequence[str] = (),
        batch_id: str | None = None,
        timeout_s: float | None = None,
    ) -> dict[str, Any]:
        """Wait up to ``min(timeout_s, 35s)`` for jobs to finish, never exceeding MCP timeouts.

        Reads job state from the store, so it also waits on jobs another gflow
        process runs. Each job comes back in its brief form; ``get_job`` has the rest.
        """
        cap = min(
            float(timeout_s if timeout_s is not None else self._settings.default_wait_timeout_s),
            _MAX_SAFE_WAIT_S,
        )
        cap = max(0.05, cap)
        targets = list(job_ids)
        if batch_id and not targets:
            targets = [j.job_id for j in self._store.list_jobs(batch_id=batch_id, limit=500)]

        loop = asyncio.get_running_loop()
        deadline = loop.time() + cap
        while True:
            jobs = [j for jid in targets if (j := self._store.get_job(jid)) is not None]
            remaining = [j.job_id for j in jobs if j.status not in _TERMINAL_STATUSES]
            left = deadline - loop.time()
            if not remaining or left <= 0:
                break
            await asyncio.sleep(min(_JOB_POLL_S, left))
        return {
            "done": len(remaining) == 0,
            "timed_out": len(remaining) > 0,
            "waited_cap_s": cap,
            "counts": _count_statuses(jobs),
            "remaining_job_ids": remaining,
            "jobs": [j.to_brief_dict() for j in jobs],
        }

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        job = self._store.get_job(job_id)
        if job is None:
            return {"status": "error", "error": f"Unknown job_id: {job_id}"}
        if job.status in _TERMINAL_STATUSES:
            return {"status": "ok", "job_id": job_id, "job_status": job.status, "changed": False}
        if job.status == "queued":
            # The owning process, this one or another, reads this before it runs the job.
            job.status = "cancelled"
            self._store.save_job(job)
            task = self._tasks.get(job_id)
            if task and not task.done():
                task.cancel()
            return {"status": "ok", "job_id": job_id, "job_status": "cancelled", "changed": True}
        return {
            "status": "ok",
            "job_id": job_id,
            "job_status": job.status,
            "changed": False,
            "note": "Job is already running; local cancel cannot stop Flow once submitted.",
        }

    def _is_cancelled(self, job_id: str) -> bool:
        current = self._store.get_job(job_id)
        return current is not None and current.status == "cancelled"

    async def _run_job(self, job_id: str) -> None:
        job = self._store.get_job(job_id)
        if job is None or job.status in _TERMINAL_STATUSES:
            return

        tried: set[str] = set()
        max_attempts = self._settings.max_failover_attempts
        required_credits = _required_credits(job)
        if job.kind == "video":
            # Video picks by balance; a balance older than the cache TTL is read again.
            await self._scheduler.refresh_credits()

        for _ in range(max_attempts):
            profile = await self._acquire_profile(
                job.kind,
                preferred=job.preferred_profile if not tried else None,
                exclude=frozenset(tried),
                required_credits=required_credits,
            )
            if self._is_cancelled(job_id):
                return
            if profile is None:
                job.status = "failed"
                job.error = {
                    "type": "no-healthy-profile",
                    "detail": (
                        "No healthy profile with sufficient credits is available "
                        f"(tried={sorted(tried)})"
                    ),
                    "charged": "no",
                }
                self._store.save_job(job)
                return

            tried.add(profile)
            outcome = await self._execute_on_lane(job, profile)
            decision = self._scheduler.record_attempt(profile, outcome)
            if outcome.status == "succeeded":
                job.status = "succeeded"
                job.assigned_profile = profile
                job.result = outcome.ledger_dict()
                job.error = None
                self._store.save_job(job)
                return

            if outcome.charged != "no" or not decision.retry_other_profile:
                job.status = "indeterminate" if outcome.status == "indeterminate" else "failed"
                job.assigned_profile = profile
                job.may_have_spent = outcome.charged != "no"
                job.error = {
                    "type": outcome.error_type,
                    "detail": outcome.error_detail,
                    "charged": outcome.charged,
                    "media_id": outcome.media_id,
                    "exit_code": outcome.exit_code,
                }
                self._store.save_job(job)
                return

        job.status = "failed"
        job.error = {
            "type": "failover-exhausted",
            "detail": f"All {max_attempts} failover attempts failed (tried={sorted(tried)})",
            "charged": "no",
        }
        self._store.save_job(job)

    async def _acquire_profile(
        self,
        kind: JobKind,
        *,
        preferred: str | None,
        exclude: frozenset[str],
        required_credits: int | None,
    ) -> str | None:
        """Pick an idle healthy profile, queueing for as long as every eligible lane is busy."""
        while True:
            picked = self._scheduler.pick_profile(
                kind,
                preferred=preferred,
                exclude=exclude,
                required_credits=required_credits,
                allow_busy=False,
            )
            if picked is not None:
                return picked
            any_candidate = self._scheduler.pick_profile(
                kind,
                preferred=preferred,
                exclude=exclude,
                required_credits=required_credits,
                allow_busy=True,
            )
            if any_candidate is None:
                return None
            await asyncio.sleep(_LANE_POLL_S)

    async def _execute_on_lane(self, job: PoolJob, profile: str) -> ExecutionOutcome:
        lock = self._scheduler.lane_lock(profile)
        async with lock:
            self._scheduler.mark_busy(profile, True)
            started_at = utc_now_iso()
            try:
                job.status = "running"
                job.assigned_profile = profile
                self._store.save_job(job)
                await self._scheduler.wait_stagger()
                await self._scheduler.wait_lane_jitter(profile)
                started_at = utc_now_iso()
                outcome = await self._executor(profile, job.task_type, job.payload)
            except Exception as exc:  # noqa: BLE001 — capture unexpected lane errors safely
                log.warning("plus.pool.lane_exception", profile=profile, exc_info=exc)
                outcome = ExecutionOutcome(
                    status="failed",
                    profile=profile,
                    error_type=type(exc).__name__,
                    error_detail=str(exc),
                    charged="no",
                )
            finally:
                self._scheduler.mark_busy(profile, False)

            attempt = AttemptRecord(
                profile=profile,
                status=outcome.status,
                error_type=outcome.error_type,
                error_detail=outcome.error_detail,
                charged=outcome.charged,
                started_at=started_at,
                finished_at=utc_now_iso(),
            )
            job.attempts.append(attempt.to_dict())
            self._store.save_job(job)
            return outcome


def _count_statuses(jobs: Sequence[PoolJob]) -> dict[str, int]:
    counts: dict[str, int] = dict.fromkeys(
        ("queued", "running", "succeeded", "failed", "indeterminate", "cancelled"), 0
    )
    for job in jobs:
        counts[job.status] += 1
    return counts


def _skipped_ledger(payload: dict[str, Any], profile: str, existing: Path) -> dict[str, Any]:
    return ExecutionOutcome(
        status="succeeded",
        profile=profile,
        output_paths=(str(existing),),
        model=str(payload.get("model") or "") or None,
        aspect=str(payload.get("aspect") or "") or None,
        duration=ledger_duration((str(existing),), payload.get("duration")),
        skipped_existing=True,
    ).ledger_dict()


def _required_credits(job: PoolJob) -> int | None:
    if job.kind != "video":
        return None
    raw = job.payload.get("max_credits")
    return int(raw) if isinstance(raw, int) else None


def _parse_batch_item(item: dict[str, Any]) -> tuple[JobKind, str, dict[str, Any]]:
    raw_kind = str(item.get("kind") or "").lower()
    raw_mode = str(item.get("mode") or item.get("task_type") or "").lower()
    if raw_kind == "image" or raw_mode in ("t2i", "i2i"):
        has_refs = bool(item.get("ref_paths") or item.get("refs") or item.get("reference_images"))
        task_type = raw_mode if raw_mode in ("t2i", "i2i") else ("i2i" if has_refs else "t2i")
        payload = _extract_image_payload(item)
        return "image", task_type, payload
    mode = raw_mode if raw_mode in ("t2v", "i2v", "r2v") else _infer_video_mode(item)
    payload = _extract_video_payload(item, mode)
    return "video", mode, payload


def _infer_video_mode(item: dict[str, Any]) -> str:
    if item.get("start_image") or item.get("initial_frame"):
        return "i2v"
    if item.get("reference_images") or item.get("reference_entities"):
        return "r2v"
    return "t2v"


def _extract_image_payload(item: dict[str, Any]) -> dict[str, Any]:
    ref_paths = list(item.get("ref_paths") or item.get("reference_images") or [])
    payload: dict[str, Any] = {
        "prompt": item["prompt"],
        "aspect": item.get("aspect", "9:16"),
        "model": item.get("model", "nano2"),
        "count": int(item.get("count", 1)),
    }
    if ref_paths:
        payload["ref_paths"] = ref_paths
    for key in (
        "refs",
        "reference_entities",
        "reference_entity_names",
        "project_id",
        "project_name",
        "out_dir",
    ):
        if item.get(key) is not None:
            payload[key] = item[key]
    out_file = item.get("output_file") or item.get("out_file")
    if out_file:
        payload["output_file"] = str(out_file)
    return payload


def _extract_video_payload(item: dict[str, Any], mode: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "prompt": item["prompt"],
        "mode": mode,
        "aspect": item.get("aspect", "9:16"),
        "model": item.get("model", "omni-1.2-flash"),
        "count": int(item.get("count", 1)),
    }
    start = item.get("start_image") or item.get("initial_frame")
    if start:
        payload["start_image"] = str(start)
    for key in (
        "end_image",
        "duration",
        "max_credits",
        "reference_images",
        "reference_entities",
        "reference_entity_names",
        "project_id",
        "project_name",
        "out_dir",
        "resolution",
    ):
        if item.get(key) is not None:
            payload[key] = item[key]
    out_file = item.get("output_file") or item.get("out_file")
    if out_file:
        payload["output_file"] = str(out_file)
    return payload


_default_manager: PoolJobManager | None = None


def get_pool_manager() -> PoolJobManager:
    global _default_manager
    if _default_manager is None:
        _default_manager = PoolJobManager()
    return _default_manager


def reset_pool_manager() -> None:
    global _default_manager
    _default_manager = None
