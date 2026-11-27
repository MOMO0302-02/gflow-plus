"""Non-blocking multi-account job manager — eliminates 50s MCP sync timeouts.

Jobs are persisted immediately and executed in background asyncio tasks across
all healthy accounts (1 lane per profile, staggered cold starts, automatic
failover on non-charged errors). Callers poll status or call ``wait_jobs`` with
a bounded timeout (<= 35s).
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any, cast

import structlog

from gflow_cli.plus.config import PlusSettings, get_plus_settings
from gflow_cli.plus.pool.executor import (
    check_existing_output,
    execute_via_worker,
    ledger_duration,
    lookup_media_owner,
    validate_payload,
)
from gflow_cli.plus.pool.models import AttemptRecord, ExecutionOutcome, JobKind, JobStatus, PoolJob
from gflow_cli.plus.pool.owner import OwnerRegistry
from gflow_cli.plus.pool.scheduler import AccountPoolScheduler
from gflow_cli.plus.pool.state_store import PlusStateStore, utc_now_iso
from gflow_cli.plus.pool.workflows import (
    extract_ref_tokens,
    is_job_ref,
    materialize_payload,
    parse_batch_item,
    rewrite_payload_refs,
    validate_no_cycles,
)

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
        self._executor_accepts_submit_hook = _accepts_kwarg(self._executor, "on_submit_done")
        self._owners = OwnerRegistry(self._store.db_path.parent / "plus_owners")
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._batch_projects: dict[tuple[str, str], str] = {}

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
        depends_on: Sequence[str] = (),
        skip_if_exists: bool = True,
        start_background: bool = True,
        job_id: str | None = None,
    ) -> PoolJob:
        """Validate, persist, and immediately launch one background generation job."""
        deps = list(dict.fromkeys(depends_on))
        normalized = validate_payload(task_type, payload, allow_job_refs=bool(deps))
        resolved_profile = preferred_profile
        if kind in ("upscale", "extend") and not resolved_profile:
            raw_mid = str(normalized.get("media_id") or "")
            if raw_mid and not is_job_ref(raw_mid):
                owner_profile, owner_project = lookup_media_owner(raw_mid)
                if owner_profile:
                    resolved_profile = owner_profile
                if owner_project and not normalized.get("project_id"):
                    normalized["project_id"] = owner_project

        resolved_job_id = job_id or f"job-{uuid.uuid4().hex[:10]}"

        existing = check_existing_output(normalized) if (skip_if_exists and not deps) else None
        if existing is not None:
            profile = resolved_profile or "cached"
            job = PoolJob(
                job_id=resolved_job_id,
                batch_id=batch_id,
                label=label,
                kind=kind,
                task_type=task_type,
                status="succeeded",
                preferred_profile=resolved_profile,
                assigned_profile=profile,
                payload=normalized,
                result=_skipped_ledger(normalized, profile, existing),
                depends_on=deps,
                owner=self._owners.owner_id,
            )
            self._store.create_job(job)
            return job

        job = PoolJob(
            job_id=resolved_job_id,
            batch_id=batch_id,
            label=label,
            kind=kind,
            task_type=task_type,
            status="queued",
            preferred_profile=resolved_profile,
            payload=normalized,
            depends_on=deps,
            owner=self._owners.owner_id,
        )
        self._store.create_job(job)

        if start_background:
            self._start(resolved_job_id)
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
        """Submit a batch (or dependency DAG) of items across all healthy lanes."""
        if not items:
            msg = "Batch items must not be empty"
            raise ValueError(msg)
        resolved_batch = batch_id or f"batch-{uuid.uuid4().hex[:8]}"
        prepared = self._prepare_batch_items(items, skip_if_exists=skip_if_exists)
        jobs: list[PoolJob] = []
        for entry in prepared:
            job = await self.submit_job(
                job_id=entry["job_id"],
                kind=entry["kind"],
                task_type=entry["task_type"],
                payload=entry["payload"],
                batch_id=resolved_batch,
                label=entry["label"],
                preferred_profile=entry["profile"],
                depends_on=entry["depends_on"],
                skip_if_exists=entry["skip_if_exists"],
                start_background=False,
            )
            jobs.append(job)
        for job in jobs:
            if job.status == "queued":
                self._start(job.job_id)
        return {
            "batch_id": resolved_batch,
            "total": len(jobs),
            "skipped_existing": sum(1 for j in jobs if j.status == "succeeded"),
            "queued": sum(1 for j in jobs if j.status == "queued"),
            "jobs": [j.to_public_dict() for j in jobs],
        }

    def _prepare_batch_items(
        self,
        items: Sequence[dict[str, Any]],
        *,
        skip_if_exists: bool,
    ) -> list[dict[str, Any]]:
        """Pre-allocate IDs, resolve ``job:<label|index>`` refs, and reject cycles upfront."""
        allocated_ids: list[str] = [f"job-{uuid.uuid4().hex[:10]}" for _ in items]
        labels: list[str] = [
            str(item.get("label") or f"item-{idx:02d}") for idx, item in enumerate(items, start=1)
        ]
        label_to_id: dict[str, str] = {}
        index_to_id: dict[str, str] = {}
        for idx, (jid, lbl) in enumerate(zip(allocated_ids, labels, strict=True), start=1):
            label_to_id[lbl] = jid
            index_to_id[str(idx)] = jid

        def _resolve_token(tok: str) -> str:
            if tok in label_to_id:
                return label_to_id[tok]
            if tok in index_to_id:
                return index_to_id[tok]
            if self._store.get_job(tok) is not None:
                return tok
            raise ValueError(f"Unknown upstream job reference: {tok!r}")

        kind_by_id: dict[str, JobKind] = {}
        prepared: list[dict[str, Any]] = []
        edges: dict[str, list[str]] = {}
        for idx, item in enumerate(items):
            jid = allocated_ids[idx]
            raw_media = item.get("media_id")
            upstream_kind: JobKind | None = None
            if is_job_ref(raw_media):
                ref_id = _resolve_token(str(raw_media)[4:].strip())
                upstream_kind = kind_by_id.get(ref_id)
            kind, task_type, payload = parse_batch_item(item, upstream_kind=upstream_kind)
            kind_by_id[jid] = kind
            raw_deps = item.get("depends_on")
            explicit: list[str] = (
                [str(d) for d in cast("list[object]", raw_deps)]
                if isinstance(raw_deps, list)
                else ([str(raw_deps)] if raw_deps else [])
            )
            dep_tokens = extract_ref_tokens(payload, explicit)
            dep_ids = [_resolve_token(tok) for tok in dep_tokens]
            rewritten = rewrite_payload_refs(payload, _resolve_token)
            edges[jid] = dep_ids
            prepared.append(
                {
                    "job_id": jid,
                    "label": labels[idx],
                    "kind": kind,
                    "task_type": task_type,
                    "payload": rewritten,
                    "profile": item.get("profile"),
                    "depends_on": dep_ids,
                    "skip_if_exists": bool(item.get("skip_if_exists", skip_if_exists)),
                }
            )
        validate_no_cycles(edges)
        return prepared

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
            jobs = self._store.get_jobs(targets)
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

    async def retry_jobs(
        self,
        *,
        job_id: str | None = None,
        batch_id: str | None = None,
        force_spent: bool = False,
        preferred_profile: str | None = None,
    ) -> dict[str, Any]:
        """Re-submit one failed/cancelled job or all failed/cancelled jobs in a batch.

        Hard credit-safety invariant: jobs with ``may_have_spent=True`` or status
        ``indeterminate`` are skipped unless ``force_spent=True`` is explicitly set.
        """
        targets: list[PoolJob] = []
        if job_id:
            found = self._store.get_job(job_id)
            if found is None:
                return {"status": "error", "error": f"Unknown job_id: {job_id}"}
            targets = [found]
        elif batch_id:
            targets = [
                j
                for j in self._store.list_jobs(batch_id=batch_id, limit=500)
                if j.status in ("failed", "cancelled", "indeterminate")
            ]
        else:
            return {"status": "error", "error": "Provide job_id or batch_id to retry."}

        retried: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for orig in targets:
            if orig.status not in ("failed", "cancelled", "indeterminate"):
                skipped.append(
                    {"job_id": orig.job_id, "reason": f"status is {orig.status}, not retryable"}
                )
                continue
            if (orig.may_have_spent or orig.status == "indeterminate") and not force_spent:
                skipped.append(
                    {
                        "job_id": orig.job_id,
                        "reason": (
                            "may_have_spent is true; check Flow / gflow_plus_list_media first, "
                            "or pass force_spent=True to confirm re-submission"
                        ),
                    }
                )
                continue
            new_job = await self.submit_job(
                kind=orig.kind,
                task_type=orig.task_type,
                payload=dict(orig.payload),
                batch_id=orig.batch_id,
                label=orig.label,
                preferred_profile=preferred_profile or orig.preferred_profile,
                depends_on=orig.depends_on,
                skip_if_exists=True,
            )
            retried.append({"original_job_id": orig.job_id, "new_job": new_job.to_brief_dict()})
        return {
            "status": "ok",
            "retried_count": len(retried),
            "skipped_count": len(skipped),
            "retried": retried,
            "skipped": skipped,
        }

    def _is_cancelled(self, job_id: str) -> bool:
        current = self._store.get_job(job_id)
        return current is not None and current.status == "cancelled"

    async def _wait_dependencies(self, job: PoolJob) -> dict[str, PoolJob] | None:
        """Wait for all upstream dependencies; fail this job immediately if any fails."""
        while True:
            if self._is_cancelled(job.job_id):
                return None
            deps = {d.job_id: d for d in self._store.get_jobs(job.depends_on)}
            missing = [dep_id for dep_id in job.depends_on if dep_id not in deps]
            if missing:
                self._fail_dependency(job, f"Upstream job(s) missing: {missing}")
                return None
            broken = [
                f"{d.label or d.job_id} ({d.status})"
                for d in deps.values()
                if d.status in ("failed", "cancelled", "indeterminate")
            ]
            if broken:
                self._fail_dependency(
                    job, f"Upstream dependency did not succeed: {', '.join(broken)}"
                )
                return None
            if all(d.status == "succeeded" for d in deps.values()):
                return deps
            await asyncio.sleep(_JOB_POLL_S)

    def _fail_dependency(self, job: PoolJob, detail: str) -> None:
        job.status = "failed"
        job.error = {"type": "dependency-failed", "detail": detail, "charged": "no"}
        self._store.save_job(job)

    async def _resolve_dependencies_for_run(self, job: PoolJob) -> bool:
        """Wait for dependencies, materialize placeholders, and handle skip-if-exists.

        Returns True when the job is ready to execute on a lane, False when finished/failed.
        """
        deps = await self._wait_dependencies(job)
        if deps is None:
            return False
        try:
            resolved_payload, inferred_profile = materialize_payload(job, deps)
            job.payload = validate_payload(job.task_type, resolved_payload)
            if inferred_profile and not job.preferred_profile:
                job.preferred_profile = inferred_profile
        except Exception as exc:  # noqa: BLE001 — surface resolution errors cleanly
            self._fail_dependency(job, str(exc))
            return False
        existing = check_existing_output(job.payload)
        if existing is not None:
            profile = job.preferred_profile or "cached"
            job.status = "succeeded"
            job.assigned_profile = profile
            job.result = _skipped_ledger(job.payload, profile, existing)
            self._store.save_job(job)
            return False
        self._store.save_job(job)
        return True

    async def _run_job(self, job_id: str) -> None:
        job = self._store.get_job(job_id)
        if job is None or job.status in _TERMINAL_STATUSES:
            return
        if job.depends_on and not await self._resolve_dependencies_for_run(job):
            return

        tried: set[str] = set()
        pinned = job.kind in ("upscale", "extend")
        max_attempts = 1 if pinned else self._settings.max_failover_attempts
        required_credits = _required_credits(job)
        if job.kind == "video":
            # Video picks by balance; a balance older than the cache TTL is read again.
            await self._scheduler.refresh_credits()

        for _ in range(max_attempts):
            acquired = await self._acquire_profile(
                job.kind,
                preferred=job.preferred_profile if not tried else None,
                exclude=frozenset(tried),
                required_credits=required_credits,
                pin_preferred=pinned and bool(job.preferred_profile),
            )
            if acquired is None:
                if self._is_cancelled(job_id):
                    return
                job.status = "failed"
                job.error = {
                    "type": "no-healthy-profile",
                    "detail": (
                        "No healthy profile with sufficient credits is available "
                        f"(tried={sorted(tried)}, preferred={job.preferred_profile})"
                    ),
                    "charged": "no",
                }
                self._store.save_job(job)
                return

            profile, egress = acquired
            if self._is_cancelled(job_id):
                self._scheduler.release_lane(profile, egress, kind=job.kind)
                return

            tried.add(profile)
            outcome = await self._execute_on_lane(job, profile, egress)
            decision = self._scheduler.record_attempt(profile, outcome, kind=job.kind)
            if outcome.status == "succeeded":
                if self._settings.sticky_project and job.batch_id and outcome.project_id:
                    self._batch_projects[(job.batch_id, profile)] = outcome.project_id
                job.status = "succeeded"
                job.assigned_profile = profile
                job.result = outcome.ledger_dict()
                job.error = None
                self._store.save_job(job)
                return

            if job.batch_id:
                err_text = (outcome.error_detail or outcome.error_type or "").lower()
                if any(tok in err_text for tok in ("project not found", "no project found", "404")):
                    self._batch_projects.pop((job.batch_id, profile), None)

            if pinned or outcome.charged != "no" or not decision.retry_other_profile:
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
        pin_preferred: bool = False,
    ) -> tuple[str, str] | None:
        """Pick and reserve an idle healthy profile + egress slot."""
        while True:
            picked = self._scheduler.pick_profile(
                kind,
                preferred=preferred,
                exclude=exclude,
                required_credits=required_credits,
                allow_busy=False,
                pin_preferred=pin_preferred,
            )
            if picked is not None:
                egress = self._scheduler.reserve_lane(picked, kind=kind)
                return picked, egress
            any_candidate = self._scheduler.pick_profile(
                kind,
                preferred=preferred,
                exclude=exclude,
                required_credits=required_credits,
                allow_busy=True,
                pin_preferred=pin_preferred,
            )
            if any_candidate is None:
                return None
            await asyncio.sleep(_LANE_POLL_S)

    def _prepare_execution_payload(self, job: PoolJob, profile: str) -> dict[str, Any]:
        """Adapt payload for ``profile``: strip cross-account refs on failover and
        inject sticky ``project_id`` when none was explicitly requested."""
        exec_payload = dict(job.payload)

        # If a job bound in-project refs to its upstream profile but failed over to
        # another account, fall back to the local file paths so the new account uploads cleanly.
        if job.preferred_profile and profile != job.preferred_profile:
            stripped_in_project = False
            for slot in ("start_image", "end_image"):
                ref_key = f"{slot}_ref"
                local_key = f"{ref_key}_local_path"
                if exec_payload.get(ref_key) and exec_payload.get(local_key):
                    exec_payload[slot] = exec_payload[local_key]
                    for suffix in ("", "_display_name", "_local_path", "_local_sha256"):
                        exec_payload.pop(f"{ref_key}{suffix}", None)
                    stripped_in_project = True
            if job.kind == "image" and exec_payload.get("refs") and exec_payload.get("ref_meta"):
                meta_map = cast("dict[str, dict[str, Any]]", exec_payload["ref_meta"])
                raw_refs = cast("list[str]", exec_payload["refs"])
                fallback_paths = [
                    str(meta_map[mid]["local_path"])
                    for mid in raw_refs
                    if mid in meta_map and meta_map[mid].get("local_path")
                ]
                if len(fallback_paths) == len(raw_refs):
                    exec_payload["ref_paths"] = fallback_paths
                    exec_payload.pop("refs", None)
                    exec_payload.pop("ref_meta", None)
                    stripped_in_project = True
            if stripped_in_project:
                exec_payload.pop("project_id", None)

        if (
            self._settings.sticky_project
            and job.kind in ("image", "video")
            and not exec_payload.get("project_id")
            and not exec_payload.get("project_name")
        ):
            sticky_pid = (
                self._batch_projects.get((job.batch_id, profile)) if job.batch_id else None
            ) or self._scheduler.get_sticky_project(profile)
            if sticky_pid:
                exec_payload["project_id"] = sticky_pid
        return exec_payload

    async def _execute_on_lane(
        self,
        job: PoolJob,
        profile: str,
        egress: str,
    ) -> ExecutionOutcome:
        lock = self._scheduler.lane_lock(profile)
        proxy = self._scheduler.resolve_profile_proxy(profile)
        use_pipeline = (
            job.kind == "video" and self._settings.max_inflight_videos_per_profile > 1
        )
        lock_held = False
        submit_released = False

        def _release_submit_gate() -> None:
            nonlocal lock_held, submit_released
            if lock_held:
                lock_held = False
                lock.release()
            if not submit_released:
                submit_released = True
                self._scheduler.release_submit_gate(profile, egress)

        try:
            await lock.acquire()
            lock_held = True
            started_at = utc_now_iso()
            try:
                job.status = "running"
                job.assigned_profile = profile
                self._store.save_job(job)
                await self._scheduler.wait_stagger()
                await self._scheduler.wait_egress_pacing(egress, profile, kind=job.kind)
                await self._scheduler.wait_lane_jitter(profile)
                exec_payload = self._prepare_execution_payload(job, profile)
                started_at = utc_now_iso()
                submit_hook = _release_submit_gate if use_pipeline else None
                if self._executor is execute_via_worker:
                    outcome = await execute_via_worker(
                        profile,
                        job.task_type,
                        exec_payload,
                        proxy=proxy,
                        on_submit_done=submit_hook,
                    )
                elif submit_hook is not None and self._executor_accepts_submit_hook:
                    outcome = await cast("Any", self._executor)(
                        profile,
                        job.task_type,
                        exec_payload,
                        on_submit_done=submit_hook,
                    )
                else:
                    outcome = await self._executor(profile, job.task_type, exec_payload)
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
                _release_submit_gate()
                self._scheduler.note_egress_finished(egress, profile)

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
        finally:
            _release_submit_gate()
            self._scheduler.release_lane(
                profile,
                egress,
                kind=job.kind,
                submit_already_released=True,
            )


def _accepts_kwarg(fn: Any, kwarg: str) -> bool:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    for param in sig.parameters.values():
        if param.kind == inspect.Parameter.VAR_KEYWORD or param.name == kwarg:
            return True
    return False


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
    if job.kind not in ("video", "extend"):
        return None
    raw = job.payload.get("max_credits")
    return int(raw) if isinstance(raw, int) else None


_default_manager: PoolJobManager | None = None


def get_pool_manager() -> PoolJobManager:
    global _default_manager
    if _default_manager is None:
        _default_manager = PoolJobManager()
    return _default_manager


def reset_pool_manager() -> None:
    global _default_manager
    _default_manager = None
