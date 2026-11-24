"""Execution bridge that runs one generation payload via upstream FlowWorker.

Reusing ``worker.codec`` and ``worker.daemon.FlowWorker`` guarantees that every
generated image/video is validated identically, recorded into ``gflow.db`` for
``gflow data download`` recovery, and honours ``--max-credits`` and checkpoints.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any

from gflow_cli.config import get_settings
from gflow_cli.errors import GFlowError, QueueSchemaError
from gflow_cli.plus.pool.models import ChargedState, ExecutionOutcome
from gflow_cli.plus.win_stealth import taskbar_stealth_guard
from gflow_cli.worker import codec
from gflow_cli.worker.daemon import FlowWorker
from gflow_cli.worker.queue import QueueTask

_IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".webp")


def check_existing_output(payload: dict[str, Any]) -> Path | None:
    """Return existing non-empty ``output_file`` when resumable skip applies.

    An image may sit under another image suffix: gflow renames a ``.png`` target
    to ``.jpg`` when Flow serves JPEG bytes (``paths.correct_image_extension``).
    """
    raw = payload.get("output_file")
    if not isinstance(raw, str) or not raw.strip():
        return None
    target = Path(raw)
    candidates = [target]
    if target.suffix.lower() in _IMAGE_SUFFIXES:
        candidates += [target.with_suffix(s) for s in _IMAGE_SUFFIXES]
    for candidate in candidates:
        if candidate.is_file() and candidate.stat().st_size > 0:
            return candidate
    return None


def validate_payload(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize a generation payload via upstream ``worker.codec``."""
    decoded = codec.decode_payload(task_type, payload)
    return codec.encode_payload(task_type, decoded)


async def execute_via_worker(
    profile: str,
    task_type: str,
    payload: dict[str, Any],
) -> ExecutionOutcome:
    """Execute one generation payload on ``profile`` using upstream ``FlowWorker``."""
    try:
        encoded = validate_payload(task_type, payload)
    except QueueSchemaError as exc:
        return ExecutionOutcome(
            status="failed",
            profile=profile,
            error_type=exc.problem_type,
            error_detail=exc.detail or str(exc),
            charged="no",
            exit_code=30,
        )

    settings = get_settings()
    db_path = settings.db_path or (settings.home / "gflow.db")
    task_id = f"plus-{uuid.uuid4().hex[:12]}"
    claimant = f"plus:{profile}:{os.getpid()}"

    worker = FlowWorker(profile_name=profile, db_path=str(db_path))
    try:
        worker.repo.enqueue_task(
            task_id=task_id,
            profile_name=profile,
            task_type=task_type,
            payload=encoded,
        )
        claimed = worker.repo.claim_task(task_id, claimant)
        if claimed is None:
            return ExecutionOutcome(
                status="failed",
                profile=profile,
                error_type="claim-failed",
                error_detail=f"Failed to claim queued task {task_id}",
                charged="no",
            )
        async with taskbar_stealth_guard():
            await worker.run_task(claimed)
        final = worker.repo.get_task(task_id)
    except GFlowError as exc:
        charged: ChargedState = exc.charged
        return ExecutionOutcome(
            status="failed",
            profile=profile,
            error_type=exc.problem_type,
            error_detail=exc.detail or str(exc),
            charged=charged,
        )
    finally:
        worker.close()

    if final is None:
        return ExecutionOutcome(
            status="failed",
            profile=profile,
            error_type="task-missing",
            error_detail=f"Task {task_id} vanished after execution",
            charged="maybe",
        )
    return _outcome_from_task(final, profile=profile, db_path=db_path)


def _outcome_from_task(task: QueueTask, *, profile: str, db_path: Path) -> ExecutionOutcome:
    payload = task.payload
    model = str(payload.get("model") or "") or None
    aspect = str(payload.get("aspect") or "") or None
    raw_duration = payload.get("duration")
    duration = int(raw_duration) if isinstance(raw_duration, int) else None

    if task.status == "completed":
        paths = _resolve_output_paths(task, db_path=db_path)
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=task.flow_media_id,
            output_paths=paths,
            model=model,
            aspect=aspect,
            duration=duration,
            charged="no",
        )

    err = task.error or {}
    raw_charged = str(err.get("charged") or "no")
    charged: ChargedState = (
        "yes" if raw_charged == "yes" else "maybe" if raw_charged == "maybe" else "no"
    )
    status = "indeterminate" if task.status == "indeterminate" else "failed"
    raw_exit = err.get("exit_code")
    return ExecutionOutcome(
        status=status,
        profile=profile,
        media_id=task.flow_media_id,
        model=model,
        aspect=aspect,
        duration=duration,
        error_type=str(err.get("type") or err.get("title") or status),
        error_detail=str(err.get("detail") or err.get("title") or "generation failed"),
        charged=charged,
        exit_code=int(raw_exit) if isinstance(raw_exit, int) else None,
    )


def _resolve_output_paths(task: QueueTask, *, db_path: Path) -> tuple[str, ...]:
    existing = check_existing_output(task.payload)
    if existing is not None:
        return (str(existing),)
    if not task.flow_media_id or not db_path.exists():
        return ()
    con = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    try:
        rows = con.execute(
            """
            SELECT lf.path
            FROM local_files lf
            JOIN assets a ON a.id = lf.asset_id
            WHERE a.flow_media_id = ? AND lf.superseded_at IS NULL
            ORDER BY lf.id DESC
            """,
            (task.flow_media_id,),
        ).fetchall()
    finally:
        con.close()
    return tuple(str(r[0]) for r in rows if r[0])
