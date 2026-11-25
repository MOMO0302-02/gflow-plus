"""Execution bridge that runs one pool payload via upstream FlowWorker or FlowApiClient.

Reusing ``worker.codec`` and ``worker.daemon.FlowWorker`` guarantees that every
generated image/video is validated identically, recorded into ``gflow.db`` for
``gflow data download`` recovery, and honours ``--max-credits`` and checkpoints.
All execution lanes share ``WarmBrowserPool`` so consecutive jobs on one profile
reuse its live Chrome window instead of paying a cold start every time.
"""

from __future__ import annotations

import os
import sqlite3
import struct
import uuid
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

from gflow_cli.api.image_upscale import TargetResolution
from gflow_cli.api.transports.migrated_video_upscale import VALID_VIDEO_SCALES
from gflow_cli.api.video import is_media_uuid
from gflow_cli.cli_image import lookup_project_in_catalog
from gflow_cli.config import get_settings
from gflow_cli.errors import GFlowError, QueueSchemaError
from gflow_cli.plus.pool.browser_pool import get_warm_browser_pool
from gflow_cli.plus.pool.models import ChargedState, ExecutionOutcome
from gflow_cli.plus.pool.workflows import is_job_ref, payload_has_job_refs
from gflow_cli.plus.win_stealth import taskbar_stealth_guard
from gflow_cli.worker import codec
from gflow_cli.worker.daemon import FlowWorker
from gflow_cli.worker.queue import QueueTask

_IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".webp")
MEDIA_EDIT_TASK_TYPES: frozenset[str] = frozenset(
    {"upscale_image", "upscale_video", "extend_video"}
)


def mp4_duration_s(path: Path) -> float | None:
    """Seconds from an MP4's ``moov/mvhd`` header, or None when the file has none.

    gflow cannot know a clip's length when none was requested (Flow keeps the last
    one picked), and PyAV is only an optional extra, so the ledger reads the file.
    """
    with path.open("rb") as f:
        while len(header := f.read(8)) == 8:
            size, kind = struct.unpack(">I4s", header)
            if size == 1:
                size = struct.unpack(">Q", f.read(8))[0] - 8
            if size == 0:
                body_len = None
            elif size < 8:
                return None
            else:
                body_len = size - 8
            if kind == b"moov":
                return _mvhd_seconds(f.read() if body_len is None else f.read(body_len))
            if body_len is None:
                return None
            f.seek(body_len, os.SEEK_CUR)
    return None


def _mvhd_seconds(moov: bytes) -> float | None:
    at = moov.find(b"mvhd")
    if at < 4:
        return None
    fields = at + 8  # past the box type, version and flags
    if moov[at + 4] == 1:
        timescale, duration = struct.unpack_from(">IQ", moov, fields + 16)
    else:
        timescale, duration = struct.unpack_from(">II", moov, fields + 8)
    return round(duration / timescale, 2) if timescale else None


def ledger_duration(output_paths: Sequence[str], requested: object) -> float | None:
    """The delivered clip's real length, else the length that was asked for."""
    for raw in output_paths:
        path = Path(raw)
        if path.suffix.lower() == ".mp4" and path.is_file():
            seconds = mp4_duration_s(path)
            if seconds is not None:
                return seconds
    return float(requested) if isinstance(requested, int) else None


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


def lookup_media_owner(
    media_id: str,
    *,
    db_path: Path | None = None,
) -> tuple[str | None, str | None]:
    """Resolve ``(profile_name, flow_project_id)`` for a catalogued Flow ``media_id``."""
    settings = get_settings()
    resolved_db = db_path or settings.db_path or (settings.home / "gflow.db")
    if not resolved_db.is_file():
        return None, None
    con = sqlite3.connect(f"file:{resolved_db.as_posix()}?mode=ro", uri=True)
    try:
        row = con.execute(
            """
            SELECT profile_name, flow_project_id
            FROM assets
            WHERE flow_media_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (media_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None, None
    finally:
        con.close()
    if row is None:
        return None, None
    profile_name = str(row[0]) if row[0] else None
    project_id = str(row[1]) if row[1] else None
    return profile_name, project_id


def _validate_media_id(raw: object, *, allow_job_refs: bool) -> str:
    media_id = str(raw or "").strip()
    if allow_job_refs and is_job_ref(media_id):
        return media_id
    if not is_media_uuid(media_id):
        raise QueueSchemaError(f"media_id {media_id!r} must be a valid UUID")
    return media_id


def _validate_media_edit_payload(
    task_type: str,
    payload: dict[str, Any],
    *,
    allow_job_refs: bool,
) -> dict[str, Any]:
    out = dict(payload)
    out["media_id"] = _validate_media_id(out.get("media_id"), allow_job_refs=allow_job_refs)
    if task_type == "upscale_image":
        scale = str(out.get("scale") or "2k").strip().lower()
        try:
            TargetResolution.from_cli(scale)
        except ValueError as exc:
            raise QueueSchemaError(str(exc)) from exc
        out["scale"] = scale
        return out
    if task_type == "upscale_video":
        scale = str(out.get("scale") or "1080p").strip().lower()
        if scale not in VALID_VIDEO_SCALES:
            raise QueueSchemaError(f"scale must be one of {VALID_VIDEO_SCALES}, got {scale!r}")
        out["scale"] = scale
        return out
    prompt = str(out.get("prompt") or "").strip()
    if not prompt:
        raise QueueSchemaError("extend_video requires a non-empty prompt")
    out["prompt"] = prompt
    max_credits = out.get("max_credits")
    if max_credits is not None and (not isinstance(max_credits, int) or max_credits < 0):
        raise QueueSchemaError("max_credits must be a non-negative integer")
    return out


def validate_payload(
    task_type: str,
    payload: dict[str, Any],
    *,
    allow_job_refs: bool = False,
) -> dict[str, Any]:
    """Validate and normalize a generation, upscale, or extend payload."""
    if task_type in MEDIA_EDIT_TASK_TYPES:
        return _validate_media_edit_payload(task_type, payload, allow_job_refs=allow_job_refs)
    if allow_job_refs and payload_has_job_refs(payload):
        prompt = str(payload.get("prompt") or "").strip()
        if not prompt:
            raise QueueSchemaError(f"{task_type} requires a non-empty prompt")
        return dict(payload)
    decoded = codec.decode_payload(task_type, payload)
    return codec.encode_payload(task_type, decoded)


async def execute_via_worker(
    profile: str,
    task_type: str,
    payload: dict[str, Any],
) -> ExecutionOutcome:
    """Execute one generation, upscale, or extend payload on ``profile``."""
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

    if task_type in MEDIA_EDIT_TASK_TYPES:
        return await _execute_media_edit(profile, task_type, encoded)

    settings = get_settings()
    db_path = settings.db_path or (settings.home / "gflow.db")
    task_id = f"plus-{uuid.uuid4().hex[:12]}"
    claimant = f"plus:{profile}:{os.getpid()}"
    browser_pool = get_warm_browser_pool()

    def _open_client(
        *,
        profile_dir: Path,
        headless: bool,
        transport: str | None,
        out_dir: Path,
        settings: Any = None,
    ) -> Any:
        return browser_pool.checkout(
            profile,
            profile_dir=profile_dir,
            headless=headless,
            transport=transport,
            out_dir=out_dir,
            settings=settings,
        )

    worker = FlowWorker(
        profile_name=profile,
        db_path=str(db_path),
        client_opener=_open_client,
    )
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


async def _run_edit_on_client(
    client: Any,
    *,
    profile: str,
    task_type: str,
    media_id: str,
    project_id: str,
    payload: dict[str, Any],
    out_path: Path,
    mark_billed: Any,
) -> ExecutionOutcome:
    if task_type == "upscale_image":
        scale = str(payload["scale"])
        target = await client.upsample_image(
            media_id=media_id,
            project_id=project_id,
            target_resolution=TargetResolution.from_cli(scale),
            out_path=out_path,
        )
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=media_id,
            output_paths=(str(target),),
            model=f"upscale:{scale}",
            charged="no",
        )
    if task_type == "upscale_video":
        scale = str(payload["scale"])
        if scale == "4k":
            mark_billed()
        target = await client.upsample_video(
            media_id=media_id,
            project_id=project_id,
            scale=scale,
            out_path=out_path,
        )
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=media_id,
            output_paths=(str(target),),
            model=f"upscale:{scale}",
            duration=ledger_duration((str(target),), None),
            charged="no",
        )

    def _on_extend_started(_started: Any) -> None:
        mark_billed()

    outcome, target = await client.extend_video_migrated(
        media_id=media_id,
        project_id=project_id,
        prompt=str(payload["prompt"]),
        aspect=str(payload["aspect"]) if payload.get("aspect") else None,
        out_path=out_path,
        on_started=_on_extend_started,
        max_credits=payload.get("max_credits"),
    )
    if target is None:
        billed = mark_billed(check_only=True)
        return ExecutionOutcome(
            status="indeterminate" if billed else "failed",
            profile=profile,
            media_id=getattr(outcome, "media_id", None) or media_id,
            error_type="extend-incomplete",
            error_detail="Flow extend finished without returning a video file.",
            charged="maybe" if billed else "no",
        )
    new_media_id = getattr(outcome, "media_id", None) or media_id
    paths = (str(target),)
    return ExecutionOutcome(
        status="succeeded",
        profile=profile,
        media_id=str(new_media_id),
        output_paths=paths,
        model="video-extend",
        aspect=str(payload["aspect"]) if payload.get("aspect") else None,
        duration=ledger_duration(paths, None),
        charged="no",
    )


async def _execute_media_edit(
    profile: str,
    task_type: str,
    payload: dict[str, Any],
) -> ExecutionOutcome:
    """Execute ``upscale_image``, ``upscale_video``, or ``extend_video`` via WarmBrowserPool."""
    settings = get_settings()
    media_id = str(payload["media_id"])
    project_id = str(payload.get("project_id") or "").strip() or lookup_project_in_catalog(
        media_id, profile
    )
    if not project_id:
        _, catalog_proj = lookup_media_owner(media_id)
        project_id = catalog_proj
    if not project_id:
        return ExecutionOutcome(
            status="failed",
            profile=profile,
            media_id=media_id,
            error_type="project-required",
            error_detail=(
                f"Could not resolve owning project for media {media_id!r} on profile {profile!r}. "
                "Pass project_id explicitly."
            ),
            charged="no",
        )

    output_root = Path(str(payload["out_dir"])) if payload.get("out_dir") else settings.output_dir
    out_path = _resolve_edit_output_path(task_type, payload, output_root, media_id)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    browser_pool = get_warm_browser_pool()
    profile_dir = settings.profile_subdir(profile)
    started_billed = False

    def _mark_billed(*, check_only: bool = False) -> bool:
        nonlocal started_billed
        if not check_only:
            started_billed = True
        return started_billed

    try:
        async with (
            taskbar_stealth_guard(),
            browser_pool.checkout(
                profile,
                profile_dir=profile_dir,
                headless=settings.headless,
                transport=settings.transport,
                out_dir=output_root,
                settings=settings,
            ) as client,
        ):
            return await _run_edit_on_client(
                client,
                profile=profile,
                task_type=task_type,
                media_id=media_id,
                project_id=project_id,
                payload=payload,
                out_path=out_path,
                mark_billed=_mark_billed,
            )
    except GFlowError as exc:
        charged: ChargedState = (
            exc.charged if exc.charged != "no" else ("maybe" if started_billed else "no")
        )
        return ExecutionOutcome(
            status="indeterminate" if charged != "no" else "failed",
            profile=profile,
            media_id=media_id,
            error_type=exc.problem_type,
            error_detail=exc.detail or str(exc),
            charged=charged,
        )


def _resolve_edit_output_path(
    task_type: str,
    payload: dict[str, Any],
    output_root: Path,
    media_id: str,
) -> Path:
    explicit = payload.get("output_file")
    if isinstance(explicit, str) and explicit.strip():
        return Path(explicit)
    today = date.today().isoformat()
    if task_type == "upscale_image":
        scale = str(payload["scale"])
        return output_root / "images" / today / f"{media_id}_{scale}.png"
    if task_type == "upscale_video":
        scale = str(payload["scale"])
        ext = "gif" if scale == "270p" else "mp4"
        return output_root / "videos" / today / f"{media_id}_{scale}.{ext}"
    return output_root / "videos" / today / f"{media_id}_extended.mp4"


def _outcome_from_task(task: QueueTask, *, profile: str, db_path: Path) -> ExecutionOutcome:
    payload = task.payload
    model = str(payload.get("model") or "") or None
    aspect = str(payload.get("aspect") or "") or None
    raw_duration = payload.get("duration")
    duration = float(raw_duration) if isinstance(raw_duration, int) else None

    if task.status == "completed":
        paths = _resolve_output_paths(task, db_path=db_path)
        return ExecutionOutcome(
            status="succeeded",
            profile=profile,
            media_id=task.flow_media_id,
            output_paths=paths,
            model=model,
            aspect=aspect,
            duration=ledger_duration(paths, raw_duration),
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
