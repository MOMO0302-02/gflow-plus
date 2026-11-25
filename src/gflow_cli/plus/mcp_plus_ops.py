"""Extended operational, workflow, and cross-account media MCP tools for gflow-plus.

Kept in a dedicated module alongside ``mcp_plus.py`` so each file stays concise.
All generation/edit submissions return immediately with a ``job_id`` and execute
in the background per-profile warm browser pool.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field

from gflow_cli.config import get_settings
from gflow_cli.data import queries as data_queries
from gflow_cli.errors import QueueSchemaError
from gflow_cli.mcp import tools as common
from gflow_cli.mcp.server import server
from gflow_cli.plus import mcp_plus as core

_MediaKind = Annotated[
    Literal["image", "video"],
    Field(description="Whether media_id is an image or a video."),
]
_UpscaleScale = Annotated[
    str | None,
    Field(
        description=(
            "Target scale: '2k' or '4k' for images (default '2k', free); "
            "'1080p', '720p', '270p' (GIF, free) or '4k' (Ultra, billed) for videos."
        )
    ),
]
_ControlAction = Annotated[
    Literal["clear_cooldown", "renew", "exclude", "include", "close_browser"],
    Field(
        description=(
            "clear_cooldown: clear cooldown on one or all profiles; "
            "renew: headlessly renew a profile's Flow session and refresh credits; "
            "exclude/include: disable or re-enable a profile in automatic scheduling; "
            "close_browser: close idle warm Chrome window(s) immediately."
        )
    ),
]


def query_cross_account_media(
    *,
    kind: Literal["all", "image", "video"] = "all",
    profile: str | None = None,
    query: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    """Query the local ``gflow.db`` media catalog across all accounts or one profile."""
    settings = get_settings()
    db_path = settings.db_path or (settings.home / "gflow.db")
    if not db_path.is_file():
        return {"status": "ok", "count": 0, "items": []}

    capped_limit = max(1, min(limit, 200))
    safe_offset = max(0, offset)
    fetch_n = max(capped_limit + safe_offset, 200) if query else (capped_limit + safe_offset)
    needle = query.strip().lower() if query and query.strip() else None

    entries: list[tuple[str, dict[str, Any]]] = []
    if kind in ("all", "image"):
        for img in data_queries.list_images(
            db_path=db_path, profile=profile, limit=fetch_n, offset=0
        ):
            rec = _format_media_row(
                kind="image",
                media_id=img.media_id,
                profile=img.profile,
                project_id=img.project_id,
                model=img.model,
                aspect=img.aspect,
                duration=None,
                prompt=img.prompt,
                local_path=img.local_path,
                created_at=img.created_at.isoformat(),
            )
            if _matches_query(rec, img.prompt, needle):
                entries.append((rec["created_at"], rec))

    if kind in ("all", "video"):
        for vid in data_queries.list_videos(
            db_path=db_path, profile=profile, limit=fetch_n, offset=0
        ):
            rec = _format_media_row(
                kind="video",
                media_id=vid.media_id,
                profile=vid.profile,
                project_id=vid.project_id,
                model=vid.model,
                aspect=vid.aspect,
                duration=vid.duration,
                prompt=vid.prompt,
                local_path=vid.local_path,
                created_at=vid.created_at.isoformat(),
            )
            if _matches_query(rec, vid.prompt, needle):
                entries.append((rec["created_at"], rec))

    entries.sort(key=lambda pair: pair[0], reverse=True)
    page = [rec for _, rec in entries[safe_offset : safe_offset + capped_limit]]
    return {"status": "ok", "count": len(page), "total_matched": len(entries), "items": page}


def _format_media_row(
    *,
    kind: str,
    media_id: str,
    profile: str,
    project_id: str | None,
    model: str | None,
    aspect: str | None,
    duration: float | None,
    prompt: str | None,
    local_path: str | None,
    created_at: str,
) -> dict[str, Any]:
    short_prompt = None
    if prompt:
        short_prompt = prompt if len(prompt) <= 160 else f"{prompt[:157]}..."
    rec: dict[str, Any] = {
        "media_id": media_id,
        "kind": kind,
        "profile": profile,
        "project_id": project_id,
        "model": model,
        "aspect": aspect,
        "prompt": short_prompt,
        "local_path": local_path,
        "file_exists": bool(local_path and Path(local_path).is_file()),
        "created_at": created_at,
    }
    if duration is not None:
        rec["duration"] = duration
    return rec


def _matches_query(rec: dict[str, Any], full_prompt: str | None, needle: str | None) -> bool:
    if needle is None:
        return True
    haystack = " ".join(
        str(part)
        for part in (
            rec.get("media_id"),
            rec.get("profile"),
            rec.get("model"),
            rec.get("local_path"),
            full_prompt,
        )
        if part
    ).lower()
    return needle in haystack


@server.tool(
    name="gflow_plus_submit_upscale",
    description=(
        "Submit a non-blocking image or video upscale job into the multi-account pool and "
        "return immediately with a job_id (avoids 50s MCP timeouts). Automatically routes to "
        "the account that owns media_id via the local catalog and reuses its warm browser. "
        "Image upscale (2k/4k) and video 1080p/720p/270p are free; video 4k spends Ultra credits. "
        "out_file and out_dir, if given, must resolve inside the configured output directory "
        "(GFLOW_CLI_OUTPUT_DIR)."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_submit_upscale(
    media_id: Annotated[
        str, Field(description="Flow media UUID of the image or video to upscale.")
    ],
    *,
    media_kind: _MediaKind = "image",
    scale: _UpscaleScale = None,
    project_id: str | None = None,
    out_file: core._OutFile = None,  # pyright: ignore[reportPrivateUsage]
    out_dir: core._OutDir = None,  # pyright: ignore[reportPrivateUsage]
    profile: core._Profile = None,  # pyright: ignore[reportPrivateUsage]
    label: core._Label = None,  # pyright: ignore[reportPrivateUsage]
    skip_if_exists: core._SkipIfExists = True,  # pyright: ignore[reportPrivateUsage]
) -> dict[str, Any]:
    """Enqueue an image or video upscale job in the owning profile's background lane."""
    outputs: dict[str, Any] = {"output_file": out_file, "out_dir": out_dir}
    if (refused := core._confine_outputs(outputs)) is not None:  # pyright: ignore[reportPrivateUsage]
        return refused

    task_type = "upscale_image" if media_kind == "image" else "upscale_video"
    resolved_scale = (scale or ("2k" if media_kind == "image" else "1080p")).strip().lower()
    payload: dict[str, Any] = {"media_id": media_id, "scale": resolved_scale}
    if project_id:
        payload["project_id"] = project_id
    if outputs["output_file"]:
        payload["output_file"] = outputs["output_file"]
    if outputs["out_dir"]:
        payload["out_dir"] = outputs["out_dir"]

    try:
        job = await core._pool().submit_job(  # pyright: ignore[reportPrivateUsage]
            kind="upscale",
            task_type=task_type,
            payload=payload,
            label=label,
            preferred_profile=profile,
            skip_if_exists=skip_if_exists,
        )
    except QueueSchemaError as exc:
        return common._bad_param("Invalid Upscale", exc.detail or str(exc))  # pyright: ignore[reportPrivateUsage]
    return {"status": "ok", "job": job.to_brief_dict()}


@server.tool(
    name="gflow_plus_submit_extend",
    description=(
        "Submit a non-blocking single-segment video extend job on flow.google.com and return "
        "immediately with a job_id (avoids 50s MCP timeouts). Automatically routes to the "
        "profile owning media_id, spends Veo credits, and never auto-retries across accounts. "
        "out_file and out_dir, if given, must resolve inside the configured output directory "
        "(GFLOW_CLI_OUTPUT_DIR)."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_submit_extend(
    media_id: Annotated[str, Field(description="Flow media UUID of the source video clip.")],
    prompt: Annotated[
        str, Field(description="Continuation prompt describing the extended segment.")
    ],
    *,
    aspect: Annotated[str | None, Field(description="Optional aspect ratio: 9:16 or 16:9.")] = None,
    project_id: str | None = None,
    max_credits: Annotated[
        int | None,
        Field(ge=0, description="Refuse submit when Flow quotes more credits than this."),
    ] = None,
    out_file: core._OutFile = None,  # pyright: ignore[reportPrivateUsage]
    out_dir: core._OutDir = None,  # pyright: ignore[reportPrivateUsage]
    profile: core._Profile = None,  # pyright: ignore[reportPrivateUsage]
    label: core._Label = None,  # pyright: ignore[reportPrivateUsage]
    skip_if_exists: core._SkipIfExists = True,  # pyright: ignore[reportPrivateUsage]
) -> dict[str, Any]:
    """Enqueue a single-segment video extend job in the owning profile's background lane."""
    outputs: dict[str, Any] = {"output_file": out_file, "out_dir": out_dir}
    if (refused := core._confine_outputs(outputs)) is not None:  # pyright: ignore[reportPrivateUsage]
        return refused

    payload: dict[str, Any] = {"media_id": media_id, "prompt": prompt}
    for key, val in (
        ("aspect", aspect),
        ("project_id", project_id),
        ("max_credits", max_credits),
        ("output_file", outputs["output_file"]),
        ("out_dir", outputs["out_dir"]),
    ):
        if val is not None:
            payload[key] = val

    try:
        job = await core._pool().submit_job(  # pyright: ignore[reportPrivateUsage]
            kind="extend",
            task_type="extend_video",
            payload=payload,
            label=label,
            preferred_profile=profile,
            skip_if_exists=skip_if_exists,
        )
    except QueueSchemaError as exc:
        return common._bad_param("Invalid Extend", exc.detail or str(exc))  # pyright: ignore[reportPrivateUsage]
    return {"status": "ok", "job": job.to_brief_dict()}


@server.tool(
    name="gflow_plus_retry_job",
    description=(
        "Re-submit one failed/cancelled pool job or every failed/cancelled job in a batch. "
        "Safety guard: refuses jobs with may_have_spent=true or status='indeterminate' unless "
        "force_spent=true is explicitly passed, preventing accidental double-billing."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_retry_job(
    *,
    job_id: str | None = None,
    batch_id: str | None = None,
    force_spent: Annotated[
        bool,
        Field(
            description=(
                "Set true only after verifying on Flow that an indeterminate job was not billed."
            )
        ),
    ] = False,
    profile: core._Profile = None,  # pyright: ignore[reportPrivateUsage]
) -> dict[str, Any]:
    """Retry failed or cancelled pool jobs safely."""
    return await core._pool().retry_jobs(  # pyright: ignore[reportPrivateUsage]
        job_id=job_id,
        batch_id=batch_id,
        force_spent=force_spent,
        preferred_profile=profile,
    )


@server.tool(
    name="gflow_plus_profile_control",
    description=(
        "Manage multi-account pool lanes without restarting MCP (zero credits): clear "
        "cooldowns, headlessly renew a profile's Flow session, temporarily exclude/include "
        "an account from scheduling, or close idle warm Chrome windows."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_profile_control(
    action: _ControlAction,
    *,
    profile: Annotated[
        str | None,
        Field(
            description=(
                "Target profile name; omit or pass '*' for clear_cooldown/close_browser on all."
            )
        ),
    ] = None,
) -> dict[str, Any]:
    """Perform a runtime maintenance action on one or all pool profiles."""
    scheduler = core._pool().scheduler  # pyright: ignore[reportPrivateUsage]
    if action == "clear_cooldown":
        cleared = scheduler.clear_cooldown(profile)
        return {"status": "ok", "action": action, "profile": profile or "*", "cleared": cleared}
    if action == "close_browser":
        closed = await scheduler.close_warm_browsers(profile)
        return {"status": "ok", "action": action, "profile": profile or "*", "closed": closed}
    if not profile or profile == "*":
        return common._bad_param(  # pyright: ignore[reportPrivateUsage]
            "Profile Required",
            f"action={action!r} requires a specific profile name.",
        )
    if action == "exclude":
        scheduler.set_disabled(profile, disabled=True)
        await scheduler.close_warm_browsers(profile)
        return {"status": "ok", "action": action, "profile": profile, "disabled": True}
    if action == "include":
        scheduler.set_disabled(profile, disabled=False)
        return {"status": "ok", "action": action, "profile": profile, "disabled": False}
    info = await scheduler.renew_profile(profile)
    return {"status": "ok", "action": action, "profile": profile, "account": info}


@server.tool(
    name="gflow_plus_list_media",
    description=(
        "Search or list recently generated images and videos across ALL accounts in the local "
        "gflow.db catalog (zero credits, no browser). Returns media_id, owning profile, "
        "project_id, local_path, file_exists, model, aspect, duration, and prompt snippet so "
        "you can directly feed results into upscale, extend, assemble, or trash."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_list_media(
    *,
    kind: Annotated[
        Literal["all", "image", "video"], Field(description="Filter by media type.")
    ] = "all",
    profile: Annotated[
        str | None, Field(description="Filter to one profile; omit for all accounts.")
    ] = None,
    query: Annotated[
        str | None,
        Field(
            description="Case-insensitive substring match against prompt, media_id, model, or path."
        ),
    ] = None,
    limit: Annotated[int, Field(ge=1, le=200, description="Max items to return.")] = 20,
    offset: Annotated[int, Field(ge=0, description="Pagination offset.")] = 0,
) -> dict[str, Any]:
    """List or search local catalogued assets across all saved Flow accounts."""
    return query_cross_account_media(
        kind=kind,
        profile=profile,
        query=query,
        limit=limit,
        offset=offset,
    )
