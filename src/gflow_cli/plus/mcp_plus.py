"""MCP tools for gflow-cli Plus multi-account pool and non-blocking batches.

Every generation tool here returns in milliseconds with a ``job_id`` or
``batch_id`` and executes in background per-profile lanes (1 lane per account,
staggered Chrome launch, Windows taskbar stealth, automatic non-charged
failover). Agents poll with ``gflow_plus_job_status`` or
``gflow_plus_wait_jobs`` (hard-capped at 35s), completely avoiding 50s MCP
client timeouts.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import Field

from gflow_cli.mcp import tools as common
from gflow_cli.mcp.server import server
from gflow_cli.plus.pool.manager import PoolJobManager, get_pool_manager
from gflow_cli.plus.pool.models import JobStatus
from gflow_cli.plus.win_stealth import hide_offscreen_chrome_windows

PLUS_SPEND_TOOLS: tuple[str, ...] = (
    "gflow_plus_submit_image",
    "gflow_plus_submit_video",
    "gflow_plus_submit_batch",
)

_ImageAspect = Annotated[
    str, Field(description="Aspect ratio: 9:16 (portrait), 16:9, 1:1, 4:3 or 3:4.")
]
_VideoAspect = Annotated[str, Field(description="Aspect ratio: 9:16 (portrait) or 16:9.")]
_ImageModel = Annotated[
    str,
    Field(description="Image model alias: nano2, nano-pro, imagen4 or nano2-lite (gflow models)."),
]
_VideoModel = Annotated[
    str,
    Field(
        description=(
            "Video model alias: omni-1.2-flash (up to 15s), omni-flash, veo-lite, veo-fast or "
            "veo-quality (up to 8s); gflow models lists them."
        )
    ),
]
_EntityIds = Annotated[
    list[str] | None,
    Field(
        description=(
            "Saved character entity ids to attach. On flow.google.com each id needs its "
            "display name at the same position in reference_entity_names; "
            "gflow_character_list returns both."
        )
    ),
]
_EntityNames = Annotated[
    list[str] | None,
    Field(description="Display name of each reference_entities id, same order."),
]
_OutFile = Annotated[
    str | None,
    Field(
        description=(
            "File to save the result to, inside the configured output directory (absolute, "
            "or relative to it). With skip_if_exists, a non-empty file already there is kept "
            "and the job is skipped without spending credits."
        )
    ),
]
_OutDir = Annotated[
    str | None, Field(description="Folder to save results in when out_file is not given.")
]
_Profile = Annotated[
    str | None,
    Field(description="Run on this saved profile only; omit to let the pool pick an account."),
]
_Label = Annotated[str | None, Field(description="Your own name for the job, shown in status.")]
_SkipIfExists = Annotated[
    bool, Field(description="Skip when out_file already exists and is non-empty.")
]
_JobIds = Annotated[list[str] | None, Field(description="job_id values from the submit tools.")]
_BatchId = Annotated[str | None, Field(description="batch_id from gflow_plus_submit_batch.")]


def _pool() -> PoolJobManager:
    """The pool, after taking over jobs a previous gflow process left unfinished."""
    manager = get_pool_manager()
    manager.recover_orphans()
    return manager


def _confine_outputs(item: dict[str, Any]) -> dict[str, Any] | None:
    """Confine an item's write paths to the output directory, in place, like the
    upstream generate tools (GHSA-hqfj-4jww-76f2); the 400 envelope when one escapes."""
    for key in ("out_file", "output_file"):
        if item.get(key):
            resolved = common._resolve_out_file(str(item[key]))  # pyright: ignore[reportPrivateUsage]
            if isinstance(resolved, dict):
                return resolved
            item[key] = resolved
    if item.get("out_dir"):
        try:
            confined = common._confine_to_output_dir(str(item["out_dir"]))  # pyright: ignore[reportPrivateUsage]
        except ValueError as exc:
            return common._bad_param("Invalid Output", str(exc))  # pyright: ignore[reportPrivateUsage]
        item["out_dir"] = str(confined)
    return None


@server.tool(
    name="gflow_plus_health",
    description=(
        "Check health, credit balances, cooldowns, and recommended image/video lanes "
        "across all saved Flow profiles. Zero credits spent."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_health(
    *,
    refresh: bool = True,
    renew: bool = False,
) -> dict[str, Any]:
    """Inspect all profiles in the multi-account pool."""
    manager = _pool()
    if refresh:
        return await manager.scheduler.refresh_credits(force=True, renew=renew)
    return manager.scheduler.health_report()


@server.tool(
    name="gflow_plus_pool_status",
    description=(
        "Read cached multi-account lane status, active job counts, and recommended "
        "profiles without opening a browser or hitting the network."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_pool_status() -> dict[str, Any]:
    """Return instant cached pool status."""
    manager = _pool()
    report = manager.scheduler.health_report()
    recent = manager.list_jobs(limit=20)
    return {
        **report,
        "recent_jobs_count": len(recent),
        "active_jobs": sum(1 for j in recent if j.status in ("queued", "running")),
    }


@server.tool(
    name="gflow_plus_submit_image",
    description=(
        "Submit an image generation job (t2i or i2i) into the multi-account pool and "
        "return immediately with a job_id (never hits 50s MCP timeouts). Automatically "
        "picks an idle healthy profile, staggers Chrome launch, hides taskbar icons on "
        "Windows, and fails over to another account on non-charged errors. out_file and "
        "out_dir, if given, must resolve inside the configured output directory "
        "(GFLOW_CLI_OUTPUT_DIR); relative paths are taken from it."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_submit_image(
    prompt: str,
    *,
    aspect: _ImageAspect = "9:16",
    model: _ImageModel = "nano2",
    count: int = 1,
    ref_paths: Annotated[
        list[str] | None,
        Field(description="Local reference image paths; given, the job is image-to-image."),
    ] = None,
    reference_entities: _EntityIds = None,
    reference_entity_names: _EntityNames = None,
    out_file: _OutFile = None,
    out_dir: _OutDir = None,
    project_id: str | None = None,
    project_name: str | None = None,
    profile: _Profile = None,
    label: _Label = None,
    skip_if_exists: _SkipIfExists = True,
) -> dict[str, Any]:
    """Enqueue one image generation job into the multi-account pool."""
    outputs: dict[str, Any] = {"output_file": out_file, "out_dir": out_dir}
    if (refused := _confine_outputs(outputs)) is not None:
        return refused
    payload: dict[str, Any] = {
        "prompt": prompt,
        "aspect": aspect,
        "model": model,
        "count": count,
    }
    if ref_paths:
        payload["ref_paths"] = ref_paths
    if reference_entities:
        payload["reference_entities"] = reference_entities
    if reference_entity_names:
        payload["reference_entity_names"] = reference_entity_names
    if outputs["output_file"]:
        payload["output_file"] = outputs["output_file"]
    if outputs["out_dir"]:
        payload["out_dir"] = outputs["out_dir"]
    if project_id:
        payload["project_id"] = project_id
    if project_name:
        payload["project_name"] = project_name

    task_type = "i2i" if ref_paths else "t2i"
    job = await _pool().submit_job(
        kind="image",
        task_type=task_type,
        payload=payload,
        label=label,
        preferred_profile=profile,
        skip_if_exists=skip_if_exists,
    )
    return {"status": "ok", "job": job.to_public_dict()}


@server.tool(
    name="gflow_plus_submit_video",
    description=(
        "Submit a video generation job (t2v, i2v, or r2v) into the multi-account pool and "
        "return immediately with a job_id (never hits 50s MCP timeouts). Defaults to 9:16 "
        "and omni-1.2-flash, picks the highest-credit idle profile, and fails over only "
        "when guaranteed uncharged. out_file and out_dir, if given, must resolve inside "
        "the configured output directory (GFLOW_CLI_OUTPUT_DIR); relative paths are "
        "taken from it."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_submit_video(
    prompt: str,
    *,
    mode: Annotated[
        Literal["t2v", "i2v", "r2v"] | None,
        Field(
            description=(
                "t2v text only, i2v from start_image, r2v from reference images or "
                "characters; omitted, it follows from the inputs."
            )
        ),
    ] = None,
    aspect: _VideoAspect = "9:16",
    model: _VideoModel = "omni-1.2-flash",
    duration: Annotated[
        int | None, Field(description="Seconds; the model's default when omitted.")
    ] = None,
    count: int = 1,
    max_credits: Annotated[
        int | None,
        Field(ge=0, description="Refuse to submit when Flow quotes more credits than this."),
    ] = None,
    start_image: Annotated[str | None, Field(description="Local first-frame image (i2v).")] = None,
    end_image: Annotated[str | None, Field(description="Local last-frame image (i2v).")] = None,
    reference_images: Annotated[
        list[str] | None, Field(description="Local reference image paths (r2v).")
    ] = None,
    reference_entities: _EntityIds = None,
    reference_entity_names: _EntityNames = None,
    out_file: _OutFile = None,
    out_dir: _OutDir = None,
    project_id: str | None = None,
    project_name: str | None = None,
    profile: _Profile = None,
    label: _Label = None,
    skip_if_exists: _SkipIfExists = True,
) -> dict[str, Any]:
    """Enqueue one video generation job into the multi-account pool."""
    outputs: dict[str, Any] = {"output_file": out_file, "out_dir": out_dir}
    if (refused := _confine_outputs(outputs)) is not None:
        return refused
    resolved_mode: str = mode or (
        "i2v" if start_image else ("r2v" if (reference_images or reference_entities) else "t2v")
    )
    payload: dict[str, Any] = {
        "prompt": prompt,
        "mode": resolved_mode,
        "aspect": aspect,
        "model": model,
        "count": count,
    }
    for key, val in (
        ("duration", duration),
        ("max_credits", max_credits),
        ("start_image", start_image),
        ("end_image", end_image),
        ("reference_images", reference_images),
        ("reference_entities", reference_entities),
        ("reference_entity_names", reference_entity_names),
        ("output_file", outputs["output_file"]),
        ("out_dir", outputs["out_dir"]),
        ("project_id", project_id),
        ("project_name", project_name),
    ):
        if val is not None:
            payload[key] = val

    job = await _pool().submit_job(
        kind="video",
        task_type=resolved_mode,
        payload=payload,
        label=label,
        preferred_profile=profile,
        skip_if_exists=skip_if_exists,
    )
    return {"status": "ok", "job": job.to_public_dict()}


@server.tool(
    name="gflow_plus_submit_batch",
    description=(
        "Submit a list of image/video shots to run concurrently across all healthy accounts "
        "(1 lane per account, automatic stagger, Windows taskbar hidden, skips already-built "
        "output files). Returns batch_id immediately; poll with gflow_plus_wait_jobs. Each "
        "item's out_file / out_dir must resolve inside the configured output directory "
        "(GFLOW_CLI_OUTPUT_DIR); one that does not refuses the whole batch."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_submit_batch(
    items: Annotated[
        list[dict[str, Any]],
        Field(
            description=(
                'Shots to run. Each is {"kind": "image"|"video", "prompt": ..., plus any '
                "argument of gflow_plus_submit_image or gflow_plus_submit_video}, e.g. "
                '{"kind": "video", "prompt": "...", "start_image": "D:/a.png", '
                '"out_file": "D:/out/01.mp4"}.'
            )
        ),
    ],
    *,
    batch_id: Annotated[str | None, Field(description="Your own id for the batch.")] = None,
    skip_if_exists: _SkipIfExists = True,
) -> dict[str, Any]:
    """Submit a batch of generation items into the multi-account pool."""
    confined_items = [dict(item) for item in items]
    for index, item in enumerate(confined_items, start=1):
        if (refused := _confine_outputs(item)) is not None:
            refused["error"]["detail"] = f"items[{index}]: {refused['error']['detail']}"
            return refused
    result = await _pool().submit_batch(
        confined_items,
        batch_id=batch_id,
        skip_if_exists=skip_if_exists,
    )
    return {"status": "ok", **result}


@server.tool(
    name="gflow_plus_job_status",
    description=(
        "Inspect one job_id, one batch_id, or recent pool jobs with delivery ledger "
        "(local output_paths, profile, model, aspect, duration, media_id)."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_job_status(
    *,
    job_id: str | None = None,
    batch_id: str | None = None,
    job_status: JobStatus | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Query status of one job, a whole batch, or recent jobs."""
    manager = _pool()
    if job_id:
        job = manager.get_job(job_id)
        if job is None:
            return {"status": "error", "error": f"Unknown job_id: {job_id}"}
        return {"status": "ok", "job": job.to_public_dict()}
    if batch_id:
        return {"status": "ok", **manager.batch_summary(batch_id)}
    jobs = manager.list_jobs(status=job_status, limit=limit)
    return {"status": "ok", "count": len(jobs), "jobs": [j.to_public_dict() for j in jobs]}


@server.tool(
    name="gflow_plus_wait_jobs",
    description=(
        "Wait safely for one or more pool jobs (or a batch_id) to finish. Timeout is "
        "hard-capped at 35 seconds (default 25s) so MCP tool calls never time out at 50s; "
        "call again if done is false. Returns status counts and each job's result "
        "(output_paths, media_id, profile) or error; gflow_plus_job_status has the full "
        "record with every attempt."
    ),
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_wait_jobs(
    *,
    job_ids: _JobIds = None,
    batch_id: _BatchId = None,
    timeout_seconds: Annotated[
        float, Field(description="Seconds to wait, at most 35; call again while done is false.")
    ] = 25.0,
) -> dict[str, Any]:
    """Bounded-wait helper for background pool jobs."""
    result = await _pool().wait_jobs(
        job_ids=job_ids or (),
        batch_id=batch_id,
        timeout_s=timeout_seconds,
    )
    return {"status": "ok", **result}


@server.tool(
    name="gflow_plus_cancel_job",
    description="Cancel a queued pool job before it starts running.",
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_cancel_job(job_id: str) -> dict[str, Any]:
    """Cancel a queued job in the multi-account pool."""
    return _pool().cancel_job(job_id)


@server.tool(
    name="gflow_plus_hide_taskbar",
    description="Hide Windows taskbar entries for off-screen gflow Chrome windows immediately.",
)
@common._guarded  # pyright: ignore[reportPrivateUsage]
async def gflow_plus_hide_taskbar() -> dict[str, Any]:
    """Trigger an immediate Windows taskbar stealth sweep."""
    return {"status": "ok", "hidden_windows": hide_offscreen_chrome_windows()}
