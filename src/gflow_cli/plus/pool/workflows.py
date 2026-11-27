"""Batch item parsing and dependency chaining for gflow-plus workflows.

Allows a single ``gflow_plus_submit_batch`` call to declare multi-step pipelines
such as ``image -> upscale_image``, ``image -> i2v video``, or ``video -> extend``
using ``job:<label>`` references in path or ``media_id`` fields, plus explicit
``depends_on`` edges.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from difflib import get_close_matches
from typing import Any, cast

from gflow_cli.plus.pool.models import JobKind, PoolJob

JOB_REF_PREFIX = "job:"
_PATH_REF_FIELDS = ("start_image", "end_image")
_LIST_REF_FIELDS = ("ref_paths", "reference_images")

_COMMON_BATCH_KEYS: frozenset[str] = frozenset(
    {
        "kind",
        "mode",
        "task_type",
        "label",
        "profile",
        "depends_on",
        "skip_if_exists",
        "out_file",
        "output_file",
        "out_dir",
        "project_id",
        "project_name",
    }
)
_IMAGE_BATCH_KEYS: frozenset[str] = _COMMON_BATCH_KEYS | frozenset(
    {
        "prompt",
        "aspect",
        "model",
        "count",
        "ref_paths",
        "reference_images",
        "refs",
        "reference_entities",
        "reference_entity_names",
    }
)
_VIDEO_BATCH_KEYS: frozenset[str] = _COMMON_BATCH_KEYS | frozenset(
    {
        "prompt",
        "aspect",
        "model",
        "count",
        "duration",
        "max_credits",
        "start_image",
        "initial_frame",
        "end_image",
        "reference_images",
        "reference_entities",
        "reference_entity_names",
        "resolution",
    }
)
_UPSCALE_BATCH_KEYS: frozenset[str] = _COMMON_BATCH_KEYS | frozenset(
    {"media_id", "media_kind", "scale", "project"}
)
_EXTEND_BATCH_KEYS: frozenset[str] = _COMMON_BATCH_KEYS | frozenset(
    {"media_id", "prompt", "aspect", "max_credits", "project"}
)


def _reject_unknown_keys(
    item: dict[str, Any],
    allowed: frozenset[str],
    *,
    kind_label: str,
) -> None:
    """Fail fast on misspelled batch item keys instead of silently dropping them."""
    unknown = sorted(k for k in item if k not in allowed)
    if not unknown:
        return
    hints: list[str] = []
    allowed_list = sorted(allowed)
    for key in unknown:
        suggestion = get_close_matches(key, allowed_list, n=1, cutoff=0.55)
        if suggestion:
            hints.append(f"{key!r} (did you mean {suggestion[0]!r}?)")
        else:
            hints.append(repr(key))
    msg = f"Unknown field(s) for {kind_label} batch item: {', '.join(hints)}"
    raise ValueError(msg)


def is_job_ref(value: object) -> bool:
    """True when ``value`` is a ``job:<token>`` workflow placeholder."""
    return isinstance(value, str) and value.startswith(JOB_REF_PREFIX) and len(value) > 4


def ref_token(value: str) -> str:
    return value[len(JOB_REF_PREFIX) :].strip()


def payload_has_job_refs(payload: dict[str, Any]) -> bool:
    """True if any path or ``media_id`` field carries a ``job:<token>`` reference."""
    for key in (*_PATH_REF_FIELDS, "media_id"):
        if is_job_ref(payload.get(key)):
            return True
    for key in _LIST_REF_FIELDS:
        raw = payload.get(key)
        if isinstance(raw, list) and any(is_job_ref(item) for item in cast("list[object]", raw)):
            return True
    return False


def _iter_payload_refs(payload: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for key in (*_PATH_REF_FIELDS, "media_id"):
        val = payload.get(key)
        if is_job_ref(val):
            refs.append(ref_token(str(val)))
    for key in _LIST_REF_FIELDS:
        raw = payload.get(key)
        if isinstance(raw, list):
            refs.extend(ref_token(str(e)) for e in cast("list[object]", raw) if is_job_ref(e))
    return refs


def extract_ref_tokens(
    payload: dict[str, Any],
    explicit_deps: Sequence[str] = (),
) -> list[str]:
    """Collect deduplicated upstream reference tokens from payload fields and ``depends_on``."""
    raw_tokens = [
        ref_token(dep) if is_job_ref(dep) else str(dep).strip() for dep in explicit_deps
    ] + _iter_payload_refs(payload)
    return list(dict.fromkeys(tok for tok in raw_tokens if tok))


def rewrite_payload_refs(
    payload: dict[str, Any],
    resolve_token: Callable[[str], str],
) -> dict[str, Any]:
    """Rewrite ``job:<label|index>`` tokens inside ``payload`` to canonical ``job:<job_id>``."""
    out = dict(payload)
    for key in (*_PATH_REF_FIELDS, "media_id"):
        val = out.get(key)
        if is_job_ref(val):
            out[key] = f"{JOB_REF_PREFIX}{resolve_token(ref_token(str(val)))}"
    for key in _LIST_REF_FIELDS:
        raw = out.get(key)
        if isinstance(raw, list):
            out[key] = [
                f"{JOB_REF_PREFIX}{resolve_token(ref_token(str(entry)))}"
                if is_job_ref(entry)
                else entry
                for entry in cast("list[object]", raw)
            ]
    return out


def validate_no_cycles(edges: dict[str, list[str]]) -> None:
    """Raise ``ValueError`` if ``edges`` (job_id -> upstream job_ids) contains a cycle."""
    state: dict[str, int] = {}  # 0=unvisited, 1=visiting, 2=visited

    def _visit(node: str) -> None:
        mark = state.get(node, 0)
        if mark == 1:
            raise ValueError(f"Cyclic dependency detected involving job {node!r}")
        if mark == 2:
            return
        state[node] = 1
        for dep in edges.get(node, ()):
            if dep in edges:
                _visit(dep)
        state[node] = 2

    for node in edges:
        _visit(node)


def _lookup_dep(dep_jobs: dict[str, PoolJob], raw: str, field_name: str) -> PoolJob:
    dep_id = ref_token(raw)
    upstream = dep_jobs.get(dep_id)
    if upstream is None:
        raise ValueError(f"{field_name}: upstream job {dep_id!r} not found")
    return upstream


def _first_output_path(upstream: PoolJob, field_name: str) -> str:
    raw_paths = (upstream.result or {}).get("output_paths")
    paths: list[object] = cast("list[object]", raw_paths) if isinstance(raw_paths, list) else []
    if not paths:
        raise ValueError(
            f"{field_name}: upstream job {upstream.job_id!r} produced no local output path"
        )
    return str(paths[0])


def _apply_media_id_ref(payload: dict[str, Any], dep_jobs: dict[str, PoolJob]) -> str | None:
    raw_media = payload.get("media_id")
    if not is_job_ref(raw_media):
        return None
    upstream = _lookup_dep(dep_jobs, str(raw_media), "media_id")
    media_id = (upstream.result or {}).get("media_id")
    if not media_id:
        raise ValueError(f"media_id: upstream job {upstream.job_id!r} produced no Flow media_id")
    payload["media_id"] = str(media_id)
    upstream_proj = (upstream.result or {}).get("project_id") or upstream.payload.get("project_id")
    if upstream_proj and not payload.get("project_id"):
        payload["project_id"] = str(upstream_proj)
    if upstream.assigned_profile and upstream.assigned_profile != "cached":
        return upstream.assigned_profile
    return None


def materialize_payload(
    job: PoolJob,
    dep_jobs: dict[str, PoolJob],
) -> tuple[dict[str, Any], str | None]:
    """Substitute ``job:<job_id>`` placeholders using succeeded upstream jobs.

    Returns ``(resolved_payload, inferred_profile)`` where ``inferred_profile``
    is populated when ``media_id`` was inherited from an upstream job.
    """
    payload = dict(job.payload)
    for key in _PATH_REF_FIELDS:
        val = payload.get(key)
        if is_job_ref(val):
            payload[key] = _first_output_path(_lookup_dep(dep_jobs, str(val), key), key)
    for key in _LIST_REF_FIELDS:
        raw = payload.get(key)
        if isinstance(raw, list):
            payload[key] = [
                _first_output_path(_lookup_dep(dep_jobs, str(e), key), key) if is_job_ref(e) else e
                for e in cast("list[object]", raw)
            ]
    inferred_profile = _apply_media_id_ref(payload, dep_jobs)
    return payload, inferred_profile


def parse_batch_item(
    item: dict[str, Any],
    *,
    upstream_kind: JobKind | None = None,
) -> tuple[JobKind, str, dict[str, Any]]:
    """Parse one batch item dict into ``(kind, task_type, payload)``."""
    raw_kind = str(item.get("kind") or "").strip().lower()
    raw_mode = str(item.get("mode") or item.get("task_type") or "").strip().lower()

    if raw_kind in ("upscale_image", "upscale_video", "upscale") or raw_mode in (
        "upscale_image",
        "upscale_video",
    ):
        _reject_unknown_keys(item, _UPSCALE_BATCH_KEYS, kind_label="upscale")
        task_type = _infer_upscale_task_type(item, raw_kind, raw_mode, upstream_kind)
        return "upscale", task_type, _extract_upscale_payload(item, task_type)

    if raw_kind in ("extend", "extend_video") or raw_mode in ("extend", "extend_video"):
        _reject_unknown_keys(item, _EXTEND_BATCH_KEYS, kind_label="extend")
        return "extend", "extend_video", _extract_extend_payload(item)

    if raw_kind == "image" or raw_mode in ("t2i", "i2i"):
        _reject_unknown_keys(item, _IMAGE_BATCH_KEYS, kind_label="image")
        has_refs = bool(item.get("ref_paths") or item.get("refs") or item.get("reference_images"))
        task_type = raw_mode if raw_mode in ("t2i", "i2i") else ("i2i" if has_refs else "t2i")
        return "image", task_type, _extract_image_payload(item)

    if raw_kind and raw_kind != "video":
        msg = (
            f"Unknown batch item kind {raw_kind!r}; expected one of "
            "'image', 'video', 'upscale_image', 'upscale_video', 'upscale', 'extend_video'"
        )
        raise ValueError(msg)
    _reject_unknown_keys(item, _VIDEO_BATCH_KEYS, kind_label="video")
    mode = raw_mode if raw_mode in ("t2v", "i2v", "r2v") else _infer_video_mode(item)
    return "video", mode, _extract_video_payload(item, mode)


def _infer_upscale_task_type(
    item: dict[str, Any],
    raw_kind: str,
    raw_mode: str,
    upstream_kind: JobKind | None,
) -> str:
    if raw_kind == "upscale_image" or raw_mode == "upscale_image":
        return "upscale_image"
    if raw_kind == "upscale_video" or raw_mode == "upscale_video":
        return "upscale_video"
    media_kind = str(item.get("media_kind") or "").strip().lower()
    if media_kind == "image":
        return "upscale_image"
    if media_kind == "video":
        return "upscale_video"
    scale = str(item.get("scale") or "").strip().lower()
    if scale in ("1080p", "720p", "270p"):
        return "upscale_video"
    if scale == "2k":
        return "upscale_image"
    if upstream_kind == "video":
        return "upscale_video"
    return "upscale_image"


def _infer_video_mode(item: dict[str, Any]) -> str:
    if item.get("start_image") or item.get("initial_frame") or item.get("end_image"):
        return "i2v"
    if item.get("reference_images") or item.get("reference_entities"):
        return "r2v"
    return "t2v"


def _extract_image_payload(item: dict[str, Any]) -> dict[str, Any]:
    prompt = str(item.get("prompt") or "").strip()
    if not prompt:
        raise ValueError("image batch item requires a non-empty 'prompt'")
    ref_paths = list(item.get("ref_paths") or item.get("reference_images") or [])
    payload: dict[str, Any] = {
        "prompt": prompt,
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
    prompt = str(item.get("prompt") or "").strip()
    if not prompt:
        raise ValueError("video batch item requires a non-empty 'prompt'")
    payload: dict[str, Any] = {
        "prompt": prompt,
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


def _extract_upscale_payload(item: dict[str, Any], task_type: str) -> dict[str, Any]:
    default_scale = "2k" if task_type == "upscale_image" else "1080p"
    payload: dict[str, Any] = {
        "media_id": str(item.get("media_id") or ""),
        "scale": str(item.get("scale") or default_scale).strip().lower(),
    }
    for key in ("project_id", "project", "out_dir"):
        if item.get(key) is not None:
            target_key = "project_id" if key == "project" else key
            payload[target_key] = item[key]
    out_file = item.get("output_file") or item.get("out_file")
    if out_file:
        payload["output_file"] = str(out_file)
    return payload


def _extract_extend_payload(item: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "media_id": str(item.get("media_id") or ""),
        "prompt": str(item.get("prompt") or ""),
    }
    for key in ("aspect", "project_id", "project", "max_credits", "out_dir"):
        if item.get(key) is not None:
            target_key = "project_id" if key == "project" else key
            payload[target_key] = item[key]
    out_file = item.get("output_file") or item.get("out_file")
    if out_file:
        payload["output_file"] = str(out_file)
    return payload
