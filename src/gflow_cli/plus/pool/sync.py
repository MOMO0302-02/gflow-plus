"""Post-upgrade logged-in account synchronization across home directories and state DB.

Handles the two common sync gaps after upgrading or switching ``GFLOW_CLI_HOME``:
1. Authenticated ``profile_*`` directories left behind in the OS default home
   (or an explicit legacy backup directory) are safely copied into the active
   ``GFLOW_CLI_HOME`` without copying transient Chrome lock files or touching
   active/leased profiles.
2. ``plus_state.db`` is aligned with disk: imported profiles inherit cached
   credits/disabled flags (never stale cooldowns/errors), orphaned DB rows for
   removed profiles are pruned, and an optional zero-credit refresh updates live
   balances and session health.
"""

from __future__ import annotations

import re
import shutil
import sqlite3
import tomllib
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import structlog

from gflow_cli import paths, profile_store
from gflow_cli.config import get_settings
from gflow_cli.errors import ProfileLockedError
from gflow_cli.profile_lease import ProfileLease
from gflow_cli.winsec import ensure_profile_hardened

if TYPE_CHECKING:
    from gflow_cli.plus.pool.scheduler import AccountPoolScheduler

log = structlog.get_logger(__name__)

_SAFE_PROFILE_NAME_RE = re.compile(r"^[\w\-]{1,128}$")
_TRANSIENT_LOCK_NAMES: frozenset[str] = frozenset(
    {
        "SingletonLock",
        "SingletonCookie",
        "SingletonSocket",
        "DevToolsActivePort",
        "lockfile",
        "parent.lock",
        "LOCK",
    }
)
_MTIME_EPSILON_S = 1.0

SyncAction = Literal["import", "update", "skip"]


def _ignore_transient_files(_src: str, names: list[str]) -> set[str]:
    return {
        name
        for name in names
        if name in _TRANSIENT_LOCK_NAMES or name.startswith(".com.google.Chrome.")
    }


def _cookies_mtime(pdir: Path) -> float | None:
    try:
        cookie_file = paths.get_cookies_path(pdir)
    except FileNotFoundError:
        return None
    try:
        return cookie_file.stat().st_mtime
    except OSError:
        return None


def discover_source_homes(
    target_home: Path,
    from_homes: Sequence[str | Path] | None = None,
) -> list[Path]:
    """Resolve deduplicated source home directories distinct from ``target_home``.

    Explicit ``from_homes`` entries must exist and be directories. When no explicit
    sources are supplied, the OS default home is automatically inspected if it
    exists and differs from ``target_home``.
    """
    resolved_target = target_home.expanduser().resolve()
    candidates: list[Path] = []
    seen: set[Path] = {resolved_target}

    if from_homes:
        for raw in from_homes:
            src = Path(raw).expanduser().resolve()
            if not src.is_dir():
                msg = f"Source home directory does not exist: {src}"
                raise ValueError(msg)
            if src not in seen:
                seen.add(src)
                candidates.append(src)
        return candidates

    default_home = paths.default_home().expanduser().resolve()
    if default_home not in seen and default_home.is_dir():
        candidates.append(default_home)
    return candidates


def _read_source_default_profile(source_home: Path) -> str | None:
    cfg = source_home / profile_store.CONFIG_FILENAME
    if not cfg.is_file():
        return None
    try:
        data = tomllib.loads(cfg.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return None
    raw = data.get("default_profile")
    return raw if isinstance(raw, str) and raw else None


def _read_source_account_states(source_home: Path) -> dict[str, dict[str, Any]]:
    db_path = source_home / "plus_state.db"
    if not db_path.is_file():
        return {}
    con: sqlite3.Connection | None = None
    try:
        con = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=5.0)
        con.row_factory = sqlite3.Row
        tables = {
            str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "plus_account_state" not in tables:
            return {}
        rows = con.execute("SELECT * FROM plus_account_state").fetchall()
        return {str(r["profile"]): dict(r) for r in rows}
    except sqlite3.Error:
        return {}
    finally:
        if con is not None:
            con.close()


def _plan_profile_sync(
    name: str,
    src_dir: Path,
    dst_dir: Path,
    *,
    target_home: Path,
    overwrite_newer: bool,
) -> tuple[SyncAction, str | None]:
    if not _SAFE_PROFILE_NAME_RE.match(name):
        return ("skip", "unsafe_name")
    if not dst_dir.resolve().is_relative_to(target_home.resolve()):
        return ("skip", "unsafe_name")

    src_mtime = _cookies_mtime(src_dir)
    if src_mtime is None:
        return ("skip", "source_no_cookies")

    if not dst_dir.exists():
        return ("import", None)

    dst_mtime = _cookies_mtime(dst_dir)
    if dst_mtime is None:
        return ("update", None)

    if not overwrite_newer:
        return ("skip", "already_exists")

    if src_mtime > dst_mtime + _MTIME_EPSILON_S:
        return ("update", None)
    return ("skip", "target_is_newer_or_equal")


def _copy_profile_dir(src_dir: Path, dst_dir: Path) -> None:
    """Copy a Chromium profile directory safely via a sibling staging directory."""
    parent = dst_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex[:8]
    staging = parent / f".sync_tmp_{dst_dir.name}_{token}"
    backup = parent / f".sync_bak_{dst_dir.name}_{token}"
    try:
        shutil.copytree(src_dir, staging, ignore=_ignore_transient_files)
        ensure_profile_hardened(staging)
        if dst_dir.exists():
            dst_dir.rename(backup)
        try:
            staging.rename(dst_dir)
        except Exception:
            if backup.exists() and not dst_dir.exists():
                backup.rename(dst_dir)
            raise
        if backup.exists():
            shutil.rmtree(backup, ignore_errors=True)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def _import_account_state(
    scheduler: AccountPoolScheduler,
    name: str,
    source_states: dict[str, dict[str, Any]],
) -> bool:
    src_row = source_states.get(name)
    if not src_row or scheduler.store.get_account_state(name):
        return False
    raw_credits = src_row.get("credits")
    raw_checked = src_row.get("credits_checked_at")
    raw_disabled = src_row.get("disabled")
    credits = int(raw_credits) if isinstance(raw_credits, int) else None
    checked_at = float(raw_checked) if isinstance(raw_checked, (int, float)) else None
    disabled = bool(raw_disabled) if raw_disabled is not None else None
    if credits is None and checked_at is None and not disabled:
        return False
    scheduler.store.upsert_account_state(
        name,
        credits=credits,
        credits_checked_at=checked_at,
        disabled=disabled,
    )
    return True


def _sync_default_profile(
    target_home: Path,
    source_homes: Sequence[Path],
    planned_profiles: set[str],
    *,
    dry_run: bool,
) -> str | None:
    current_default = _read_source_default_profile(target_home)
    prefix = profile_store.PROFILE_DIR_PREFIX
    if current_default and (target_home / f"{prefix}{current_default}").is_dir():
        return None
    for src_home in source_homes:
        candidate = _read_source_default_profile(src_home)
        if not candidate or not _SAFE_PROFILE_NAME_RE.match(candidate):
            continue
        dst_dir = target_home / f"{prefix}{candidate}"
        exists_after_sync = candidate in planned_profiles or _cookies_mtime(dst_dir) is not None
        if not exists_after_sync:
            continue
        if not dry_run:
            profile_store.set_default_profile(candidate)
        return candidate
    return None


async def _apply_profile_copy(
    scheduler: AccountPoolScheduler,
    *,
    name: str,
    src_dir: Path,
    dst_dir: Path,
) -> str | None:
    """Copy ``src_dir`` into ``dst_dir`` under ``ProfileLease``; returns skip reason on failure."""
    if scheduler.is_busy(name):
        return "target_busy"
    await scheduler.close_warm_browsers(name)
    try:
        with ProfileLease(dst_dir):
            _copy_profile_dir(src_dir, dst_dir)
    except ProfileLockedError:
        return "target_busy"
    except OSError as exc:
        log.warning("plus.sync.copy_failed", profile=name, error=str(exc))
        return "copy_failed"
    return None


def _discover_disk_profiles(target_home: Path) -> set[str]:
    if not target_home.is_dir():
        return set()
    prefix = profile_store.PROFILE_DIR_PREFIX
    return {
        entry.name[len(prefix) :]
        for entry in target_home.iterdir()
        if entry.is_dir() and entry.name.startswith(prefix)
    }


async def _sync_from_source_home(
    scheduler: AccountPoolScheduler,
    *,
    src_home: Path,
    target_home: Path,
    overwrite_newer: bool,
    dry_run: bool,
    imported: list[dict[str, Any]],
    updated: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    state_imported: list[str],
    planned_profiles: set[str],
) -> None:
    prefix = profile_store.PROFILE_DIR_PREFIX
    source_states = _read_source_account_states(src_home)
    for entry in sorted(src_home.iterdir()):
        if not entry.is_dir() or not entry.name.startswith(prefix):
            continue
        name = entry.name[len(prefix) :]
        dst_dir = target_home / f"{prefix}{name}"
        action, reason = _plan_profile_sync(
            name,
            entry,
            dst_dir,
            target_home=target_home,
            overwrite_newer=overwrite_newer,
        )
        if action == "skip":
            skipped.append(
                {"profile": name, "source_home": str(src_home), "reason": reason or "skipped"}
            )
            continue

        if not dry_run:
            fail_reason = await _apply_profile_copy(
                scheduler, name=name, src_dir=entry, dst_dir=dst_dir
            )
            if fail_reason is not None:
                skipped.append(
                    {"profile": name, "source_home": str(src_home), "reason": fail_reason}
                )
                continue
            if action == "import" and _import_account_state(scheduler, name, source_states):
                state_imported.append(name)

        record = {
            "profile": name,
            "source_home": str(src_home),
            "email": profile_store.read_account_file(entry),
        }
        planned_profiles.add(name)
        (imported if action == "import" else updated).append(record)


def _prune_or_preview_state(
    scheduler: AccountPoolScheduler,
    target_home: Path,
    planned_profiles: set[str],
    *,
    dry_run: bool,
) -> list[str]:
    disk_profiles = _discover_disk_profiles(target_home) | planned_profiles
    if dry_run:
        return sorted(p for p in scheduler.store.list_account_states() if p not in disk_profiles)
    return scheduler.store.prune_account_states(disk_profiles)


async def sync_accounts_into_pool(
    scheduler: AccountPoolScheduler,
    *,
    target_home: Path | None = None,
    from_homes: Sequence[str | Path] | None = None,
    overwrite_newer: bool = False,
    prune_state: bool = True,
    refresh: bool = True,
    renew: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Synchronize logged-in profiles from legacy/other homes and align ``plus_state.db``."""
    resolved_target = (target_home or get_settings().home).expanduser().resolve()
    source_homes = discover_source_homes(resolved_target, from_homes)

    imported: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    state_imported: list[str] = []
    planned_profiles: set[str] = set()

    for src_home in source_homes:
        await _sync_from_source_home(
            scheduler,
            src_home=src_home,
            target_home=resolved_target,
            overwrite_newer=overwrite_newer,
            dry_run=dry_run,
            imported=imported,
            updated=updated,
            skipped=skipped,
            state_imported=state_imported,
            planned_profiles=planned_profiles,
        )

    default_synced = _sync_default_profile(
        resolved_target, source_homes, planned_profiles, dry_run=dry_run
    )
    pruned = (
        _prune_or_preview_state(scheduler, resolved_target, planned_profiles, dry_run=dry_run)
        if prune_state
        else []
    )

    did_refresh = not dry_run and refresh
    health = (
        await scheduler.refresh_credits(force=True, renew=renew)
        if did_refresh
        else scheduler.health_report()
    )

    return {
        "status": "ok",
        "dry_run": dry_run,
        "target_home": str(resolved_target),
        "source_homes_scanned": [str(p) for p in source_homes],
        "imported": imported,
        "updated": updated,
        "skipped": skipped,
        "default_profile_synced": default_synced,
        "state_imported": sorted(state_imported),
        "pruned_state_profiles": pruned,
        "refreshed": did_refresh,
        "health": health,
    }
