"""Independent SQLite persistence for Plus jobs and account cooldowns.

Stored in ``<GFLOW_CLI_HOME>/plus_state.db`` so upstream ``gflow.db`` schema
and migrations are never touched.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Collection, Generator, Sequence
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from gflow_cli.plus.pool.models import JobKind, JobStatus, PoolJob

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS plus_jobs (
    job_id TEXT PRIMARY KEY,
    batch_id TEXT,
    label TEXT,
    kind TEXT NOT NULL,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL,
    preferred_profile TEXT,
    assigned_profile TEXT,
    payload_json TEXT NOT NULL,
    result_json TEXT,
    error_json TEXT,
    may_have_spent INTEGER NOT NULL DEFAULT 0,
    attempts_json TEXT NOT NULL DEFAULT '[]',
    depends_on_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    owner TEXT
);
CREATE INDEX IF NOT EXISTS idx_plus_jobs_batch ON plus_jobs(batch_id);
CREATE INDEX IF NOT EXISTS idx_plus_jobs_status ON plus_jobs(status);

CREATE TABLE IF NOT EXISTS plus_account_state (
    profile TEXT PRIMARY KEY,
    credits INTEGER,
    credits_checked_at REAL,
    cooldown_until REAL,
    cooldown_reason TEXT,
    last_error TEXT,
    last_used_at REAL,
    disabled INTEGER NOT NULL DEFAULT 0,
    proxy TEXT
);

CREATE TABLE IF NOT EXISTS plus_egress_state (
    egress_key TEXT PRIMARY KEY,
    cooldown_until REAL,
    cooldown_reason TEXT,
    last_used_at REAL,
    last_profile TEXT
);
"""


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@contextmanager
def _open(db_path: Path) -> Generator[sqlite3.Connection]:
    """One transaction on a connection that is closed afterwards.

    ``with sqlite3.connect(...)`` only commits; the connection lives on until GC, which
    leaks handles in a long-running MCP server and prints a ResourceWarning wherever
    the collector happens to run.
    """
    con = sqlite3.connect(db_path, timeout=10.0)
    try:
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        with con:
            yield con
    finally:
        con.close()


class PlusStateStore:
    """Thread-safe lightweight SQLite repository for Plus runtime state."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.executescript(_SCHEMA_SQL)
            columns = {r["name"] for r in con.execute("PRAGMA table_info(plus_jobs)")}
            if "owner" not in columns:
                con.execute("ALTER TABLE plus_jobs ADD COLUMN owner TEXT")
            if "depends_on_json" not in columns:
                con.execute(
                    "ALTER TABLE plus_jobs ADD COLUMN depends_on_json TEXT NOT NULL DEFAULT '[]'"
                )
            acc_cols = {r["name"] for r in con.execute("PRAGMA table_info(plus_account_state)")}
            if "disabled" not in acc_cols:
                con.execute(
                    "ALTER TABLE plus_account_state ADD COLUMN disabled INTEGER NOT NULL DEFAULT 0"
                )
            if "proxy" not in acc_cols:
                con.execute("ALTER TABLE plus_account_state ADD COLUMN proxy TEXT")

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> AbstractContextManager[sqlite3.Connection]:
        return _open(self._db_path)

    # --- Account state ------------------------------------------------------

    def get_account_state(self, profile: str) -> dict[str, Any]:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM plus_account_state WHERE profile = ?", (profile,)
            ).fetchone()
        return dict(row) if row is not None else {}

    def list_account_states(self) -> dict[str, dict[str, Any]]:
        with self._connect() as con:
            rows = con.execute("SELECT * FROM plus_account_state").fetchall()
        return {str(r["profile"]): dict(r) for r in rows}

    def upsert_account_state(
        self,
        profile: str,
        *,
        credits: int | None = None,
        credits_checked_at: float | None = None,
        cooldown_until: float | None = None,
        cooldown_reason: str | None = None,
        last_error: str | None = None,
        last_used_at: float | None = None,
        clear_cooldown: bool = False,
        disabled: bool | None = None,
        proxy: str | None = None,
        clear_proxy: bool = False,
    ) -> None:
        # One statement, so an update never reads a row another process is about to change:
        # a field left as None keeps the stored value; clear_cooldown/clear_proxy null their fields.
        disabled_val = None if disabled is None else (1 if disabled else 0)
        proxy_val = None if clear_proxy else proxy
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO plus_account_state (
                    profile, credits, credits_checked_at, cooldown_until,
                    cooldown_reason, last_error, last_used_at, disabled, proxy
                ) VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, 0), ?)
                ON CONFLICT(profile) DO UPDATE SET
                    credits = COALESCE(excluded.credits, credits),
                    credits_checked_at = COALESCE(excluded.credits_checked_at, credits_checked_at),
                    cooldown_until = CASE WHEN ? THEN NULL
                        ELSE COALESCE(excluded.cooldown_until, cooldown_until) END,
                    cooldown_reason = CASE WHEN ? THEN NULL
                        ELSE COALESCE(excluded.cooldown_reason, cooldown_reason) END,
                    last_error = COALESCE(excluded.last_error, last_error),
                    last_used_at = COALESCE(excluded.last_used_at, last_used_at),
                    disabled = COALESCE(?, disabled),
                    proxy = CASE WHEN ? THEN NULL
                        ELSE COALESCE(excluded.proxy, proxy) END
                """,
                (
                    profile,
                    credits,
                    credits_checked_at,
                    None if clear_cooldown else cooldown_until,
                    None if clear_cooldown else cooldown_reason,
                    last_error,
                    last_used_at,
                    disabled_val,
                    proxy_val,
                    clear_cooldown,
                    clear_cooldown,
                    disabled_val,
                    clear_proxy,
                ),
            )

    def clear_all_cooldowns(self) -> int:
        """Clear cooldowns across every stored profile and egress group."""
        with self._connect() as con:
            cur = con.execute(
                "UPDATE plus_account_state SET cooldown_until = NULL, cooldown_reason = NULL "
                "WHERE cooldown_until IS NOT NULL OR cooldown_reason IS NOT NULL"
            )
            con.execute(
                "UPDATE plus_egress_state SET cooldown_until = NULL, cooldown_reason = NULL "
                "WHERE cooldown_until IS NOT NULL OR cooldown_reason IS NOT NULL"
            )
            return cur.rowcount

    def clear_all_proxies(self) -> int:
        """Clear persisted per-profile proxy overrides across every stored profile."""
        with self._connect() as con:
            cur = con.execute(
                "UPDATE plus_account_state SET proxy = NULL WHERE proxy IS NOT NULL"
            )
            return cur.rowcount

    # --- Egress state -------------------------------------------------------

    def get_egress_state(self, egress_key: str) -> dict[str, Any]:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM plus_egress_state WHERE egress_key = ?", (egress_key,)
            ).fetchone()
        return dict(row) if row is not None else {}

    def list_egress_states(self) -> dict[str, dict[str, Any]]:
        with self._connect() as con:
            rows = con.execute("SELECT * FROM plus_egress_state").fetchall()
        return {str(r["egress_key"]): dict(r) for r in rows}

    def upsert_egress_state(
        self,
        egress_key: str,
        *,
        cooldown_until: float | None = None,
        cooldown_reason: str | None = None,
        last_used_at: float | None = None,
        last_profile: str | None = None,
        clear_cooldown: bool = False,
    ) -> None:
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO plus_egress_state (
                    egress_key, cooldown_until, cooldown_reason, last_used_at, last_profile
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(egress_key) DO UPDATE SET
                    cooldown_until = CASE WHEN ? THEN NULL
                        ELSE COALESCE(excluded.cooldown_until, cooldown_until) END,
                    cooldown_reason = CASE WHEN ? THEN NULL
                        ELSE COALESCE(excluded.cooldown_reason, cooldown_reason) END,
                    last_used_at = COALESCE(excluded.last_used_at, last_used_at),
                    last_profile = COALESCE(excluded.last_profile, last_profile)
                """,
                (
                    egress_key,
                    None if clear_cooldown else cooldown_until,
                    None if clear_cooldown else cooldown_reason,
                    last_used_at,
                    last_profile,
                    clear_cooldown,
                    clear_cooldown,
                ),
            )

    def prune_account_states(self, valid_profiles: Collection[str]) -> list[str]:
        """Delete state rows whose profile no longer exists on disk and return their names."""
        keep = set(valid_profiles)
        with self._connect() as con:
            rows = con.execute("SELECT profile FROM plus_account_state").fetchall()
            stale = sorted(str(r["profile"]) for r in rows if str(r["profile"]) not in keep)
            if stale:
                marks = ",".join("?" * len(stale))
                con.execute(
                    f"DELETE FROM plus_account_state WHERE profile IN ({marks})",  # noqa: S608
                    tuple(stale),
                )
        return stale

    # --- Jobs ---------------------------------------------------------------

    def create_job(self, job: PoolJob) -> PoolJob:
        now = utc_now_iso()
        job.created_at = job.created_at or now
        job.updated_at = job.updated_at or now
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO plus_jobs (
                    job_id, batch_id, label, kind, task_type, status,
                    preferred_profile, assigned_profile, payload_json,
                    result_json, error_json, may_have_spent, attempts_json,
                    depends_on_json, created_at, updated_at, owner
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.job_id,
                    job.batch_id,
                    job.label,
                    job.kind,
                    job.task_type,
                    job.status,
                    job.preferred_profile,
                    job.assigned_profile,
                    json.dumps(job.payload, ensure_ascii=False),
                    json.dumps(job.result, ensure_ascii=False) if job.result is not None else None,
                    json.dumps(job.error, ensure_ascii=False) if job.error is not None else None,
                    1 if job.may_have_spent else 0,
                    json.dumps(job.attempts, ensure_ascii=False),
                    json.dumps(job.depends_on, ensure_ascii=False),
                    job.created_at,
                    job.updated_at,
                    job.owner,
                ),
            )
        return job

    def save_job(self, job: PoolJob) -> None:
        job.updated_at = utc_now_iso()
        with self._connect() as con:
            con.execute(
                """
                UPDATE plus_jobs SET
                    status = ?,
                    preferred_profile = ?,
                    assigned_profile = ?,
                    payload_json = ?,
                    result_json = ?,
                    error_json = ?,
                    may_have_spent = ?,
                    attempts_json = ?,
                    depends_on_json = ?,
                    updated_at = ?,
                    owner = ?
                WHERE job_id = ?
                """,
                (
                    job.status,
                    job.preferred_profile,
                    job.assigned_profile,
                    json.dumps(job.payload, ensure_ascii=False),
                    json.dumps(job.result, ensure_ascii=False) if job.result is not None else None,
                    json.dumps(job.error, ensure_ascii=False) if job.error is not None else None,
                    1 if job.may_have_spent else 0,
                    json.dumps(job.attempts, ensure_ascii=False),
                    json.dumps(job.depends_on, ensure_ascii=False),
                    job.updated_at,
                    job.owner,
                    job.job_id,
                ),
            )

    def get_job(self, job_id: str) -> PoolJob | None:
        with self._connect() as con:
            row = con.execute("SELECT * FROM plus_jobs WHERE job_id = ?", (job_id,)).fetchone()
        return _row_to_job(row) if row is not None else None

    def get_jobs(self, job_ids: Sequence[str]) -> list[PoolJob]:
        """The jobs that exist among ``job_ids``, in that order, read in one query."""
        if not job_ids:
            return []
        marks = ",".join("?" * len(job_ids))
        with self._connect() as con:
            rows = con.execute(
                f"SELECT * FROM plus_jobs WHERE job_id IN ({marks})",  # noqa: S608 — only placeholders
                tuple(job_ids),
            ).fetchall()
        by_id = {job.job_id: job for job in map(_row_to_job, rows)}
        return [by_id[jid] for jid in job_ids if jid in by_id]

    def list_jobs(
        self,
        *,
        batch_id: str | None = None,
        status: JobStatus | None = None,
        limit: int = 50,
    ) -> list[PoolJob]:
        capped = max(1, min(limit, 500))
        with self._connect() as con:
            if batch_id is not None and status is not None:
                rows = con.execute(
                    "SELECT * FROM plus_jobs WHERE batch_id = ? AND status = ? "
                    "ORDER BY created_at DESC LIMIT ?",
                    (batch_id, status, capped),
                ).fetchall()
            elif batch_id is not None:
                rows = con.execute(
                    "SELECT * FROM plus_jobs WHERE batch_id = ? ORDER BY created_at DESC LIMIT ?",
                    (batch_id, capped),
                ).fetchall()
            elif status is not None:
                rows = con.execute(
                    "SELECT * FROM plus_jobs WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, capped),
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT * FROM plus_jobs ORDER BY created_at DESC LIMIT ?",
                    (capped,),
                ).fetchall()
        return [_row_to_job(r) for r in rows]

    def list_unfinished_jobs(self) -> list[PoolJob]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM plus_jobs WHERE status IN ('queued', 'running') ORDER BY created_at"
            ).fetchall()
        return [_row_to_job(r) for r in rows]


_VALID_KINDS: frozenset[str] = frozenset({"image", "video", "upscale", "extend"})


def _row_to_job(row: sqlite3.Row) -> PoolJob:
    raw_kind = str(row["kind"])
    kind: JobKind = cast("JobKind", raw_kind) if raw_kind in _VALID_KINDS else "video"
    status: JobStatus = row["status"]
    raw_deps = row["depends_on_json"] if "depends_on_json" in row.keys() else None
    return PoolJob(
        job_id=str(row["job_id"]),
        batch_id=row["batch_id"],
        label=row["label"],
        kind=kind,
        task_type=str(row["task_type"]),
        status=status,
        preferred_profile=row["preferred_profile"],
        assigned_profile=row["assigned_profile"],
        payload=json.loads(row["payload_json"]),
        result=json.loads(row["result_json"]) if row["result_json"] else None,
        error=json.loads(row["error_json"]) if row["error_json"] else None,
        may_have_spent=bool(row["may_have_spent"]),
        attempts=json.loads(row["attempts_json"]) if row["attempts_json"] else [],
        depends_on=json.loads(raw_deps) if raw_deps else [],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        owner=row["owner"],
    )
