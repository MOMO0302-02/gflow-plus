"""Independent SQLite persistence for Plus jobs and account cooldowns.

Stored in ``<GFLOW_CLI_HOME>/plus_state.db`` so upstream ``gflow.db`` schema
and migrations are never touched.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
    last_used_at REAL
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
    ) -> None:
        cur = self.get_account_state(profile)
        new_credits = credits if credits is not None else cur.get("credits")
        new_checked = (
            credits_checked_at if credits_checked_at is not None else cur.get("credits_checked_at")
        )
        if clear_cooldown:
            new_cd_until = None
            new_cd_reason = None
        else:
            new_cd_until = (
                cooldown_until if cooldown_until is not None else cur.get("cooldown_until")
            )
            new_cd_reason = (
                cooldown_reason if cooldown_reason is not None else cur.get("cooldown_reason")
            )
        new_err = last_error if last_error is not None else cur.get("last_error")
        new_used = last_used_at if last_used_at is not None else cur.get("last_used_at")

        with self._connect() as con:
            con.execute(
                """
                INSERT INTO plus_account_state (
                    profile, credits, credits_checked_at, cooldown_until,
                    cooldown_reason, last_error, last_used_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile) DO UPDATE SET
                    credits = excluded.credits,
                    credits_checked_at = excluded.credits_checked_at,
                    cooldown_until = excluded.cooldown_until,
                    cooldown_reason = excluded.cooldown_reason,
                    last_error = excluded.last_error,
                    last_used_at = excluded.last_used_at
                """,
                (
                    profile,
                    new_credits,
                    new_checked,
                    new_cd_until,
                    new_cd_reason,
                    new_err,
                    new_used,
                ),
            )

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
                    created_at, updated_at, owner
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    assigned_profile = ?,
                    result_json = ?,
                    error_json = ?,
                    may_have_spent = ?,
                    attempts_json = ?,
                    updated_at = ?,
                    owner = ?
                WHERE job_id = ?
                """,
                (
                    job.status,
                    job.assigned_profile,
                    json.dumps(job.result, ensure_ascii=False) if job.result is not None else None,
                    json.dumps(job.error, ensure_ascii=False) if job.error is not None else None,
                    1 if job.may_have_spent else 0,
                    json.dumps(job.attempts, ensure_ascii=False),
                    job.updated_at,
                    job.owner,
                    job.job_id,
                ),
            )

    def get_job(self, job_id: str) -> PoolJob | None:
        with self._connect() as con:
            row = con.execute("SELECT * FROM plus_jobs WHERE job_id = ?", (job_id,)).fetchone()
        return _row_to_job(row) if row is not None else None

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


def _row_to_job(row: sqlite3.Row) -> PoolJob:
    kind: JobKind = "image" if row["kind"] == "image" else "video"
    status: JobStatus = row["status"]
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
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
        owner=row["owner"],
    )
