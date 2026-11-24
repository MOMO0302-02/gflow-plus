from __future__ import annotations

import gc
import sqlite3
import warnings
from pathlib import Path

from gflow_cli.plus.pool.models import PoolJob
from gflow_cli.plus.pool.state_store import PlusStateStore


def test_store_operations_leave_no_connection_open(tmp_path: Path) -> None:
    # Unclosed connections surfaced as a ResourceWarning printed into an unrelated
    # CLI test's output, wherever the collector happened to run.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        store = PlusStateStore(tmp_path / "plus_state.db")
        store.upsert_account_state("acc", credits=5)
        job = store.create_job(
            PoolJob(
                job_id="j1", kind="image", task_type="t2i", status="queued", payload={"prompt": "p"}
            )
        )
        job.status = "succeeded"
        store.save_job(job)
        assert store.get_job("j1") is not None
        assert store.list_unfinished_jobs() == []
        del store
        gc.collect()
    leaks = [w for w in caught if issubclass(w.category, ResourceWarning)]
    assert not leaks, [str(w.message) for w in leaks]


def test_owner_column_is_added_to_an_existing_database(tmp_path: Path) -> None:
    db = tmp_path / "plus_state.db"
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE plus_jobs (job_id TEXT PRIMARY KEY, batch_id TEXT, label TEXT, "
        "kind TEXT NOT NULL, task_type TEXT NOT NULL, status TEXT NOT NULL, "
        "preferred_profile TEXT, assigned_profile TEXT, payload_json TEXT NOT NULL, "
        "result_json TEXT, error_json TEXT, may_have_spent INTEGER NOT NULL DEFAULT 0, "
        "attempts_json TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL, "
        "updated_at TEXT NOT NULL)"
    )
    con.execute(
        "INSERT INTO plus_jobs (job_id, kind, task_type, status, payload_json, created_at, "
        "updated_at) VALUES ('old', 'image', 't2i', 'queued', '{}', 'x', 'x')"
    )
    con.commit()
    con.close()

    old = PlusStateStore(db).get_job("old")
    assert old is not None
    assert old.owner is None
