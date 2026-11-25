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


def test_get_jobs_reads_several_jobs_in_the_order_asked(tmp_path: Path) -> None:
    store = PlusStateStore(tmp_path / "plus_state.db")
    for jid in ("a", "b", "c"):
        store.create_job(
            PoolJob(job_id=jid, kind="image", task_type="t2i", status="queued", payload={})
        )

    assert [j.job_id for j in store.get_jobs(["c", "missing", "a"])] == ["c", "a"]
    assert store.get_jobs([]) == []


def test_account_state_update_keeps_fields_it_was_not_given(tmp_path: Path) -> None:
    store = PlusStateStore(tmp_path / "plus_state.db")
    store.upsert_account_state(
        "acc", credits=50, cooldown_until=99.0, cooldown_reason="waf", last_error="boom"
    )
    store.upsert_account_state("acc", last_used_at=7.0)
    state = store.get_account_state("acc")
    assert (state["credits"], state["cooldown_reason"], state["last_used_at"]) == (50, "waf", 7.0)

    store.upsert_account_state("acc", credits=60, last_error="", clear_cooldown=True)
    state = store.get_account_state("acc")
    assert (state["credits"], state["cooldown_until"], state["cooldown_reason"]) == (60, None, None)
    assert state["last_error"] == ""


def test_prune_account_states_removes_only_missing_profiles(tmp_path: Path) -> None:
    store = PlusStateStore(tmp_path / "plus_state.db")
    store.upsert_account_state("keep_a", credits=100)
    store.upsert_account_state("keep_b", credits=200)
    store.upsert_account_state("gone_c", credits=300)
    store.upsert_account_state("gone_a", credits=400)

    removed = store.prune_account_states({"keep_a", "keep_b"})
    assert removed == ["gone_a", "gone_c"]
    assert set(store.list_account_states()) == {"keep_a", "keep_b"}

    assert store.prune_account_states({"keep_a", "keep_b"}) == []
    assert store.prune_account_states(set()) == ["keep_a", "keep_b"]
    assert store.list_account_states() == {}
