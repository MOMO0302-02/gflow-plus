"""Typed data structures for the multi-account pool and async job runner."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

JobKind = Literal["image", "video"]
JobStatus = Literal["queued", "running", "succeeded", "failed", "indeterminate", "cancelled"]
ChargedState = Literal["no", "maybe", "yes"]


@dataclass(frozen=True)
class AccountHealth:
    """Point-in-time health and scheduling state for one gflow profile."""

    profile: str
    email: str | None
    is_default: bool
    cookies_present: bool
    credits: int | None
    credits_checked_at: float | None
    busy: bool
    cooldown_until: float | None
    cooldown_reason: str | None
    last_error: str | None
    last_used_at: float | None
    healthy: bool

    def to_dict(self, *, now: float) -> dict[str, Any]:
        remaining = 0.0
        if self.cooldown_until is not None and self.cooldown_until > now:
            remaining = round(self.cooldown_until - now, 1)
        return {
            "profile": self.profile,
            "email": self.email,
            "is_default": self.is_default,
            "cookies_present": self.cookies_present,
            "credits": self.credits,
            "busy": self.busy,
            "cooldown_remaining_s": remaining,
            "cooldown_reason": self.cooldown_reason if remaining > 0 else None,
            "last_error": self.last_error,
            "healthy": self.healthy,
        }


@dataclass(frozen=True)
class AttemptRecord:
    """One profile attempt recorded on a PoolJob for observability and auditing."""

    profile: str
    status: Literal["succeeded", "failed", "indeterminate"]
    error_type: str | None = None
    error_detail: str | None = None
    charged: ChargedState = "no"
    started_at: str = ""
    finished_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "status": self.status,
            "error_type": self.error_type,
            "error_detail": self.error_detail,
            "charged": self.charged,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


@dataclass(frozen=True)
class ExecutionOutcome:
    """Result of executing one generation payload against one concrete profile."""

    status: Literal["succeeded", "failed", "indeterminate"]
    profile: str
    media_id: str | None = None
    output_paths: tuple[str, ...] = ()
    model: str | None = None
    aspect: str | None = None
    duration: float | None = None
    error_type: str | None = None
    error_detail: str | None = None
    charged: ChargedState = "no"
    exit_code: int | None = None
    skipped_existing: bool = False

    def ledger_dict(self) -> dict[str, Any]:
        """Standard delivery ledger aligned with multi-account production tracking."""
        return {
            "profile": self.profile,
            "media_id": self.media_id,
            "output_paths": list(self.output_paths),
            "model": self.model,
            "aspect": self.aspect,
            "duration": self.duration,
            "skipped_existing": self.skipped_existing,
        }


def _empty_attempts() -> list[dict[str, Any]]:
    return []


@dataclass
class PoolJob:
    """Persistent state of an async multi-account generation job."""

    job_id: str
    kind: JobKind
    task_type: str
    status: JobStatus
    payload: dict[str, Any]
    batch_id: str | None = None
    label: str | None = None
    preferred_profile: str | None = None
    assigned_profile: str | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    may_have_spent: bool = False
    attempts: list[dict[str, Any]] = field(default_factory=_empty_attempts)
    created_at: str = ""
    updated_at: str = ""
    owner: str | None = None

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "batch_id": self.batch_id,
            "label": self.label,
            "kind": self.kind,
            "task_type": self.task_type,
            "status": self.status,
            "profile": self.assigned_profile,
            "result": self.result,
            "error": self.error,
            "may_have_spent": self.may_have_spent,
            "attempts": self.attempts,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    def to_brief_dict(self) -> dict[str, Any]:
        """What a polling agent needs per job; ``to_public_dict`` keeps the full record."""
        return {
            "job_id": self.job_id,
            "label": self.label,
            "status": self.status,
            "profile": self.assigned_profile,
            "result": self.result,
            "error": self.error,
            "may_have_spent": self.may_have_spent,
        }
