"""Multi-account scheduler: health check, credit-aware picking, lane locks, stagger & failover."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog

from gflow_cli import profile_store
from gflow_cli.errors import (
    AuthExpiredError,
    FlowAccessUnavailableError,
    NetworkError,
    ProfileLockedError,
    RateLimitError,
    SecurityError,
)
from gflow_cli.image_batch import resolve_jitter_range
from gflow_cli.plus.config import PlusSettings, get_plus_settings
from gflow_cli.plus.pool.models import AccountHealth, ExecutionOutcome, JobKind
from gflow_cli.plus.pool.state_store import PlusStateStore
from gflow_cli.services import credits as credits_service

log = structlog.get_logger(__name__)

CreditsInspector = Callable[..., Awaitable[dict[str, Any]]]

_AUTH_ERROR_TOKENS = (
    "auth",
    "identity-recheck",
    "identityrecheck",
    "flow-landing",
    "account-chooser",
)
_RATE_LIMIT_TOKENS = ("rate-limit", "ratelimit", "429")
_CREDIT_ERROR_TOKENS = ("insufficient-credits", "credit-cap", "insufficientcredits")
_LOCK_ERROR_TOKENS = ("profile-locked", "profilelocked")
_WAF_TOKENS = ("waf-rejection", "unusual_activity")
_COOLDOWNS_A_READ_CLEARS = frozenset(
    {"auth_required", "credits_read_failed", "insufficient_credits", None}
)
_TRANSIENT_PRE_SUBMIT_TOKENS = (
    "ui-selector-drift",
    "uiselectordrift",
    "transport-timeout",
    "network",
    "recaptcha",
)


@dataclass(frozen=True)
class FailoverDecision:
    """Action to take after an attempt fails on one profile."""

    retry_other_profile: bool
    cooldown_s: float
    cooldown_reason: str | None
    mark_credits_zero: bool = False


def classify_failover(
    outcome: ExecutionOutcome,
    settings: PlusSettings,
) -> FailoverDecision:
    """Decide whether a failed attempt may safely fail over to another profile.

    Hard safety invariant: if ``outcome.charged`` is not ``"no"``, NEVER retry
    on another profile — that risks double-spending credits.
    """
    token = f"{outcome.error_type or ''} {outcome.error_detail or ''}".lower()
    if outcome.status == "failed" and any(k in token for k in _WAF_TOKENS):
        # Cool the profile down even when the submit may have been charged: another
        # request on it while Flow flags unusual activity is refused again.
        return FailoverDecision(
            retry_other_profile=outcome.charged == "no",
            cooldown_s=settings.waf_cooldown_s,
            cooldown_reason="waf_unusual_activity",
        )
    if outcome.status != "failed" or outcome.charged != "no":
        return FailoverDecision(retry_other_profile=False, cooldown_s=0.0, cooldown_reason=None)

    if any(k in token for k in _LOCK_ERROR_TOKENS):
        return FailoverDecision(retry_other_profile=True, cooldown_s=0.0, cooldown_reason=None)
    if any(k in token for k in _RATE_LIMIT_TOKENS):
        return FailoverDecision(
            retry_other_profile=True,
            cooldown_s=settings.rate_limit_cooldown_s,
            cooldown_reason="rate_limited",
        )
    if any(k in token for k in _CREDIT_ERROR_TOKENS):
        return FailoverDecision(
            retry_other_profile=True,
            cooldown_s=settings.rate_limit_cooldown_s,
            cooldown_reason="insufficient_credits",
            mark_credits_zero="insufficient" in token,
        )
    if any(k in token for k in _AUTH_ERROR_TOKENS):
        return FailoverDecision(
            retry_other_profile=True,
            cooldown_s=settings.auth_error_cooldown_s,
            cooldown_reason="auth_required",
        )
    if any(k in token for k in _TRANSIENT_PRE_SUBMIT_TOKENS):
        return FailoverDecision(
            retry_other_profile=True,
            cooldown_s=settings.transient_cooldown_s,
            cooldown_reason="transient_pre_submit_error",
        )
    return FailoverDecision(retry_other_profile=False, cooldown_s=0.0, cooldown_reason=None)


class AccountPoolScheduler:
    """Coordinates multiple Flow profiles as independent parallel execution lanes."""

    def __init__(
        self,
        *,
        store: PlusStateStore | None = None,
        settings: PlusSettings | None = None,
        profile_lister: Callable[[], list[profile_store.ProfileMeta]] | None = None,
        credits_inspector: CreditsInspector | None = None,
        clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], Awaitable[None]] | None = None,
        jitter_range: Callable[[str | None], tuple[float, float]] | None = None,
    ) -> None:
        self._settings = settings or get_plus_settings()
        self._store = store or PlusStateStore(self._settings.resolved_state_db_path())
        self._list_profiles = profile_lister or profile_store.list_profiles
        self._inspect = credits_inspector or credits_service.inspect_profile
        self._clock = clock or time.time
        self._sleep = sleeper or asyncio.sleep
        self._jitter_range = jitter_range or resolve_jitter_range

        self._lane_locks: dict[str, asyncio.Lock] = {}
        self._busy_profiles: set[str] = set()
        self._used_lanes: set[str] = set()
        self._stagger_lock = asyncio.Lock()
        self._refresh_lock = asyncio.Lock()
        self._last_launch_ts: float = 0.0

    @property
    def store(self) -> PlusStateStore:
        return self._store

    @property
    def settings(self) -> PlusSettings:
        return self._settings

    def lane_lock(self, profile: str) -> asyncio.Lock:
        lock = self._lane_locks.get(profile)
        if lock is None:
            lock = asyncio.Lock()
            self._lane_locks[profile] = lock
        return lock

    def is_busy(self, profile: str) -> bool:
        return profile in self._busy_profiles

    def mark_busy(self, profile: str, busy: bool) -> None:
        if busy:
            self._busy_profiles.add(profile)
        else:
            self._busy_profiles.discard(profile)

    async def wait_stagger(self) -> float:
        """Stagger browser launches across profiles to avoid Windows focus theft."""
        gap = self._settings.stagger_seconds
        if gap <= 0:
            return 0.0
        async with self._stagger_lock:
            now = self._clock()
            elapsed = now - self._last_launch_ts
            wait_s = max(0.0, gap - elapsed) if self._last_launch_ts > 0 else 0.0
            if wait_s > 0:
                await self._sleep(wait_s)
            self._last_launch_ts = self._clock()
            return wait_s

    async def wait_lane_jitter(self, profile: str) -> float:
        """Pause between consecutive submits on one profile, as upstream batches do.

        ``GFLOW_CLI_JITTER_RANGE`` sets the range (default 0.5-1.5 s); a back-to-back
        burst on one profile is what raises its WAF score.
        """
        if profile not in self._used_lanes:
            self._used_lanes.add(profile)
            return 0.0
        low, high = self._jitter_range(None)
        delay = random.uniform(low, high)  # noqa: S311 — pacing, not crypto
        if delay > 0:
            await self._sleep(delay)
        return delay

    def _eligible_profiles(self) -> list[profile_store.ProfileMeta]:
        allow = self._settings.allowlist()
        block = self._settings.blocklist()
        metas = self._list_profiles()
        if allow:
            allow_set = set(allow)
            metas = [m for m in metas if m.name in allow_set]
        return [m for m in metas if m.name not in block]

    def snapshot_health(self) -> list[AccountHealth]:
        """Return cached account health without triggering network or browser calls."""
        now = self._clock()
        states = self._store.list_account_states()
        out: list[AccountHealth] = []
        for meta in self._eligible_profiles():
            st = states.get(meta.name, {})
            cd_until = st.get("cooldown_until")
            in_cooldown = isinstance(cd_until, (int, float)) and float(cd_until) > now
            out.append(
                AccountHealth(
                    profile=meta.name,
                    email=meta.google_account,
                    is_default=meta.is_default,
                    cookies_present=meta.cookies_present,
                    credits=st.get("credits"),
                    credits_checked_at=st.get("credits_checked_at"),
                    busy=self.is_busy(meta.name),
                    cooldown_until=float(cd_until) if isinstance(cd_until, (int, float)) else None,
                    cooldown_reason=st.get("cooldown_reason"),
                    last_error=st.get("last_error"),
                    last_used_at=st.get("last_used_at"),
                    healthy=meta.cookies_present and not in_cooldown,
                )
            )
        return out

    async def refresh_credits(self, *, force: bool = False, renew: bool = False) -> dict[str, Any]:
        """Refresh idle profiles' balances concurrently and update the local cache.

        A busy profile keeps its cached balance: its browser holds the profile lease,
        so a read would only fail with ``ProfileLockedError``. Each read holds the
        profile's lane, so a job waits for it instead of colliding with it.
        """
        async with self._refresh_lock:
            if not force and self._cache_fresh(self._clock()):
                return self.health_report()
            idle = [
                m.name
                for m in self._eligible_profiles()
                if m.cookies_present and not self.is_busy(m.name)
            ]
            await asyncio.gather(*(self._refresh_one(name, renew=renew) for name in idle))
        return self.health_report()

    async def _refresh_one(self, name: str, *, renew: bool) -> None:
        async with self.lane_lock(name):
            self.mark_busy(name, True)
            try:
                reply = await self._inspect(name, refresh=renew)
            except SecurityError:
                raise
            except (ProfileLockedError, NetworkError) as exc:
                # Held by another gflow process, or a network blip: the account is fine.
                self._store.upsert_account_state(name, last_error=f"{type(exc).__name__}")
                return
            except Exception as exc:  # noqa: BLE001 — one account's failure must not hide the rest
                self._cool_down_after_read_failure(name, exc)
                return
            finally:
                self.mark_busy(name, False)
        # A balance read proves the session, not the WAF score or the rate limit.
        reason = self._store.get_account_state(name).get("cooldown_reason")
        self._store.upsert_account_state(
            name,
            credits=int(reply["credits"]),
            credits_checked_at=self._clock(),
            last_error="",
            clear_cooldown=reason in _COOLDOWNS_A_READ_CLEARS,
        )

    def _cool_down_after_read_failure(self, name: str, exc: Exception) -> None:
        if isinstance(exc, RateLimitError):
            cooldown_s, reason = self._settings.rate_limit_cooldown_s, "rate_limited"
        elif isinstance(exc, (AuthExpiredError, FlowAccessUnavailableError)):
            cooldown_s, reason = self._settings.auth_error_cooldown_s, "auth_required"
        else:
            cooldown_s, reason = self._settings.transient_cooldown_s, "credits_read_failed"
        now = self._clock()
        self._store.upsert_account_state(
            name,
            credits_checked_at=now,
            cooldown_until=now + cooldown_s,
            cooldown_reason=reason,
            last_error=f"{type(exc).__name__}: {exc}",
        )

    def _cache_fresh(self, now: float) -> bool:
        ttl = self._settings.credit_cache_ttl_s
        if ttl <= 0:
            return False
        health = [h for h in self.snapshot_health() if h.cookies_present]
        if not health:
            return False
        return all(
            h.credits_checked_at is not None and (now - h.credits_checked_at) < ttl for h in health
        )

    def health_report(self) -> dict[str, Any]:
        now = self._clock()
        accounts = self.snapshot_health()
        healthy_accounts = [a for a in accounts if a.healthy]
        known_credits = [a.credits for a in accounts if a.credits is not None]
        return {
            "status": "ok" if healthy_accounts else "degraded",
            "total_profiles": len(accounts),
            "healthy_profiles": len(healthy_accounts),
            "busy_profiles": sum(1 for a in accounts if a.busy),
            "total_credits": sum(known_credits) if known_credits else None,
            "recommended_image_profile": self.pick_profile("image"),
            "recommended_video_profile": self.pick_profile("video"),
            "profiles": [a.to_dict(now=now) for a in accounts],
        }

    def pick_profile(
        self,
        kind: JobKind,
        *,
        preferred: str | None = None,
        exclude: frozenset[str] = frozenset(),
        required_credits: int | None = None,
        allow_busy: bool = False,
    ) -> str | None:
        """Select the best available profile for a job."""
        candidates = [a for a in self.snapshot_health() if a.healthy and a.profile not in exclude]
        if not allow_busy:
            idle = [a for a in candidates if not a.busy]
            if idle:
                candidates = idle
            else:
                return None

        if preferred and preferred not in exclude:
            for item in candidates:
                if item.profile == preferred:
                    return item.profile

        if kind == "video":
            min_need = max(self._settings.min_video_credits, required_credits or 0)
            candidates = [a for a in candidates if a.credits is None or a.credits >= min_need]
            if not candidates:
                return None
            candidates.sort(
                key=lambda a: (
                    1 if a.busy else 0,
                    -(a.credits if a.credits is not None else -1),
                    a.last_used_at or 0.0,
                )
            )
            return candidates[0].profile

        if not candidates:
            return None
        candidates.sort(key=lambda a: (1 if a.busy else 0, a.last_used_at or 0.0))
        return candidates[0].profile

    def record_attempt(self, profile: str, outcome: ExecutionOutcome) -> FailoverDecision:
        """Update account cooldown/LRU state after one attempt and return failover decision."""
        now = self._clock()
        if outcome.status == "succeeded":
            self._store.upsert_account_state(
                profile,
                last_used_at=now,
                last_error="",
                clear_cooldown=True,
            )
            return FailoverDecision(retry_other_profile=False, cooldown_s=0.0, cooldown_reason=None)

        decision = classify_failover(outcome, self._settings)
        cd_until = now + decision.cooldown_s if decision.cooldown_s > 0 else None
        self._store.upsert_account_state(
            profile,
            credits=0 if decision.mark_credits_zero else None,
            cooldown_until=cd_until,
            cooldown_reason=decision.cooldown_reason,
            last_error=outcome.error_detail or outcome.error_type or outcome.status,
            last_used_at=now,
        )
        log.info(
            "plus.pool.attempt_failed",
            profile=profile,
            error_type=outcome.error_type,
            charged=outcome.charged,
            retry_other=decision.retry_other_profile,
            cooldown_s=decision.cooldown_s,
        )
        return decision
