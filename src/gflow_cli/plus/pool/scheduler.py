"""Multi-account scheduler: health check, credit-aware picking, lane locks, stagger & failover."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

from gflow_cli import profile_store
from gflow_cli.config import get_settings, validate_browser_proxy_url
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
from gflow_cli.plus.pool.browser_pool import WarmBrowserPool, get_warm_browser_pool
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


def _match_preferred(
    candidates: list[AccountHealth],
    *,
    preferred: str | None,
    exclude: frozenset[str],
) -> str | None:
    if not preferred or preferred in exclude:
        return None
    return next((item.profile for item in candidates if item.profile == preferred), None)


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
        browser_pool: WarmBrowserPool | None = None,
    ) -> None:
        self._settings = settings or get_plus_settings()
        self._store = store or PlusStateStore(self._settings.resolved_state_db_path())
        self._list_profiles = profile_lister or profile_store.list_profiles
        self._inspect = credits_inspector or credits_service.inspect_profile
        self._clock = clock or time.time
        self._sleep = sleeper or asyncio.sleep
        self._jitter_range = jitter_range or resolve_jitter_range
        self._browser_pool = browser_pool or get_warm_browser_pool()

        self._lane_locks: dict[str, asyncio.Lock] = {}
        self._busy_profiles: set[str] = set()
        self._submitting_profiles: set[str] = set()
        self._inflight_videos: dict[str, int] = {}
        self._egress_active: dict[str, int] = {}
        self._egress_active_profiles: dict[str, dict[str, int]] = {}
        self._egress_pacing_locks: dict[str, asyncio.Lock] = {}
        self._egress_last_submit: dict[str, float] = {}
        self._egress_last_finish: dict[str, float] = {}
        self._egress_last_profile: dict[str, str] = {}
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

    @property
    def browser_pool(self) -> WarmBrowserPool:
        return self._browser_pool

    def lane_lock(self, profile: str) -> asyncio.Lock:
        lock = self._lane_locks.get(profile)
        if lock is None:
            lock = asyncio.Lock()
            self._lane_locks[profile] = lock
        return lock

    def is_busy(self, profile: str) -> bool:
        return (
            profile in self._busy_profiles
            or profile in self._submitting_profiles
            or self._inflight_videos.get(profile, 0) > 0
        )

    def inflight_videos(self, profile: str) -> int:
        return self._inflight_videos.get(profile, 0)

    def mark_busy(self, profile: str, busy: bool) -> None:
        if busy:
            self._busy_profiles.add(profile)
        else:
            self._busy_profiles.discard(profile)

    def resolve_profile_proxy(
        self,
        profile: str,
        *,
        account_state: dict[str, Any] | None = None,
    ) -> str | None:
        """Return effective normalized proxy URL for ``profile`` (DB > env map > global)."""
        st = (
            account_state
            if account_state is not None
            else self._store.get_account_state(profile)
        )
        raw_db = st.get("proxy")
        if isinstance(raw_db, str) and raw_db.strip():
            return validate_browser_proxy_url(raw_db)
        env_map = self._settings.parsed_profile_proxies()
        if profile in env_map:
            return env_map[profile]
        return get_settings().browser_proxy

    def egress_group(
        self,
        profile: str,
        *,
        account_state: dict[str, Any] | None = None,
    ) -> str:
        """Canonical egress group key shared by profiles leaving via the same IP/proxy."""
        return self.resolve_profile_proxy(profile, account_state=account_state) or "direct"

    def is_egress_busy(self, egress_key: str) -> bool:
        limit = self._settings.max_concurrent_per_egress
        if limit <= 0:
            return False
        if self._egress_active.get(egress_key, 0) >= limit:
            return True
        active_profs = self._egress_active_profiles.get(egress_key)
        return bool(active_profs and len(active_profs) >= limit)

    def can_schedule_profile_now(self, profile: str, egress_key: str, kind: JobKind) -> bool:
        """True if ``profile`` can enter the submit gate right now for ``kind``.

        Enforces two invariants simultaneously:
        1. Same-egress submit concurrency never exceeds ``max_concurrent_per_egress``
           and never mixes distinct accounts beyond that cap (no cross-account same-IP
           concurrency when cap == 1).
        2. A single account may pipeline up to ``max_inflight_videos_per_profile``
           videos (submit clicks serialized; rendering tabs concurrent).
        """
        if profile in self._submitting_profiles or profile in self._busy_profiles:
            return False
        inflight = self._inflight_videos.get(profile, 0)
        if kind == "video":
            if inflight >= self._settings.max_inflight_videos_per_profile:
                return False
        elif inflight > 0:
            return False

        limit = self._settings.max_concurrent_per_egress
        if limit > 0:
            if self._egress_active.get(egress_key, 0) >= limit:
                return False
            active_profs = self._egress_active_profiles.get(egress_key, {})
            if profile not in active_profs and len(active_profs) >= limit:
                return False
        return True

    def reserve_lane(self, profile: str, *, kind: JobKind = "image") -> str:
        """Synchronously reserve profile submit gate, inflight slot, and egress slot."""
        egress = self.egress_group(profile)
        self._submitting_profiles.add(profile)
        self._egress_active[egress] = self._egress_active.get(egress, 0) + 1
        if kind == "video":
            self._inflight_videos[profile] = self._inflight_videos.get(profile, 0) + 1
        else:
            self._busy_profiles.add(profile)
        per_egress = self._egress_active_profiles.setdefault(egress, {})
        per_egress[profile] = per_egress.get(profile, 0) + 1
        return egress

    def release_submit_gate(self, profile: str, egress_key: str) -> None:
        """Release the submit gate after Flow accepts a video submit (render stays in flight)."""
        if profile in self._submitting_profiles:
            self._submitting_profiles.discard(profile)
            cur = self._egress_active.get(egress_key, 0)
            if cur <= 1:
                self._egress_active.pop(egress_key, None)
            else:
                self._egress_active[egress_key] = cur - 1

    def release_lane(
        self,
        profile: str,
        egress_key: str,
        *,
        kind: JobKind = "image",
        submit_already_released: bool = False,
    ) -> None:
        """Release all remaining reservations (submit gate if still held + inflight slot)."""
        if not submit_already_released:
            self.release_submit_gate(profile, egress_key)
        if kind == "video":
            cur_v = self._inflight_videos.get(profile, 0)
            if cur_v <= 1:
                self._inflight_videos.pop(profile, None)
            else:
                self._inflight_videos[profile] = cur_v - 1
        else:
            self._busy_profiles.discard(profile)
        per_egress = self._egress_active_profiles.get(egress_key)
        if per_egress is not None:
            cur_p = per_egress.get(profile, 0)
            if cur_p <= 1:
                per_egress.pop(profile, None)
            else:
                per_egress[profile] = cur_p - 1
            if not per_egress:
                self._egress_active_profiles.pop(egress_key, None)

    def note_egress_finished(self, egress_key: str, profile: str) -> None:
        """Record completion timestamp on ``egress_key`` for cross-account spacing."""
        now = self._clock()
        self._egress_last_finish[egress_key] = now
        self._egress_last_profile[egress_key] = profile

    async def wait_egress_pacing(
        self,
        egress_key: str,
        profile: str,
        *,
        kind: JobKind = "video",
    ) -> float:
        """Enforce adaptive, jittered, kind-aware spacing on ``egress_key``.

        - Images use ``image_egress_min_interval_s``; videos/extends/upscales use
          ``egress_min_interval_s`` (cross-account switches always use at least
          ``egress_min_interval_s``).
        - Multiplies base spacing by the egress AIMD ``backoff_multiplier`` (raised
          after WAF/429 and decayed gradually after consecutive successes).
        - Applies uniform human-like jitter ``[1.0, 1.0 + egress_jitter_ratio]``.
        - Inserts a periodic breather pause every ``egress_breather_every`` videos.
        """
        base_gap = (
            self._settings.image_egress_min_interval_s
            if kind == "image"
            else self._settings.egress_min_interval_s
        )
        switch_gap = self._settings.egress_min_interval_s
        if base_gap <= 0 and switch_gap <= 0:
            return 0.0
        lock = self._egress_pacing_locks.get(egress_key)
        if lock is None:
            lock = asyncio.Lock()
            self._egress_pacing_locks[egress_key] = lock
        async with lock:
            now = self._clock()
            egress_st = self._store.get_egress_state(egress_key)
            raw_mult = egress_st.get("backoff_multiplier")
            backoff = max(1.0, float(raw_mult)) if isinstance(raw_mult, (int, float)) else 1.0
            videos_since = int(egress_st.get("videos_since_breather") or 0)

            last_submit = self._egress_last_submit.get(egress_key, 0.0)
            last_finish = self._egress_last_finish.get(egress_key, 0.0)
            last_profile = self._egress_last_profile.get(egress_key)
            if last_profile is None:
                stored_ts = egress_st.get("last_used_at")
                if isinstance(stored_ts, (int, float)) and float(stored_ts) > last_finish:
                    last_finish = float(stored_ts)
                stored_prof = egress_st.get("last_profile")
                if isinstance(stored_prof, str) and stored_prof:
                    last_profile = stored_prof

            jitter_ratio = self._settings.egress_jitter_ratio
            jitter_factor = 1.0 + (
                random.uniform(0.0, jitter_ratio) if jitter_ratio > 0 else 0.0  # noqa: S311
            )

            effective_gap = base_gap * backoff * jitter_factor
            effective_switch_gap = max(base_gap, switch_gap) * backoff * jitter_factor

            breather_triggered = (
                kind in ("video", "extend")
                and self._settings.egress_breather_every > 0
                and self._settings.egress_breather_s > 0
                and videos_since >= self._settings.egress_breather_every
            )
            if breather_triggered:
                breather_gap = self._settings.egress_breather_s * backoff * jitter_factor
                effective_gap = max(effective_gap, breather_gap)

            wait_s = 0.0
            if last_submit > 0 and effective_gap > 0:
                wait_s = max(wait_s, effective_gap - (now - last_submit))
            if (
                last_finish > 0
                and last_profile
                and last_profile != profile
                and effective_switch_gap > 0
            ):
                wait_s = max(wait_s, effective_switch_gap - (now - last_finish))
            if breather_triggered and wait_s == 0.0 and last_submit == 0.0:
                wait_s = effective_gap
            if wait_s > 0:
                log.debug(
                    "plus.pool.egress_pacing",
                    egress_group=egress_key,
                    profile=profile,
                    kind=kind,
                    wait_s=round(wait_s, 2),
                    backoff=backoff,
                    breather=breather_triggered,
                )
                await self._sleep(wait_s)
            if breather_triggered:
                self._store.upsert_egress_state(egress_key, videos_since_breather=0)
            self._egress_last_submit[egress_key] = self._clock()
            self._egress_last_profile[egress_key] = profile
            return wait_s

    def get_sticky_project(self, profile: str) -> str | None:
        """Return the active non-expired sticky Flow project ID for ``profile``, if any."""
        if not self._settings.sticky_project or self._settings.sticky_project_ttl_s <= 0:
            return None
        st = self._store.get_account_state(profile)
        pid = st.get("sticky_project_id")
        updated = st.get("sticky_project_updated_at")
        if not isinstance(pid, str) or not pid.strip():
            return None
        if isinstance(updated, (int, float)):
            if (self._clock() - float(updated)) > self._settings.sticky_project_ttl_s:
                return None
        return pid.strip()

    def set_sticky_project(self, profile: str, project_id: str) -> None:
        """Persist ``project_id`` as ``profile``'s sticky project."""
        if not self._settings.sticky_project or not project_id.strip():
            return
        self._store.upsert_account_state(
            profile,
            sticky_project_id=project_id.strip(),
            sticky_project_updated_at=self._clock(),
        )

    def clear_sticky_project(self, profile: str | None = None) -> int:
        """Clear cached sticky Flow project for one profile or all profiles."""
        if profile is None or profile == "*":
            return self._store.clear_all_sticky_projects()
        self._store.upsert_account_state(profile, clear_sticky_project=True)
        return 1

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
        egress_states = self._store.list_egress_states()
        out: list[AccountHealth] = []
        for meta in self._eligible_profiles():
            st = states.get(meta.name, {})
            cd_until = st.get("cooldown_until")
            in_cooldown = isinstance(cd_until, (int, float)) and float(cd_until) > now
            disabled = bool(st.get("disabled", 0))
            proxy = self.resolve_profile_proxy(meta.name, account_state=st)
            egress = proxy or "direct"
            eg_st = egress_states.get(egress, {})
            eg_cd_until = eg_st.get("cooldown_until")
            eg_in_cooldown = isinstance(eg_cd_until, (int, float)) and float(eg_cd_until) > now
            raw_proj = st.get("sticky_project_id")
            raw_proj_ts = st.get("sticky_project_updated_at")
            sticky_proj: str | None = None
            if (
                self._settings.sticky_project
                and isinstance(raw_proj, str)
                and raw_proj.strip()
                and (
                    self._settings.sticky_project_ttl_s <= 0
                    or not isinstance(raw_proj_ts, (int, float))
                    or (now - float(raw_proj_ts)) <= self._settings.sticky_project_ttl_s
                )
            ):
                sticky_proj = raw_proj.strip()
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
                    healthy=(
                        meta.cookies_present
                        and not in_cooldown
                        and not eg_in_cooldown
                        and not disabled
                    ),
                    disabled=disabled,
                    proxy=proxy,
                    egress_group=egress,
                    egress_busy=self.is_egress_busy(egress),
                    egress_cooldown_until=(
                        float(eg_cd_until) if isinstance(eg_cd_until, (int, float)) else None
                    ),
                    egress_cooldown_reason=eg_st.get("cooldown_reason"),
                    sticky_project_id=sticky_proj,
                    inflight_videos=self.inflight_videos(meta.name),
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
        proxy = self.resolve_profile_proxy(name)
        async with self.lane_lock(name):
            self.mark_busy(name, True)
            try:
                await self._browser_pool.evict(name)
                if self._inspect is credits_service.inspect_profile:
                    reply = await self._inspect(name, refresh=renew, proxy=proxy)
                else:
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
        egress_states = self._store.list_egress_states()
        healthy_accounts = [a for a in accounts if a.healthy]
        known_credits = [a.credits for a in accounts if a.credits is not None]
        egress_summary: dict[str, dict[str, Any]] = {}
        egress_profiles: dict[str, list[str]] = {}
        for acc in accounts:
            if acc.egress_group not in egress_summary:
                rem = (
                    round(acc.egress_cooldown_until - now, 1)
                    if acc.egress_cooldown_until is not None and acc.egress_cooldown_until > now
                    else 0.0
                )
                eg_st = egress_states.get(acc.egress_group, {})
                raw_mult = eg_st.get("backoff_multiplier")
                backoff = (
                    round(float(raw_mult), 2) if isinstance(raw_mult, (int, float)) else 1.0
                )
                sticky_prof = (
                    self._egress_last_profile.get(acc.egress_group)
                    or eg_st.get("last_profile")
                )
                active_map = self._egress_active_profiles.get(acc.egress_group, {})
                prof_list: list[str] = []
                egress_profiles[acc.egress_group] = prof_list
                egress_summary[acc.egress_group] = {
                    "egress_group": acc.egress_group,
                    "profiles": prof_list,
                    "sticky_profile": (
                        sticky_prof if self._settings.sticky_profile_per_egress else None
                    ),
                    "active": self._egress_active.get(acc.egress_group, 0),
                    "inflight": sum(active_map.values()),
                    "busy": acc.egress_busy,
                    "backoff_multiplier": backoff,
                    "cooldown_remaining_s": rem,
                    "cooldown_reason": acc.egress_cooldown_reason if rem > 0 else None,
                }
            egress_profiles[acc.egress_group].append(acc.profile)
        return {
            "status": "ok" if healthy_accounts else "degraded",
            "total_profiles": len(accounts),
            "healthy_profiles": len(healthy_accounts),
            "busy_profiles": sum(1 for a in accounts if a.busy),
            "total_credits": sum(known_credits) if known_credits else None,
            "recommended_image_profile": self.pick_profile("image"),
            "recommended_video_profile": self.pick_profile("video"),
            "warm_browsers": self._browser_pool.warm_profiles(),
            "egress_groups": list(egress_summary.values()),
            "max_concurrent_per_egress": self._settings.max_concurrent_per_egress,
            "max_inflight_videos_per_profile": self._settings.max_inflight_videos_per_profile,
            "sticky_profile_per_egress": self._settings.sticky_profile_per_egress,
            "sticky_project": self._settings.sticky_project,
            "profiles": [a.to_dict(now=now) for a in accounts],
        }

    def clear_cooldown(self, profile: str | None = None) -> int:
        """Clear cooldown state for one profile (and its egress group) or every stored profile."""
        if profile is None or profile == "*":
            return self._store.clear_all_cooldowns()
        egress = self.egress_group(profile)
        self._store.upsert_account_state(profile, clear_cooldown=True, last_error="")
        self._store.upsert_egress_state(
            egress,
            clear_cooldown=True,
            backoff_multiplier=1.0,
            consecutive_successes=0,
        )
        return 1

    async def set_proxy(self, profile: str, proxy: str) -> str:
        """Validate and persist a per-profile proxy, evicting any warm browser on the old route."""
        normalized = validate_browser_proxy_url(proxy)
        if normalized is None:
            msg = "proxy must be a non-empty scheme://host:port URL"
            raise ValueError(msg)
        self._store.upsert_account_state(profile, proxy=normalized)
        await self._browser_pool.evict(profile)
        return normalized

    async def clear_proxy(self, profile: str | None = None) -> int:
        """Clear per-profile proxy override for one profile or all profiles."""
        if profile is None or profile == "*":
            cleared = self._store.clear_all_proxies()
            await self._browser_pool.close_all()
            return cleared
        self._store.upsert_account_state(profile, clear_proxy=True)
        await self._browser_pool.evict(profile)
        return 1

    def set_disabled(self, profile: str, *, disabled: bool) -> None:
        """Persist a manual disable/enable toggle for ``profile``."""
        self._store.upsert_account_state(profile, disabled=disabled)

    async def renew_profile(self, profile: str) -> dict[str, Any]:
        """Headlessly renew one profile's session and refresh its credit balance."""
        await self._refresh_one(profile, renew=True)
        now = self._clock()
        for account in self.snapshot_health():
            if account.profile == profile:
                return account.to_dict(now=now)
        return {"profile": profile, "healthy": False}

    async def close_warm_browsers(self, profile: str | None = None) -> int:
        """Close warm Chrome windows for one profile or all profiles."""
        if profile is None or profile == "*":
            return await self._browser_pool.close_all()
        return 1 if await self._browser_pool.evict(profile) else 0

    def prune_stale_accounts(self) -> list[str]:
        """Remove ``plus_state.db`` rows whose profile directories no longer exist on disk."""
        disk_profiles = {meta.name for meta in self._list_profiles()}
        return self._store.prune_account_states(disk_profiles)

    async def sync_accounts(
        self,
        *,
        from_homes: Sequence[str | Path] | None = None,
        overwrite_newer: bool = False,
        prune_state: bool = True,
        refresh: bool = True,
        renew: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Synchronize logged-in accounts from legacy/other homes and align pool state."""
        from gflow_cli.plus.pool.sync import sync_accounts_into_pool

        return await sync_accounts_into_pool(
            self,
            from_homes=from_homes,
            overwrite_newer=overwrite_newer,
            prune_state=prune_state,
            refresh=refresh,
            renew=renew,
            dry_run=dry_run,
        )

    def pick_profile(
        self,
        kind: JobKind,
        *,
        preferred: str | None = None,
        exclude: frozenset[str] = frozenset(),
        required_credits: int | None = None,
        allow_busy: bool = False,
        pin_preferred: bool = False,
    ) -> str | None:
        """Select the best available profile for a job.

        - Enforces per-egress concurrency and single-profile video pipelining limits.
        - When ``sticky_profile_per_egress`` is enabled, keeps routing work on each
          egress group's established primary account instead of round-robin switching
          accounts on the same IP; automatically fails over when that primary cools
          down or runs low on credits.
        """
        candidates = [a for a in self.snapshot_health() if a.healthy and a.profile not in exclude]
        if not allow_busy:
            candidates = [
                a
                for a in candidates
                if self.can_schedule_profile_now(a.profile, a.egress_group, kind)
            ]
        if not candidates:
            return None

        preferred_hit = _match_preferred(candidates, preferred=preferred, exclude=exclude)
        if preferred_hit is not None or pin_preferred:
            return preferred_hit

        if kind == "video":
            min_need = max(self._settings.min_video_credits, required_credits or 0)
            candidates = [a for a in candidates if a.credits is None or a.credits >= min_need]
            if not candidates:
                return None

        egress_states = (
            self._store.list_egress_states() if self._settings.sticky_profile_per_egress else {}
        )

        def _sticky_rank(acc: AccountHealth) -> int:
            if not self._settings.sticky_profile_per_egress:
                return 0
            sticky = self._egress_last_profile.get(acc.egress_group) or egress_states.get(
                acc.egress_group, {}
            ).get("last_profile")
            if not sticky:
                return 1
            return 0 if acc.profile == sticky else 2

        if kind == "video":
            candidates.sort(
                key=lambda a: (
                    0 if self.can_schedule_profile_now(a.profile, a.egress_group, kind) else 1,
                    _sticky_rank(a),
                    -(a.credits if a.credits is not None else -1),
                    a.last_used_at or 0.0,
                )
            )
            return candidates[0].profile

        candidates.sort(
            key=lambda a: (
                0 if self.can_schedule_profile_now(a.profile, a.egress_group, kind) else 1,
                _sticky_rank(a),
                a.last_used_at or 0.0,
            )
        )
        return candidates[0].profile

    def record_attempt(
        self,
        profile: str,
        outcome: ExecutionOutcome,
        *,
        kind: JobKind = "image",
    ) -> FailoverDecision:
        """Update account/egress cooldown, AIMD backoff, sticky project, and local credits."""
        now = self._clock()
        egress = self.egress_group(profile)
        eg_st = self._store.get_egress_state(egress)
        raw_mult = eg_st.get("backoff_multiplier")
        cur_mult = max(1.0, float(raw_mult)) if isinstance(raw_mult, (int, float)) else 1.0
        cur_streak = int(eg_st.get("consecutive_successes") or 0)
        cur_vids = int(eg_st.get("videos_since_breather") or 0)

        if outcome.status == "succeeded":
            new_streak = cur_streak + 1
            new_mult = cur_mult
            if cur_mult > 1.0 and new_streak >= 5:
                new_mult = max(1.0, round(cur_mult * 0.8, 2))
                new_streak = 0
            new_vids = cur_vids + (1 if kind in ("video", "extend") else 0)

            # Local credit deduction for paid video/extend completions so we don't
            # hammer Flow with extra balance checks on every clip.
            updated_credits: int | None = None
            updated_checked_at: float | None = None
            if kind in ("video", "extend") and not outcome.skipped_existing:
                acc_st = self._store.get_account_state(profile)
                cached_credits = acc_st.get("credits")
                if isinstance(cached_credits, int):
                    spent = (
                        outcome.quoted_credits
                        if outcome.quoted_credits is not None
                        else self._settings.min_video_credits
                    )
                    updated_credits = max(0, cached_credits - max(0, spent))
                    updated_checked_at = now

            sticky_pid = (
                outcome.project_id
                if (self._settings.sticky_project and outcome.project_id)
                else None
            )
            self._store.upsert_account_state(
                profile,
                credits=updated_credits,
                credits_checked_at=updated_checked_at,
                last_used_at=now,
                last_error="",
                clear_cooldown=True,
                sticky_project_id=sticky_pid,
                sticky_project_updated_at=now if sticky_pid else None,
            )
            self._store.upsert_egress_state(
                egress,
                last_used_at=now,
                last_profile=profile,
                backoff_multiplier=new_mult,
                consecutive_successes=new_streak,
                videos_since_breather=new_vids,
                clear_cooldown=True,
            )
            self._egress_last_profile[egress] = profile
            return FailoverDecision(retry_other_profile=False, cooldown_s=0.0, cooldown_reason=None)

        decision = classify_failover(outcome, self._settings)
        cd_until = now + decision.cooldown_s if decision.cooldown_s > 0 else None
        err_text = (outcome.error_detail or outcome.error_type or outcome.status or "").lower()
        clear_stale_proj = any(
            tok in err_text for tok in ("project not found", "no project found", "404")
        )
        self._store.upsert_account_state(
            profile,
            credits=0 if decision.mark_credits_zero else None,
            cooldown_until=cd_until,
            cooldown_reason=decision.cooldown_reason,
            last_error=outcome.error_detail or outcome.error_type or outcome.status,
            last_used_at=now,
            clear_sticky_project=clear_stale_proj,
        )
        if decision.cooldown_reason in ("waf_unusual_activity", "rate_limited"):
            new_mult = min(4.0, round(cur_mult * 1.8, 2))
            new_streak = 0
        else:
            new_mult = cur_mult
            new_streak = cur_streak

        if (
            decision.cooldown_reason == "waf_unusual_activity"
            and self._settings.egress_waf_cooldown_s > 0
        ):
            self._store.upsert_egress_state(
                egress,
                cooldown_until=now + self._settings.egress_waf_cooldown_s,
                cooldown_reason="egress_waf_unusual_activity",
                last_used_at=now,
                backoff_multiplier=new_mult,
                consecutive_successes=new_streak,
            )
        else:
            self._store.upsert_egress_state(
                egress,
                last_used_at=now,
                backoff_multiplier=new_mult,
                consecutive_successes=new_streak,
            )
        log.info(
            "plus.pool.attempt_failed",
            profile=profile,
            egress_group=egress,
            error_type=outcome.error_type,
            charged=outcome.charged,
            retry_other=decision.retry_other_profile,
            cooldown_s=decision.cooldown_s,
            backoff_multiplier=new_mult,
        )
        return decision
