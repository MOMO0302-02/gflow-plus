"""Per-profile warm browser pool for gflow-plus.

Keeps at most one live ``FlowApiClient`` (Playwright persistent Chrome context)
per profile for ``GFLOW_PLUS_WARM_BROWSER_TTL_S`` seconds between jobs on that
lane. Eliminates the 6-12s cold launch overhead on back-to-back jobs and avoids
repeated Chrome startups that raise Flow's WAF score, while automatically
closing idle browsers so ``ProfileLease`` is never held indefinitely.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog

from gflow_cli.api.client import FlowApiClient
from gflow_cli.config import Settings
from gflow_cli.errors import (
    AuthExpiredError,
    ProfileLockedError,
    WafRejectionError,
)
from gflow_cli.plus.config import PlusSettings, get_plus_settings

log = structlog.get_logger(__name__)

_EVICT_ERROR_TYPES = (AuthExpiredError, WafRejectionError, ProfileLockedError)
_EVICT_ERROR_TOKENS = ("targetclosed", "target page", "browser has been closed", "context closed")


def _client_is_alive(client: Any) -> bool:
    """Best-effort liveness check on a cached FlowApiClient."""
    ctx = getattr(client, "_context", None)
    if ctx is None:
        return False
    is_closed = getattr(ctx, "is_closed", None)
    if callable(is_closed):
        try:
            if is_closed():
                return False
        except Exception:  # noqa: BLE001
            return False
    page = getattr(client, "_page", None)
    if page is not None:
        page_closed = getattr(page, "is_closed", None)
        if callable(page_closed):
            try:
                if page_closed():
                    return False
            except Exception:  # noqa: BLE001
                return False
    return True


def _should_evict_on_error(exc: BaseException) -> bool:
    if isinstance(exc, _EVICT_ERROR_TYPES):
        return True
    name_and_msg = f"{type(exc).__name__} {exc}".lower()
    return any(tok in name_and_msg for tok in _EVICT_ERROR_TOKENS)


@dataclass
class _WarmSlot:
    profile: str
    client: Any
    headless: bool
    transport: str | None
    last_used_at: float
    proxy: str | None = None
    reap_task: asyncio.Task[None] | None = None
    active_leases: int = 0
    pending_evict: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def in_use(self) -> bool:
        return self.active_leases > 0


class WarmBrowserPool:
    """Caches one live ``FlowApiClient`` per profile with bounded idle TTL."""

    def __init__(
        self,
        *,
        settings: PlusSettings | None = None,
        client_factory: Callable[..., Any] = FlowApiClient,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory
        self._clock = clock or time.monotonic
        self._slots: dict[str, _WarmSlot] = {}
        self._guard = asyncio.Lock()

    def _ttl(self) -> float:
        cfg = self._settings or get_plus_settings()
        return float(cfg.warm_browser_ttl_s)

    def warm_profiles(self) -> list[str]:
        """Return profiles currently holding a warm browser slot."""
        return sorted(self._slots.keys())

    @asynccontextmanager
    async def checkout(
        self,
        profile: str,
        *,
        profile_dir: Path,
        headless: bool,
        transport: str | None,
        out_dir: Path,
        settings: Settings | None = None,
    ) -> AsyncGenerator[Any]:
        """Borrow a warm ``FlowApiClient`` for ``profile`` or open a fresh one."""
        ttl = self._ttl()
        if ttl <= 0:
            async with self._client_factory(
                profile_dir=profile_dir,
                headless=headless,
                transport=transport,
                out_dir=out_dir,
                settings=settings,
            ) as fresh:
                yield fresh
            return

        slot = await self._acquire_slot(
            profile,
            profile_dir=profile_dir,
            headless=headless,
            transport=transport,
            out_dir=out_dir,
            settings=settings,
            ttl=ttl,
        )
        evict_after = False
        try:
            yield slot.client
        except BaseException as exc:
            evict_after = _should_evict_on_error(exc) or not _client_is_alive(slot.client)
            raise
        finally:
            should_drop = False
            async with self._guard:
                slot.active_leases = max(0, slot.active_leases - 1)
                if evict_after or not _client_is_alive(slot.client):
                    slot.pending_evict = True
                if slot.active_leases == 0:
                    if slot.pending_evict:
                        should_drop = True
                    else:
                        slot.last_used_at = self._clock()
                        self._schedule_reap(profile, slot, ttl)
            if should_drop:
                await self._drop_slot(profile, slot)

    async def _acquire_slot(
        self,
        profile: str,
        *,
        profile_dir: Path,
        headless: bool,
        transport: str | None,
        out_dir: Path,
        settings: Settings | None,
        ttl: float,
    ) -> _WarmSlot:
        stale: _WarmSlot | None = None
        effective_proxy = getattr(settings, "browser_proxy", None) if settings is not None else None
        async with self._guard:
            existing = self._slots.get(profile)
            if existing is not None:
                self._cancel_reap(existing)
                expired = (
                    existing.active_leases == 0
                    and (self._clock() - existing.last_used_at) > ttl
                )
                matches = (
                    existing.headless == headless
                    and existing.transport == transport
                    and existing.proxy == effective_proxy
                    and not existing.pending_evict
                    and not expired
                    and _client_is_alive(existing.client)
                )
                if matches:
                    existing.active_leases += 1
                    existing.client.out_dir = out_dir
                    if settings is not None:
                        existing.client.settings = settings
                    log.debug(
                        "plus.browser_pool.reuse",
                        profile=profile,
                        active_leases=existing.active_leases,
                    )
                    return existing
                self._slots.pop(profile, None)
                stale = existing

        if stale is not None and stale.active_leases == 0:
            await self._close_client(profile, stale.client)

        client = self._client_factory(
            profile_dir=profile_dir,
            headless=headless,
            transport=transport,
            out_dir=out_dir,
            settings=settings,
        )
        entered = await client.__aenter__()
        slot = _WarmSlot(
            profile=profile,
            client=entered,
            headless=headless,
            transport=transport,
            last_used_at=self._clock(),
            proxy=effective_proxy,
            active_leases=1,
        )
        async with self._guard:
            self._slots[profile] = slot
        log.debug("plus.browser_pool.opened", profile=profile, ttl_s=ttl)
        return slot

    def _cancel_reap(self, slot: _WarmSlot) -> None:
        if slot.reap_task is not None and not slot.reap_task.done():
            slot.reap_task.cancel()
        slot.reap_task = None

    def _schedule_reap(self, profile: str, slot: _WarmSlot, ttl: float) -> None:
        self._cancel_reap(slot)

        async def _reap() -> None:
            try:
                await asyncio.sleep(ttl)
            except asyncio.CancelledError:
                return
            await self._drop_if_idle(profile, slot)

        slot.reap_task = asyncio.create_task(_reap(), name=f"plus-reap-{profile}")

    async def _drop_if_idle(self, profile: str, slot: _WarmSlot) -> None:
        async with self._guard:
            current = self._slots.get(profile)
            if current is not slot or slot.in_use:
                return
            self._slots.pop(profile, None)
        await self._close_client(profile, slot.client)
        log.debug("plus.browser_pool.reaped", profile=profile)

    async def _drop_slot(self, profile: str, slot: _WarmSlot) -> None:
        async with self._guard:
            if self._slots.get(profile) is slot:
                self._slots.pop(profile, None)
        self._cancel_reap(slot)
        await self._close_client(profile, slot.client)

    async def evict(self, profile: str) -> bool:
        """Close and remove a warm browser for ``profile`` if one is cached."""
        async with self._guard:
            slot = self._slots.pop(profile, None)
        if slot is None:
            return False
        self._cancel_reap(slot)
        await self._close_client(profile, slot.client)
        log.info("plus.browser_pool.evicted", profile=profile)
        return True

    async def close_all(self) -> int:
        """Close every cached warm browser across all profiles."""
        async with self._guard:
            slots = list(self._slots.values())
            self._slots.clear()
        for slot in slots:
            self._cancel_reap(slot)
        for slot in slots:
            await self._close_client(slot.profile, slot.client)
        return len(slots)

    async def _close_client(self, profile: str, client: Any) -> None:
        try:
            await client.__aexit__(None, None, None)
        except Exception as exc:  # noqa: BLE001 — closing must not crash callers
            log.warning(
                "plus.browser_pool.close_failed",
                profile=profile,
                error=type(exc).__name__,
            )


_default_browser_pool: WarmBrowserPool | None = None


def get_warm_browser_pool() -> WarmBrowserPool:
    """Return the process-wide warm browser pool singleton."""
    global _default_browser_pool
    if _default_browser_pool is None:
        _default_browser_pool = WarmBrowserPool()
    return _default_browser_pool


def reset_warm_browser_pool() -> None:
    """Reset the warm browser pool singleton (used in tests)."""
    global _default_browser_pool
    _default_browser_pool = None
