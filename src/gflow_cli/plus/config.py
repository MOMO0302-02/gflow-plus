"""Independent configuration for gflow-cli Plus (``GFLOW_PLUS_*`` env vars).

Kept strictly separate from upstream ``gflow_cli.config.Settings`` so upstream
merges never conflict on configuration fields.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from gflow_cli.config import get_settings


class PlusSettings(BaseSettings):
    """Runtime settings for the multi-account pool and async batch runner."""

    model_config = SettingsConfigDict(
        env_prefix="GFLOW_PLUS_",
        extra="ignore",
    )

    profiles: str = Field(
        default="",
        description="Comma-separated allowlist of profile names (empty = all authenticated).",
    )
    excluded_profiles: str = Field(
        default="",
        description="Comma-separated profile names excluded from automatic scheduling.",
    )
    min_video_credits: int = Field(
        default=20,
        ge=0,
        description="Minimum Veo credits required before routing a video job to an account.",
    )
    credit_cache_ttl_s: float = Field(
        default=300.0,
        ge=0.0,
        description="Seconds to reuse a profile's cached credit balance before refreshing.",
    )
    stagger_seconds: float = Field(
        default=2.5,
        ge=0.0,
        description="Minimum delay between browser cold-starts across profiles.",
    )
    rate_limit_cooldown_s: float = Field(
        default=180.0,
        ge=0.0,
        description="Cooldown after HTTP 429 / rate-limit errors.",
    )
    auth_error_cooldown_s: float = Field(
        default=1800.0,
        ge=0.0,
        description="Cooldown after auth expiry or /about identity recheck.",
    )
    transient_cooldown_s: float = Field(
        default=30.0,
        ge=0.0,
        description="Short cooldown after pre-submit transient UI/network failures.",
    )
    waf_cooldown_s: float = Field(
        default=600.0,
        ge=0.0,
        description=(
            "Cooldown after Flow's WAF refuses a submit (PUBLIC_ERROR_UNUSUAL_ACTIVITY). "
            "Observed 2026-11-24: the refusal cleared about 10 minutes later."
        ),
    )
    max_failover_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum distinct profile attempts for a non-charged failure.",
    )
    default_wait_timeout_s: float = Field(
        default=25.0,
        ge=1.0,
        le=35.0,
        description="Default MCP wait cap (well under common 50s tool timeouts).",
    )
    state_db_path: Path | None = Field(
        default=None,
        description="Optional override path for the independent Plus SQLite state DB.",
    )

    def allowlist(self) -> tuple[str, ...]:
        return _split_csv(self.profiles)

    def blocklist(self) -> frozenset[str]:
        return frozenset(_split_csv(self.excluded_profiles))

    def resolved_state_db_path(self) -> Path:
        if self.state_db_path is not None:
            return self.state_db_path
        return get_settings().home / "plus_state.db"


def _split_csv(raw: str) -> tuple[str, ...]:
    items = [part.strip() for part in raw.split(",") if part.strip()]
    return tuple(dict.fromkeys(items))


@lru_cache(maxsize=1)
def get_plus_settings() -> PlusSettings:
    return PlusSettings()


def reset_plus_settings() -> None:
    get_plus_settings.cache_clear()
