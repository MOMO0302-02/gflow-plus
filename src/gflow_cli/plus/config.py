"""Independent configuration for gflow-cli Plus (``GFLOW_PLUS_*`` env vars).

Kept strictly separate from upstream ``gflow_cli.config.Settings`` so upstream
merges never conflict on configuration fields.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from gflow_cli.config import get_settings, validate_browser_proxy_url


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
    warm_browser_ttl_s: float = Field(
        default=120.0,
        ge=0.0,
        description=(
            "Seconds to keep an idle per-profile Chrome window warm between pool jobs "
            "(0 disables warm reuse)."
        ),
    )
    hide_sync_tools: bool = Field(
        default=False,
        description=(
            "When true, hide legacy blocking single-account MCP generation/upscale tools "
            "(gflow_generate_*, gflow_upscale_*) so agents always use gflow_plus_submit_*."
        ),
    )
    profile_proxies: str = Field(
        default="",
        description=(
            "Optional comma-separated per-profile browser proxy map "
            "('profA=http://127.0.0.1:7891,profB=http://127.0.0.1:7892'). "
            "Runtime bindings set via profile-control override this map."
        ),
    )
    max_concurrent_per_egress: int = Field(
        default=1,
        ge=0,
        description=(
            "Maximum concurrent jobs sharing the same egress IP/proxy group "
            "(1 prevents same-IP WAF cascading across accounts; 0 disables the cap)."
        ),
    )
    egress_min_interval_s: float = Field(
        default=15.0,
        ge=0.0,
        description=(
            "Minimum spacing in seconds between consecutive submits sharing the same "
            "egress IP/proxy group (also applied after switching accounts on one egress)."
        ),
    )
    egress_waf_cooldown_s: float = Field(
        default=180.0,
        ge=0.0,
        description=(
            "Egress-level cooldown in seconds applied to an egress IP/proxy group after "
            "any account on it hits Flow's WAF (PUBLIC_ERROR_UNUSUAL_ACTIVITY), protecting "
            "sibling accounts on the same IP without blocking other residential proxies."
        ),
    )
    state_db_path: Path | None = Field(
        default=None,
        description="Optional override path for the independent Plus SQLite state DB.",
    )

    @field_validator("profile_proxies", mode="before")
    @classmethod
    def _validate_profile_proxies(cls, value: object) -> str:
        if value is None or value == "":
            return ""
        if not isinstance(value, str):
            msg = "GFLOW_PLUS_PROFILE_PROXIES must be a string"
            raise ValueError(msg)
        parse_profile_proxies(value)
        return value.strip()

    def parsed_profile_proxies(self) -> dict[str, str]:
        return parse_profile_proxies(self.profile_proxies)

    def allowlist(self) -> tuple[str, ...]:
        return _split_csv(self.profiles)

    def blocklist(self) -> frozenset[str]:
        return frozenset(_split_csv(self.excluded_profiles))

    def resolved_state_db_path(self) -> Path:
        if self.state_db_path is not None:
            return self.state_db_path
        return get_settings().home / "plus_state.db"


def parse_profile_proxies(raw: str) -> dict[str, str]:
    """Parse ``profile=proxy_url,...`` into a normalized ``{profile: proxy_url}`` map."""
    out: dict[str, str] = {}
    if not raw or not raw.strip():
        return out
    for entry in raw.replace(";", ",").split(","):
        part = entry.strip()
        if not part:
            continue
        if "=" not in part:
            msg = (
                "GFLOW_PLUS_PROFILE_PROXIES entries must be 'profile=scheme://host:port' "
                "separated by commas"
            )
            raise ValueError(msg)
        profile_name, raw_proxy = part.split("=", 1)
        profile_name = profile_name.strip()
        if not profile_name:
            msg = "GFLOW_PLUS_PROFILE_PROXIES entry has an empty profile name"
            raise ValueError(msg)
        normalized = validate_browser_proxy_url(raw_proxy)
        if normalized is None:
            msg = f"GFLOW_PLUS_PROFILE_PROXIES entry for {profile_name!r} has an empty proxy URL"
            raise ValueError(msg)
        out[profile_name] = normalized
    return out


def _split_csv(raw: str) -> tuple[str, ...]:
    items = [part.strip() for part in raw.split(",") if part.strip()]
    return tuple(dict.fromkeys(items))


@lru_cache(maxsize=1)
def get_plus_settings() -> PlusSettings:
    return PlusSettings()


def reset_plus_settings() -> None:
    get_plus_settings.cache_clear()
