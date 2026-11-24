"""Unit tests for Windows taskbar stealth helpers."""

from __future__ import annotations

import pytest

from gflow_cli.plus.win_stealth import (
    _WS_EX_APPWINDOW,
    _WS_EX_TOOLWINDOW,
    compute_stealth_exstyle,
    is_offscreen_rect,
    is_stealth_enabled,
)


def test_offscreen_rect_matches_only_offscreen_windows() -> None:
    assert is_offscreen_rect(-30000, -30000) is True
    assert is_offscreen_rect(-12000, 0) is True
    assert is_offscreen_rect(0, -15000) is True
    # Normal desktop and secondary monitor coordinates must NEVER be touched
    assert is_offscreen_rect(0, 0) is False
    assert is_offscreen_rect(-1920, 0) is False
    assert is_offscreen_rect(100, 200) is False


def test_compute_stealth_exstyle_sets_toolwindow_and_clears_appwindow() -> None:
    original = _WS_EX_APPWINDOW | 0x00000100
    updated = compute_stealth_exstyle(original)
    assert updated & _WS_EX_TOOLWINDOW
    assert not (updated & _WS_EX_APPWINDOW)
    assert updated & 0x00000100


def test_stealth_env_toggle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GFLOW_PLUS_HIDE_TASKBAR", "0")
    assert is_stealth_enabled() is False
