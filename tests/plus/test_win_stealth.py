"""Unit tests for Windows taskbar stealth helpers."""

from __future__ import annotations

import pytest

from gflow_cli.plus.win_stealth import (
    _WS_EX_APPWINDOW,
    _WS_EX_TOOLWINDOW,
    compute_stealth_exstyle,
    find_leaked_owned_popups,
    is_allowed_process_image,
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


def test_allowed_process_image_only_matches_chrome_and_chromium() -> None:
    assert (
        is_allowed_process_image(r"C:\Program Files\Google\Chrome\Application\chrome.exe") is True
    )
    assert is_allowed_process_image(r"D:\Apps\chrome-win\CHROME.EXE") is True
    assert is_allowed_process_image(r"D:\Apps\chromium\chromium.exe") is True
    # Electron / Chromium desktop apps share Chrome_WidgetWin_1 and must never be touched
    assert is_allowed_process_image(r"C:\Program Files\TencentDocs\TencentDocs.exe") is False
    assert is_allowed_process_image(r"D:\Apps\Feishu\Feishu.exe") is False
    assert is_allowed_process_image("") is False


def test_find_leaked_owned_popups_matches_only_onscreen_children_of_offscreen_chrome() -> None:
    offscreen_main = 1001
    user_main = 2001
    windows = [
        # Off-screen gflow main Chrome window
        (offscreen_main, 0, -30000, -30000),
        # Translate bubble clamped by Chrome onto the visible desktop (0, 0)
        (1002, offscreen_main, 0, 0),
        # Another popup already off-screen does not need hiding
        (1003, offscreen_main, -29500, -29500),
        # User's own foreground Chrome window and its translate bubble must stay untouched
        (user_main, 0, 100, 100),
        (2002, user_main, 120, 140),
    ]
    assert find_leaked_owned_popups(windows, {offscreen_main}) == [1002]
