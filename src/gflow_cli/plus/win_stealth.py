"""Windows taskbar stealth helper for off-screen headed Chrome windows.

Google Flow's reCAPTCHA Enterprise rejects headless browsers, so gflow launches
real headed Chrome placed off-screen (``--window-position=-30000,-30000``). On
Windows, off-screen ``WS_EX_APPWINDOW`` windows still occupy taskbar buttons
and flash orange/red on focus events.

This module strips ``WS_EX_APPWINDOW`` and adds ``WS_EX_TOOLWINDOW`` ONLY on
non-minimized top-level ``Chrome_WidgetWin_1`` windows belonging to Chrome/Chromium
whose coordinates are already off-screen (``left <= -10000`` or ``top <= -10000``),
leaving the user's own visible or minimized Chrome and Electron windows completely
untouched. The window stays ``WS_VISIBLE`` (shown with ``SW_SHOWNOACTIVATE``), so
Chromium rendering and reCAPTCHA behave identically while taskbar icons disappear
completely.

Controlled by ``GFLOW_PLUS_HIDE_TASKBAR`` (default ``1`` / enabled on Windows).
Non-Windows platforms are a clean no-op.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import os
import sys
from collections.abc import AsyncGenerator
from ctypes import wintypes

import structlog

log = structlog.get_logger(__name__)

_OFFSCREEN_THRESHOLD = -10000
_GWL_EXSTYLE = -20
_WS_EX_TOOLWINDOW = 0x00000080
_WS_EX_APPWINDOW = 0x00040000
_SW_HIDE = 0
_SW_SHOWNOACTIVATE = 4
_FLASHW_STOP = 0
_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_ALLOWED_PROCESS_NAMES = frozenset({"chrome.exe", "chromium.exe"})


def is_stealth_enabled() -> bool:
    """True when Windows taskbar hiding is active."""
    if sys.platform != "win32":
        return False
    raw = os.environ.get("GFLOW_PLUS_HIDE_TASKBAR", "1").strip().lower()
    return raw not in ("0", "false", "off", "no", "n")


def is_offscreen_rect(left: int, top: int) -> bool:
    """True when a window sits at gflow's off-screen launch coordinates."""
    return left <= _OFFSCREEN_THRESHOLD or top <= _OFFSCREEN_THRESHOLD


def is_allowed_process_image(image_path: str) -> bool:
    """True only for Chrome/Chromium executables (never Electron apps like TencentDocs)."""
    return os.path.basename(image_path.replace("\\", "/")).lower() in _ALLOWED_PROCESS_NAMES


def compute_stealth_exstyle(current_exstyle: int) -> int:
    """Return extended window style that removes a window from the taskbar."""
    return (current_exstyle | _WS_EX_TOOLWINDOW) & ~_WS_EX_APPWINDOW


def hide_offscreen_chrome_windows() -> int:
    """Hide taskbar entries for off-screen Chrome windows on Windows.

    Returns the number of windows transitioned into taskbar-hidden tool-window
    mode. Safe no-op on non-Windows platforms.
    """
    if not is_stealth_enabled():
        return 0
    return _win32_hide_offscreen_chrome()


class _FLASHWINFO(ctypes.Structure):
    _fields_ = [
        ("cbSize", wintypes.UINT),
        ("hwnd", wintypes.HWND),
        ("dwFlags", wintypes.DWORD),
        ("uCount", wintypes.UINT),
        ("dwTimeout", wintypes.DWORD),
    ]


def _get_window_process_image(user32: object, kernel32: object, hwnd: int) -> str | None:
    """Return full executable path for ``hwnd``'s owning process, or ``None``."""
    pid = wintypes.DWORD(0)
    user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))  # type: ignore[attr-defined]
    if pid.value == 0:
        return None
    handle = kernel32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False, pid.value)  # type: ignore[attr-defined]
    if not handle:
        return None
    try:
        buf = ctypes.create_unicode_buffer(1024)
        size = wintypes.DWORD(len(buf))
        if not kernel32.QueryFullProcessImageNameW(handle, 0, buf, ctypes.byref(size)):  # type: ignore[attr-defined]
            return None
        return buf.value
    finally:
        kernel32.CloseHandle(handle)  # type: ignore[attr-defined]


def _win32_hide_offscreen_chrome() -> int:
    user32 = ctypes.windll.user32  # type: ignore[attr-defined]
    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.QueryFullProcessImageNameW.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPWSTR,
        ctypes.POINTER(wintypes.DWORD),
    ]
    kernel32.QueryFullProcessImageNameW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    hidden_count = 0

    enum_proc_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

    def _callback(hwnd: int, _lparam: int) -> bool:
        nonlocal hidden_count
        if not user32.IsWindowVisible(hwnd):
            return True
        # Minimized Windows apps report GetWindowRect == (-32000, -32000) while
        # remaining WS_VISIBLE; never hide minimized user windows.
        if user32.IsIconic(hwnd):
            return True
        class_buf = ctypes.create_unicode_buffer(64)
        user32.GetClassNameW(hwnd, class_buf, 64)
        if class_buf.value != "Chrome_WidgetWin_1":
            return True

        image_path = _get_window_process_image(user32, kernel32, hwnd)
        if not image_path or not is_allowed_process_image(image_path):
            return True

        rect = wintypes.RECT()
        if not user32.GetWindowRect(hwnd, ctypes.byref(rect)):
            return True
        if not is_offscreen_rect(int(rect.left), int(rect.top)):
            return True

        cur_style = int(user32.GetWindowLongPtrW(hwnd, _GWL_EXSTYLE))
        new_style = compute_stealth_exstyle(cur_style)
        if new_style != cur_style:
            # Windows Explorer only drops the taskbar button when the window
            # briefly hides while WS_EX_TOOLWINDOW is applied, then re-shows
            # without stealing focus (SW_SHOWNOACTIVATE).
            user32.ShowWindow(hwnd, _SW_HIDE)
            user32.SetWindowLongPtrW(hwnd, _GWL_EXSTYLE, new_style)
            user32.ShowWindow(hwnd, _SW_SHOWNOACTIVATE)
            hidden_count += 1

        flash = _FLASHWINFO(
            cbSize=ctypes.sizeof(_FLASHWINFO),
            hwnd=hwnd,
            dwFlags=_FLASHW_STOP,
            uCount=0,
            dwTimeout=0,
        )
        user32.FlashWindowEx(ctypes.byref(flash))
        return True

    try:
        user32.EnumWindows(enum_proc_type(_callback), 0)
    except Exception as exc:  # noqa: BLE001 — stealth must never abort a generation
        log.debug("plus.win_stealth.enum_failed", exc_info=exc)
    return hidden_count


@contextlib.asynccontextmanager
async def taskbar_stealth_guard(*, poll_interval_s: float = 0.25) -> AsyncGenerator[None, None]:
    """Background watcher that hides newly launched off-screen Chrome windows."""
    if not is_stealth_enabled():
        yield
        return

    stop = asyncio.Event()

    async def _watch() -> None:
        while not stop.is_set():
            hide_offscreen_chrome_windows()
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_interval_s)
            except TimeoutError:
                continue

    task = asyncio.create_task(_watch(), name="gflow-plus-win-stealth")
    try:
        yield
    finally:
        stop.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
