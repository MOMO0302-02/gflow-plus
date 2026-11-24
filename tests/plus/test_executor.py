from __future__ import annotations

import struct
from pathlib import Path

from gflow_cli.plus.pool.executor import mp4_duration_s


def _box(kind: bytes, body: bytes) -> bytes:
    return struct.pack(">I4s", 8 + len(body), kind) + body


def _mvhd(version: int, timescale: int, duration: int) -> bytes:
    if version == 1:
        fields = struct.pack(">QQIQ", 0, 0, timescale, duration)
    else:
        fields = struct.pack(">IIII", 0, 0, timescale, duration)
    return _box(b"mvhd", bytes([version, 0, 0, 0]) + fields + b"\0" * 80)


def test_reads_the_movie_duration_after_the_media_data(tmp_path: Path) -> None:
    # Flow's clips put moov after mdat; the parser must skip mdat, not scan it.
    clip = tmp_path / "clip.mp4"
    clip.write_bytes(
        _box(b"ftyp", b"isom\0\0\0\0")
        + _box(b"mdat", b"\0" * 4096)
        + _box(b"moov", _mvhd(0, 1000, 4033))
    )
    assert mp4_duration_s(clip) == 4.03


def test_reads_a_version_1_header(tmp_path: Path) -> None:
    clip = tmp_path / "clip.mp4"
    clip.write_bytes(_box(b"moov", _mvhd(1, 90000, 15 * 90000)))
    assert mp4_duration_s(clip) == 15.0


def test_a_file_that_is_not_an_mp4_has_no_duration(tmp_path: Path) -> None:
    not_mp4 = tmp_path / "x.mp4"
    not_mp4.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\0" * 64)
    assert mp4_duration_s(not_mp4) is None
