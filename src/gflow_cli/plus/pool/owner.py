"""Which process owns a pool job, held as a kernel lock for that process's lifetime.

The OS drops the lock when its process exits, however it exits, so a job whose
owner lock can be taken was orphaned: a restarted MCP server, a killed CLI run.
Same primitive as ``gflow_cli.profile_lease``: ``msvcrt.locking`` / ``fcntl.flock``.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl


def _try_lock(fd: int) -> bool:
    try:
        if sys.platform == "win32":
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _unlock(fd: int) -> None:
    if sys.platform == "win32":
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)


def _open_lock_file(path: Path) -> int:
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    if os.fstat(fd).st_size == 0:
        os.write(fd, b"\0")  # msvcrt.locking needs the locked byte to exist
    os.lseek(fd, 0, os.SEEK_SET)
    return fd


class OwnerRegistry:
    """This process's owner id, and whether another owner id is still alive."""

    def __init__(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self._dir = directory
        self.owner_id = uuid.uuid4().hex[:12]
        self._fd = _open_lock_file(directory / f"{self.owner_id}.lock")
        if not _try_lock(self._fd):
            raise RuntimeError(f"Could not lock new owner file {self.owner_id}.lock")

    def is_alive(self, owner_id: str | None) -> bool:
        if owner_id == self.owner_id:
            return True
        if not owner_id:
            return False
        path = self._dir / f"{owner_id}.lock"
        if not path.exists():
            return False
        fd = _open_lock_file(path)
        try:
            if not _try_lock(fd):
                return True
            _unlock(fd)
        finally:
            os.close(fd)
        path.unlink(missing_ok=True)
        return False
