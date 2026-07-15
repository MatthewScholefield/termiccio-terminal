"""Snapshot and retained-tail coordination for terminal sessions."""

from __future__ import annotations

import time
from dataclasses import dataclass
import asyncio
from typing import Callable

from loguru import logger

from .xterm_worker import HeadlessXtermWorker

SNAPSHOT_FORMAT = 'xterm-serialize-v1'
DEFAULT_SNAPSHOT_BYTE_THRESHOLD = 50 * 1024
DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 1.0
DEFAULT_SCROLLBACK = 1000


@dataclass(frozen=True)
class TerminalOutputChunk:
    data: str
    update_id: int


@dataclass(frozen=True)
class TerminalSnapshot:
    format: str
    data: str
    update_id: int
    rows: int
    cols: int
    scrollback: int
    background: str | None = None


class TerminalStateCompactor:
    """Mirrors PTY output into xterm-headless and periodically snapshots it."""

    def __init__(
        self,
        terminal_id: str,
        *,
        rows: int,
        cols: int,
        worker: HeadlessXtermWorker,
        scrollback: int = DEFAULT_SCROLLBACK,
        byte_threshold: int = DEFAULT_SNAPSHOT_BYTE_THRESHOLD,
        interval_seconds: float = DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
        theme: dict[str, str] | None = None,
        on_snapshot: Callable[[TerminalSnapshot], None] | None = None,
    ):
        self.terminal_id = terminal_id
        self.rows = rows
        self.cols = cols
        self.scrollback = scrollback
        self.worker = worker
        self.byte_threshold = byte_threshold
        self.interval_seconds = interval_seconds
        self.theme = theme
        self.on_snapshot = on_snapshot
        self.snapshot: TerminalSnapshot | None = None
        self._bytes_since_snapshot = 0
        self._last_snapshot_at = 0.0

    @classmethod
    async def create(
        cls,
        terminal_id: str,
        *,
        rows: int,
        cols: int,
        worker: HeadlessXtermWorker,
        scrollback: int = DEFAULT_SCROLLBACK,
        byte_threshold: int = DEFAULT_SNAPSHOT_BYTE_THRESHOLD,
        interval_seconds: float = DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
        theme: dict[str, str] | None = None,
        on_snapshot: Callable[[TerminalSnapshot], None] | None = None,
    ) -> 'TerminalStateCompactor':
        compactor = cls(
            terminal_id,
            rows=rows,
            cols=cols,
            worker=worker,
            scrollback=scrollback,
            byte_threshold=byte_threshold,
            interval_seconds=interval_seconds,
            theme=theme,
            on_snapshot=on_snapshot,
        )
        await compactor.initialize()
        return compactor

    async def initialize(self) -> None:
        await self.worker.create(
            self.terminal_id,
            rows=self.rows,
            cols=self.cols,
            scrollback=self.scrollback,
            theme=self.theme,
        )

    async def write(self, data: str, update_id: int) -> TerminalSnapshot | None:
        await self.worker.write(self.terminal_id, data)
        self._bytes_since_snapshot += len(data.encode())
        if not self._should_snapshot():
            return None
        return await self.snapshot_now(update_id)

    async def resize(
        self, *, rows: int, cols: int, update_id: int
    ) -> TerminalSnapshot | None:
        self.rows = rows
        self.cols = cols
        await self.worker.resize(self.terminal_id, rows=rows, cols=cols)
        if self.snapshot is None:
            return None
        return await self.snapshot_now(update_id)

    async def snapshot_now(self, update_id: int) -> TerminalSnapshot:
        worker_snapshot = await self.worker.snapshot(self.terminal_id)
        self.snapshot = TerminalSnapshot(
            format=SNAPSHOT_FORMAT,
            data=worker_snapshot.data,
            update_id=update_id,
            rows=self.rows,
            cols=self.cols,
            scrollback=self.scrollback,
            background=worker_snapshot.background,
        )
        self._bytes_since_snapshot = 0
        self._last_snapshot_at = time.monotonic()
        if self.on_snapshot:
            asyncio.create_task(asyncio.to_thread(self._notify_snapshot, self.snapshot))
        return self.snapshot

    def _notify_snapshot(self, snapshot: TerminalSnapshot) -> None:
        try:
            self.on_snapshot(snapshot)
        except Exception:
            logger.exception(
                'Terminal snapshot observer failed for {}', self.terminal_id
            )

    async def dispose(self) -> None:
        await self.worker.dispose(self.terminal_id)

    def _should_snapshot(self) -> bool:
        return (
            self._bytes_since_snapshot >= self.byte_threshold
            and time.monotonic() - self._last_snapshot_at >= self.interval_seconds
        )
