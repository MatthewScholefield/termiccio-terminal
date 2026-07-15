import asyncio
import codecs
import os
import shlex
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Sequence

from loguru import logger
from virtual_term import VirtualTerm, TerminalDeadError

from .compactor import (
    DEFAULT_SCROLLBACK,
    TerminalOutputChunk,
    TerminalSnapshot,
    TerminalStateCompactor,
)
from .xterm_worker import HeadlessXtermWorker

PtyCommand = str | Sequence[str]


@dataclass
class TerminalSession:
    """Manages a single PTY terminal session.

    Wraps a :class:`~virtual_term.VirtualTerm`, accumulating output into a
    replay buffer, tracking command return codes, and exposing asyncio events
    that the WebSocket handler (or any consumer) can await.
    """

    pty_process: VirtualTerm
    output_chunks: List[TerminalOutputChunk] = field(default_factory=list)
    replay_chunks: List[TerminalOutputChunk] = field(default_factory=list)
    base_update_id: int = 0
    next_update_id: int = 0
    command_results: List[int] = field(default_factory=list)
    command_index_to_update_id: List[int] = field(default_factory=list)
    new_content_event: asyncio.Event = field(default_factory=asyncio.Event)
    new_command_result_event: asyncio.Event = field(default_factory=asyncio.Event)
    session_dead_event: asyncio.Event = field(default_factory=asyncio.Event)
    monitor_task: asyncio.Task | None = None
    monitor_command_results_task: asyncio.Task | None = None
    active_connections: int = 0
    on_complete: Callable[[], None] | None = None
    on_output: Callable[[str, int], None] | None = None
    on_snapshot: Callable[[TerminalSnapshot], None] | None = None
    cwd: Path | None = None
    compactor: TerminalStateCompactor | None = None
    state_worker: HeadlessXtermWorker | None = None
    _output_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _complete: bool = False
    _owns_worker: bool = False
    _compactor_disposed: bool = False
    _compaction_failed: bool = False
    _shutdown_requested: bool = False
    _compaction_queue: asyncio.Queue[tuple] = field(
        default_factory=lambda: asyncio.Queue(maxsize=1024)
    )
    _compaction_task: asyncio.Task | None = None
    _observer_queue: asyncio.Queue[tuple[str, int]] = field(
        default_factory=asyncio.Queue
    )
    _observer_task: asyncio.Task | None = None

    @property
    def id(self) -> str:
        return self.pty_process.id

    @property
    def update_id(self) -> int:
        return self.next_update_id

    @property
    def output_buffer(self) -> List[str]:
        """Return retained output strings for compatibility with older callers."""
        return [chunk.data for chunk in self.output_chunks]

    @property
    def current_cwd(self) -> Path | None:
        """Return the live working directory of the spawned shell process."""
        try:
            child_pid = self.pty_process.pty_process.child_pid
            if child_pid:
                return Path(os.readlink(f'/proc/{child_pid}/cwd'))
        except (OSError, FileNotFoundError):
            pass
        return self.cwd

    @classmethod
    async def create(
        cls,
        cwd: Path | None = None,
        dimensions=(24, 80),
        shell: str | None = None,
        command: PtyCommand | None = None,
        env: dict[str, str] | None = None,
        on_complete: Callable[[], None] | None = None,
        on_output: Callable[[str, int], None] | None = None,
        on_snapshot: Callable[[TerminalSnapshot], None] | None = None,
        terminal_theme: dict[str, str] | None = None,
        state_worker: HeadlessXtermWorker | None = None,
        scrollback: int = DEFAULT_SCROLLBACK,
        compaction_enabled: bool = True,
    ) -> 'TerminalSession':
        """Create and start monitoring a new terminal session.

        Args:
            cwd: Working directory for the shell.
            dimensions: ``(rows, cols)`` terminal dimensions.
            shell: Shell executable path (defaults to ``$SHELL``).
            command: Command to spawn directly as the PTY child. When set, no
                shell is launched first.
            env: Extra environment variables to inject into the spawned shell.
            on_complete: Callback invoked once when the session terminates.
            state_worker: Worker used to mirror output into xterm-headless when
                compaction is enabled. If omitted, the session creates and owns one.
            scrollback: Headless xterm scrollback setting.
            compaction_enabled: Whether to maintain compacted xterm snapshots. When
                false, no worker is created unless one is explicitly supplied.
        """
        if shell and command:
            raise ValueError('Cannot specify both shell and command.')

        if command is not None:
            argv = shlex.split(command) if isinstance(command, str) else list(command)
            if not argv:
                raise ValueError('PTY command cannot be empty.')
            pty_process = await VirtualTerm.spawn_command(
                argv, dimensions=dimensions, cwd=cwd, env=env
            )
        else:
            pty_process = await VirtualTerm.spawn(
                dimensions=dimensions, cwd=cwd, shell=shell, env=env
            )
        worker = state_worker
        if compaction_enabled and worker is None:
            worker = HeadlessXtermWorker()
        rows, cols = dimensions
        session = cls(
            pty_process=pty_process,
            on_complete=on_complete,
            on_output=on_output,
            on_snapshot=on_snapshot,
            cwd=cwd,
            state_worker=worker,
            _owns_worker=worker is not None and state_worker is None,
        )
        if compaction_enabled:
            session.compactor = TerminalStateCompactor(
                session.id,
                rows=rows,
                cols=cols,
                worker=worker,
                scrollback=scrollback,
                theme=terminal_theme,
                on_snapshot=on_snapshot,
            )
            session._ensure_compaction_task()
        else:
            session._compactor_disposed = True
        session.monitor_command_results_task = asyncio.create_task(
            session._monitor_command_results_task_impl()
        )
        session.monitor_task = asyncio.create_task(session._monitor_output_task_impl())
        return session

    async def append_output(self, data: str):
        """Append one decoded PTY chunk and enqueue best-effort compaction."""
        async with self._output_lock:
            self.next_update_id += 1
            chunk = TerminalOutputChunk(data=data, update_id=self.next_update_id)
            self.output_chunks.append(chunk)
            self.replay_chunks.append(chunk)
            if self.compactor and not self._compaction_failed:
                self._ensure_compaction_task()
                self._enqueue_compaction(('write', data, chunk.update_id))
        self.new_content_event.set()
        if self.on_output:
            self._observer_queue.put_nowait((data, chunk.update_id))
            if self._observer_task is None:
                self._observer_task = asyncio.create_task(self._run_output_observer())

    async def _run_output_observer(self) -> None:
        while True:
            data, update_id = await self._observer_queue.get()
            await asyncio.to_thread(self._notify_output, data, update_id)

    def _notify_output(self, data: str, update_id: int) -> None:
        try:
            self.on_output(data, update_id)
        except Exception:
            logger.exception('Terminal output observer failed for {}', self.id)

    async def resize(self, rows: int, cols: int):
        """Enqueue a mirror resize after the real PTY has already resized."""
        if not self.compactor or self._compaction_failed:
            return
        self._ensure_compaction_task()
        self._enqueue_compaction(('resize', rows, cols, self.next_update_id))

    async def reconnect_payload(
        self, requested_update_id: int
    ) -> tuple[TerminalSnapshot | None, List[TerminalOutputChunk]]:
        """Return the snapshot and/or retained chunks needed for reconnect."""
        async with self._output_lock:
            if requested_update_id >= self.base_update_id:
                return None, [
                    chunk
                    for chunk in self.output_chunks
                    if chunk.update_id > requested_update_id
                ]

            snapshot = self.compactor.snapshot if self.compactor else None
            if snapshot:
                return snapshot, [
                    chunk
                    for chunk in self.output_chunks
                    if chunk.update_id > snapshot.update_id
                ]
            replay_chunks = [
                chunk
                for chunk in self.replay_chunks
                if chunk.update_id > requested_update_id
            ]
            if replay_chunks:
                logger.warning(
                    'Terminal session {} replaying compacted output without a '
                    'snapshot for requested update_id {}',
                    self.id,
                    requested_update_id,
                )
                return None, replay_chunks
            return None, list(self.output_chunks)

    async def shutdown(self):
        """Terminate the PTY process and cancel background monitor tasks."""
        try:
            self._shutdown_requested = True
            with suppress(TerminalDeadError):
                await self.pty_process.terminate()
        finally:
            if self.monitor_task:
                self.monitor_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self.monitor_task
            if self.monitor_command_results_task:
                self.monitor_command_results_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self.monitor_command_results_task
            await self._dispose_compactor()
            if self._observer_task:
                self._observer_task.cancel()
            self._mark_complete('shutdown requested')

    async def _dispose_compactor(self):
        if self._compactor_disposed:
            return
        self._compactor_disposed = True
        if self._compaction_task:
            self._compaction_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._compaction_task
        elif self.compactor:
            with suppress(Exception):
                await self.compactor.dispose()
        if self._owns_worker and self.state_worker:
            await self.state_worker.shutdown()

    def _enqueue_compaction(self, operation: tuple) -> None:
        try:
            self._compaction_queue.put_nowait(operation)
        except asyncio.QueueFull:
            self._compaction_failed = True
            if self._compaction_task:
                self._compaction_task.cancel()
            logger.warning(
                'Terminal state compaction backlog full for {}; snapshots disabled',
                self.id,
            )

    def _ensure_compaction_task(self):
        if self._compaction_task is None:
            self._compaction_task = asyncio.create_task(self._run_compaction())

    async def _run_compaction(self):
        assert self.compactor
        try:
            await self.compactor.initialize()
            while True:
                operation = await self._compaction_queue.get()
                if operation[0] == 'stop':
                    break
                if operation[0] == 'write':
                    snapshot = await self.compactor.write(operation[1], operation[2])
                else:
                    snapshot = await self.compactor.resize(
                        rows=operation[1],
                        cols=operation[2],
                        update_id=operation[3],
                    )
                if snapshot:
                    async with self._output_lock:
                        self._apply_snapshot_compaction(snapshot)
        except Exception:
            self._compaction_failed = True
            logger.exception(
                'Terminal state compaction failed for {}; future snapshots disabled '
                'and replay output retained',
                self.id,
            )
        finally:
            with suppress(Exception):
                await self.compactor.dispose()

    def _discard_chunks_through(self, update_id: int):
        self.base_update_id = max(self.base_update_id, update_id)
        self.output_chunks = [
            chunk for chunk in self.output_chunks if chunk.update_id > update_id
        ]

    def _apply_snapshot_compaction(self, snapshot: TerminalSnapshot):
        self._discard_chunks_through(snapshot.update_id)
        self.replay_chunks = [
            TerminalOutputChunk(data=snapshot.data, update_id=snapshot.update_id),
            *[
                chunk
                for chunk in self.replay_chunks
                if chunk.update_id > snapshot.update_id
            ],
        ]

    def _mark_complete(self, reason: str):
        """Mark the session complete and invoke its completion callback once."""
        if self._complete:
            return
        self._complete = True
        logger.info('Terminal session {} completed: {}', self.id, reason)
        self.session_dead_event.set()
        if self.on_complete:
            self.on_complete()

    async def _monitor_output_task_impl(self):
        assert self.monitor_command_results_task
        decoder = codecs.getincrementaldecoder('utf-8')()
        completion_reason = 'pty output stream ended'
        try:
            async for output in self.pty_process.read_output_stream():
                if not output:
                    completion_reason = 'pty output stream returned an empty chunk'
                    break
                decoded = decoder.decode(output)
                if decoded:
                    await self.append_output(decoded)
        except EOFError:
            completion_reason = 'pty output stream reached EOF'
            logger.info('Terminal session {} output stream reached EOF', self.id)
        except Exception:
            completion_reason = 'pty output monitor failed'
            logger.exception('Terminal session {} output monitor failed', self.id)
        finally:
            if self._shutdown_requested:
                completion_reason = 'shutdown requested'
            decoded = decoder.decode(b'', final=True)
            if decoded:
                await self.append_output(decoded)
            with suppress(TerminalDeadError):
                await self.pty_process.terminate()
            self.monitor_command_results_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.monitor_command_results_task
            await self._dispose_compactor()
            self._mark_complete(completion_reason)

    async def _monitor_command_results_task_impl(self):
        try:
            async for return_code in self.pty_process.read_command_result_stream():
                self.command_results.append(return_code)
                self.command_index_to_update_id.append(self.update_id)
                self.new_command_result_event.set()
        except EOFError:
            logger.debug(
                'Terminal session {} command result stream reached EOF', self.id
            )
