import asyncio
import codecs
import os
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List

from virtual_term import VirtualTerm, TerminalDeadError

from .compactor import (
    DEFAULT_SCROLLBACK,
    TerminalOutputChunk,
    TerminalSnapshot,
    TerminalStateCompactor,
)
from .xterm_worker import HeadlessXtermWorker


@dataclass
class TerminalSession:
    """Manages a single PTY terminal session.

    Wraps a :class:`~virtual_term.VirtualTerm`, accumulating output into a
    replay buffer, tracking command return codes, and exposing asyncio events
    that the WebSocket handler (or any consumer) can await.
    """

    pty_process: VirtualTerm
    output_chunks: List[TerminalOutputChunk] = field(default_factory=list)
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
    cwd: Path | None = None
    compactor: TerminalStateCompactor | None = None
    state_worker: HeadlessXtermWorker | None = None
    _output_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _complete: bool = False
    _owns_worker: bool = False
    _compactor_disposed: bool = False

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
        env: dict[str, str] | None = None,
        on_complete: Callable[[], None] | None = None,
        state_worker: HeadlessXtermWorker | None = None,
        scrollback: int = DEFAULT_SCROLLBACK,
    ) -> 'TerminalSession':
        """Create and start monitoring a new terminal session.

        Args:
            cwd: Working directory for the shell.
            dimensions: ``(rows, cols)`` terminal dimensions.
            shell: Shell executable path (defaults to ``$SHELL``).
            env: Extra environment variables to inject into the spawned shell.
            on_complete: Callback invoked once when the session terminates.
            state_worker: Worker used to mirror output into xterm-headless.
            scrollback: Headless xterm scrollback setting.
        """
        saved_env = None
        if env:
            saved_env = dict(os.environ)
            os.environ.update(env)
        try:
            pty_process = await VirtualTerm.spawn(
                dimensions=dimensions, cwd=cwd, shell=shell
            )
        finally:
            if saved_env is not None:
                os.environ.clear()
                os.environ.update(saved_env)
        worker = state_worker or HeadlessXtermWorker()
        rows, cols = dimensions
        session = cls(
            pty_process=pty_process,
            on_complete=on_complete,
            cwd=cwd,
            state_worker=worker,
            _owns_worker=state_worker is None,
        )
        try:
            session.compactor = await TerminalStateCompactor.create(
                session.id,
                rows=rows,
                cols=cols,
                worker=worker,
                scrollback=scrollback,
            )
        except Exception:
            with suppress(TerminalDeadError):
                await pty_process.terminate()
            if session._owns_worker:
                await worker.shutdown()
            raise
        session.monitor_command_results_task = asyncio.create_task(
            session._monitor_command_results_task_impl()
        )
        session.monitor_task = asyncio.create_task(session._monitor_output_task_impl())
        return session

    async def append_output(self, data: str):
        """Append one decoded PTY chunk, mirror it, and compact when eligible."""
        async with self._output_lock:
            self.next_update_id += 1
            chunk = TerminalOutputChunk(data=data, update_id=self.next_update_id)
            self.output_chunks.append(chunk)
            if self.compactor:
                snapshot = await self.compactor.write(data, chunk.update_id)
                if snapshot:
                    self._discard_chunks_through(snapshot.update_id)
        self.new_content_event.set()

    async def resize(self, rows: int, cols: int):
        """Update the headless mirror size and refresh snapshot metadata."""
        async with self._output_lock:
            if not self.compactor:
                return
            snapshot = await self.compactor.resize(
                rows=rows, cols=cols, update_id=self.next_update_id
            )
            if snapshot:
                self._discard_chunks_through(snapshot.update_id)

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
            return None, list(self.output_chunks)

    async def shutdown(self):
        """Terminate the PTY process and cancel background monitor tasks."""
        try:
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
            self._mark_complete()

    async def _dispose_compactor(self):
        if self._compactor_disposed:
            return
        self._compactor_disposed = True
        if self.compactor:
            with suppress(Exception):
                await self.compactor.dispose()
        if self._owns_worker and self.state_worker:
            await self.state_worker.shutdown()

    def _discard_chunks_through(self, update_id: int):
        self.base_update_id = max(self.base_update_id, update_id)
        self.output_chunks = [
            chunk for chunk in self.output_chunks if chunk.update_id > update_id
        ]

    def _mark_complete(self):
        """Mark the session complete and invoke its completion callback once."""
        if self._complete:
            return
        self._complete = True
        self.session_dead_event.set()
        if self.on_complete:
            self.on_complete()

    async def _monitor_output_task_impl(self):
        assert self.monitor_command_results_task
        decoder = codecs.getincrementaldecoder('utf-8')()
        try:
            async for output in self.pty_process.read_output_stream():
                if not output:
                    break
                decoded = decoder.decode(output)
                if decoded:
                    await self.append_output(decoded)
        except EOFError:
            pass
        finally:
            decoded = decoder.decode(b'', final=True)
            if decoded:
                await self.append_output(decoded)
            with suppress(TerminalDeadError):
                await self.pty_process.terminate()
            self.monitor_command_results_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.monitor_command_results_task
            await self._dispose_compactor()
            self._mark_complete()

    async def _monitor_command_results_task_impl(self):
        try:
            async for return_code in self.pty_process.read_command_result_stream():
                self.command_results.append(return_code)
                self.command_index_to_update_id.append(self.update_id)
                self.new_command_result_event.set()
        except EOFError:
            pass
