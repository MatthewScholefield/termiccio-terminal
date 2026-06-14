import asyncio
import codecs
import os
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List

from virtual_term import VirtualTerm, TerminalDeadError


@dataclass
class TerminalSession:
    """Manages a single PTY terminal session.

    Wraps a :class:`~virtual_term.VirtualTerm`, accumulating output into a
    replay buffer, tracking command return codes, and exposing asyncio events
    that the WebSocket handler (or any consumer) can await.
    """

    pty_process: VirtualTerm
    output_buffer: List[str] = field(default_factory=list)
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

    @property
    def id(self) -> str:
        return self.pty_process.id

    @property
    def update_id(self) -> int:
        return len(self.output_buffer)

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
    ) -> 'TerminalSession':
        """Create and start monitoring a new terminal session.

        Args:
            cwd: Working directory for the shell.
            dimensions: ``(rows, cols)`` terminal dimensions.
            shell: Shell executable path (defaults to ``$SHELL``).
            env: Extra environment variables to inject into the spawned shell.
            on_complete: Callback invoked once when the session terminates.
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
        session = cls(pty_process=pty_process, on_complete=on_complete, cwd=cwd)
        session.monitor_command_results_task = asyncio.create_task(
            session._monitor_command_results_task_impl()
        )
        session.monitor_task = asyncio.create_task(session._monitor_output_task_impl())
        return session

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

    async def _monitor_output_task_impl(self):
        assert self.monitor_command_results_task
        decoder = codecs.getincrementaldecoder('utf-8')()
        try:
            async for output in self.pty_process.read_output_stream():
                decoded = decoder.decode(output)
                if decoded:
                    self.output_buffer.append(decoded)
                    self.new_content_event.set()
        except EOFError:
            pass
        decoded = decoder.decode(b'', final=True)
        if decoded:
            self.output_buffer.append(decoded)
            self.new_content_event.set()
        self.monitor_command_results_task.cancel()
        with suppress(asyncio.CancelledError):
            await self.monitor_command_results_task
        self.session_dead_event.set()
        if self.on_complete:
            self.on_complete()

    async def _monitor_command_results_task_impl(self):
        try:
            async for return_code in self.pty_process.read_command_result_stream():
                self.command_results.append(return_code)
                self.command_index_to_update_id.append(len(self.output_buffer))
                self.new_command_result_event.set()
        except EOFError:
            pass
