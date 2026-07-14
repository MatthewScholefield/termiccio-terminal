from pathlib import Path
from typing import Callable, Dict, Sequence, Tuple

from loguru import logger

from virtual_term import TerminalDeadError
from .compactor import TerminalSnapshot
from .session import TerminalSession
from .xterm_worker import HeadlessXtermWorker

PtyCommand = str | Sequence[str]


class PTYManager:
    """Registry that owns all live :class:`TerminalSession` instances.

    A single ``PTYManager`` is typically shared across all terminal routes in
    an application.  It is safe to use as a singleton.
    """

    def __init__(self, state_worker: HeadlessXtermWorker | None = None):
        self.sessions: Dict[str, TerminalSession] = {}
        self.state_worker = state_worker

    async def create_session(
        self,
        rows: int,
        cols: int,
        cwd: Path | None = None,
        shell: str | None = None,
        command: PtyCommand | None = None,
        env: dict[str, str] | None = None,
        on_complete: Callable[[str], None] | None = None,
        on_output: Callable[[str, int], None] | None = None,
        on_snapshot: Callable[[TerminalSnapshot], None] | None = None,
        terminal_theme: dict[str, str] | None = None,
        compaction_enabled: bool = True,
    ) -> str:
        """Spawn a new session and return its id.

        A shared state worker is created only when a compaction-enabled session
        needs one; an explicitly supplied worker remains manager-owned.
        """
        if compaction_enabled and self.state_worker is None:
            self.state_worker = HeadlessXtermWorker()

        def on_session_complete():
            self.sessions.pop(session.id, None)
            if on_complete:
                on_complete(session.id)

        session = await TerminalSession.create(
            dimensions=(rows, cols),
            cwd=cwd,
            shell=shell,
            command=command,
            env=env,
            on_complete=on_session_complete,
            on_output=on_output,
            on_snapshot=on_snapshot,
            terminal_theme=terminal_theme,
            state_worker=self.state_worker,
            compaction_enabled=compaction_enabled,
        )
        self.sessions[session.id] = session
        return session.id

    def get_session(self, session_id: str) -> TerminalSession:
        return self.sessions[session_id]

    def is_session(self, session_id: str) -> bool:
        return session_id in self.sessions

    async def shutdown(self):
        """Gracefully terminate every active session."""
        logger.info('Shutting down PTY manager...')
        for session in list(self.sessions.values()):
            await session.shutdown()
        self.sessions.clear()
        if self.state_worker:
            await self.state_worker.shutdown()
        logger.info('PTY manager shutdown complete')

    async def write_input(self, session_id: str, data: str):
        session = self.get_session(session_id)
        try:
            await session.pty_process.write(data.encode())
        except TerminalDeadError:
            await session.shutdown()
            self.sessions.pop(session_id, None)

    async def resize_terminal(self, session_id: str, rows: int, cols: int):
        session = self.get_session(session_id)
        await session.pty_process.setwinsize(rows, cols)
        await session.resize(rows, cols)

    def get_terminal_size(self, session_id: str) -> Tuple[int, int]:
        session = self.get_session(session_id)
        return session.pty_process.getwinsize()

    def get_cwd(self, session_id: str) -> Path | None:
        """Return the live working directory of *session_id*."""
        session = self.get_session(session_id)
        return session.current_cwd
