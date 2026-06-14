from pathlib import Path
from typing import Dict, Tuple

from loguru import logger

from virtual_term import TerminalDeadError
from .session import TerminalSession


class PTYManager:
    """Registry that owns all live :class:`TerminalSession` instances.

    A single ``PTYManager`` is typically shared across all terminal routes in
    an application.  It is safe to use as a singleton.
    """

    def __init__(self):
        self.sessions: Dict[str, TerminalSession] = {}

    async def create_session(
        self,
        rows: int,
        cols: int,
        cwd: Path | None = None,
        shell: str | None = None,
        env: dict[str, str] | None = None,
    ) -> str:
        """Spawn a new session and return its id."""

        def on_session_complete():
            del self.sessions[session.id]

        session = await TerminalSession.create(
            dimensions=(rows, cols),
            cwd=cwd,
            shell=shell,
            env=env,
            on_complete=on_session_complete,
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
        logger.info('PTY manager shutdown complete')

    async def write_input(self, session_id: str, data: str):
        session = self.get_session(session_id)
        try:
            await session.pty_process.write(data.encode())
        except TerminalDeadError:
            await session.shutdown()
            del self.sessions[session_id]

    async def resize_terminal(self, session_id: str, rows: int, cols: int):
        session = self.get_session(session_id)
        await session.pty_process.setwinsize(rows, cols)

    def get_terminal_size(self, session_id: str) -> Tuple[int, int]:
        session = self.get_session(session_id)
        return session.pty_process.getwinsize()

    def get_cwd(self, session_id: str) -> Path | None:
        """Return the live working directory of *session_id*."""
        session = self.get_session(session_id)
        return session.current_cwd
