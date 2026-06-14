"""FastAPI router factory for terminal endpoints.

Usage (zero-config)::

    from termiccio_terminal import create_terminal_router
    router, manager = create_terminal_router()
    app.include_router(router)

With custom CWD resolution and request model::

    from pydantic import BaseModel
    from termiccio_terminal import create_terminal_router

    class MyRequest(BaseModel):
        project_name: str
        rows: int = 24
        cols: int = 80

    def resolve_cwd(req: MyRequest):
        return Path('/projects') / req.project_name

    router, manager = create_terminal_router(
        request_model=MyRequest,
        resolve_cwd=resolve_cwd,
    )
"""

from pathlib import Path
from typing import Callable

from fastapi import APIRouter, HTTPException, WebSocket
from pydantic import BaseModel

from .handler import TerminalWebsocketHandler
from .manager import PTYManager
from .schemas import CreateTerminalRequest, CreateTerminalResponse, CwdResponse

ResolveCwd = Callable[[BaseModel], Path | str | None]


def create_terminal_router(
    *,
    pty_manager: PTYManager | None = None,
    prefix: str = '',
    request_model: type[BaseModel] = CreateTerminalRequest,
    resolve_cwd: ResolveCwd | None = None,
    shell: str | None = None,
    env: dict[str, str] | None = None,
    mkdir: bool = True,
) -> tuple[APIRouter, PTYManager]:
    """Build a FastAPI :class:`~fastapi.APIRouter` with standard terminal endpoints.

    Endpoints provided:

    * ``POST   {prefix}/terminals``                  -- create a session
    * ``GET    {prefix}/terminals/{session_id}/cwd`` -- get live CWD
    * ``WS     {prefix}/terminals/{session_id}/ws``  -- streaming I/O

    Args:
        pty_manager: Existing :class:`PTYManager` to use.  A new one is created
            when omitted.
        prefix: URL prefix applied to every route (e.g. ``"/api"``).
        request_model: Pydantic model for the create-terminal request body.
            Defaults to :class:`~termiccio_terminal.schemas.CreateTerminalRequest`.
        resolve_cwd: Callback that receives the parsed request and returns the
            working directory for the new session (or ``None`` for the default
            shell CWD).  When omitted, the ``cwd`` field of *request_model* is
            used if present.
        shell: Override the shell executable for every session.
        env: Extra environment variables injected into every session.
        mkdir: When ``True`` (default), ``mkdir -p`` the resolved CWD before
            spawning.

    Returns:
        A ``(router, pty_manager)`` tuple.  The *pty_manager* is the same
        instance that was passed in, or the newly created one.
    """
    manager = pty_manager or PTYManager()

    if resolve_cwd is None:

        def resolve_cwd(request: BaseModel) -> Path | str | None:  # noqa: E306
            cwd = getattr(request, 'cwd', None)
            return Path(cwd) if cwd else None

    router = APIRouter(prefix=prefix)

    @router.post('/terminals', response_model=CreateTerminalResponse)
    async def create_terminal(request: request_model):  # type: ignore[valid-type]
        cwd = resolve_cwd(request)
        cwd_path = Path(cwd) if cwd else None
        if cwd_path and mkdir:
            cwd_path.mkdir(parents=True, exist_ok=True)
        session_id = await manager.create_session(
            request.rows,
            request.cols,
            cwd=cwd_path,
            shell=shell,
            env=env,
        )
        return CreateTerminalResponse(session_id=session_id)

    @router.get('/terminals/{session_id}/cwd', response_model=CwdResponse)
    async def get_terminal_cwd(session_id: str):
        if not manager.is_session(session_id):
            raise HTTPException(status_code=404, detail='Session not found')
        cwd = manager.get_cwd(session_id)
        return CwdResponse(cwd=str(cwd) if cwd else None)

    @router.websocket('/terminals/{session_id}/ws')
    async def websocket_endpoint(
        websocket: WebSocket, session_id: str, update_id: int = 0
    ):
        await TerminalWebsocketHandler(
            websocket, session_id, update_id, pty_manager=manager
        ).handle()

    return router, manager
