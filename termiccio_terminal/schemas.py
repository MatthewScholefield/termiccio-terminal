"""Pydantic models defining the terminal WebSocket protocol and REST payloads."""

from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field


# =====================================================================
# REST request / response models
# =====================================================================


class CreateTerminalRequest(BaseModel):
    """Default request body for ``POST /terminals``.

    Apps that need app-specific fields (e.g. ``project_name``, ``agent_id``)
    should subclass this model and pass the subclass to
    :func:`~termiccio_terminal.router.create_terminal_router` via the
    ``request_model`` parameter.
    """

    rows: int = 24
    cols: int = 80
    cwd: Optional[str] = None


class CreateTerminalResponse(BaseModel):
    session_id: str


class CwdResponse(BaseModel):
    cwd: Optional[str] = None


# =====================================================================
# Incoming WebSocket messages (client -> server)
# =====================================================================


class StdinTerminalMessage(BaseModel):
    type: Literal['stdin'] = 'stdin'
    data: str


class ResizeTerminalMessage(BaseModel):
    type: Literal['resize'] = 'resize'
    rows: int
    cols: int


class GetSizeTerminalMessage(BaseModel):
    type: Literal['get_size'] = 'get_size'


IncomingTerminalMessage = Annotated[
    Union[StdinTerminalMessage, ResizeTerminalMessage, GetSizeTerminalMessage],
    Field(discriminator='type'),
]


# =====================================================================
# Server-sent WebSocket messages (server -> client)
# =====================================================================


class OutputTerminalMessage(BaseModel):
    type: Literal['output'] = 'output'
    data: str
    update_id: int


class SnapshotTerminalMessage(BaseModel):
    type: Literal['snapshot'] = 'snapshot'
    format: Literal['xterm-serialize-v1'] = 'xterm-serialize-v1'
    data: str
    update_id: int
    rows: int
    cols: int


class SizeTerminalMessage(BaseModel):
    type: Literal['size'] = 'size'
    rows: int
    cols: int


class CommandFinishTerminalMessage(BaseModel):
    type: Literal['command_finish'] = 'command_finish'
    command_index: int
    return_code: int


class SessionExitTerminalMessage(BaseModel):
    """Sent once when the underlying PTY process terminates permanently.

    Clients should treat this as a terminal close (the session will not come
    back), distinct from a transient WebSocket disconnect.
    """

    type: Literal['session_exit'] = 'session_exit'
    return_code: int = 0


class ErrorTerminalMessage(BaseModel):
    type: Literal['error'] = 'error'
    error_type: str
    message: str


ServerSentTerminalMessage = Annotated[
    Union[
        OutputTerminalMessage,
        SnapshotTerminalMessage,
        SizeTerminalMessage,
        CommandFinishTerminalMessage,
        SessionExitTerminalMessage,
        ErrorTerminalMessage,
    ],
    Field(discriminator='type'),
]
