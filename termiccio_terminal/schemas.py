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


class SizeTerminalMessage(BaseModel):
    type: Literal['size'] = 'size'
    rows: int
    cols: int


class CommandFinishTerminalMessage(BaseModel):
    type: Literal['command_finish'] = 'command_finish'
    command_index: int
    return_code: int


class ErrorTerminalMessage(BaseModel):
    type: Literal['error'] = 'error'
    error_type: str
    message: str


ServerSentTerminalMessage = Annotated[
    Union[
        OutputTerminalMessage,
        SizeTerminalMessage,
        CommandFinishTerminalMessage,
        ErrorTerminalMessage,
    ],
    Field(discriminator='type'),
]
