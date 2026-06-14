"""termiccio-terminal -- Reusable async PTY terminal sessions for FastAPI.

Public API::

    from termiccio_terminal import (
        PTYManager,
        TerminalSession,
        TerminalWebsocketHandler,
        create_terminal_router,
    )
"""

from .handler import TerminalWebsocketHandler
from .manager import PTYManager
from .router import create_terminal_router
from .schemas import (
    CommandFinishTerminalMessage,
    CreateTerminalRequest,
    CreateTerminalResponse,
    CwdResponse,
    ErrorTerminalMessage,
    GetSizeTerminalMessage,
    IncomingTerminalMessage,
    OutputTerminalMessage,
    ResizeTerminalMessage,
    ServerSentTerminalMessage,
    SizeTerminalMessage,
    StdinTerminalMessage,
)
from .session import TerminalSession

__version__ = '0.1.0'

__all__ = [
    'TerminalSession',
    'PTYManager',
    'TerminalWebsocketHandler',
    'create_terminal_router',
    # schemas
    'CreateTerminalRequest',
    'CreateTerminalResponse',
    'CwdResponse',
    'StdinTerminalMessage',
    'ResizeTerminalMessage',
    'GetSizeTerminalMessage',
    'IncomingTerminalMessage',
    'OutputTerminalMessage',
    'SizeTerminalMessage',
    'CommandFinishTerminalMessage',
    'ErrorTerminalMessage',
    'ServerSentTerminalMessage',
]
