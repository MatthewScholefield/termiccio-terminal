# termiccio-terminal

> Reusable async PTY terminal session management with WebSocket streaming for FastAPI.

`termiccio-terminal` extracts the non-trivial terminal plumbing -- session
lifecycle, output buffering/replay, command-finish tracking, and a
multiplexed WebSocket handler -- into a standalone library that any FastAPI
application can drop in.

The complex async logic (buffer replay, event races, task cancellation) lives
in **one place**. Applications only decide *how* sessions are created and what
metadata they carry.

---

## Quick start

```sh
uv add termiccio-terminal
```

### Zero-config router

```python
from fastapi import FastAPI
from termiccio_terminal import create_terminal_router

app = FastAPI()
terminal_router, pty_manager = create_terminal_router(prefix="/api")
app.include_router(terminal_router)
```

This gives you three endpoints:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/terminals` | Create a new terminal session |
| `GET` | `/api/terminals/{session_id}/cwd` | Get the live working directory |
| `WS` | `/api/terminals/{session_id}/ws?update_id=N` | Stream terminal I/O |

### Custom CWD resolution

Most apps need to control *where* a terminal is spawned. Pass a
`resolve_cwd` callback and (optionally) a custom request model:

```python
from pathlib import Path
from pydantic import BaseModel
from termiccio_terminal import create_terminal_router

class CreateTerminalRequest(BaseModel):
    project_name: str
    rows: int = 24
    cols: int = 80

def resolve_cwd(request: CreateTerminalRequest) -> Path:
    return Path("/projects") / request.project_name

terminal_router, pty_manager = create_terminal_router(
    request_model=CreateTerminalRequest,
    resolve_cwd=resolve_cwd,
    prefix="/api",
)
```

### Building blocks (advanced)

For full control over session creation -- e.g. associating sessions with
agents, spawning a custom shell, or adding metadata -- use the classes
directly. You still reuse the entire WebSocket streaming protocol with **zero
duplication**:

```python
from termiccio_terminal import PTYManager, TerminalWebsocketHandler

pty_manager = PTYManager()

@app.post("/agents/{agent_id}/terminal")
async def create_agent_terminal(agent_id: str):
    agent = agent_registry.get(agent_id)
    session_id = await pty_manager.create_session(
        rows=24, cols=80,
        cwd=agent.project_folder,
        shell="claude",
    )
    agent.terminal_session_id = session_id
    return {"session_id": session_id}

@app.websocket("/terminals/{session_id}/ws")
async def terminal_ws(websocket: WebSocket, session_id: str, update_id: int = 0):
    await TerminalWebsocketHandler(
        websocket, session_id, update_id, pty_manager=pty_manager,
    ).handle()
```

---

## WebSocket protocol

### Client → Server

| `type` | Fields | Action |
|--------|--------|--------|
| `stdin` | `data: str` | Write text to the terminal |
| `resize` | `rows: int`, `cols: int` | Resize the PTY |
| `get_size` | | Request the current terminal size |

### Server → Client

| `type` | Fields | Description |
|--------|--------|-------------|
| `output` | `data: str`, `update_id: int` | Buffered/new terminal output |
| `size` | `rows: int`, `cols: int` | Response to `get_size` |
| `command_finish` | `command_index: int`, `return_code: int` | A command completed |
| `error` | `error_type: str`, `message: str` | Error (e.g. session not found) |

### Buffer replay via `update_id`

When connecting, pass `?update_id=N` to receive all output since update ID
*N*. This enables seamless reconnection: a client that disconnected at
`update_id=42` reconnects with `?update_id=42` and immediately receives the
buffered output it missed, then continues streaming live.

---

## API reference

### `create_terminal_router(...)`

```python
create_terminal_router(
    *,
    pty_manager: PTYManager | None = None,
    prefix: str = "",
    request_model: type[BaseModel] = CreateTerminalRequest,
    resolve_cwd: Callable[[BaseModel], Path | str | None] | None = None,
    shell: str | None = None,
    env: dict[str, str] | None = None,
    mkdir: bool = True,
) -> tuple[APIRouter, PTYManager]
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pty_manager` | `None` (creates one) | Shared manager instance |
| `prefix` | `""` | URL prefix for all routes |
| `request_model` | `CreateTerminalRequest` | Pydantic model for `POST /terminals` |
| `resolve_cwd` | extracts `cwd` field | Callback mapping request → working directory |
| `shell` | `None` (`$SHELL`) | Shell executable override |
| `env` | `None` | Extra environment variables |
| `mkdir` | `True` | Create the CWD directory if it doesn't exist |

Returns `(router, pty_manager)`.

### Classes

| Class | Description |
|-------|-------------|
| `PTYManager` | Registry of live sessions; create/get/write/resize/shutdown |
| `TerminalSession` | Wraps a PTY process, manages output buffer and asyncio events |
| `TerminalWebsocketHandler` | Multiplexed WebSocket handler with buffer replay |

---

## Development

```sh
git clone https://github.com/MatthewScholefield/termiccio-terminal.git
cd termiccio-terminal
uv sync
uv run pytest
```

## License

MIT
