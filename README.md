# termiccio-terminal

> Host fully interactive terminal sessions in your FastAPI app and stream them to an [xterm.js](https://xtermjs.org/) frontend over WebSockets.

`termiccio-terminal` gives you the backend building blocks for a web-based terminal: spawn PTY shells, relay input/output in real time, track command exit codes, and restore xterm.js snapshots on reconnect. Drop in the included FastAPI router or wire up the components yourself.

## Features

- **Live PTY sessions** -- spawn interactive shells (`bash`, `zsh`, `sh`) or direct PTY commands like `claude`
- **WebSocket streaming** -- real-time bidirectional I/O designed for xterm.js
- **Snapshot reconnect** -- clients reconnect with `?update_id=N` and receive either retained output or the latest serialized xterm.js snapshot plus a short tail
- **Command tracking** -- every command's exit code is captured and streamed as a `command_finish` event
- **Customizable** -- control the working directory, shell, environment variables, and request shape per session
- **Plug-and-play router** or use the session manager / WebSocket handler directly

## Install

```sh
uv add termiccio-terminal
```

The backend runtime must have Node 18+ available. The bundled worker uses
`@xterm/headless` and `@xterm/addon-serialize` to mirror PTY state; when running
from source or deploying the backend, install the included `package.json`
dependencies with `npm install`.

## Quick start

The easiest way is the factory, which returns a ready-made FastAPI router:

```python
from fastapi import FastAPI
from termiccio_terminal import create_terminal_router

app = FastAPI()
terminal_router, pty_manager = create_terminal_router(prefix="/api")
app.include_router(terminal_router)
```

That gives you:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/terminals` | Create a new terminal session |
| `GET` | `/api/terminals/{session_id}/cwd` | Get the live working directory |
| `WS` | `/api/terminals/{session_id}/ws?update_id=N` | Stream terminal I/O |

## Customizing session creation

You'll often want to control *where* a terminal starts or what request fields are accepted. Define a Pydantic model and a `resolve_cwd` callback:

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

You can also set a default `shell`, spawn a direct `command`, or inject `env` variables for every session created through the router.

## Using the building blocks directly

For full control over session creation -- e.g. associating sessions with agents, spawning a custom shell, or adding metadata -- use `PTYManager` and `TerminalWebsocketHandler` on your own routes:

```python
from fastapi import FastAPI, WebSocket
from termiccio_terminal import PTYManager, TerminalWebsocketHandler

app = FastAPI()
pty_manager = PTYManager()

@app.post("/agents/{agent_id}/terminal")
async def create_agent_terminal(agent_id: str):
    agent = agent_registry.get(agent_id)
    session_id = await pty_manager.create_session(
        rows=24, cols=80,
        cwd=agent.project_folder,
        command=["claude"],
    )
    agent.terminal_session_id = session_id
    return {"session_id": session_id}

@app.websocket("/terminals/{session_id}/ws")
async def terminal_ws(websocket: WebSocket, session_id: str, update_id: int = 0):
    await TerminalWebsocketHandler(
        websocket, session_id, update_id, pty_manager=pty_manager,
    ).handle()
```

## WebSocket protocol

The WebSocket endpoint accepts and sends JSON messages discriminated by a `type` field. This maps cleanly onto xterm.js's `onData` / `write` API.

### Client → Server

| `type` | Fields | Action |
|--------|--------|--------|
| `stdin` | `data: str` | Write text to the terminal |
| `resize` | `rows: int`, `cols: int` | Resize the PTY |
| `get_size` | | Request the current terminal size |

### Server → Client

| `type` | Fields | Description |
|--------|--------|-------------|
| `snapshot` | `format: "xterm-serialize-v1"`, `data: str`, `update_id: int`, `rows: int`, `cols: int` | Serialized xterm.js restore state |
| `output` | `data: str`, `update_id: int` | Terminal output (write to xterm.js) |
| `size` | `rows: int`, `cols: int` | Response to `get_size` |
| `command_finish` | `command_index: int`, `return_code: int` | A command completed |
| `error` | `error_type: str`, `message: str` | Error (e.g. session not found) |

### Reconnecting with `update_id`

Every `output` message includes an `update_id` that increments with each chunk.
If the WebSocket drops, reconnect with `?update_id=N` using the last ID you
received. When that ID is still in the retained tail, the server sends the
missed `output` chunks. When older output has been compacted, the server first
sends a `snapshot` message and then any newer `output` chunks. Clients should
reset xterm.js, write the snapshot `data`, record the snapshot `update_id`, and
then continue applying live `output` messages.

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
    command: str | Sequence[str] | None = None,
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
| `command` | `None` | Command to spawn directly as the PTY child instead of launching a shell |
| `env` | `None` | Extra environment variables |
| `mkdir` | `True` | Create the CWD directory if it doesn't exist |

Returns `(router, pty_manager)`.

### Classes

| Class | Description |
|-------|-------------|
| `PTYManager` | Registry of live sessions; create/get/write/resize/shutdown |
| `TerminalSession` | Wraps a PTY process, manages retained output, snapshots, and asyncio events |
| `TerminalWebsocketHandler` | Multiplexed WebSocket handler with snapshot reconnect |

## Concurrency: single worker only

Sessions live entirely in process memory -- `PTYManager` holds a plain dict of
`TerminalSession` objects, and each session owns a forked PTY file descriptor.
There is no external store (Redis, shared memory, etc.) that would allow
sessions to cross process boundaries.

**You must run uvicorn with a single worker** (`--workers 1`). With multiple
workers, a `POST /terminals` hitting worker A and a WebSocket connection
landing on worker B will fail with "session not found."

This is the standard tradeoff for in-process WebSocket state. If you need to
scale horizontally, run multiple single-worker instances behind a load
balancer with **sticky sessions** (route all requests for a given
`session_id` to the same process).

## Development

```sh
git clone https://github.com/MatthewScholefield/termiccio-terminal.git
cd termiccio-terminal
uv sync
uv run pytest
```

## License

MIT
