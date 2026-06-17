"""Async bridge to a Node-hosted headless xterm mirror."""

from __future__ import annotations

import asyncio
import json
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from importlib import resources
from itertools import count
from typing import Any

from loguru import logger


@dataclass(frozen=True)
class WorkerSnapshot:
    data: str


class HeadlessXtermWorkerExited(RuntimeError):
    """Raised when the Node worker exits while commands are in flight."""


class HeadlessXtermWorker:
    """Long-lived Node subprocess that owns headless xterm instances."""

    _DIAGNOSTIC_TAIL_LINES = 40
    _RECENT_COMMAND_COUNT = 20
    _DEFAULT_COMMAND_TIMEOUT_SECONDS = 30.0
    _DEFAULT_STREAM_LIMIT_BYTES = 64 * 1024 * 1024

    def __init__(
        self,
        *,
        node_executable: str = 'node',
        command_timeout_seconds: float = _DEFAULT_COMMAND_TIMEOUT_SECONDS,
        stream_limit_bytes: int = _DEFAULT_STREAM_LIMIT_BYTES,
    ):
        self.node_executable = node_executable
        self.command_timeout_seconds = command_timeout_seconds
        self.stream_limit_bytes = stream_limit_bytes
        self._process: asyncio.subprocess.Process | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._stdin_lock = asyncio.Lock()
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._pending_commands: dict[int, dict[str, Any]] = {}
        self._request_ids = count(1)
        self._reader_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None
        self._stderr_tail: deque[str] = deque(maxlen=self._DIAGNOSTIC_TAIL_LINES)
        self._recent_commands: deque[dict[str, Any]] = deque(
            maxlen=self._RECENT_COMMAND_COUNT
        )
        self._shutdown_requested = False
        self._stdout_reader_error: str | None = None

    async def create(
        self, terminal_id: str, *, rows: int, cols: int, scrollback: int
    ) -> None:
        await self._command(
            {
                'type': 'create',
                'terminal_id': terminal_id,
                'rows': rows,
                'cols': cols,
                'scrollback': scrollback,
            }
        )

    async def write(self, terminal_id: str, data: str) -> None:
        await self._command({'type': 'write', 'terminal_id': terminal_id, 'data': data})

    async def resize(self, terminal_id: str, *, rows: int, cols: int) -> None:
        await self._command(
            {
                'type': 'resize',
                'terminal_id': terminal_id,
                'rows': rows,
                'cols': cols,
            }
        )

    async def snapshot(self, terminal_id: str) -> WorkerSnapshot:
        response = await self._command({'type': 'snapshot', 'terminal_id': terminal_id})
        data = response.get('data')
        if not isinstance(data, str):
            raise RuntimeError('Headless xterm worker returned an invalid snapshot')
        return WorkerSnapshot(data=data)

    async def dispose(self, terminal_id: str) -> None:
        await self._command({'type': 'dispose', 'terminal_id': terminal_id})

    async def shutdown(self) -> None:
        self._shutdown_requested = True
        async with self._lifecycle_lock:
            process = self._process
            self._process = None
            if process:
                await self._terminate_process(process)
        for task in (self._reader_task, self._stderr_task):
            if task:
                task.cancel()
        self._reader_task = None
        self._stderr_task = None
        for future in self._pending.values():
            if not future.done():
                future.set_exception(RuntimeError('Headless xterm worker stopped'))
        self._pending.clear()
        self._pending_commands.clear()

    async def _command(self, command: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_started()
        assert self._process and self._process.stdin

        request_id = next(self._request_ids)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[request_id] = future
        self._pending_commands[request_id] = self._describe_command(command)
        self._recent_commands.append(
            {'request_id': request_id, **self._pending_commands[request_id]}
        )
        payload = {**command, 'request_id': request_id}
        line = json.dumps(payload, separators=(',', ':')).encode() + b'\n'

        async with self._stdin_lock:
            try:
                self._process.stdin.write(line)
                await self._process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError) as exc:
                self._pending.pop(request_id, None)
                self._pending_commands.pop(request_id, None)
                if not future.done():
                    future.set_exception(exc)

        try:
            response = await asyncio.wait_for(
                future, timeout=self.command_timeout_seconds
            )
        except asyncio.TimeoutError as exc:
            diagnostic = self._worker_exited_error()
            self._pending.pop(request_id, None)
            self._pending_commands.pop(request_id, None)
            await self._stop_broken_worker()
            raise TimeoutError(
                f'Headless xterm worker command timed out after '
                f'{self.command_timeout_seconds:g}s\n{diagnostic}'
            ) from exc
        if response.get('ok') is not True:
            raise RuntimeError(
                str(response.get('error') or 'Headless xterm worker command failed')
            )
        return response

    def _describe_command(self, command: dict[str, Any]) -> dict[str, Any]:
        """Return non-sensitive command metadata for worker diagnostics."""
        description = {
            'type': command.get('type'),
            'terminal_id': command.get('terminal_id'),
        }
        if command.get('type') == 'write':
            data = command.get('data')
            if isinstance(data, str):
                description['data_chars'] = len(data)
                description['data_bytes'] = len(data.encode())
        for key in ('rows', 'cols', 'scrollback'):
            if key in command:
                description[key] = command[key]
        return {key: value for key, value in description.items() if value is not None}

    async def _ensure_started(self) -> None:
        async with self._lifecycle_lock:
            process = self._process
            reader_running = self._reader_task and not self._reader_task.done()
            if process and process.returncode is None and reader_running:
                return

            if process:
                await self._terminate_process(process)

            worker_path = resources.files(__package__).joinpath('xterm_worker.mjs')
            self._shutdown_requested = False
            self._stderr_tail.clear()
            self._recent_commands.clear()
            self._stdout_reader_error = None
            self._process = await asyncio.create_subprocess_exec(
                self.node_executable,
                str(worker_path),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=self.stream_limit_bytes,
            )
            self._reader_task = asyncio.create_task(self._read_stdout())
            self._stderr_task = asyncio.create_task(self._read_stderr())

    async def _read_stdout(self) -> None:
        assert self._process and self._process.stdout
        try:
            while line := await self._process.stdout.readline():
                try:
                    response = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning(
                        'Ignoring invalid headless xterm worker response: {!r}', line
                    )
                    continue
                request_id = response.get('request_id')
                future = self._pending.pop(request_id, None)
                self._pending_commands.pop(request_id, None)
                if future and not future.done():
                    future.set_result(response)
        except Exception as exc:
            self._stdout_reader_error = repr(exc)
            logger.exception('Headless xterm worker stdout reader failed')
        finally:
            if self._shutdown_requested:
                error = RuntimeError('Headless xterm worker stopped')
            else:
                process = self._process
                if process and process.returncode is None:
                    with suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(process.wait(), timeout=0.2)
                error = self._worker_exited_error()
                logger.error('Headless xterm worker exited unexpectedly\n{}', error)
                await self._stop_broken_worker()
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(error)
            self._pending.clear()
            self._pending_commands.clear()

    async def _read_stderr(self) -> None:
        assert self._process and self._process.stderr
        while line := await self._process.stderr.readline():
            decoded = line.decode(errors='replace').rstrip()
            self._stderr_tail.append(decoded)
            logger.warning('headless xterm worker: {}', decoded)

    def _worker_exited_error(self) -> HeadlessXtermWorkerExited:
        process = self._process
        lines = ['Headless xterm worker exited']
        if process:
            lines.append(f'returncode={process.returncode!r}')
        if self._stdout_reader_error:
            lines.append(f'stdout_reader_error={self._stdout_reader_error}')
        if self._pending_commands:
            lines.append(
                'pending_commands='
                + json.dumps(
                    [
                        {'request_id': request_id, **command}
                        for request_id, command in self._pending_commands.items()
                    ],
                    ensure_ascii=True,
                )
            )
        if self._recent_commands:
            lines.append(
                'recent_commands='
                + json.dumps(list(self._recent_commands), ensure_ascii=True)
            )
        if self._stderr_tail:
            lines.append('stderr_tail:\n' + '\n'.join(self._stderr_tail))
        return HeadlessXtermWorkerExited('\n'.join(lines))

    async def _stop_broken_worker(self) -> None:
        async with self._lifecycle_lock:
            process = self._process
            self._process = None
            if process:
                await self._terminate_process(process)

    async def _terminate_process(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=2)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
