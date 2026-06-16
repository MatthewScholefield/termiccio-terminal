"""Async bridge to a Node-hosted headless xterm mirror."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from importlib import resources
from itertools import count
from typing import Any

from loguru import logger


@dataclass(frozen=True)
class WorkerSnapshot:
    data: str


class HeadlessXtermWorker:
    """Long-lived Node subprocess that owns headless xterm instances."""

    def __init__(self, *, node_executable: str = 'node'):
        self.node_executable = node_executable
        self._process: asyncio.subprocess.Process | None = None
        self._stdin_lock = asyncio.Lock()
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._request_ids = count(1)
        self._reader_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None

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
        process = self._process
        self._process = None
        if process and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=2)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        for task in (self._reader_task, self._stderr_task):
            if task:
                task.cancel()
        self._reader_task = None
        self._stderr_task = None
        for future in self._pending.values():
            if not future.done():
                future.set_exception(RuntimeError('Headless xterm worker stopped'))
        self._pending.clear()

    async def _command(self, command: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_started()
        assert self._process and self._process.stdin

        request_id = next(self._request_ids)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[request_id] = future
        payload = {**command, 'request_id': request_id}
        line = json.dumps(payload, separators=(',', ':')).encode() + b'\n'

        async with self._stdin_lock:
            try:
                self._process.stdin.write(line)
                await self._process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError) as exc:
                self._pending.pop(request_id, None)
                if not future.done():
                    future.set_exception(exc)

        response = await future
        if response.get('ok') is not True:
            raise RuntimeError(
                str(response.get('error') or 'Headless xterm worker command failed')
            )
        return response

    async def _ensure_started(self) -> None:
        if self._process and self._process.returncode is None:
            return

        worker_path = resources.files(__package__).joinpath('xterm_worker.mjs')
        self._process = await asyncio.create_subprocess_exec(
            self.node_executable,
            str(worker_path),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
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
                if future and not future.done():
                    future.set_result(response)
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(RuntimeError('Headless xterm worker exited'))
            self._pending.clear()

    async def _read_stderr(self) -> None:
        assert self._process and self._process.stderr
        while line := await self._process.stderr.readline():
            logger.warning(
                'headless xterm worker: {}', line.decode(errors='replace').rstrip()
            )
