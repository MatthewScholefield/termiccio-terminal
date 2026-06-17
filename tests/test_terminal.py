"""Integration tests for termiccio-terminal.

These tests spawn real PTY processes, so they require a POSIX system with
/bin/sh available.
"""

import asyncio
import json
from pathlib import Path

import pytest
from fastapi import FastAPI, WebSocketDisconnect
from fastapi.testclient import TestClient

from termiccio_terminal import (
    PTYManager,
    TerminalSession,
    TerminalWebsocketHandler,
    create_terminal_router,
)
from termiccio_terminal.compactor import (
    DEFAULT_SNAPSHOT_BYTE_THRESHOLD,
    TerminalStateCompactor,
)
from termiccio_terminal.schemas import CwdResponse
from termiccio_terminal.xterm_worker import HeadlessXtermWorker, WorkerSnapshot


class FakeStateWorker:
    def __init__(self):
        self.buffers = {}
        self.created = []
        self.resized = []
        self.disposed = []
        self.snapshot_count = 0
        self.shutdown_count = 0

    async def create(self, terminal_id, *, rows, cols, scrollback):
        self.created.append((terminal_id, rows, cols, scrollback))
        self.buffers[terminal_id] = ''

    async def write(self, terminal_id, data):
        self.buffers[terminal_id] += data

    async def resize(self, terminal_id, *, rows, cols):
        self.resized.append((terminal_id, rows, cols))

    async def snapshot(self, terminal_id):
        self.snapshot_count += 1
        return WorkerSnapshot(data=self.buffers[terminal_id])

    async def dispose(self, terminal_id):
        self.disposed.append(terminal_id)

    async def shutdown(self):
        self.shutdown_count += 1


class SnapshotFailingWorker(FakeStateWorker):
    async def snapshot(self, terminal_id):
        self.snapshot_count += 1
        raise RuntimeError('snapshot exploded')


class SecondSnapshotFailingWorker(FakeStateWorker):
    async def snapshot(self, terminal_id):
        self.snapshot_count += 1
        if self.snapshot_count > 1:
            raise RuntimeError('second snapshot exploded')
        return WorkerSnapshot(data=self.buffers[terminal_id])


class DummyPtyProcess:
    id = 'dummy-terminal'

    def __init__(self):
        self.terminated = False

    async def terminate(self):
        self.terminated = True


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def manager():
    m = PTYManager()
    yield m
    await m.shutdown()


@pytest.fixture
def app_with_router(tmp_path):
    from contextlib import asynccontextmanager

    router, mgr = create_terminal_router(
        resolve_cwd=lambda req: tmp_path,
    )

    @asynccontextmanager
    async def lifespan(app):
        yield
        await mgr.shutdown()

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    return app, mgr


@pytest.fixture
def app_with_custom_model(tmp_path):
    from contextlib import asynccontextmanager

    from pydantic import BaseModel

    class MyRequest(BaseModel):
        project_name: str = 'default'
        rows: int = 24
        cols: int = 80

    def resolve_cwd(req: MyRequest):
        return tmp_path / req.project_name

    router, mgr = create_terminal_router(
        request_model=MyRequest,
        resolve_cwd=resolve_cwd,
    )

    @asynccontextmanager
    async def lifespan(app):
        yield
        await mgr.shutdown()

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    return app, mgr


# ---------------------------------------------------------------------------
# PTYManager / TerminalSession unit-ish tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_and_write(manager):
    session_id = await manager.create_session(24, 80, shell='/bin/sh')
    assert session_id
    assert manager.is_session(session_id)
    session = manager.get_session(session_id)
    assert isinstance(session, TerminalSession)

    await manager.write_input(session_id, 'echo hello_world\n')
    await asyncio.sleep(0.5)

    output = ''.join(session.output_buffer)
    assert 'hello_world' in output


@pytest.mark.asyncio
async def test_compactor_snapshots_after_50kb_and_waits_one_second(monkeypatch):
    worker = FakeStateWorker()
    now = 100.0
    monkeypatch.setattr(
        'termiccio_terminal.compactor.time.monotonic',
        lambda: now,
    )
    compactor = await TerminalStateCompactor.create(
        'snap-threshold',
        rows=24,
        cols=80,
        worker=worker,
    )

    almost_threshold = 'a' * (DEFAULT_SNAPSHOT_BYTE_THRESHOLD - 1)
    assert await compactor.write(almost_threshold, 1) is None
    assert worker.snapshot_count == 0

    first_snapshot = await compactor.write('b', 2)
    assert first_snapshot is not None
    assert first_snapshot.update_id == 2
    assert worker.snapshot_count == 1

    assert await compactor.write('c' * DEFAULT_SNAPSHOT_BYTE_THRESHOLD, 3) is None
    assert worker.snapshot_count == 1

    now = 101.0
    second_snapshot = await compactor.write('d', 4)
    assert second_snapshot is not None
    assert second_snapshot.update_id == 4
    assert worker.snapshot_count == 2


@pytest.mark.asyncio
async def test_headless_worker_can_serialize_snapshot():
    worker = HeadlessXtermWorker()
    try:
        await worker.create('real-worker-snapshot', rows=24, cols=80, scrollback=1000)
        await worker.write('real-worker-snapshot', 'hello from xterm\r\n')
        snapshot = await worker.snapshot('real-worker-snapshot')
    finally:
        await worker.shutdown()

    assert 'hello from xterm' in snapshot.data


@pytest.mark.asyncio
async def test_headless_worker_can_read_large_snapshot_response():
    worker = HeadlessXtermWorker()
    try:
        await worker.create('large-worker-snapshot', rows=24, cols=100, scrollback=2000)
        output = ''.join(
            f'large snapshot line {index:04d} {"x" * 80}\r\n' for index in range(1200)
        )
        await worker.write('large-worker-snapshot', output)
        snapshot = await worker.snapshot('large-worker-snapshot')
    finally:
        await worker.shutdown()

    assert 'large snapshot line 1199' in snapshot.data
    assert len(snapshot.data.encode()) > 64 * 1024


@pytest.mark.asyncio
async def test_reconnect_before_compaction_receives_retained_output_only():
    worker = FakeStateWorker()
    compactor = await TerminalStateCompactor.create(
        'before-compact',
        rows=24,
        cols=80,
        worker=worker,
        byte_threshold=999,
    )
    session = TerminalSession(pty_process=DummyPtyProcess(), compactor=compactor)

    await session.append_output('one')
    await session.append_output('two')

    snapshot, chunks = await session.reconnect_payload(1)
    assert snapshot is None
    assert [chunk.data for chunk in chunks] == ['two']


@pytest.mark.asyncio
async def test_reconnect_after_compaction_receives_snapshot_plus_new_tail():
    worker = FakeStateWorker()
    compactor = await TerminalStateCompactor.create(
        'after-compact',
        rows=24,
        cols=80,
        worker=worker,
        byte_threshold=1,
        interval_seconds=0,
    )
    session = TerminalSession(pty_process=DummyPtyProcess(), compactor=compactor)

    await session.append_output('snapshot-base')
    compactor.byte_threshold = 999
    await session.append_output('tail')

    snapshot, chunks = await session.reconnect_payload(0)
    assert snapshot is not None
    assert snapshot.data == 'snapshot-base'
    assert snapshot.update_id == 1
    assert [chunk.data for chunk in chunks] == ['tail']
    assert session.output_buffer == ['tail']


@pytest.mark.asyncio
async def test_compactor_failure_keeps_session_live_and_retains_output():
    worker = SnapshotFailingWorker()
    compactor = await TerminalStateCompactor.create(
        'failing-compact',
        rows=24,
        cols=80,
        worker=worker,
        byte_threshold=1,
        interval_seconds=0,
    )
    session = TerminalSession(pty_process=DummyPtyProcess(), compactor=compactor)

    await session.append_output('still-live')
    await session.append_output('still-retained')

    snapshot, chunks = await session.reconnect_payload(0)
    assert snapshot is None
    assert [chunk.data for chunk in chunks] == ['still-live', 'still-retained']
    assert session.session_dead_event.is_set() is False
    assert session.output_buffer == ['still-live', 'still-retained']
    assert worker.snapshot_count == 1


@pytest.mark.asyncio
async def test_compactor_failure_after_snapshot_keeps_snapshot_reconnect():
    worker = SecondSnapshotFailingWorker()
    compactor = await TerminalStateCompactor.create(
        'failing-after-snapshot',
        rows=24,
        cols=80,
        worker=worker,
        byte_threshold=1,
        interval_seconds=0,
    )
    session = TerminalSession(pty_process=DummyPtyProcess(), compactor=compactor)

    await session.append_output('snapshot-base')
    await session.append_output('tail')

    snapshot, chunks = await session.reconnect_payload(0)
    assert snapshot is not None
    assert snapshot.data == 'snapshot-base'
    assert [chunk.data for chunk in chunks] == ['tail']
    assert session.session_dead_event.is_set() is False
    assert session.output_buffer == ['tail']
    assert worker.snapshot_count == 2


@pytest.mark.asyncio
async def test_reconnect_falls_back_to_compacted_replay_if_snapshot_missing():
    worker = FakeStateWorker()
    compactor = await TerminalStateCompactor.create(
        'missing-snapshot-fallback',
        rows=24,
        cols=80,
        worker=worker,
        byte_threshold=1,
        interval_seconds=0,
    )
    session = TerminalSession(pty_process=DummyPtyProcess(), compactor=compactor)

    await session.append_output('snapshot-base')
    compactor.snapshot = None
    compactor.byte_threshold = 999
    await session.append_output('tail')

    snapshot, chunks = await session.reconnect_payload(0)
    assert snapshot is None
    assert [chunk.data for chunk in chunks] == ['snapshot-base', 'tail']
    assert [chunk.update_id for chunk in chunks] == [1, 2]


@pytest.mark.asyncio
async def test_resize_updates_snapshot_metadata_and_mirror_dimensions():
    worker = FakeStateWorker()
    compactor = await TerminalStateCompactor.create(
        'resize-snapshot',
        rows=24,
        cols=80,
        worker=worker,
        byte_threshold=1,
        interval_seconds=0,
    )
    session = TerminalSession(pty_process=DummyPtyProcess(), compactor=compactor)
    await session.append_output('x')

    await session.resize(30, 100)

    assert worker.resized == [('resize-snapshot', 30, 100)]
    assert compactor.snapshot is not None
    assert compactor.snapshot.rows == 30
    assert compactor.snapshot.cols == 100


@pytest.mark.asyncio
async def test_session_shutdown_disposes_worker_terminal():
    worker = FakeStateWorker()
    compactor = await TerminalStateCompactor.create(
        'dispose-me',
        rows=24,
        cols=80,
        worker=worker,
    )
    pty_process = DummyPtyProcess()
    session = TerminalSession(
        pty_process=pty_process,
        compactor=compactor,
        state_worker=worker,
    )

    await session.shutdown()

    assert pty_process.terminated is True
    assert worker.disposed == ['dispose-me']


@pytest.mark.asyncio
async def test_cwd_tracking(manager, tmp_path):
    session_id = await manager.create_session(24, 80, cwd=tmp_path, shell='/bin/sh')
    session = manager.get_session(session_id)
    await asyncio.sleep(0.3)

    cwd = session.current_cwd
    assert cwd is not None
    assert cwd == tmp_path

    # Change directory and verify live tracking
    await manager.write_input(session_id, 'cd /tmp\n')
    await asyncio.sleep(0.3)
    assert session.current_cwd == Path('/tmp')


@pytest.mark.asyncio
async def test_get_cwd_via_manager(manager, tmp_path):
    session_id = await manager.create_session(24, 80, cwd=tmp_path, shell='/bin/sh')
    await asyncio.sleep(0.3)
    cwd = manager.get_cwd(session_id)
    assert cwd == tmp_path


@pytest.mark.asyncio
async def test_resize(manager):
    session_id = await manager.create_session(24, 80, shell='/bin/sh')
    await manager.resize_terminal(session_id, 30, 100)
    rows, cols = manager.get_terminal_size(session_id)
    assert rows == 30
    assert cols == 100


@pytest.mark.asyncio
async def test_command_results(manager):
    session_id = await manager.create_session(24, 80, shell='/bin/sh')
    await manager.write_input(session_id, 'false\n')
    await asyncio.sleep(0.5)
    session = manager.get_session(session_id)
    assert len(session.command_results) >= 1
    # The last command result is from `false` (exit code 1).
    # Earlier results come from the spawn-time prompt setup command.
    assert session.command_results[-1] == 1


@pytest.mark.asyncio
async def test_exec_replacing_shell_completes_session(manager):
    """Agent-style launches use `exec`, so command exit is terminal exit."""
    session_id = await manager.create_session(24, 80, shell='/bin/sh')
    session = manager.get_session(session_id)

    await manager.write_input(session_id, 'exec true\r')

    await asyncio.wait_for(session.session_dead_event.wait(), timeout=2)
    await asyncio.wait_for(session.monitor_task, timeout=2)
    assert not manager.is_session(session_id)


@pytest.mark.asyncio
async def test_env_injection(manager):
    session_id = await manager.create_session(
        24, 80, shell='/bin/sh', env={'MY_TEST_VAR': 'abc123'}
    )
    await manager.write_input(session_id, 'echo $MY_TEST_VAR\n')
    await asyncio.sleep(0.5)
    session = manager.get_session(session_id)
    output = ''.join(session.output_buffer)
    assert 'abc123' in output


@pytest.mark.asyncio
async def test_session_not_found(manager):
    assert not manager.is_session('nonexistent-id')


# ---------------------------------------------------------------------------
# REST endpoint tests (via TestClient)
# ---------------------------------------------------------------------------


def test_create_terminal_endpoint(app_with_router):
    app, mgr = app_with_router
    with TestClient(app) as client:
        resp = client.post('/terminals', json={})
        assert resp.status_code == 200
        data = resp.json()
        assert 'session_id' in data
        assert mgr.is_session(data['session_id'])


def test_create_terminal_with_cwd(app_with_router, tmp_path):
    app, mgr = app_with_router
    with TestClient(app) as client:
        resp = client.post('/terminals', json={'cwd': str(tmp_path)})
        assert resp.status_code == 200
        session_id = resp.json()['session_id']

        # Give the shell time to start
        import time

        time.sleep(0.5)

        cwd_resp = client.get(f'/terminals/{session_id}/cwd')
        assert cwd_resp.status_code == 200
        parsed = CwdResponse(**cwd_resp.json())
        assert parsed.cwd is not None


def test_create_terminal_custom_model(app_with_custom_model, tmp_path):
    app, mgr = app_with_custom_model
    with TestClient(app) as client:
        resp = client.post('/terminals', json={'project_name': 'myproject'})
        assert resp.status_code == 200
        session_id = resp.json()['session_id']
        assert mgr.is_session(session_id)
        assert (tmp_path / 'myproject').is_dir()


def test_cwd_endpoint_missing_session(app_with_router):
    app, _ = app_with_router
    with TestClient(app) as client:
        resp = client.get('/terminals/nope/cwd')
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# WebSocket tests (via TestClient)
# ---------------------------------------------------------------------------


def test_websocket_echo(app_with_router):
    """Send stdin, receive output back over WebSocket."""
    app, _ = app_with_router
    with TestClient(app) as client:
        # Create a session first
        resp = client.post('/terminals', json={})
        session_id = resp.json()['session_id']

        with client.websocket_connect(f'/terminals/{session_id}/ws?update_id=0') as ws:
            ws.send_text(json.dumps({'type': 'stdin', 'data': 'echo ws_test_123\n'}))

            # Collect messages until we see our echo
            found = False
            for _ in range(20):
                msg = ws.receive_json()
                if msg['type'] == 'output' and 'ws_test_123' in msg.get('data', ''):
                    found = True
                    break
            assert found, 'Did not receive echoed output'


def test_websocket_get_size(app_with_router):
    app, _ = app_with_router
    with TestClient(app) as client:
        resp = client.post('/terminals', json={})
        session_id = resp.json()['session_id']

        with client.websocket_connect(f'/terminals/{session_id}/ws?update_id=0') as ws:
            ws.send_text(json.dumps({'type': 'get_size'}))
            found = False
            for _ in range(20):
                msg = ws.receive_json()
                if msg['type'] == 'size':
                    found = True
                    assert msg['rows'] == 24
                    assert msg['cols'] == 80
                    break
            assert found, 'Did not receive size message'


def test_websocket_command_finish(app_with_router):
    app, _ = app_with_router
    with TestClient(app) as client:
        resp = client.post('/terminals', json={})
        session_id = resp.json()['session_id']

        with client.websocket_connect(f'/terminals/{session_id}/ws?update_id=0') as ws:
            ws.send_text(json.dumps({'type': 'stdin', 'data': 'true\n'}))
            found = False
            for _ in range(30):
                msg = ws.receive_json()
                if msg['type'] == 'command_finish':
                    found = True
                    assert msg['return_code'] == 0
                    break
            assert found, 'Did not receive command_finish message'


def test_websocket_session_exit(app_with_router):
    """An ``exec``-launched process dying emits a ``session_exit`` message."""
    app, _ = app_with_router
    with TestClient(app) as client:
        resp = client.post('/terminals', json={})
        session_id = resp.json()['session_id']

        with client.websocket_connect(f'/terminals/{session_id}/ws?update_id=0') as ws:
            ws.send_text(json.dumps({'type': 'stdin', 'data': 'exec true\r'}))
            found = False
            for _ in range(30):
                msg = ws.receive_json()
                if msg['type'] == 'session_exit':
                    found = True
                    assert 'return_code' in msg
                    break
            assert found, 'Did not receive session_exit message'


def test_websocket_session_not_found(app_with_router):
    app, _ = app_with_router
    with TestClient(app) as client:
        with client.websocket_connect('/terminals/nonexistent/ws?update_id=0') as ws:
            msg = ws.receive_json()
            assert msg['type'] == 'error'
            assert msg['error_type'] == 'session_not_found'


@pytest.mark.asyncio
async def test_websocket_handler_drains_expected_disconnect_task_exceptions():
    class FakeWebSocket:
        closed = False

        async def accept(self):
            pass

        async def close(self):
            self.closed = True

    class FakeManager:
        def __init__(self, session):
            self.session = session

        def is_session(self, session_id):
            return True

        def get_session(self, session_id):
            return self.session

    class FakeHandler(TerminalWebsocketHandler):
        async def publish_output_task(self, session):
            raise WebSocketDisconnect(code=1006)

        async def publish_command_finish_task(self, session):
            await asyncio.Event().wait()

        async def forward_terminal_input_task(self, session):
            return None

        async def wait_for_session_complete(self, session):
            await asyncio.Event().wait()

    session = TerminalSession(pty_process=DummyPtyProcess())
    websocket = FakeWebSocket()
    handler = FakeHandler(
        websocket,
        session.id,
        0,
        pty_manager=FakeManager(session),
    )

    await handler.handle()

    assert websocket.closed
    assert session.active_connections == 0


@pytest.mark.asyncio
async def test_websocket_handler_reraises_unexpected_task_exceptions():
    class FakeWebSocket:
        async def accept(self):
            pass

        async def close(self):
            pass

    class FakeManager:
        def __init__(self, session):
            self.session = session

        def is_session(self, session_id):
            return True

        def get_session(self, session_id):
            return self.session

    class FakeHandler(TerminalWebsocketHandler):
        async def publish_output_task(self, session):
            raise RuntimeError('boom')

        async def publish_command_finish_task(self, session):
            await asyncio.Event().wait()

        async def forward_terminal_input_task(self, session):
            return None

        async def wait_for_session_complete(self, session):
            await asyncio.Event().wait()

    session = TerminalSession(pty_process=DummyPtyProcess())
    handler = FakeHandler(
        FakeWebSocket(),
        session.id,
        0,
        pty_manager=FakeManager(session),
    )

    with pytest.raises(RuntimeError, match='boom'):
        await handler.handle()

    assert session.active_connections == 0


def test_websocket_buffer_replay(app_with_router):
    """Output produced before connecting should be replayed via update_id=0."""
    app, _ = app_with_router
    with TestClient(app) as client:
        resp = client.post('/terminals', json={})
        session_id = resp.json()['session_id']

        # Give the shell time to produce initial output
        import time

        time.sleep(0.5)

        # Connect with update_id=0 -- should receive all buffered output
        with client.websocket_connect(f'/terminals/{session_id}/ws?update_id=0') as ws:
            found_output = False
            for _ in range(10):
                msg = ws.receive_json()
                if msg['type'] == 'output':
                    found_output = True
                    break
            assert found_output, 'Did not receive replayed buffer output'


# ---------------------------------------------------------------------------
# Building-blocks test (handler with manual manager)
# ---------------------------------------------------------------------------


def test_building_blocks_manual_manager():
    """Apps can use PTYManager + TerminalWebsocketHandler directly."""
    from contextlib import asynccontextmanager
    from fastapi import WebSocket

    mgr = PTYManager()

    @asynccontextmanager
    async def lifespan(app):
        yield
        await mgr.shutdown()

    router_app = FastAPI(lifespan=lifespan)

    @router_app.post('/my-terminal')
    async def create():
        sid = await mgr.create_session(24, 80, shell='/bin/sh')
        return {'session_id': sid}

    @router_app.websocket('/my-terminal/{sid}/ws')
    async def ws_endpoint(websocket: WebSocket, sid: str, update_id: int = 0):
        await TerminalWebsocketHandler(
            websocket, sid, update_id, pty_manager=mgr
        ).handle()

    with TestClient(router_app) as client:
        resp = client.post('/my-terminal')
        assert resp.status_code == 200
        sid = resp.json()['session_id']

        with client.websocket_connect(f'/my-terminal/{sid}/ws') as ws:
            ws.send_text(
                json.dumps({'type': 'stdin', 'data': 'echo building_blocks\n'})
            )
            found = False
            for _ in range(20):
                msg = ws.receive_json()
                if msg['type'] == 'output' and 'building_blocks' in msg.get('data', ''):
                    found = True
                    break
            assert found
