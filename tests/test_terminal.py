"""Integration tests for termiccio-terminal.

These tests spawn real PTY processes, so they require a POSIX system with
/bin/sh available.
"""

import asyncio
import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from termiccio_terminal import (
    PTYManager,
    TerminalSession,
    TerminalWebsocketHandler,
    create_terminal_router,
)
from termiccio_terminal.schemas import CwdResponse


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def manager():
    m = PTYManager()
    yield m
    asyncio.get_event_loop_policy()
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(m.shutdown())
    finally:
        loop.close()


@pytest.fixture
def app_with_router(tmp_path):
    router, mgr = create_terminal_router(
        resolve_cwd=lambda req: tmp_path,
    )
    app = FastAPI()
    app.include_router(router)
    return app, mgr


@pytest.fixture
def app_with_custom_model(tmp_path):
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
    app = FastAPI()
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


def test_websocket_session_not_found(app_with_router):
    app, _ = app_with_router
    with TestClient(app) as client:
        with client.websocket_connect('/terminals/nonexistent/ws?update_id=0') as ws:
            msg = ws.receive_json()
            assert msg['type'] == 'error'
            assert msg['error_type'] == 'session_not_found'


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
