import asyncio
from contextlib import suppress

from fastapi import WebSocket, WebSocketDisconnect
from loguru import logger
from pydantic import TypeAdapter

from .manager import PTYManager
from .schemas import (
    CommandFinishTerminalMessage,
    ErrorTerminalMessage,
    IncomingTerminalMessage,
    OutputTerminalMessage,
    ServerSentTerminalMessage,
    SizeTerminalMessage,
)
from .session import TerminalSession


class TerminalWebsocketHandler:
    """WebSocket endpoint handler for a single terminal session.

    Runs four concurrent tasks per connection:

    * **publish_output**  -- replays buffered output from *update_id*, then
      streams new chunks.
    * **publish_command_finish** -- replays missed command results, then
      streams new ones.
    * **forward_input** -- receives ``stdin``/``resize``/``get_size`` messages
      and dispatches them to the :class:`PTYManager`.
    * **wait_for_session_complete** -- closes the connection when the session
      dies.

    The handler is intentionally agnostic about *how* sessions are created --
    it only needs the ``session_id``, an ``update_id`` for buffer replay, and a
    :class:`PTYManager` to look up and drive the session.
    """

    def __init__(
        self,
        websocket: WebSocket,
        session_id: str,
        update_id: int,
        *,
        pty_manager: PTYManager,
    ):
        self.websocket = websocket
        self.session_id = session_id
        self.initial_update_id = update_id
        self.pty_manager = pty_manager

    async def handle(self):
        """Accept the WebSocket and run the streaming loop until completion."""
        await self.websocket.accept()
        session = await self.retrieve_session()
        if not session:
            return
        tasks = [
            asyncio.create_task(self.publish_output_task(session)),
            asyncio.create_task(self.publish_command_finish_task(session)),
            asyncio.create_task(self.forward_terminal_input_task(session)),
            asyncio.create_task(self.wait_for_session_complete(session)),
        ]
        session.active_connections += 1
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            session.active_connections -= 1
            for task in tasks:
                if not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            with suppress(WebSocketDisconnect, RuntimeError):
                await self.websocket.close()

    # ------------------------------------------------------------------
    # Message helpers
    # ------------------------------------------------------------------

    async def receive_message(self) -> IncomingTerminalMessage:
        return TypeAdapter(IncomingTerminalMessage).validate_json(
            await self.websocket.receive_text()
        )

    async def send_message(self, message: ServerSentTerminalMessage):
        await self.websocket.send_json(message.model_dump())

    async def handle_message(self, message: IncomingTerminalMessage):
        if message.type == 'stdin':
            await self.pty_manager.write_input(self.session_id, message.data)
        elif message.type == 'resize':
            await self.pty_manager.resize_terminal(
                self.session_id, message.rows, message.cols
            )
        elif message.type == 'get_size':
            size = self.pty_manager.get_terminal_size(self.session_id)
            await self.send_message(SizeTerminalMessage(rows=size[0], cols=size[1]))
        else:
            raise RuntimeError('Unhandled message type: {}'.format(message.type))

    async def retrieve_session(self) -> TerminalSession | None:
        if not self.pty_manager.is_session(self.session_id):
            await self.send_message(
                ErrorTerminalMessage(
                    error_type='session_not_found',
                    message='Session does not exist',
                )
            )
            await self.websocket.close()
            return None
        return self.pty_manager.get_session(self.session_id)

    # ------------------------------------------------------------------
    # Concurrent tasks
    # ------------------------------------------------------------------

    async def publish_output_task(self, session: TerminalSession):
        update_id = self.initial_update_id
        try:
            while True:
                new_chunks = session.output_buffer[update_id:]
                if new_chunks:
                    update_id += len(new_chunks)
                    await self.send_message(
                        OutputTerminalMessage(
                            data=''.join(new_chunks), update_id=update_id
                        )
                    )
                await session.new_content_event.wait()
                session.new_content_event.clear()
        except WebSocketDisconnect:
            pass

    async def publish_command_finish_task(self, session: TerminalSession):
        command_id = 0
        while (
            command_id < len(session.command_results)
            and session.command_index_to_update_id[command_id]
            <= self.initial_update_id
        ):
            command_id += 1
        try:
            while True:
                for new_command_id in range(command_id, len(session.command_results)):
                    await self.send_message(
                        CommandFinishTerminalMessage(
                            command_index=new_command_id,
                            return_code=session.command_results[new_command_id],
                        )
                    )
                    command_id = new_command_id + 1
                await session.new_command_result_event.wait()
                session.new_command_result_event.clear()
        except WebSocketDisconnect:
            pass

    async def forward_terminal_input_task(self, session: TerminalSession):
        try:
            while True:
                message = await self.receive_message()
                await self.handle_message(message)
        except WebSocketDisconnect:
            logger.info('Client disconnected ({})', self.session_id)

    async def wait_for_session_complete(self, session: TerminalSession):
        await session.session_dead_event.wait()
