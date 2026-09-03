from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable

import aiohttp
from aiohttp import web

from pymear.utils.logger import VerboseLogger


class WebsocketPipe:
    """
    Manages a single bidirectional WebSocket endpoint for one feature source.

    Knows nothing about hub events, custom events, or transforms: it only
    handles client connections, outbound broadcast, and inbound message
    routing for the source it belongs to.
    """

    def __init__(
        self,
        name: str,
        on_message: Callable[[dict], Awaitable[None]] | None,
        heartbeat: float,
        queue_maxsize: int,
        client_send_timeout: float,
        logger: VerboseLogger,
    ) -> None:
        self.name = name
        self.on_message = on_message
        self.heartbeat = heartbeat
        self.queue_maxsize = queue_maxsize
        self.client_send_timeout = client_send_timeout
        self.logger = logger
        self._queues: set[asyncio.Queue[dict]] = set()

    def push(self, payload: dict) -> None:
        """Broadcast a payload to all connected clients, evicting stale queues."""
        for queue in list(self._queues):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                self.logger.warning(
                    "Client queue full on source '%s'; dropped oldest payload",
                    self.name,
                )
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                self.logger.warning(
                    "Client queue still full on source '%s'; dropped newest payload",
                    self.name,
                )

    async def handle(self, request: web.Request) -> web.WebSocketResponse:
        """Accept a client connection and run its send/receive lifecycle."""
        ws = web.WebSocketResponse(heartbeat=self.heartbeat)
        await ws.prepare(request)

        queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=self.queue_maxsize)
        self._queues.add(queue)
        self.logger.info_v(
            "Client connected to source '%s' (queues: %d)", self.name, len(self._queues)
        )

        sender_task = asyncio.create_task(self._sender(ws, queue))
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if self.on_message is not None:
                        try:
                            payload = json.loads(msg.data)
                        except json.JSONDecodeError:
                            self.logger.warning(
                                "Invalid JSON on source '%s'", self.name
                            )
                            continue
                        try:
                            await self.on_message(payload)
                        except Exception:  # noqa: BLE001
                            self.logger.error_v(
                                "Error in on_message for source '%s'", self.name
                            )
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.info_v("WebSocket error on source '%s'", self.name)
                    break
        finally:
            sender_task.cancel()
            await asyncio.gather(sender_task, return_exceptions=True)
            self._queues.discard(queue)
            self.logger.info_v(
                "Client disconnected from source '%s' (queues: %d)",
                self.name,
                len(self._queues),
            )

        return ws

    async def _sender(
        self, ws: web.WebSocketResponse, queue: asyncio.Queue[dict]
    ) -> None:
        while True:
            payload = await queue.get()
            try:
                await asyncio.wait_for(
                    ws.send_str(json.dumps(payload)), timeout=self.client_send_timeout
                )
            except (ConnectionResetError, TimeoutError):
                self.logger.warning(
                    "Client send failed on source '%s'; closing websocket", self.name
                )
                await ws.close()
                break
