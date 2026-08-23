from __future__ import annotations

import asyncio
import logging
import socket
from collections import deque
from collections.abc import Awaitable, Callable
from pathlib import Path

import aiohttp
from aiohttp import web
from contracts.events import Event
from contracts.events_mapper import decapsulate, encapsulate

logger = logging.getLogger(__name__)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


class FeatureRuntime:
    """
    Feature Foundation (WebSocket → SSE):
    - Common base for all features: connects to hub via WebSocket, filters relevant events, buffers them, and restreams as SSE to served HTML/JS pages.

    Usage:
    runtime = FeatureRuntime(
        name="chat",
        event_types=[ChatMessageEvent],
        static_dir=Path(__file__).parent
    )
    asyncio.run(runtime.run())
    """

    def __init__(
        self,
        name: str,
        event_types: list[type[Event]],
        static_dir: Path,
        port: int | None = None,
        hub_url: str = "ws://localhost:8765/ws",
        buffer_size: int = 20,
        on_event: Callable[[Event], Awaitable[None] | None] | None = None,
    ):
        self.name = name
        self._event_types = tuple(event_types)
        self.static_dir = static_dir
        self.port = port if port is not None else _find_free_port()
        self.hub_url = hub_url
        self._buffer: deque[dict] = deque(maxlen=buffer_size)
        self._sse_queues: set[asyncio.Queue[dict]] = set()
        self._on_event = on_event

    async def run(self) -> None:
        app = self._build_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, port=self.port)
        await site.start()
        logger.info(
            "Feature %s: UI servie sur http://localhost:%s", self.name, self.port
        )

        await self._listen_hub()

    def _build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/events", self._sse_handler)
        app.router.add_static("/", path=self.static_dir, show_index=False)
        app.router.add_get("/", self._index_handler)
        return app

    async def _index_handler(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(self.static_dir / "index.html")

    async def _sse_handler(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            }
        )
        await response.prepare(request)

        queue: asyncio.Queue[dict] = asyncio.Queue()
        for payload in self._buffer:
            await queue.put(payload)
        self._sse_queues.add(queue)

        try:
            while True:
                payload = await queue.get()
                await response.write(self._to_sse_frame(payload))
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            self._sse_queues.discard(queue)

        return response

    @staticmethod
    def _to_sse_frame(payload: dict) -> bytes:
        import json

        return f"data: {json.dumps(payload)}\n\n".encode()

    async def _listen_hub(self) -> None:
        async with aiohttp.ClientSession() as session:
            async for ws in self._connect_with_retry(session):
                try:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        await self._handle_hub_message(msg.data)
                except ConnectionResetError:
                    logger.warning(
                        "Feature %s: connexion hub perdue, reconnexion...", self.name
                    )

    async def _connect_with_retry(self, session: aiohttp.ClientSession):
        delay = 1
        while True:
            try:
                async with session.ws_connect(self.hub_url) as ws:
                    delay = 1
                    yield ws
            except (aiohttp.ClientError, ConnectionRefusedError):
                logger.warning(
                    "Feature %s: hub injoignable, nouvel essai dans %ss",
                    self.name,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def _handle_hub_message(self, raw: str) -> None:
        import json

        try:
            payload = json.loads(raw)
            event = decapsulate(payload)
        except (ValueError, KeyError):
            return

        if not isinstance(event, self._event_types):
            return

        if self._on_event is not None:
            try:
                result = self._on_event(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("Feature %s: erreur dans on_event", self.name)

        encoded = encapsulate(event)
        self._buffer.append(encoded)
        for queue in self._sse_queues:
            await queue.put(encoded)
