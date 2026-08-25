from __future__ import annotations

import asyncio
import json
import logging
import socket
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Generic, TypeVar

import aiohttp
from aiohttp import web

from pymear.contracts.events import Event
from pymear.contracts.events_mapper import decapsulate, encapsulate

E = TypeVar("E", bound=Event)
logger = logging.getLogger(__name__)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


class FeatureRuntime(Generic[E]):
    """
    Feature Foundation (WebSocket -> SSE):
    - Common base for all features: connects to hub via WebSocket, filters relevant events,
      buffers them, and restreams as SSE to served HTML/JS pages.
    - Also registers itself with the routing proxy (name + port) so it becomes reachable
      under http://localhost:<proxy_port>/<name>/ without needing a fixed port.

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
        event_types: list[type[E]],
        static_dir: Path,
        port: int | None = None,
        hub_port: int = 8765,
        proxy_port: int = 9000,
    ):
        self.name = name
        self._event_types = tuple(event_types)
        self.static_dir = static_dir
        self.port = port if port is not None else _find_free_port()
        self.hub_url = f"ws://localhost:{hub_port}/ws"
        self.proxy_url = f"ws://localhost:{proxy_port}/register"
        self._sse_queues: set[asyncio.Queue[dict]] = set()
        self._events: list[Callable[[E], Awaitable[None] | None]] = []

    async def run(self) -> None:
        app = self._build_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, port=self.port)
        await site.start()
        logger.info("Feature '%s': UI served on http://localhost:%s", self.name, self.port)

        await asyncio.gather(self._listen_hub(), self._register_with_proxy())


    def add_handler(self, handler: Callable[[E], Awaitable[None] | None]) -> None:
        self._events.append(handler)

    def _build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/events", self._sse_handler)
        app.router.add_get("/", self._index_handler)
        app.router.add_static("/", path=self.static_dir, show_index=False)
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
        return f"data: {json.dumps(payload)}\n\n".encode()

    async def _connect_with_retry(self, session: aiohttp.ClientSession, url: str, label: str):
        delay = 1
        while True:
            try:
                async with session.ws_connect(url) as ws:
                    delay = 1
                    yield ws
            except (aiohttp.ClientError, ConnectionRefusedError):
                logger.warning(
                    "Feature %s: %s injoignable, nouvel essai dans %ss",
                    self.name,
                    label,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def _listen_hub(self) -> None:
        async with aiohttp.ClientSession() as session:
            async for ws in self._connect_with_retry(session, self.hub_url, "hub"):
                try:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        await self._handle_hub_message(msg.data)
                except ConnectionResetError:
                    logger.warning(
                        "Feature %s: connexion hub perdue, reconnexion...", self.name
                    )

    async def _register_with_proxy(self) -> None:
        async with aiohttp.ClientSession() as session:
            async for ws in self._connect_with_retry(session, self.proxy_url, "proxy"):
                try:
                    await ws.send_str(json.dumps({"name": self.name, "port": self.port}))
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.ERROR:
                            break
                except ConnectionResetError:
                    logger.warning(
                        "Feature %s: connexion proxy perdue, reconnexion...", self.name
                    )

    async def _handle_hub_message(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
            event = decapsulate(payload)
        except (ValueError, KeyError):
            return

        if not isinstance(event, self._event_types):
            return

        for event_handler in self._events:
            try:
                result = event_handler(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("Feature %s: error in event handler", self.name)

        encoded = encapsulate(event)
        for queue in self._sse_queues:
            await queue.put(encoded)
