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
from pymear.contracts.events_mapper import decapsulate

E = TypeVar("E", bound=Event)
logger = logging.getLogger(__name__)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]

class _Source(Generic[E]):
    def __init__(self, name: str, transform: Callable[[E], dict | None]):
        self.name = name
        self.transform = transform
        self.queues: set[asyncio.Queue[dict]] = set()

class FeatureRuntime(Generic[E]):
    """
    Feature Foundation (WebSocket -> SSE):
    - Common base for all features: connects to hub via WebSocket and filters relevant events.
    - add_handler registers a server-side reaction to an event, with no effect on any SSE stream.
    - add_source registers a dedicated SSE stream at /events/<name>, whose transform both
      filters (return None to skip) and shapes the payload sent to that stream's frontend.
      Each source keeps its own set of connected SSE clients.
    - Also registers itself with the routing proxy (name + port) so it becomes reachable
      under http://localhost:<proxy_port>/<name>/ without needing a fixed port.

    Usage:
    runtime = FeatureRuntime(
        name="chat",
        event_types=[ChatMessageEvent],
        static_dir=Path(__file__).parent
    )
    runtime.add_source("chat", lambda e: {"user": e.user, "text": e.text})
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
        self._sources: dict[str, _Source[E]] = {}

    async def run(self) -> None:
        app = self._build_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, port=self.port)
        await site.start()
        logger.info("Feature '%s': UI served on http://localhost:%s", self.name, self.port)

        await asyncio.gather(self._listen_hub(), self._register_with_proxy())

    async def send(self, source_name: str, payload: dict) -> None:
        source = self._sources.get(source_name)
        if source is None:
            logger.warning("Feature %s: source '%s' unknown", self.name, source_name)
            return
        for queue in source.queues:
            await queue.put(payload)

    def add_source(self, name: str, transform: Callable[[E], dict | None]) -> None:
        """
        Registers a dedicated SSE stream at /events/<name>.
        transform receives the typed event and returns the payload to send to the
        frontend, or None to filter this event out of this specific stream.
        """
        self._sources[name] = _Source(name, transform)

    def _build_app(self) -> web.Application:
        app = web.Application()
        for source_name in self._sources:
            app.router.add_get(f"/events/{source_name}", self._make_sse_handler(source_name))
        app.router.add_get("/", self._index_handler)
        app.router.add_static("/", path=self.static_dir, show_index=False)
        return app

    async def _index_handler(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(self.static_dir / "index.html")

    def _make_sse_handler(
        self, source_name: str
    ) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
        async def handler(request: web.Request) -> web.StreamResponse:
            source = self._sources[source_name]
            response = web.StreamResponse(
                headers={
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                }
            )
            await response.prepare(request)

            queue: asyncio.Queue[dict] = asyncio.Queue()
            source.queues.add(queue)

            try:
                while True:
                    payload = await queue.get()
                    await response.write(self._to_sse_frame(payload))
            except (ConnectionResetError, asyncio.CancelledError):
                pass
            finally:
                source.queues.discard(queue)

            return response

        return handler

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
                    "Feature %s: %s unreachable, new try in %ss",
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
                        "Feature %s: lost connection to hub, reconnecting...", self.name
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
                        "Feature %s: lost connection to proxy, reconnecting...", self.name
                    )

    async def _handle_hub_message(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
            event = decapsulate(payload)
        except (ValueError, KeyError):
            return

        if not isinstance(event, self._event_types):
            return

        for source in self._sources.values():
            try:
                filtered = source.transform(event)
            except Exception:
                logger.exception(
                    "Feature %s: error in source '%s' transform", self.name, source.name
                )
                continue

            if filtered is None:
                continue

            for queue in source.queues:
                await queue.put(filtered)
