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
    def __init__(
        self,
        name: str,
        transform: Callable[[E], dict | None] | None,
        on_message: Callable[[dict], Awaitable[None]] | None,
    ):
        self.name = name
        self.transform = transform
        self.on_message = on_message
        self.queues: set[asyncio.Queue[dict]] = set()


class Feature(Generic[E]):
    """
    Feature Foundation (WebSocket hub -> WebSocket clients):
    - Common base for all features: connects to hub via WebSocket and filters relevant events.
    - add_source registers a dedicated WebSocket endpoint at /ws/<name>, with two optional callbacks:
        - transform: receives a typed hub event and returns the payload to push to connected
          clients, or None to filter this event out of this specific stream.
        - on_message: receives a parsed dict when a frontend client sends a message on this channel.
      A source with only on_message (no transform) acts as a receive-only channel.
      A source with only transform (no on_message) acts as a push-only channel.
    - Also registers itself with the routing proxy (name + port) so it becomes reachable
      under http://localhost:<proxy_port>/<name>/ without needing a fixed port.

    Usage:
    feature = Feature(
        name="chat",
        event_types=[ChatMessageEvent],
        static_dir=Path(__file__).parent
    )
    feature.add_source(
        "chat",
        transform=lambda e: {"user": e.user, "text": e.text},
        on_message=lambda payload: handle_incoming(payload),
    )
    asyncio.run(feature.run())
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
        self.hub_url = f"ws://localhost:{hub_port}/internal/ws"
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

    def add_source(
        self,
        name: str,
        transform: Callable[[E], dict | None] | None = None,
        on_message: Callable[[dict], Awaitable[None]] | None = None,
    ) -> None:
        """
        Registers a dedicated WebSocket endpoint at /ws/<name>.
        transform receives a typed hub event and returns the payload to push to connected
        clients, or None to filter this event out of this specific stream. If None, no data
        is pushed from the hub to clients on this source.
        on_message is called with the parsed payload when a client sends a message on this
        channel. If None, incoming client messages are silently ignored.
        """
        self._sources[name] = _Source(name, transform, on_message)

    def _build_app(self) -> web.Application:
        app = web.Application()
        for source_name in self._sources:
            app.router.add_get(f"/ws/{source_name}", self._make_ws_handler(source_name))
        app.router.add_get("/", self._index_handler)
        app.router.add_static("/", path=self.static_dir, show_index=False)
        return app

    async def _index_handler(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(self.static_dir / "index.html")

    def _make_ws_handler(
        self, source_name: str
    ) -> Callable[[web.Request], Awaitable[web.WebSocketResponse]]:
        async def handler(request: web.Request) -> web.WebSocketResponse:
            source = self._sources[source_name]
            ws = web.WebSocketResponse()
            await ws.prepare(request)

            queue: asyncio.Queue[dict] = asyncio.Queue()
            source.queues.add(queue)

            async def sender() -> None:
                while True:
                    payload = await queue.get()
                    try:
                        await ws.send_str(json.dumps(payload))
                    except ConnectionResetError:
                        break

            sender_task = asyncio.create_task(sender())
            try:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if source.on_message is not None:
                            try:
                                payload = json.loads(msg.data)
                            except json.JSONDecodeError:
                                logger.warning(
                                    "Feature %s: invalid JSON on source '%s'",
                                    self.name,
                                    source_name,
                                )
                                continue
                            try:
                                await source.on_message(payload)
                            except Exception:
                                logger.exception(
                                    "Feature %s: error in source '%s' on_message",
                                    self.name,
                                    source_name,
                                )
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break
            finally:
                sender_task.cancel()
                source.queues.discard(queue)

            return ws

        return handler

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
            if source.transform is None:
                continue

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
