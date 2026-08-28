from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Generic, TypeVar

import aiohttp
from aiohttp import web

from pymear.contracts.events import Event
from pymear.contracts.events_mapper import decapsulate
from pymear.utils.logger import VerboseLogger

E = TypeVar("E", bound=Event)


def _find_free_port() -> int:
    """Find an available TCP port dynamically."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


class _Source(Generic[E]):
    """Internal container for a WebSocket source configuration."""

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
    WebSocket Hub Feature Manager for Twitch Channel Operations.

    Provides a foundation for building stream features that connect to a central hub
    via WebSocket and expose filtered event streams to WebSocket clients. Each feature
    serves a static UI and registers with a routing proxy for discovery.

    Core Capabilities:
        - Subscribe to typed hub events with automatic filtering
        - Expose bidirectional WebSocket channels per source
        - Transform outbound events with custom payload logic
        - Handle inbound messages from connected clients
        - Auto-reconnect hub and proxy connections with exponential backoff
        - Serve static assets (HTML, JS, CSS) for frontend integration

    Usage:
        feature = Feature(
            name="chat",
            event_types=[ChatMessageEvent],
            static_dir=Path(__file__).parent,
            verbose=True
        )
        feature.add_source(
            "chat",
            transform=lambda e: {"user": e.user, "text": e.text},
            on_message=lambda payload: handle_incoming(payload),
        )
        asyncio.run(feature.run())
    """

    # ==================== PUBLIC API ====================

    def __init__(
        self,
        name: str,
        event_types: list[type[E]],
        static_dir: Path,
        port: int | None = None,
        hub_port: int = 8765,
        proxy_port: int = 9000,
        verbose: bool = False,
    ) -> None:
        self.name = name
        self._event_types = tuple(event_types)
        self.static_dir = static_dir
        self.port = port if port is not None else _find_free_port()
        self.hub_url = f"http://localhost:{hub_port}/internal/events"
        self.proxy_port = proxy_port
        self.proxy_url = f"ws://localhost:{proxy_port}/register"
        self._sources: dict[str, _Source[E]] = {}
        self.logger = VerboseLogger(self.__class__.__name__, verbose)

        self.logger.info_v("Feature '%s' initialized on port %d", self.name, self.port)

    def add_source(
        self,
        name: str,
        transform: Callable[[E], dict | None] | None = None,
        on_message: Callable[[dict], Awaitable[None]] | None = None,
    ) -> None:
        """
        Register a bidirectional WebSocket source for this feature.

        Creates a dedicated endpoint at /ws/<name> that can:
            - Push transformed hub events to connected clients (via transform callback)
            - Receive and forward client messages (via on_message callback)

        A source with only on_message acts as a receive-only channel.
        A source with only transform acts as a push-only channel.

        Args:
            name: Unique identifier for the source (endpoint path segment)
            transform: Optional callback to convert hub events to client payloads
                     Return None to filter out specific events from this stream
            on_message: Optional callback to process incoming client messages
        """
        self.logger.info_v("Adding source '%s' to feature '%s'", name, self.name)
        self._sources[name] = _Source(name, transform, on_message)
        self.logger.info_v(
            "Source '%s' registered: transform=%s, on_message=%s",
            name,
            transform is not None,
            on_message is not None,
        )

    async def send(self, source_name: str, payload: dict) -> None:
        """
        Push a payload to all connected clients of a specific source.

        Bypasses event filtering and delivers directly to the source's queues.

        Args:
            source_name: Identifier of the target source
            payload: Dictionary to serialize and broadcast to clients
        """
        source = self._sources.get(source_name)
        if source is None:
            self.logger.warning(
                "Cannot send to unknown source '%s' on feature '%s'",
                source_name,
                self.name,
            )
            return
        for queue in source.queues:
            await queue.put(payload)
        self.logger.info_v(
            "Pushed payload to %d client queue(s) for source '%s'",
            len(source.queues),
            source_name,
        )

    def start(self) -> None:
        """
        Synchronous entry point: runs the feature until interrupted (Ctrl+C),
        then shuts down cleanly. Wraps asyncio.run() so callers don't need
        their own KeyboardInterrupt handling.
        """
        try:
            asyncio.run(self._run())
        except KeyboardInterrupt:
            pass

    # ==================== PRIVATE METHODS ====================

    async def _run(self) -> None:
        """
        Start the feature server and begin listening for hub events.

        Orchestrates:
            1. Starting the local WebSocket/HTTP server
            2. Connecting to the hub with auto-retry
            3. Registering with the proxy with auto-retry

        This method blocks until the application is terminated.
        """
        app = self._build_app()
        runner = web.AppRunner(app)

        await runner.setup()
        site = web.TCPSite(runner, port=self.port)
        await site.start()

        self.logger.info(
            "Feature '%s' served on http://localhost:%s", self.name, self.port
        )
        self.logger.info_v("Connecting to hub at %s", self.hub_url)
        self.logger.info_v("Registering with proxy at %s", self.proxy_url)

        tasks = [
            asyncio.create_task(self._listen_hub()),
            asyncio.create_task(self._register_with_proxy()),
        ]

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            self.logger.info("Feature '%s' received shutdown signal", self.name)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await runner.cleanup()
            self.logger.info("Feature '%s' shut down cleanly", self.name)


    def _build_app(self) -> web.Application:
        """Build the aiohttp application with WebSocket routes and static file serving."""
        app = web.Application()
        for source_name in self._sources:
            app.router.add_get(f"/ws/{source_name}", self._make_ws_handler(source_name))
            self.logger.info_v("Registered route at '/ws/%s' for source '%s'", source_name, source_name)
        app.router.add_get("/", self._index_handler)
        app.router.add_static("/", path=self.static_dir, show_index=False)
        self.logger.info_v(
            "Registered routes: %d sources + static + index", len(self._sources) + 2
        )
        return app

    async def _listen_hub(self) -> None:
        """Maintain persistent SSE connection to hub with exponential backoff retry."""
        async with aiohttp.ClientSession() as session:
            async for line in self._consume_sse_with_retry(session, self.hub_url):
                await self._handle_hub_message(line)

    async def _consume_sse_with_retry(self, session: aiohttp.ClientSession, url: str):
        """
        Async generator that yields SSE `data:` payloads from the hub,
        reconnecting with exponential backoff whenever the stream drops.
        """
        delay = 1
        while True:
            try:
                async with session.get(url) as response:
                    delay = 1
                    self.logger.info_v("Connected to hub")
                    async for raw_line in response.content:
                        line = raw_line.decode("utf-8").strip()
                        if not line or not line.startswith("data:"):
                            continue
                        yield line[len("data:"):].strip()
            except (aiohttp.ClientError, ConnectionRefusedError, ConnectionResetError):
                self.logger.warning("hub unreachable at %s, retry in %ds", url, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def _register_with_proxy(self) -> None:
        """Maintain proxy registration heartbeat with exponential backoff retry."""
        async with aiohttp.ClientSession() as session:
            async for ws in self._connect_with_retry(session, self.proxy_url, "proxy"):
                self.logger.info_v("Registered with proxy on port %d", self.port)
                self.logger.info("Feature running on proxy at http://localhost:%d/%s/", self.proxy_port, self.name)
                try:
                    await ws.send_str(
                        json.dumps({"name": self.name, "port": self.port})
                    )
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.ERROR:
                            break
                except ConnectionResetError:
                    self.logger.warning("Lost proxy connection, reconnecting...")

    async def _connect_with_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
        label: str,
    ):
        """
        Generator that yields WebSocket connections with automatic reconnection.

        Implements exponential backoff starting at 1s, capped at 30s.
        """
        delay = 1
        while True:
            try:
                async with session.ws_connect(url) as ws:
                    delay = 1
                    yield ws
            except (aiohttp.ClientError, ConnectionRefusedError):
                self.logger.warning(
                    "%s unreachable at %s, retry in %ds", label, url, delay
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def _handle_hub_message(self, raw: str) -> None:
        """
        Process incoming hub messages by decapsulating, filtering, and distributing.

        Steps:
            1. Parse JSON and decapsulate into typed Event object
            2. Filter by registered event types for this feature
            3. Apply source transforms to generate client payloads
            4. Dispatch to all queued client connections
        """
        try:
            payload = json.loads(raw)
            event = decapsulate(payload)
        except (ValueError, KeyError):
            self.logger.info_v("Skipping malformed hub message")
            return

        if not isinstance(event, self._event_types):
            self.logger.info_v(
                "Event type %s not subscribed, skipping", type(event).__name__
            )
            return

        for source in self._sources.values():
            if source.transform is None:
                continue

            try:
                filtered = source.transform(event)
            except Exception:  # noqa: BLE001
                self.logger.error_v("Transform error on source '%s'", source.name)
                continue

            if filtered is None:
                self.logger.info_v(
                    "Transform filtered event on source '%s'", source.name
                )
                continue

            for queue in source.queues:
                await queue.put(filtered)

        self.logger.info_v("Dispatched event to %d source(s)", len(self._sources))

    def _make_ws_handler(
        self, source_name: str
    ) -> Callable[[web.Request], Awaitable[web.WebSocketResponse]]:
        """Create a WebSocket handler closure for a specific source."""

        async def handler(request: web.Request) -> web.WebSocketResponse:
            source = self._sources[source_name]
            ws = web.WebSocketResponse()
            await ws.prepare(request)

            queue: asyncio.Queue[dict] = asyncio.Queue()
            source.queues.add(queue)
            self.logger.info_v(
                "Client connected to source '%s' (queues: %d)",
                source_name,
                len(source.queues),
            )

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
                                self.logger.warning(
                                    "Invalid JSON on source '%s'", source_name
                                )
                                continue
                            try:
                                await source.on_message(payload)
                            except Exception:  # noqa: BLE001
                                self.logger.error_v(
                                    "Error in on_message for source '%s'", source_name
                                )
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        self.logger.info_v("WebSocket error on source '%s'", source_name)
                        break
            finally:
                sender_task.cancel()
                source.queues.discard(queue)
                self.logger.info_v(
                    "Client disconnected from source '%s' (queues: %d)",
                    source_name,
                    len(source.queues),
                )

            return ws

        return handler

    async def _index_handler(self, request: web.Request) -> web.FileResponse:
        """Serve the static index.html entry point."""
        return web.FileResponse(self.static_dir / "index.html")
