from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from pathlib import Path
from typing import Generic, TypeVar

import aiohttp
from aiohttp import web

from pymear.contracts.events import Event
from pymear.core.hub_listener import HubListener
from pymear.core.websocket_pipe import WebsocketPipe
from pymear.utils.logger import VerboseLogger

E = TypeVar("E", bound=Event)
OVERLAY_HELPER = Path(__file__).parent / "static" / "pymear-overlay.js"


def _find_free_port() -> int:
    """Find an available TCP port dynamically."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


class _SourceContext(Generic[E]):
    """Binds a WebsocketPipe to its optional hub transform."""

    def __init__(
        self,
        pipe: WebsocketPipe,
        transform: Callable[[E], dict | Event | None] | None,
    ) -> None:
        self.pipe = pipe
        self.transform = transform


class Feature(Generic[E]):
    """
    Orchestrates a feature: owns one WebsocketPipe per source, filters and
    transforms hub events via HubListener, and exposes a generic send() for
    custom, non-hub-driven triggers (keypress, awaited task completion, etc.).
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
        ws_heartbeat: float = 15,
        queue_maxsize: int = 100,
        client_send_timeout: float = 10,
    ) -> None:
        self.name = name
        self._event_types = tuple(event_types)
        self.static_dir = static_dir
        self.port = port if port is not None else _find_free_port()
        self.hub_url = f"http://localhost:{hub_port}/internal/events"
        self.proxy_port = proxy_port
        self.proxy_url = f"ws://localhost:{proxy_port}/register"
        self.ws_heartbeat = ws_heartbeat
        self.queue_maxsize = queue_maxsize
        self.client_send_timeout = client_send_timeout
        self._sources: dict[str, _SourceContext[E]] = {}
        self.logger = VerboseLogger(self.__class__.__name__, verbose)
        self._hub_listener = HubListener(self.hub_url, self._on_hub_event, self.logger)

        self.logger.info_v("Feature '%s' initialized on port %d", self.name, self.port)

    def add_source(
        self,
        name: str,
        on_events_transform: Callable[[E], dict | Event | None] | None = None,
        on_ws_response: Callable[[dict], Awaitable[None]] | None = None,
    ) -> None:
        """
        Register a bidirectional WebSocket source for this feature.

        on_event_transform binds the source to hub events: present, the source
        receives filtered/transformed events automatically. Absent, the
        source only exists via direct send() calls, e.g. from a keypress
        handler or after an awaited task completes.
        """
        self.logger.info_v("Adding source '%s' to feature '%s'", name, self.name)
        pipe = WebsocketPipe(
            name,
            on_ws_response,
            self.ws_heartbeat,
            self.queue_maxsize,
            self.client_send_timeout,
            self.logger,
        )
        self._sources[name] = _SourceContext(pipe, on_events_transform)
        self.logger.info_v(
            "Source '%s' registered: on_event_transform=%s, on_message=%s",
            name,
            on_events_transform is not None,
            on_ws_response is not None,
        )

    async def send(self, source_name: str, payload: dict) -> None:
        """
        Push a payload to all connected clients of a specific source.

        Generic entry point: used internally after a hub transform, and
        externally for any custom trigger unrelated to hub events.
        """
        ctx = self._sources.get(source_name)
        if ctx is None:
            self.logger.warning(
                "Cannot send to unknown source '%s' on feature '%s'",
                source_name,
                self.name,
            )
            return
        ctx.pipe.push(payload)
        self.logger.info_v("Pushed payload to source '%s'", source_name)

    def start(self) -> None:
        """
        Synchronous entry point: runs the feature until interrupted (Ctrl+C),
        then shuts down cleanly.
        """
        try:
            asyncio.run(self.async_start())
        except KeyboardInterrupt:
            pass

    async def async_start(self) -> None:
        """
        Start the feature server and begin listening for hub events.
        Blocks until the application is terminated.
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
            asyncio.create_task(self._hub_listener.run()),
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

    # ==================== PRIVATE METHODS ====================

    def _build_app(self) -> web.Application:
        """Build the aiohttp application with WebSocket routes and static file serving."""
        app = web.Application()
        for name, ctx in self._sources.items():
            app.router.add_get(f"/ws/{name}", ctx.pipe.handle)
            self.logger.info_v(
                "Registered route at '/ws/%s' for source '%s'", name, name
            )
        app.router.add_get("/pymear-overlay.js", self._overlay_helper_handler)
        app.router.add_get("/", self._index_handler)
        app.router.add_static("/", path=self.static_dir, show_index=False)
        self.logger.info_v(
            "Registered routes: %d sources + helper + static + index",
            len(self._sources) + 3,
        )
        return app

    async def _on_hub_event(self, event: Event) -> None:
        """Filter a hub event by type, then dispatch it through each source's transform."""
        if not isinstance(event, self._event_types):
            self.logger.info_v(
                "Event type %s not subscribed, skipping", type(event).__name__
            )
            return

        for name, ctx in self._sources.items():
            if ctx.transform is None:
                continue

            try:
                filtered = ctx.transform(event)
            except Exception:  # noqa: BLE001
                self.logger.error_v("Transform error on source '%s'", name)
                continue

            if filtered is None:
                self.logger.info_v("Transform filtered event on source '%s'", name)
                continue

            if isinstance(filtered, Event):
                filtered = asdict(filtered)

            ctx.pipe.push(filtered)

        self.logger.info_v("Dispatched event to %d source(s)", len(self._sources))

    async def _register_with_proxy(self) -> None:
        """Maintain proxy registration heartbeat with exponential backoff retry."""
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async for ws in self._connect_with_retry(session, self.proxy_url, "proxy"):
                self.logger.info_v("Registered with proxy on port %d", self.port)
                self.logger.info(
                    "Feature running on proxy at http://localhost:%d/%s/",
                    self.proxy_port,
                    self.name,
                )
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
        """Generator yielding WebSocket connections with automatic reconnection."""
        delay = 1
        while True:
            try:
                async with session.ws_connect(url, heartbeat=self.ws_heartbeat) as ws:
                    delay = 1
                    yield ws
            except (aiohttp.ClientError, ConnectionRefusedError):
                self.logger.warning(
                    "%s unreachable at %s, retry in %ds", label, url, delay
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def _overlay_helper_handler(self, request: web.Request) -> web.FileResponse:
        """Serve the reconnecting browser helper for OBS overlays."""
        return web.FileResponse(OVERLAY_HELPER)

    async def _index_handler(self, request: web.Request) -> web.FileResponse:
        """Serve the static index.html entry point."""
        return web.FileResponse(self.static_dir / "index.html")
