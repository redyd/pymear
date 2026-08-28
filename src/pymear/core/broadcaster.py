from __future__ import annotations

import asyncio
import json

from aiohttp import web

from pymear.contracts.events import Event
from pymear.contracts.events_mapper import encapsulate
from pymear.core.internal_bus import InternalBus
from pymear.utils.logger import VerboseLogger


class Broadcaster:
    """
    SSE Event Relay for Real-Time Broadcast Communication.

    Bridges the internal event bus with external SSE clients such as OBS overlays,
    Python scripts, and browser-based dashboards.

    Core Capabilities:
        - Subscribe to typed events from the internal event bus
        - Broadcast events to all connected SSE clients via JSON protocol
        - Automatic dead client cleanup on connection errors

    Usage:
        broadcaster = Broadcaster()
        broadcaster.subscribe_to(bus, ChatMessageEvent, StreamStartedEvent)
        app.router.add_get("/internal/events", broadcaster.sse_handler)
    """

    def __init__(self, verbose: bool = False):
        self._clients: set[web.StreamResponse] = set()
        self.logger = VerboseLogger(self.__class__.__name__, verbose)

    def subscribe_to(self, bus: InternalBus, *event_types: type[Event]) -> None:
        self.logger.info("Subscribing to %d event type(s)", len(event_types))
        for event_type in event_types:
            bus.subscribe(event_type, self._relay)
            self.logger.info_v("Registered subscription for %s", event_type.__name__)

    async def send(self, payload: dict) -> None:
        """Broadcast a payload to all connected SSE clients."""
        message = f"data: {json.dumps(payload)}\n\n"
        encoded = message.encode("utf-8")

        dead: list[web.StreamResponse] = []

        for client in self._clients:
            try:
                await client.write(encoded)
            except (ConnectionResetError, ConnectionError):
                dead.append(client)

        for client in dead:
            self._clients.discard(client)
            self.logger.info_v("Removed dead client from broadcast pool")

        self.logger.info_v("Broadcast to %d client(s)", len(self._clients) - len(dead))

    async def _relay(self, event: Event) -> None:
        await self.send(encapsulate(event))
        self.logger.info_v(
            "Relayed event %s to %d client(s)", type(event).__name__, len(self._clients)
        )

    async def sse_handler(self, request: web.Request) -> web.StreamResponse:
        """HTTP handler that opens an SSE stream and manages client lifecycle."""
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )
        await response.prepare(request)
        self._clients.add(response)
        self.logger.info("SSE client connected (total: %d)", len(self._clients))

        try:
            while True:
                await asyncio.sleep(15)
                await response.write(b": keep-alive\n\n")
        except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._clients.discard(response)
            self.logger.info("SSE client disconnected (total: %d)", len(self._clients))

        return response
