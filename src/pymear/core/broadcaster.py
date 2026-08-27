from __future__ import annotations

import json
import logging

from aiohttp import web

from pymear.contracts.events import Event
from pymear.contracts.events_mapper import encapsulate
from pymear.core.internal_bus import InternalBus

logger = logging.getLogger(__name__)


class Broadcaster:
    """
    WebSocket Broadcaster Relay:
    - Relays EventBus events to connected WebSocket clients (OBS overlays, Python scripts, etc.) via JSON protocol.
    - Supports bidirectional messaging: incoming client messages are rebroadcast to others.
    - Temporary design—pending dedicated command class takeover.
    """

    def __init__(self):
        self._clients: set[web.WebSocketResponse] = set()

    def subscribe_to(self, bus: InternalBus, *event_types: type[Event]) -> None:
        for event_type in event_types:
            bus.subscribe(event_type, self._relay)

    async def _relay(self, event: Event) -> None:
        await self.send(encapsulate(event))

    async def send(
        self, payload: dict, exclude: web.WebSocketResponse | None = None
    ) -> None:
        message = json.dumps(payload)
        dead: list[web.WebSocketResponse] = []
        for client in self._clients:
            if client is exclude:
                continue
            try:
                await client.send_str(message)
            except ConnectionResetError:
                dead.append(client)
        for client in dead:
            self._clients.discard(client)

    async def websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._clients.add(ws)

        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        payload = json.loads(msg.data)
                    except json.JSONDecodeError:
                        logger.warning(
                            "Invalid message: %r", msg.data
                        )
                        continue
                    await self.send(payload, exclude=ws)
                elif msg.type == web.WSMsgType.ERROR:
                    break
        finally:
            self._clients.discard(ws)

        return ws
