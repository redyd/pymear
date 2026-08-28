from __future__ import annotations

import json
from pathlib import Path

from aiohttp import web

from pymear.contracts.events import (
    ChatMessageEvent,
    CheerEvent,
    DeletedMessageEvent,
    FollowEvent,
    GiftSubscriptionEvent,
    RaidEvent,
    SubscriptionEvent,
)
from pymear.core.internal_bus import InternalBus

STATIC_DIR = Path(__file__).parent / "static"

_EVENT_MAP = {
    "chat_message": ChatMessageEvent,
    "follow": FollowEvent,
    "subscription": SubscriptionEvent,
    "gift_subscription": GiftSubscriptionEvent,
    "cheer": CheerEvent,
    "raid": RaidEvent,
    "deleted_message": DeletedMessageEvent,
}


class Dashboard:
    def __init__(self, app: web.Application, bus: InternalBus):
        self.bus = bus

        app.router.add_get("/dashboard", self._index)
        app.router.add_static("/static", STATIC_DIR)

        for event_type in _EVENT_MAP:
            app.router.add_post(f"/trigger/{event_type}", self._make_handler(event_type))

    async def _index(self, request: web.Request) -> web.FileResponse:
        return web.FileResponse(STATIC_DIR / "index.html")

    def _make_handler(self, event_type: str):
        event_cls = _EVENT_MAP[event_type]

        async def handler(request: web.Request) -> web.Response:
            try:
                body = await request.json()
            except json.JSONDecodeError:
                return web.json_response({"error": "Invalid JSON"}, status=400)

            try:
                event = event_cls(**body)
            except TypeError as e:
                return web.json_response({"error": str(e)}, status=422)

            await self.bus.publish(event)
            return web.json_response({"ok": True, "event": event_type})

        handler.__name__ = f"trigger_{event_type}"
        return handler
