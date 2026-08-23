from __future__ import annotations

import asyncio
import logging
import os

from aiohttp import web
from dotenv import load_dotenv

from pymear.contracts.events import (
    ChatMessageEvent,
    CheerEvent,
    FollowEvent,
    GiftSubscriptionEvent,
    RaidEvent,
    SubscriptionEvent,
)
from pymear.core.broadcaster import Broadcaster
from pymear.core.event_bus import EventBus
from pymear.core.event_exporter import EventExporter
from pymear.utils.badge_resolver import BadgeResolver
from pymear.utils.helix_client import get_user_id

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

load_dotenv()

HUB_PORT = int(os.getenv("HUB_PORT", "8765"))

ALL_EVENT_TYPES = (
    ChatMessageEvent,
    FollowEvent,
    SubscriptionEvent,
    GiftSubscriptionEvent,
    CheerEvent,
    RaidEvent,
)


async def start_event_exporter(app: web.Application) -> None:
    client_id = os.getenv("TWITCH_CLIENT_ID")
    token = os.getenv("TWITCH_TOKEN")
    channel = os.getenv("TWITCH_CHANNEL")
    prefix = os.getenv("TWITCH_PREFIX", "!")

    if not client_id or not token or not channel:
        print("Missing Twitch credentials: bot not started")
        return

    broadcaster_id = await get_user_id(client_id, token, channel)

    badge_resolver = BadgeResolver()
    await badge_resolver.load(client_id, token, broadcaster_id)

    bus: EventBus = app["bus"]
    exporter = EventExporter(
        token=token,
        client_id=client_id,
        prefix=prefix,
        channel=channel,
        bus=bus,
        badge_resolver=badge_resolver,
    )

    task = asyncio.create_task(exporter.start())
    task.add_done_callback(_log_exporter_error)
    app["event_exporter_task"] = task
    app["event_exporter"] = exporter


def _log_exporter_error(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    if exc := task.exception():
        print(f"EventExporter error: {exc!r}")


async def stop_event_exporter(app: web.Application) -> None:
    task = app.get("event_exporter_task")
    if task:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def create_app() -> web.Application:
    app = web.Application()

    bus = EventBus()
    broadcaster = Broadcaster()
    broadcaster.subscribe_to(bus, *ALL_EVENT_TYPES)

    app["bus"] = bus
    app["broadcaster"] = broadcaster

    app.router.add_get("/ws", broadcaster.websocket_handler)

    app.on_startup.append(start_event_exporter)
    app.on_cleanup.append(stop_event_exporter)

    return app


if __name__ == "__main__":
    web.run_app(create_app(), port=HUB_PORT)
