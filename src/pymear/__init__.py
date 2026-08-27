import asyncio

from .client.command import Command
from .client.feature import Feature
from .contracts.events import (
    ChatMessageEvent,
    CheerEvent,
    DeletedMessageEvent,
    Event,
    FollowEvent,
    GiftSubscriptionEvent,
    RaidEvent,
    SubscriptionEvent,
)
from .core.broadcaster import Broadcaster
from .core.event_exporter import EventExporter
from .core.internal_bus import InternalBus
from .server import Pymear
from .utils.speaker import TextToSpeech

__all__ = [
    "Broadcaster",
    "ChatMessageEvent",
    "CheerEvent",
    "Command",
    "DeletedMessageEvent",
    "Event",
    "EventExporter",
    "Feature",
    "FollowEvent",
    "GiftSubscriptionEvent",
    "InternalBus",
    "Pymear",
    "RaidEvent",
    "SubscriptionEvent",
    "TextToSpeech",
]

def run(hub_port: int = 8765, proxy_port: int = 9000, **credentials) -> None:
    app = Pymear(hub_port=hub_port, proxy_port=proxy_port)
    for key, value in credentials.items():
        setattr(app, key, value)
    asyncio.run(app.run())
