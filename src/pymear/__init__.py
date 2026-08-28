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
from .server import PymearServer
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
    "PymearServer",
    "RaidEvent",
    "SubscriptionEvent",
    "TextToSpeech",
]

def run(hub_port: int = 8765, proxy_port: int = 9000, verbose: bool = False, **credentials) -> None:
    app = PymearServer(hub_port=hub_port, proxy_port=proxy_port, verbose=verbose)
    for key, value in credentials.items():
        setattr(app, key, value)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        pass
