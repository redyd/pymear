import asyncio

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
from .core.event_bus import EventBus
from .core.event_exporter import EventExporter
from .core.feature_runtime import FeatureRuntime
from .server import Pymear
from .utils.badge_resolver import BadgeResolver
from .utils.speaker import TextToSpeech

__all__ = [
    "BadgeResolver",
    "Broadcaster",
    "ChatMessageEvent",
    "CheerEvent",
    "DeletedMessageEvent",
    "Event",
    "EventBus",
    "EventExporter",
    "FeatureRuntime",
    "FollowEvent",
    "GiftSubscriptionEvent",
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
