from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import ClassVar


@dataclass(frozen=True)
class Event:
    timestamp: float = field(default_factory=time.time)


@dataclass(frozen=True)
class ChatMessageEvent(Event):
    type: ClassVar[str] = "chat_message"
    user: str = ""
    text: str = ""
    color: str = ""
    badges: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class FollowEvent(Event):
    type: ClassVar[str] = "follow"
    user: str = ""


@dataclass(frozen=True)
class SubscriptionEvent(Event):
    type: ClassVar[str] = "subscription"
    user: str = ""
    months: int = 1
    tier: str = "1000"


@dataclass(frozen=True)
class GiftSubscriptionEvent(Event):
    type: ClassVar[str] = "gift_subscription"
    gifter: str = ""
    recipient: str = ""
    gifter_total: int = 1
    tier: str = "1000"


@dataclass(frozen=True)
class CheerEvent(Event):
    type: ClassVar[str] = "cheer"
    user: str = ""
    bits: int = 0
    message: str = ""


@dataclass(frozen=True)
class RaidEvent(Event):
    type: ClassVar[str] = "raid"
    raider: str = ""
    viewer_count: int = 0
