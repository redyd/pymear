from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .events import (
    ChatMessageEvent,
    CheerEvent,
    DeletedMessageEvent,
    Event,
    FollowEvent,
    GiftSubscriptionEvent,
    RaidEvent,
    SubscriptionEvent,
)

_EVENT_TYPES: dict[str, type[Event]] = {
    ChatMessageEvent.type: ChatMessageEvent,
    FollowEvent.type: FollowEvent,
    SubscriptionEvent.type: SubscriptionEvent,
    GiftSubscriptionEvent.type: GiftSubscriptionEvent,
    CheerEvent.type: CheerEvent,
    RaidEvent.type: RaidEvent,
    DeletedMessageEvent.type: DeletedMessageEvent
}


def encapsulate(event: Event) -> dict[str, Any]:
    payload = asdict(event)
    payload["type"] = event.type
    return payload


def decapsulate(payload: dict[str, Any]) -> Event:
    event_type = payload.get("type")

    if event_type is None:
        raise ValueError("Field 'type' is missing in payload")

    cls = _EVENT_TYPES.get(event_type)
    if cls is None:
        raise ValueError(f"Event type not known: {event_type!r}")

    fields = {key: value for key, value in payload.items() if key != "type"}
    return cls(**fields)
