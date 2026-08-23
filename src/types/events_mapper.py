from __future__ import annotations

from dataclasses import asdict

from .events import (
    ChatMessageEvent,
    CheerEvent,
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
}


def encapsulate(event: Event) -> dict:
    payload = asdict(event)
    payload["type"] = event.type
    return payload


def decapsulate(payload: dict) -> Event:
    event_type = payload.get("type")
    cls = _EVENT_TYPES.get(event_type)
    if cls is None:
        raise ValueError(f"Type d'event inconnu: {event_type!r}")
    fields = {key: value for key, value in payload.items() if key != "type"}
    return cls(**fields)
