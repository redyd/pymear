from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable

from contracts.events import Event

logger = logging.getLogger(__name__)

Handler = Callable[[Event], Awaitable[None]]


class EventBus:
    """
    Internal In-Process Event Bus (Dataclass-Typed):
    - subscribe(): Registers handler for exact event type only (no subclass inheritance).
    - publish(): Async dispatch—each handler runs in its own task. Slow/failing handlers never block others or the caller.
    """

    def __init__(self):
        self._handlers: dict[type[Event], list[Handler]] = defaultdict(list)

    def subscribe(self, event_type: type[Event], handler: Handler) -> None:
        self._handlers[event_type].append(handler)

    def unsubscribe(self, event_type: type[Event], handler: Handler) -> None:
        handlers = self._handlers.get(event_type)
        if handlers and handler in handlers:
            handlers.remove(handler)

    async def publish(self, event: Event) -> None:
        handlers = self._handlers.get(type(event), ())
        for handler in handlers:
            task = asyncio.ensure_future(handler(event))
            task.add_done_callback(self._log_handler_error)

    def _log_handler_error(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        if exc := task.exception():
            logger.exception("Erreur dans un handler du bus", exc_info=exc)
