from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable

from pymear.contracts.events import Event
from pymear.utils.logger import VerboseLogger

Handler = Callable[[Event], Awaitable[None]]

class InternalBus:
    """
    In-Memory Event Bus for Asynchronous Event Distribution.

    Provides a lightweight pub/sub mechanism within the process, allowing components
    to communicate via typed event dataclasses without direct coupling. Handlers run
    asynchronously and independently—slow or failing handlers never block others.

    Core Capabilities:
        - Subscribe handlers to specific event types (exact type match only)
        - Publish events with fire-and-forget async dispatch
        - Isolate handler failures—exceptions logged but don't crash the bus
        - Unsubscribe handlers dynamically at runtime
        - Automatic error tracking via task callbacks

    Architecture:
        Publishers → InternalBus → Handlers (parallel tasks)

    Usage:
        bus = InternalBus()
        bus.subscribe(ChatMessageEvent, lambda e: print(f"New message from {e.user}"))
        await bus.publish(ChatMessageEvent(user="Alice", text="Hello!"))
    """

    def __init__(self):
        self._handlers: dict[type[Event], list[Handler]] = defaultdict(list)
        self.logger = VerboseLogger(self.__class__.__name__, False)
        self.logger.info("InternalBus initialized")

    def subscribe(self, event_type: type[Event], handler: Handler) -> None:
        """
        Register a handler for a specific event type.

        Only exact type matches will trigger—subclass inheritance is not applied.

        Args:
            event_type: The Event class to listen for
            handler: Async callback accepting the event instance
        """
        self._handlers[event_type].append(handler)
        self.logger.info_v("Handler subscribed for %s (%d total handler(s))",
                          event_type.__name__, len(self._handlers[event_type]))

    def unsubscribe(self, event_type: type[Event], handler: Handler) -> None:
        """
        Remove a previously registered handler for an event type.

        Silently ignores attempts to remove unregistered handlers.

        Args:
            event_type: The Event class the handler was registered for
            handler: The exact handler function to remove
        """
        handlers = self._handlers.get(event_type)
        if handlers and handler in handlers:
            handlers.remove(handler)
            self.logger.info_v("Handler unsubscribed for %s (%d remaining)",
                              event_type.__name__, len(handlers))
        else:
            self.logger.info_v("Unsubscribe attempted but handler not found for %s",
                             event_type.__name__)

    async def publish(self, event: Event) -> None:
        """
        Dispatch an event to all registered handlers in parallel.

        Each handler runs as an independent async task—failures are isolated and
        logged without propagating to the publisher or affecting other handlers.

        Args:
            event: The event dataclass instance to distribute
        """
        handlers = self._handlers.get(type(event), ())
        self.logger.info_v("Publishing %s to %d handler(s)",
                         type(event).__name__, len(handlers))

        for handler in handlers:
            task = asyncio.ensure_future(handler(event))
            task.add_done_callback(self._log_handler_error)

        self.logger.info_v("Event %s dispatched to %d handler(s)",
                        type(event).__name__, len(handlers))

    def _log_handler_error(self, task: asyncio.Task) -> None:
        """
        Callback attached to each handler task to capture exceptions.

        Logged via exception for debugging, but does not affect task execution.
        """
        if task.cancelled():
            self.logger.info_v("Handler task cancelled")
            return
        if exc := task.exception():
            self.logger.error("Exception in %s handler", type(exc).__name__)
            self.logger.error_v("Full traceback:\n%s", exc)
