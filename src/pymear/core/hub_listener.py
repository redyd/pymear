from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable

import aiohttp

from pymear.contracts.events import Event
from pymear.contracts.events_mapper import decapsulate
from pymear.utils.logger import VerboseLogger


class HubListener:
    """
    Maintains a persistent SSE connection to the hub and forwards decapsulated
    events to a callback. Has no notion of which event types the caller
    actually wants: that filtering belongs to the receiving side.
    """

    def __init__(
        self,
        hub_url: str,
        on_event: Callable[[Event], Awaitable[None]],
        logger: VerboseLogger,
    ) -> None:
        self.hub_url = hub_url
        self.on_event = on_event
        self.logger = logger

    async def run(self) -> None:
        """Connect to the hub and dispatch events until cancelled."""
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async for line in self._consume_sse_with_retry(session, self.hub_url):
                await self._handle_message(line)

    async def _handle_message(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
            event = decapsulate(payload)
        except (ValueError, KeyError):
            self.logger.info_v("Skipping malformed hub message")
            return
        await self.on_event(event)

    async def _consume_sse_with_retry(self, session: aiohttp.ClientSession, url: str):
        delay = 1
        while True:
            try:
                async with session.get(url) as response:
                    delay = 1
                    self.logger.info_v("Connected to hub")
                    async for raw_line in response.content:
                        line = raw_line.decode("utf-8").strip()
                        if not line or not line.startswith("data:"):
                            continue
                        yield line[len("data:") :].strip()
            except (aiohttp.ClientError, ConnectionRefusedError, ConnectionResetError):
                self.logger.warning("hub unreachable at %s, retry in %ds", url, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
            except TimeoutError:
                task = asyncio.current_task()
                if task is not None and task.cancelling():
                    raise asyncio.CancelledError from None
                self.logger.warning("hub read timed out, instant retry")
