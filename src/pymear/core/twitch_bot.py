import asyncio
import logging
from collections.abc import Awaitable, Callable

from twitchio.ext import commands

logger = logging.getLogger(__name__)

Listener = Callable[..., Awaitable[None]]


class TwitchBot(commands.Bot):
    """Owns the single twitchio connection. Forwards raw events to listeners
    and exposes outgoing actions (send_message, etc.) to the rest of the app."""

    def __init__(self, token: str, client_id: str, prefix: str, channel: str):
        self._channel_name = channel.lower()
        super().__init__(
            token=token,
            client_id=client_id,
            nick=channel,
            prefix=prefix,
            initial_channels=[self._channel_name],
        )
        self._listeners: list[Listener] = []
        self._ready = asyncio.Event()

    def add_listener(self, callback: Listener) -> None:
        self._listeners.append(callback)

    async def _notify(self, kind: str, *args) -> None:
        for listener in self._listeners:
            await listener(kind, *args)

    async def event_ready(self):
        logger.info("Bot logged in as %s, channels: %s", self.nick, self.connected_channels)
        self._ready.set()

    async def event_message(self, message):
        if message.echo:
            return
        await self._notify("message", message)
        await self.handle_commands(message)

    async def event_follow(self, channel, user):
        await self._notify("follow", channel, user)

    async def event_subscription(self, user, channel, tags):
        await self._notify("subscription", user, channel, tags)

    async def event_subscription_gift(self, channel, tags):
        await self._notify("subscription_gift", channel, tags)

    async def event_cheer(self, channel, tags, message):
        await self._notify("cheer", channel, tags, message)

    async def event_raid(self, channel, tags):
        await self._notify("raid", channel, tags)

    async def event_raw_data(self, data: str) -> None:
        await self._notify("raw_data", data)

    async def send_message(self, text: str) -> None:
        await self._ready.wait()
        channel = self.get_channel(self._channel_name)
        if channel is None:
            raise RuntimeError(f"Not connected to channel '{self._channel_name}'")
        await channel.send(text)
