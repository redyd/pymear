import asyncio
from collections.abc import Awaitable, Callable

from twitchio.ext import commands

from pymear.utils.logger import VerboseLogger

Listener = Callable[..., Awaitable[None]]

class TwitchBot(commands.Bot):
    """
    Centralized Twitch IRC Connection Manager with Event Forwarding.

    Owns the single twitchio bot connection and acts as the gateway between
    Twitch IRC events and the application's event pipeline. Forwards raw events
    to registered listeners and exposes outgoing chat actions to the rest of the app.

    Core Capabilities:
        - Maintain persistent IRC connection to Twitch with auto-reconnect
        - Forward raw IRC events (messages, follows, subs, cheers, raids) to listeners
        - Expose channel messaging via send_message with readiness guard
        - Track bot readiness state for safe command execution
        - Filter echo messages to avoid processing self-sent chat

    Event Types Forwarded:
        - message: Chat messages from channel users
        - follow: New channel followers
        - subscription: Channel subscriptions with tier/tags
        - subscription_gift: Gifted subscriptions to multiple users
        - cheer: Bit donations with message content
        - raid: Incoming raids from other channels
        - raw_data: Raw IRC lines for special parsing (e.g., CLEARMSG)

    Architecture:
        TwitchIRC → TwitchBot → EventExporter → InternalBus

    Usage:
        bot = TwitchBot(token=token, client_id=cid, prefix="!", channel="mystream")
        await bot.start()
        bot.add_listener(on_event)  # Register event handler
    """

    def __init__(self, token: str, client_id: str, prefix: str, channel: str, verbose: bool = False):
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
        self.logger = VerboseLogger(self.__class__.__name__, verbose)

    def add_listener(self, callback: Listener) -> None:
        """
        Register a callback to receive raw Twitch events.

        All events forwarded by the bot will be sent to registered listeners
        in the order they were added.

        Args:
            callback: Async function accepting (kind, *args) where kind identifies the event type
        """
        self._listeners.append(callback)
        self.logger.info("Event listener registered (total: %d)", len(self._listeners))

    async def _notify(self, kind: str, *args) -> None:
        """Forward raw event to all registered listeners."""
        for listener in self._listeners:
            await listener(kind, *args)
        self.logger.info_v("Notified %d listener(s) for event type %s",
                         len(self._listeners), kind)

    async def event_ready(self):
        """Called when bot successfully connects to Twitch IRC servers."""
        self.logger.info("Bot logged in as %s, channels: %s",
                        self.nick, self.connected_channels)
        self._ready.set()

    async def event_message(self, message):
        """Forward chat messages, excluding self-sent echo messages."""
        if message.echo:
            return
        await self._notify("message", message)
        self.logger.info_v("Message event forwarded: %s", message.author)

    async def event_follow(self, channel, user):
        """Forward follow events to listeners."""
        await self._notify("follow", channel, user)
        self.logger.info_v("Follow event: user %s followed %s", user.name, channel)

    async def event_subscription(self, user, channel, tags):
        """Forward subscription events with tier and duration tags."""
        await self._notify("subscription", user, channel, tags)
        self.logger.info_v("Subscription event: user %s on %s", user.name, channel)

    async def event_subscription_gift(self, channel, tags):
        """Forward gifted subscription events with recipient and count tags."""
        await self._notify("subscription_gift", channel, tags)
        self.logger.info_v("Gift subscription event on %s", channel)

    async def event_cheer(self, channel, tags, message):
        """Forward cheer/bits events with amount and optional message."""
        await self._notify("cheer", channel, tags, message)
        self.logger.info_v("Cheer event on %s", channel)

    async def event_raid(self, channel, tags):
        """Forward raid events with raider info and viewer count."""
        await self._notify("raid", channel, tags)
        self.logger.info_v("Raid event on %s", channel)

    async def event_raw_data(self, data: str) -> None:
        """Forward raw IRC data for special protocol handling."""
        await self._notify("raw_data", data)
        self.logger.info_v("Raw data event: %r", data[:100])

    async def send_message(self, text: str) -> None:
        """
        Send a chat message to the bot's connected channel.

        Blocks until the bot is ready, then forwards the message to Twitch.
        Raises RuntimeError if the channel connection is unavailable.

        Args:
            text: Message content to send

        Raises:
            RuntimeError: If not connected to the configured channel
        """
        await self._ready.wait()
        self.logger.info_v("Waiting for bot ready signal complete")

        channel = self.get_channel(self._channel_name)
        if channel is None:
            self.logger.error("Channel '%s' not found in connected channels",
                             self._channel_name)
            raise RuntimeError(f"Not connected to channel '{self._channel_name}'")

        await channel.send(text)
        self.logger.info("Sent message to %s: %s", self._channel_name, text[:50])
