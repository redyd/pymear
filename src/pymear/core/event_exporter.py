import re

from twitchio.chatter import Chatter

from pymear.contracts.events import *
from pymear.core.internal_bus import InternalBus
from pymear.core.twitch_bot import TwitchBot
from pymear.http.interactor import Interactor
from pymear.utils.logger import VerboseLogger

_CLEARMSG_RE = re.compile(r"^@(?P<tags>\S+) :tmi\.twitch\.tv CLEARMSG #\S+ :(?P<text>.*)$")

class EventExporter:
    """
    Twitch Event Pipeline: Raw Events → Typed Bus Events.

    Listens to raw Twitch IRC events from TwitchBot and converts them into strongly-typed
    events published on the InternalBus. Acts as the translation layer between Twitch's
    protocol and the application's event contract system.

    Core Capabilities:
        - Convert raw Twitch messages into ChatMessageEvent with badge resolution
        - Publish FollowEvent when users follow the channel
        - Publish SubscriptionEvent and GiftSubscriptionEvent with tier/month tracking
        - Publish CheerEvent with bit amounts and optional message content
        - Publish RaidEvent with raider display name and viewer count
        - Publish DeletedMessageEvent by parsing CLEARMSG IRC tags

    Event Flow:
        TwitchIRC → TwitchBot → EventExporter → InternalBus → Subscribers

    Dependencies:
        - TwitchBot for raw event delivery
        - InternalBus for event distribution
        - Interactor for badge metadata resolution

    Usage:
        exporter = EventExporter(bot=bot, bus=bus, interactor=interactor)
        # Events automatically forwarded to bus subscribers
    """

    def __init__(self, bot: TwitchBot, bus: InternalBus, interactor: Interactor, verbose: bool = False) -> None:
        self.bus = bus
        self.interactor = interactor
        self.logger = VerboseLogger(self.__class__.__name__, verbose)
        bot.add_listener(self._on_event)
        self.logger.info("EventExporter registered with TwitchBot")

    async def _on_event(self, kind: str, *args) -> None:
        """Dispatch incoming Twitch events to typed handlers based on event kind."""
        handler = getattr(self, f"_handle_{kind}", None)
        if handler:
            await handler(*args)
        else:
            self.logger.info_v("No handler for event kind %s", kind)

    async def _handle_message(self, message) -> None:
        """Process chat messages and publish ChatMessageEvent with user metadata."""
        author = message.author
        default_color = "#a970ff"
        if isinstance(author, Chatter):
            badges = self.interactor.resolve_badges(author.badges or {})
            color = author.color or default_color
        else:
            badges = []
            color = default_color

        event = ChatMessageEvent(
            user=author.name or "User",
            text=message.content or "",
            color=color,
            badges=badges,
            message_id=message.id,
        )
        await self.bus.publish(event)
        self.logger.info_v("Published ChatMessageEvent for user %s", event.user)

    async def _handle_follow(self, channel, user) -> None:
        """Process follow events and publish FollowEvent."""
        event = FollowEvent(user=user.name)
        await self.bus.publish(event)
        self.logger.info("FollowEvent published for user %s", event.user)

    async def _handle_subscription(self, user, channel, tags) -> None:
        """Process subscription events with tier and cumulative month tracking."""
        months = int(tags.get("msg-param-cumulative-months") or 1)
        tier = tags.get("msg-param-sub-plan") or "1000"
        event = SubscriptionEvent(user=user.name, months=months, tier=tier)
        await self.bus.publish(event)
        self.logger.info("SubscriptionEvent published for %s (%d months, tier %s)",
                         event.user, months, tier)

    async def _handle_subscription_gift(self, channel, tags) -> None:
        """Process gifted subscription events with gifter and recipient details."""
        gifter = tags.get("display-name") or tags.get("login") or "Anonymous"
        recipient = tags.get("msg-param-recipient-display-name") or "someone"
        total = int(tags.get("msg-param-sender-count") or 1)
        tier = tags.get("msg-param-sub-plan") or "1000"
        event = GiftSubscriptionEvent(
            gifter=gifter, recipient=recipient, gifter_total=total, tier=tier
        )
        await self.bus.publish(event)
        self.logger.info("GiftSubscriptionEvent published: %s → %s (%d gifts, tier %s)",
                         gifter, recipient, total, tier)

    async def _handle_cheer(self, channel, tags, message) -> None:
        """Process cheer/bits events with amount and optional message content."""
        user = tags.get("display-name") or tags.get("login") or "Anonymous"
        bits = int(tags.get("bits") or 0)
        event = CheerEvent(user=user, bits=bits, message=message or "")
        await self.bus.publish(event)
        self.logger.info("CheerEvent published: %s cheered %d bits", user, bits)

    async def _handle_raid(self, channel, tags) -> None:
        """Process raid events with raider identity and viewer count."""
        raider = tags.get("msg-param-displayName") or tags.get("login") or "Unknown"
        viewer_count = int(tags.get("msg-param-viewerCount") or 0)
        event = RaidEvent(raider=raider, viewer_count=viewer_count)
        await self.bus.publish(event)
        self.logger.info("RaidEvent published: %s brought %d viewers", raider, viewer_count)

    async def _handle_raw_data(self, data: str) -> None:
        """
        Parse raw IRC data for CLEARMSG events and publish DeletedMessageEvent.

        Extracts user login and message ID from IRC tags to track deletions.
        """
        if "CLEARMSG" not in data:
            return

        match = _CLEARMSG_RE.match(data)
        if not match:
            self.logger.info_v("CLEARMSG regex mismatch for data: %r", data[:100])
            return

        tags = dict(
            pair.split("=", 1)
            for pair in match.group("tags").split(";")
            if "=" in pair
        )
        user = tags.get("login", "Unknown")
        message_id = tags.get("target-msg-id", "")
        event = DeletedMessageEvent(user=user, message_id=message_id)
        await self.bus.publish(event)
        self.logger.info_v("DeletedMessageEvent published: user %s, msg_id %s",
                          user, message_id)
