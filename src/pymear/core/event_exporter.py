import logging
import re

from twitchio.chatter import Chatter
from twitchio.ext import commands

from pymear.contracts.events import *
from pymear.utils.badge_resolver import BadgeResolver

_CLEARMSG_RE = re.compile(r"^@(?P<tags>\S+) :tmi\.twitch\.tv CLEARMSG #\S+ :(?P<text>.*)$")

logger = logging.getLogger(__name__)

class EventExporter(commands.Bot):
    """The exporter bot that listens to Twitch events and publishes them to the bus."""

    def __init__(
        self,
        token: str,
        client_id: str,
        prefix: str,
        channel: str,
        bus,
        badge_resolver: BadgeResolver,
    ):
        super().__init__(
            token=token,
            client_id=client_id,
            nick=channel,
            prefix=prefix,
            initial_channels=[channel],
        )
        self.bus = bus
        self.badge_resolver = badge_resolver

    async def event_ready(self):
        logger.info("Bot logged in as %s, channels: %s", self.nick, self.connected_channels)

    async def event_message(self, message):
        if message.echo:
            return

        author = message.author
        default_color = "#a970ff"

        if isinstance(author, Chatter):
            badges = self.badge_resolver.resolve(author.badges or {})
            color = author.color or default_color
        else:
            badges = []
            color = default_color

        await self.bus.publish(
            ChatMessageEvent(
                user=author.name or "User",
                text=message.content or "",
                color=color,
                badges=badges,
                message_id=message.id
            )
        )
        await self.handle_commands(message)

    async def event_follow(self, channel, user):
        await self.bus.publish(FollowEvent(user=user.name))

    async def event_subscription(self, user, channel, tags):
        months = int(tags.get("msg-param-cumulative-months") or 1)
        tier = tags.get("msg-param-sub-plan") or "1000"
        await self.bus.publish(
            SubscriptionEvent(user=user.name, months=months, tier=tier)
        )

    async def event_subscription_gift(self, channel, tags):
        gifter = tags.get("display-name") or tags.get("login") or "Anonymous"
        recipient = tags.get("msg-param-recipient-display-name") or "someone"
        total = int(tags.get("msg-param-sender-count") or 1)
        tier = tags.get("msg-param-sub-plan") or "1000"
        await self.bus.publish(
            GiftSubscriptionEvent(
                gifter=gifter, recipient=recipient, gifter_total=total, tier=tier
            )
        )

    async def event_cheer(self, channel, tags, message):
        user = tags.get("display-name") or tags.get("login") or "Anonymous"
        bits = int(tags.get("bits") or 0)
        await self.bus.publish(CheerEvent(user=user, bits=bits, message=message or ""))

    async def event_raid(self, channel, tags):
        raider = tags.get("msg-param-displayName") or tags.get("login") or "Unknown"
        viewer_count = int(tags.get("msg-param-viewerCount") or 0)
        await self.bus.publish(RaidEvent(raider=raider, viewer_count=viewer_count))

    async def event_raw_data(self, data: str) -> None:
        if "CLEARMSG" not in data:
            return

        match = _CLEARMSG_RE.match(data)
        if not match:
            return

        tags = dict(
            pair.split("=", 1) for pair in match.group("tags").split(";") if "=" in pair
        )
        user = tags.get("login", "Unknown")
        message_id = tags.get("target-msg-id", "")

        await self.bus.publish(DeletedMessageEvent(user=user, message_id=message_id))
