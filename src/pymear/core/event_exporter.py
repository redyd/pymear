import logging
import re

from twitchio.chatter import Chatter

from pymear.contracts.events import *
from pymear.core.internal_bus import InternalBus
from pymear.core.twitch_bot import TwitchBot
from pymear.http.interactor import Interactor

_CLEARMSG_RE = re.compile(r"^@(?P<tags>\S+) :tmi\.twitch\.tv CLEARMSG #\S+ :(?P<text>.*)$")

logger = logging.getLogger(__name__)


class EventExporter:
    """Listens to the raw Twitch events forwarded by TwitchBot and publishes typed events on the bus."""

    def __init__(self, bot: TwitchBot, bus: InternalBus, interactor: Interactor):
        self.bus = bus
        self.interactor = interactor
        bot.add_listener(self._on_event)

    async def _on_event(self, kind: str, *args) -> None:
        handler = getattr(self, f"_handle_{kind}", None)
        if handler:
            await handler(*args)

    async def _handle_message(self, message):
        author = message.author
        default_color = "#a970ff"
        if isinstance(author, Chatter):
            badges = self.interactor.resolve_badges(author.badges or {})
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
                message_id=message.id,
            )
        )

    async def _handle_follow(self, channel, user):
        await self.bus.publish(FollowEvent(user=user.name))

    async def _handle_subscription(self, user, channel, tags):
        months = int(tags.get("msg-param-cumulative-months") or 1)
        tier = tags.get("msg-param-sub-plan") or "1000"
        await self.bus.publish(SubscriptionEvent(user=user.name, months=months, tier=tier))

    async def _handle_subscription_gift(self, channel, tags):
        gifter = tags.get("display-name") or tags.get("login") or "Anonymous"
        recipient = tags.get("msg-param-recipient-display-name") or "someone"
        total = int(tags.get("msg-param-sender-count") or 1)
        tier = tags.get("msg-param-sub-plan") or "1000"
        await self.bus.publish(
            GiftSubscriptionEvent(gifter=gifter, recipient=recipient, gifter_total=total, tier=tier)
        )

    async def _handle_cheer(self, channel, tags, message):
        user = tags.get("display-name") or tags.get("login") or "Anonymous"
        bits = int(tags.get("bits") or 0)
        await self.bus.publish(CheerEvent(user=user, bits=bits, message=message or ""))

    async def _handle_raid(self, channel, tags):
        raider = tags.get("msg-param-displayName") or tags.get("login") or "Unknown"
        viewer_count = int(tags.get("msg-param-viewerCount") or 0)
        await self.bus.publish(RaidEvent(raider=raider, viewer_count=viewer_count))

    async def _handle_raw_data(self, data: str) -> None:
        if "CLEARMSG" not in data:
            return
        match = _CLEARMSG_RE.match(data)
        if not match:
            return
        tags = dict(pair.split("=", 1) for pair in match.group("tags").split(";") if "=" in pair)
        user = tags.get("login", "Unknown")
        message_id = tags.get("target-msg-id", "")
        await self.bus.publish(DeletedMessageEvent(user=user, message_id=message_id))
