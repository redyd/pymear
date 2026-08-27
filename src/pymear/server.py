from __future__ import annotations

import asyncio
import logging
import os

from aiohttp import web
from dotenv import load_dotenv

from pymear.contracts.events import (
    ChatMessageEvent,
    CheerEvent,
    DeletedMessageEvent,
    FollowEvent,
    GiftSubscriptionEvent,
    RaidEvent,
    SubscriptionEvent,
)
from pymear.core.broadcaster import Broadcaster
from pymear.core.event_exporter import EventExporter
from pymear.core.internal_bus import InternalBus
from pymear.core.twitch_bot import TwitchBot
from pymear.http.interactor import Interactor
from pymear.http.proxy import FeatureProxy

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

ALL_EVENT_TYPES = (
    ChatMessageEvent,
    FollowEvent,
    SubscriptionEvent,
    GiftSubscriptionEvent,
    CheerEvent,
    RaidEvent,
    DeletedMessageEvent,
)


class Pymear:
    """
    Hub Entry Point:

    - Single entry point: exposes Broadcaster's WebSocket and starts FeatureProxy to unify all features on one port.
    - hub_port & proxy_port: sensible defaults, injectable.
    - Twitch credentials (client_id, token, channel) + prefix: no safe defaults—set via properties or fall back to .env at startup.
    """

    def __init__(self, hub_port: int = 8765, proxy_port: int = 9000):
        self.hub_port = hub_port
        self.proxy_port = proxy_port

        self._client_id: str | None = None
        self._token: str | None = None
        self._channel: str | None = None
        self._prefix: str | None = None

        self.bus = InternalBus()
        self.broadcaster = Broadcaster()
        self.broadcaster.subscribe_to(self.bus, *ALL_EVENT_TYPES)
        self.proxy = FeatureProxy(port=self.proxy_port)

        self._interactor: Interactor | None = None
        self._bot: TwitchBot | None = None
        self._event_exporter: EventExporter | None = None

        self._event_bot: asyncio.Task | None = None

    @property
    def client_id(self) -> str | None:
        return self._client_id

    @client_id.setter
    def client_id(self, value: str) -> None:
        self._client_id = value

    @property
    def token(self) -> str | None:
        return self._token

    @token.setter
    def token(self, value: str) -> None:
        self._token = value

    @property
    def channel(self) -> str | None:
        return self._channel

    @channel.setter
    def channel(self, value: str) -> None:
        self._channel = value

    @property
    def prefix(self) -> str | None:
        return self._prefix

    @prefix.setter
    def prefix(self, value: str) -> None:
        self._prefix = value

    def _resolve_credentials(self) -> None:
        load_dotenv()

        if self._client_id is None:
            self._client_id = os.getenv("TWITCH_CLIENT_ID")
            if self._client_id:
                logger.info("client_id loaded from environment")

        if self._token is None:
            self._token = os.getenv("TWITCH_TOKEN")
            if self._token:
                logger.info("token loaded from environment")

        if self._channel is None:
            self._channel = os.getenv("TWITCH_CHANNEL")
            if self._channel:
                logger.info("channel loaded from environment")

        if self._prefix is None:
            env_prefix = os.getenv("TWITCH_PREFIX")
            self._prefix = env_prefix if env_prefix else "!"
            logger.info(
                "prefix set to '%s' (%s)",
                self._prefix,
                "environment" if env_prefix else "default",
            )

    def _build_hub_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/ws", self.broadcaster.websocket_handler)
        app.on_startup.append(self._start_event_exporter)
        app.on_cleanup.append(self._stop_event_exporter)
        return app

    async def _start_event_exporter(self, app: web.Application) -> None:
        if not self._client_id or not self._token or not self._channel:
            logger.warning("Twitch credentials missing: bot not started")
            return

        self._interactor = await Interactor.create(self._client_id, self._token, self._channel)
        self._bot = TwitchBot(
            token=self._token,
            client_id=self._client_id,
            prefix=self._prefix if self._prefix else "!",
            channel=self._channel
        )
        self._event_exporter = EventExporter(
            bot=self._bot,
            interactor=self._interactor,
            bus=self.bus,
        )

        task = asyncio.create_task(self._bot.start())
        task.add_done_callback(self._log_exporter_error)

        self._event_bot = task

    @staticmethod
    def _log_exporter_error(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        if exc := task.exception():
            logger.error("EventExporter error: %r", exc)

    async def _stop_event_exporter(self, app: web.Application) -> None:
        if self._event_bot:
            self._event_bot.cancel()
            await asyncio.gather(self._event_bot, return_exceptions=True)

    async def run(self) -> None:
        self._resolve_credentials()

        app = self._build_hub_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, port=self.hub_port)
        await site.start()
        logger.info("Hub: websocket on ws://localhost:%s/ws", self.hub_port)

        await self.proxy.run()


if __name__ == "__main__":
    asyncio.run(Pymear().run())
