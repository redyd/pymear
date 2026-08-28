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
from pymear.core.command_router import CommandRouter
from pymear.core.event_exporter import EventExporter
from pymear.core.internal_bus import InternalBus
from pymear.core.twitch_bot import TwitchBot
from pymear.http.interactor import Interactor
from pymear.http.proxy import FeatureProxy
from pymear.utils.logger import VerboseLogger

BANNER = r"""

    ████████  █████ ████ █████████████    ██████   ██████   ████████
    ▒▒███▒▒███▒▒███ ▒███ ▒▒███▒▒███▒▒███  ███▒▒███ ▒▒▒▒▒███ ▒▒███▒▒███
    ▒███ ▒███ ▒███ ▒███  ▒███ ▒███ ▒███ ▒███████   ███████  ▒███ ▒▒▒
    ▒███ ▒███ ▒███ ▒███  ▒███ ▒███ ▒███ ▒███▒▒▒   ███▒▒███  ▒███
    ▒███████  ▒▒███████  █████▒███ █████▒▒██████ ▒▒████████ █████
    ▒███▒▒▒    ▒▒▒▒▒███ ▒▒▒▒▒ ▒▒▒ ▒▒▒▒▒  ▒▒▒▒▒▒   ▒▒▒▒▒▒▒▒ ▒▒▒▒▒
    ▒███       ███ ▒███
    █████     ▒▒██████
    ▒▒▒▒▒       ▒▒▒▒▒▒

v1.0.0
"""

ALL_EVENT_TYPES = (
    ChatMessageEvent,
    FollowEvent,
    SubscriptionEvent,
    GiftSubscriptionEvent,
    CheerEvent,
    RaidEvent,
    DeletedMessageEvent,
)

class PymearServer:
    """
    Hub Entry Point for the Pymear Twitch bot system.

    Single entry point that exposes Broadcaster's WebSocket/SSE stream and starts FeatureProxy
    to unify all features on one port. Coordinates Twitch credentials, bot lifecycle, event routing,
    and command handling across all subsystems.
    """

    def __init__(self, hub_port: int = 8765, proxy_port: int = 9000, verbose: bool = False):
        """
        Initialize the Pymear hub with configurable ports and optional verbose logging.
        Sets up internal bus, broadcaster, and proxy while deferring credential resolution to runtime.
        """
        self.hub_port = hub_port
        self.proxy_port = proxy_port
        self.verbose = verbose

        self._client_id: str | None = None
        self._token: str | None = None
        self._channel: str | None = None
        self._prefix: str | None = None

        self.bus = InternalBus(verbose=verbose)
        self.broadcaster = Broadcaster(verbose=verbose)
        self.broadcaster.subscribe_to(self.bus, *ALL_EVENT_TYPES)
        self.proxy = FeatureProxy(port=self.proxy_port, verbose=verbose)

        self._interactor: Interactor | None = None
        self._bot: TwitchBot | None = None
        self._event_exporter: EventExporter | None = None
        self._command_router: CommandRouter | None = None

        self._event_bot: asyncio.Task | None = None

        self.logger = VerboseLogger(self.__class__.__name__, verbose)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%d/%m %H:%M:%S",
        )
        logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

    @property
    def client_id(self) -> str | None:
        """Return the Twitch Client ID."""
        return self._client_id

    @client_id.setter
    def client_id(self, value: str) -> None:
        """Set the Twitch Client ID directly (bypasses environment lookup)."""
        self._client_id = value

    @property
    def token(self) -> str | None:
        """Return the Twitch OAuth token."""
        return self._token

    @token.setter
    def token(self, value: str) -> None:
        """Set the Twitch OAuth token directly (bypasses environment lookup)."""
        self._token = value

    @property
    def channel(self) -> str | None:
        """Return the Twitch channel name."""
        return self._channel

    @channel.setter
    def channel(self, value: str) -> None:
        """Set the Twitch channel name directly (bypasses environment lookup)."""
        self._channel = value

    @property
    def prefix(self) -> str | None:
        """Return the command prefix for bot messages."""
        return self._prefix

    @prefix.setter
    def prefix(self, value: str) -> None:
        """Set the command prefix for bot messages."""
        self._prefix = value

    def _resolve_credentials(self) -> None:
        """
        Load Twitch credentials from environment variables or use injected values.
        Falls back to .env file and applies sensible defaults where appropriate.
        """
        load_dotenv()

        if self._client_id is None:
            self._client_id = os.getenv("TWITCH_CLIENT_ID")
            if self._client_id:
                self.logger.info("client_id loaded from environment")
            else:
                self.logger.warning("TWITCH_CLIENT_ID not found in environment")

        if self._token is None:
            self._token = os.getenv("TWITCH_TOKEN")
            if self._token:
                self.logger.info("token loaded from environment")
            else:
                self.logger.warning("TWITCH_TOKEN not found in environment")

        if self._channel is None:
            self._channel = os.getenv("TWITCH_CHANNEL")
            if self._channel:
                self.logger.info("channel loaded from environment")
            else:
                self.logger.warning("TWITCH_CHANNEL not found in environment")

        if self._prefix is None:
            env_prefix = os.getenv("TWITCH_PREFIX")
            self._prefix = env_prefix if env_prefix else "!"
            self.logger.info(
                "prefix set to '%s' (%s)",
                self._prefix,
                "environment" if env_prefix else "default",
            )

        self.logger.info_v(f"Credential resolution complete: client_id={'***' if self._client_id else 'None'}, token={'***' if self._token else 'None'}, channel={self._channel}")

    def _build_hub_app(self) -> web.Application:
        """
        Configure and return the aiohttp application with SSE endpoint and lifecycle hooks.
        Sets up event streaming handler and startup/cleanup callbacks for bot management.
        """
        self.logger.info_v("Building hub application with SSE endpoint")
        app = web.Application()

        app.router.add_get("/internal/events", self.broadcaster.sse_handler)
        self.logger.info_v("Registered SSE handler at /internal/events")

        app.on_startup.append(self._start_event_exporter)
        app.on_cleanup.append(self._stop_event_exporter)

        self.logger.info_v("Lifecycle hooks attached: _start_event_exporter, _stop_event_exporter")
        return app

    async def _start_event_exporter(self, app: web.Application) -> None:
        """
        Initialize and start the Twitch bot, interactor, and event exporter on app startup.
        Creates command router and binds error logging to the bot task.
        """
        if not self._client_id or not self._token or not self._channel:
            self.logger.warning("Twitch credentials missing: bot not started")
            return

        self.logger.info("Initializing Twitch bot with credentials")
        self._bot = TwitchBot(
            token=self._token,
            client_id=self._client_id,
            prefix=self._prefix if self._prefix else "!",
            channel=self._channel,
            verbose=self.verbose,
        )

        self.logger.info_v("Creating Interactor instance")
        self._interactor = await Interactor.create(self._client_id, self._token, self._channel, self._bot)

        self.logger.info_v("Instantiating EventExporter")
        self._event_exporter = EventExporter(
            bot=self._bot,
            interactor=self._interactor,
            bus=self.bus,
            verbose=self.verbose,
        )

        self.logger.info_v("Setting up CommandRouter")
        self._command_router = CommandRouter(self._interactor, app, verbose=self.verbose)

        task = asyncio.create_task(self._bot.start())
        task.add_done_callback(self._log_exporter_error)

        self._event_bot = task
        self.logger.info("Event exporter and bot started successfully")

    @staticmethod
    def _log_exporter_error(task: asyncio.Task) -> None:
        """Log any exceptions from the event exporter task if it completed with an error."""
        if task.cancelled():
            return
        if exc := task.exception():
            logger = logging.getLogger(__name__)
            logger.error("EventExporter error: %r", exc)

    async def _stop_event_exporter(self, app: web.Application) -> None:
        """
        Clean up and close all bot-related resources on app shutdown.
        Cancels tasks, closes interactor session, and ensures graceful termination.
        """
        self.logger.info_v("Starting event exporter shutdown sequence")
        if self._bot:
            self.logger.info_v("Closing Twitch bot")
            await self._bot.close()
        if self._event_bot:
            self.logger.info_v("Cancelling event bot task")
            self._event_bot.cancel()
            await asyncio.gather(self._event_bot, return_exceptions=True)
        if self._interactor:
            self.logger.info_v("Closing interactor session")
            await self._interactor.close()
        self.logger.info("Event exporter shutdown complete")

    async def run(self) -> None:
        """
        Start the Pymear hub and proxy servers, blocking until termination.
        Resolves credentials, launches HTTP server, and runs proxy concurrently.
        Ensures cleanup on exit.
        """
        print(BANNER)
        self.logger.info("Pymear hub starting")
        self._resolve_credentials()

        app = self._build_hub_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, port=self.hub_port)
        await site.start()
        self.logger.info("Hub: SSE stream on http://localhost:%s/internal/events", self.hub_port)
        self.logger.info_v(f"Hub port: {self.hub_port}, Proxy port: {self.proxy_port}")

        try:
            self.logger.info("Starting FeatureProxy")
            await self.proxy.run()
        finally:
            self.logger.info("Initiating hub cleanup")
            await runner.cleanup()
            self.logger.info("Hub shut down cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(PymearServer().run())
    except KeyboardInterrupt:
        pass
