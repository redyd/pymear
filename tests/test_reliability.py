from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pymear.client.feature import OVERLAY_HELPER, Feature
from pymear.contracts.events import ChatMessageEvent
from pymear.http.proxy import FeatureProxy


class FeatureReliabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_hub_listener_uses_unlimited_total_timeout(self) -> None:
        feature = Feature(
            name="chat",
            event_types=[ChatMessageEvent],
            static_dir=ROOT,
            port=12345,
        )

        async def empty_stream(session: aiohttp.ClientSession, url: str):
            if False:
                yield ""

        with (
            patch.object(feature, "_consume_sse_with_retry", empty_stream),
            patch("aiohttp.ClientSession", wraps=aiohttp.ClientSession) as session_factory,
        ):
            await feature._listen_hub()

        timeout = session_factory.call_args.kwargs["timeout"]
        self.assertIsNone(timeout.total)
        self.assertEqual(timeout.sock_connect, 30)

    async def test_full_client_queue_drops_oldest_payload(self) -> None:
        feature = Feature(
            name="chat",
            event_types=[ChatMessageEvent],
            static_dir=ROOT,
            port=12345,
            queue_maxsize=1,
        )
        feature.add_source("chat")
        source = feature._sources["chat"]

        import asyncio

        client_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=1)
        client_queue.put_nowait({"value": "old"})
        source.queues.add(client_queue)

        await feature.send("chat", {"value": "new"})

        self.assertEqual(client_queue.get_nowait(), {"value": "new"})

    def test_overlay_helper_exists_and_is_routed_before_static_catchall(self) -> None:
        feature = Feature(
            name="chat",
            event_types=[ChatMessageEvent],
            static_dir=ROOT,
            port=12345,
        )
        app = feature._build_app()

        self.assertTrue(OVERLAY_HELPER.exists())
        paths = [resource.canonical for resource in app.router.resources()]
        self.assertIn("/pymear-overlay.js", paths)
        self.assertLess(paths.index("/pymear-overlay.js"), paths.index("/"))


class ProxyReliabilityTests(unittest.TestCase):
    def test_proxy_defaults_to_websocket_heartbeat(self) -> None:
        proxy = FeatureProxy()

        self.assertEqual(proxy.ws_heartbeat, 15)


if __name__ == "__main__":
    unittest.main()
