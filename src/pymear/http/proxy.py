from __future__ import annotations

import asyncio
import json
import logging

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "transfer-encoding",
    "upgrade",
    "content-length",
}


class FeatureProxy:
    """
    Proxy that unifies all features on a single port.
    """

    def __init__(self, port: int = 9000):
        self.port = port
        self._routes: dict[str, int] = {}
        self._session: aiohttp.ClientSession | None = None

    async def run(self) -> None:
        self._session = aiohttp.ClientSession()
        app = self._build_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, port=self.port)
        await site.start()
        logger.info("Proxy: routing on http://localhost:%s", self.port)

        await asyncio.Event().wait()

    def _build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/register", self._register_handler)
        app.router.add_route("*", "/{name}", self._proxy_handler)
        app.router.add_route("*", "/{name}/{tail:.*}", self._proxy_handler)
        return app

    async def _register_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        registered_name: str | None = None
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            try:
                payload = json.loads(msg.data)
                registered_name = payload["name"]
                port = payload["port"]
            except (ValueError, KeyError):
                continue

            if registered_name is not None:
                self._routes[registered_name] = port
                logger.info("Proxy: '%s' registered on port %s", registered_name, port)

        if registered_name is not None:
            self._routes.pop(registered_name, None)
            logger.info("Proxy: '%s' disconnected, route removed", registered_name)

        return ws

    async def _proxy_handler(self, request: web.Request) -> web.StreamResponse:
        name = request.match_info["name"]
        tail = request.match_info.get("tail", "")
        target_port = self._routes.get(name)

        if target_port is None:
            return web.Response(status=502, text=f"Feature '{name}' unavailable")

        target_url = f"http://localhost:{target_port}/{tail}"
        if request.query_string:
            target_url += f"?{request.query_string}"

        headers = {
            k: v for k, v in request.headers.items() if k.lower() not in _HOP_BY_HOP
        }

        assert self._session is not None
        async with self._session.request(
            request.method, target_url, headers=headers, data=request.content
        ) as upstream:
            response_headers = {
                k: v
                for k, v in upstream.headers.items()
                if k.lower() not in _HOP_BY_HOP
            }
            response = web.StreamResponse(
                status=upstream.status, headers=response_headers
            )
            await response.prepare(request)
            try:
                async for chunk in upstream.content.iter_any():
                    await response.write(chunk)
            except (ConnectionResetError, aiohttp.ClientConnectionResetError):
                pass

        return response
