from __future__ import annotations

import asyncio
import json

import aiohttp
from aiohttp import web

from pymear.utils.logger import VerboseLogger

_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "transfer-encoding",
    "upgrade",
    "content-length",
}


class FeatureProxy:
    """
    Proxy that unifies all features on a single port, routing requests to registered backend services.
    Supports both HTTP and WebSocket connections with dynamic route registration via WebSocket.
    """

    def __init__(self, port: int = 9000, verbose: bool = False, ws_heartbeat: float = 15):
        """Initialize the proxy with a listening port and optional verbose logging."""
        self.port = port
        self.ws_heartbeat = ws_heartbeat
        self._routes: dict[str, int] = {}
        self._session: aiohttp.ClientSession | None = None
        self.logger = VerboseLogger(self.__class__.__name__, verbose)

    async def run(self, app: web.Application) -> None:
        """
        Start the proxy server and begin accepting incoming requests.
        Blocks until the process is terminated, then shuts down cleanly.
        """
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=30)
        self._session = aiohttp.ClientSession(timeout=timeout)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, port=self.port)
        await site.start()
        self.logger.info("Proxy started on http://localhost:%s", self.port)
        self.logger.info_v(f"Registered routes: {list(self._routes.keys())}")

        try:
            await asyncio.Event().wait()
        finally:
            await self._session.close()
            await runner.cleanup()
            self.logger.info("Proxy shut down cleanly")

    def build_app(self) -> web.Application:
        """
        Configure and return the aiohttp application with routing rules.
        Sets up handlers for feature registration and request proxying.
        """
        self.logger.info_v("Building proxy application with routes")
        app = web.Application()
        app.router.add_get("/register", self._register_handler)
        app.router.add_route("*", "/{name}", self._proxy_handler)
        app.router.add_route("*", "/{name}/{tail:.*}", self._proxy_handler)
        self.logger.info_v(
            "Route handlers configured: /register, /{name}, /{name}/{tail}"
        )
        return app

    async def _register_handler(self, request: web.Request) -> web.WebSocketResponse:
        """
        Handle WebSocket connections from features that want to register with the proxy.
        Maintains route mappings while the connection is active and cleans up on disconnect.
        """
        self.logger.info_v("WebSocket registration handler invoked")
        ws = web.WebSocketResponse(heartbeat=self.ws_heartbeat)
        await ws.prepare(request)

        registered_name: str | None = None
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            try:
                payload = json.loads(msg.data)
                registered_name = payload["name"]
                port = payload["port"]
            except (ValueError, KeyError) as e:
                self.logger.warning_v(f"Invalid registration message: {e}")
                continue

            if registered_name is not None:
                old_port = self._routes.get(registered_name)
                self._routes[registered_name] = port
                self.logger.info(
                    f"Feature '{registered_name}' registered on port {port}"
                )
                if old_port is not None and old_port != port:
                    self.logger.warning_v(
                        f"Overwriting previous registration for '{registered_name}' (port {old_port})"
                    )

        if registered_name is not None:
            self._routes.pop(registered_name, None)
            self.logger.info(f"Feature '{registered_name}' disconnected, route removed")

        self.logger.info_v("Registration WebSocket closed")
        return ws

    async def _proxy_handler(self, request: web.Request) -> web.StreamResponse:
        """
        Route HTTP requests to the appropriate backend service based on the feature name.
        Handles both standard HTTP and WebSocket upgrade requests.
        """
        if request.headers.get("Upgrade", "").lower() == "websocket":
            self.logger.info_v(
                f"WebSocket proxy requested for feature: {request.match_info['name']}"
            )
            return await self._proxy_ws_handler(request)

        name = request.match_info["name"]
        tail = request.match_info.get("tail", "")
        target_port = self._routes.get(name)
        if target_port is None:
            self.logger.warning(f"Feature '{name}' not found in route table")
            return web.Response(status=502, text=f"Feature '{name}' unavailable")

        target_url = f"http://localhost:{target_port}/{tail}"
        if request.query_string:
            target_url += f"?{request.query_string}"

        self.logger.info_v(f"Proxying {request.method} request to {target_url}")
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
            self.logger.info_v(f"Upstream responded with status {upstream.status}")
            response = web.StreamResponse(
                status=upstream.status, headers=response_headers
            )
            await response.prepare(request)
            try:
                async for chunk in upstream.content.iter_any():
                    await response.write(chunk)
            except (ConnectionResetError, aiohttp.ClientConnectionResetError) as e:
                self.logger.error_v(
                    f"Client disconnected during response streaming: {e}"
                )
        self.logger.info_v("Request proxying completed")
        return response

    async def _proxy_ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        """
        Relay a WebSocket connection between the client and the target feature.
        Bidirectionally forwards messages until either side disconnects or errors.
        """
        name = request.match_info["name"]
        tail = request.match_info.get("tail", "")
        target_port = self._routes.get(name)
        if target_port is None:
            self.logger.warning(f"Feature '{name}' not available for WebSocket proxy")
            raise web.HTTPBadGateway(text=f"Feature '{name}' unavailable")

        target_url = f"ws://localhost:{target_port}/{tail}"
        if request.query_string:
            target_url += f"?{request.query_string}"

        self.logger.info_v(f"Establishing WebSocket relay to {target_url}")
        client_ws = web.WebSocketResponse(heartbeat=self.ws_heartbeat)
        await client_ws.prepare(request)

        assert self._session is not None
        try:
            upstream_ws_context = self._session.ws_connect(
                target_url, heartbeat=self.ws_heartbeat
            )
            async with upstream_ws_context as upstream_ws:
                self.logger.info_v(
                    "WebSocket relay established, forwarding messages bidirectionally"
                )

                async def forward_to_upstream() -> str:
                    """Forward client messages to the backend service."""
                    async for msg in client_ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await upstream_ws.send_str(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            await upstream_ws.send_bytes(msg.data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            self.logger.info_v("Client WebSocket errored")
                            return "client_error"
                    return "client_closed"

                async def forward_to_client() -> str:
                    """Forward backend messages to the client."""
                    async for msg in upstream_ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await client_ws.send_str(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            await client_ws.send_bytes(msg.data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            self.logger.info_v("Upstream WebSocket errored")
                            return "upstream_error"
                    return "upstream_closed"

                task_up = asyncio.create_task(forward_to_upstream())
                task_down = asyncio.create_task(forward_to_client())
                done, pending = await asyncio.wait(
                    [task_up, task_down], return_when=asyncio.FIRST_COMPLETED
                )
                reason = "unknown"
                for task in done:
                    try:
                        reason = task.result()
                    except Exception as exc:  # noqa: BLE001
                        reason = f"relay_error:{type(exc).__name__}"
                        self.logger.warning("WebSocket relay task failed: %r", exc)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                await upstream_ws.close()
                await client_ws.close()
                self.logger.info_v("WebSocket relay closing: %s", reason)
        except aiohttp.ClientError as exc:
            self.logger.warning("WebSocket upstream unavailable for '%s': %r", name, exc)
            await client_ws.close()

        self.logger.info_v("WebSocket relay terminated")
        return client_ws
