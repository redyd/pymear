from __future__ import annotations

import inspect
import logging

from aiohttp import web
from twitchio.websocket import log

from pymear.http.interactor import Interactor

logger = logging.getLogger(__name__)

_METHOD_MAP = {
    "create": "POST",
    "get": "GET",
    "update": "PUT",
    "delete": "DELETE",
}

_BODY_METHODS = {"POST", "PUT"}
_EXCLUDED = {"load_user_id", "load_badges", "close"}


class CommandRouter:
    """
    Sits in front of Interactor and registers HTTP routes on the hub app.
    Introspects Interactor methods prefixed with create_/get_/update_/delete_
    and maps them to POST/GET/PUT/DELETE on /commands/<suffix>.
    POST and PUT bodies are parsed as raw JSON and passed as kwargs to the method.
    """

    def __init__(self, interactor: Interactor, app: web.Application) -> None:
        logger.info("CommandRouter: registering routes")
        self._interactor = interactor
        self._register(app)

    def _register(self, app: web.Application) -> None:
        for name, method in inspect.getmembers(self._interactor, predicate=inspect.iscoroutinefunction):
            if name in _EXCLUDED:
                continue
            for prefix, http_method in _METHOD_MAP.items():
                if name.startswith(f"{prefix}_"):
                    suffix = name[len(prefix) + 1:]
                    path = f"/commands/{suffix}"
                    handler = self._make_handler(method, http_method)
                    app.router.add_route(http_method, path, handler)
                    logger.info("CommandRouter: %s %s -> %s", http_method, path, name)
                    break

    def _make_handler(self, interactor_method, http_method: str):
        async def handler(request: web.Request) -> web.Response:
            try:
                if http_method in _BODY_METHODS:
                    body = await request.json()
                    result = await interactor_method(**body)
                else:
                    result = await interactor_method()

                return web.json_response(result)

            except Exception:
                logger.exception("CommandRouter: error handling %s", interactor_method.__name__)
                return web.json_response({"error": "internal error"}, status=500)

        return handler
