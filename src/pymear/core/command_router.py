from __future__ import annotations

import inspect

from aiohttp import web

from pymear.http.interactor import Interactor
from pymear.utils.logger import VerboseLogger

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
    HTTP Route Registration Layer for Interactor Commands.

    Dynamically introspects an Interactor instance and exposes its methods as REST
    endpoints on the hub's aiohttp application. Provides a thin mapping layer between
    HTTP verbs and Python method naming conventions.

    Core Capabilities:
        - Auto-register HTTP routes from Interactor method signatures
        - Map method prefixes to HTTP verbs (create→POST, get→GET, update→PUT, delete→DELETE)
        - Parse JSON request bodies for POST/PUT methods
        - Wrap interactor calls with error handling and 500 responses
        - Exclude internal methods via blacklist

    Method Naming Convention:
        - `create_X` → POST /commands/X
        - `get_X` → GET /commands/X
        - `update_X` → PUT /commands/X
        - `delete_X` → DELETE /commands/X

    Usage:
        router = CommandRouter(interactor=my_interactor, app=hub_app)
        # Routes automatically registered on /commands/<suffix>
    """

    def __init__(self, interactor: Interactor, app: web.Application, verbose: bool = False) -> None:
        self._interactor = interactor
        self.logger = VerboseLogger(self.__class__.__name__, verbose)
        self.logger.info("CommandRouter initializing on hub app")
        self._register(app)

    def _register(self, app: web.Application) -> None:
        """
        Scan Interactor for eligible methods and register corresponding HTTP routes.

        Iterates over all coroutine functions on the Interactor, skips excluded methods,
        matches prefixes to HTTP verbs, and installs handlers on the aiohttp router.
        """
        self.logger.info("Scanning Interactor for command methods")
        count = 0
        for name, method in inspect.getmembers(
            self._interactor, predicate=inspect.iscoroutinefunction
        ):
            if name in _EXCLUDED:
                self.logger.info_v("Excluding method %s", name)
                continue
            for prefix, http_method in _METHOD_MAP.items():
                if name.startswith(f"{prefix}_"):
                    suffix = name[len(prefix) + 1 :]
                    path = f"/commands/{suffix}"
                    handler = self._make_handler(method, http_method)
                    app.router.add_route(http_method, path, handler)
                    self.logger.info("%s %s -> %s", http_method, path, name)
                    self.logger.info_v(
                        "Route registered: %s %s maps to %s()", http_method, path, name
                    )
                    count += 1
                    break
        self.logger.info("Total routes registered: %d", count)

    def _make_handler(self, interactor_method, http_method: str):
        """
        Create a closure that wraps an Interactor method with HTTP request/response logic.

        Handles:
            - JSON body parsing for POST/PUT requests
            - Argument unpacking from request body
            - Success response serialization
            - Error handling with 500 JSON responses

        Returns:
            Callable handler compatible with aiohttp routing
        """

        async def handler(request: web.Request) -> web.Response:
            try:
                if http_method in _BODY_METHODS:
                    body = await request.json()
                    self.logger.info_v(
                        "Parsed request body for %s: %d keys",
                        interactor_method.__name__,
                        len(body),
                    )
                    result = await interactor_method(**body)
                else:
                    self.logger.info_v(
                        "Calling %s with no arguments", interactor_method.__name__
                    )
                    result = await interactor_method()

                self.logger.info_v(
                    "Returning success response from %s", interactor_method.__name__
                )
                return web.json_response(result)

            except Exception:  # noqa: BLE001
                self.logger.error("Error in %s handler", interactor_method.__name__)
                self.logger.error_v(
                    "Full traceback for %s failure", interactor_method.__name__
                )
                return web.json_response({"error": "internal error"}, status=500)

        return handler
