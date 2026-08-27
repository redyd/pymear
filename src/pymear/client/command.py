from __future__ import annotations

import logging
from typing import cast

import aiohttp

from pymear.contracts.commands import CreatePollRequest

logger = logging.getLogger(__name__)


class Command:
    """
    Client-side interface to the hub's CommandRouter.
    Each method maps to one HTTP route exposed by the hub.
    """

    def __init__(self, hub_port: int = 8765) -> None:
        self._base_url = f"http://localhost:{hub_port}/commands"

    async def _request(
        self,
        method: str,
        path: str,
        payload: dict | None = None
    ) -> dict:
        url = f"{self._base_url}/{path}"

        async with (
            aiohttp.ClientSession() as session,
            session.request(method, url, json=payload) as resp
        ):
            resp.raise_for_status()
            return await resp.json()

    async def _post(self, path: str, payload: dict) -> dict:
        return await self._request("POST", path, payload)

    async def _get(self, path: str) -> dict:
        return await self._request("GET", path)

    async def create_poll(
        self,
        title: str,
        choices: list[str],
        duration: int,
        channel_points_voting_enabled: bool = True,
        channel_points_per_vote: int = 0,
        bits_voting_enabled: bool = False,
        bits_per_vote: int = 0,
    ) -> str:
        req = CreatePollRequest(
            title=title,
            choices=choices,
            duration=duration,
            channel_points_voting_enabled=channel_points_voting_enabled,
            channel_points_per_vote=channel_points_per_vote,
            bits_voting_enabled=bits_voting_enabled,
            bits_per_vote=bits_per_vote,
        )
        return cast(str, await self._post("poll", req.to_dict()))

    async def get_follower_count(self) -> int:
        return cast(int, await self._get("follower_count"))

    async def get_subscriber_count(self) -> int:
        return cast(int, await self._get("subscriber_count"))
