from __future__ import annotations

from typing import cast

import aiohttp

from pymear.contracts.commands import CreatePollRequest
from pymear.utils.logger import VerboseLogger


class Command:
    """
    Client-side interface for managing Twitch channel operations via the hub's CommandRouter.

    This class provides programmatic access to core streaming features including:
        - Poll creation with configurable voting options (channel points, bits)
        - Channel analytics retrieval (follower count, subscriber count)

    All operations are executed asynchronously over HTTP/REST and support
    verbose logging for debugging and audit trails.

    Attributes:
        _base_url (str): The base endpoint URL for the hub's command router
        logger (VerboseLogger): Logging utility for operation tracking
    """

    def __init__(self, hub_port: int = 8765, verbose: bool = False) -> None:
        self._base_url = f"http://localhost:{hub_port}/commands"
        self.logger = VerboseLogger(self.__class__.__name__, verbose)

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
        channel_points_voting_enabled: bool = False,
        channel_points_per_vote: int = 0,
        bits_voting_enabled: bool = False,
        bits_per_vote: int = 0,
    ) -> str:
        """
        Create a new interactive poll for the connected Twitch channel.

        Configures voter engagement by enabling channel points or bits as
        voting currencies. Supports custom vote costs per option.

        Args:
            title: Display name for the poll
            choices: List of selectable poll options
            duration: Poll lifetime in seconds
            channel_points_voting_enabled: Allow channel points for voting (default: False)
            channel_points_per_vote: Cost in channel points per vote (default: 0)
            bits_voting_enabled: Allow bits for voting (default: False)
            bits_per_vote: Cost in bits per vote (default: 0)

        Returns:
            str: Unique identifier for the created poll

        Example:
            >>> poll_id = await client.create_poll(
            ...     title="Next Game?",
            ...     choices=["Valorant", "Apex", "Fortnite"],
            ...     duration=300
            ... )
        """
        req = CreatePollRequest(
            title=title,
            choices=choices,
            duration=duration,
            channel_points_voting_enabled=channel_points_voting_enabled,
            channel_points_per_vote=channel_points_per_vote,
            bits_voting_enabled=bits_voting_enabled,
            bits_per_vote=bits_per_vote,
        )
        self.logger.info_v(f"Creating poll: {title}")
        res = cast(str, await self._post("poll", req.to_dict()))
        self.logger.info_v(f"Poll created: {res}")
        return res

    async def get_follower_count(self) -> int:
        """
        Retrieve the total number of followers for the connected channel.

        Returns:
            int: Current follower count

        Note:
            Value reflects real-time data from the hub at call time.
        """
        self.logger.info_v("Getting follower count")
        res = cast(int, await self._get("follower_count"))
        self.logger.info_v(f"Follower count: {res}")
        return res

    async def get_subscriber_count(self) -> int:
        """
        Retrieve the total number of subscribers for the connected channel.

        Returns:
            int: Current subscriber count

        Note:
            Value reflects real-time data from the hub at call time.
        """
        self.logger.info_v("Getting subscriber count")
        res = cast(int, await self._get("subscriber_count"))
        self.logger.info_v(f"Subscriber count: {res}")
        return res
