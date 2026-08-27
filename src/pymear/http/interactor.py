from __future__ import annotations

import logging
from typing import Self

import aiohttp

HELIX_URL = "https://api.twitch.tv/helix"
logger = logging.getLogger(__name__)


def _clean_token(token: str) -> str:
    return token.replace("oauth:", "")


class Interactor:
    """
    Interact with the Twitch Helix API.
    Use the `create` class method to instantiate, and it should be only called once, on init.
    """

    def __init__(
        self,
        client_id: str,
        token: str,
        session: aiohttp.ClientSession,
    ) -> None:
        self._user_id: str = ""
        self._session = session
        self.client_id = client_id
        self.token = _clean_token(token)
        self._badge_urls: dict[tuple[str, str], str] = {}

    @classmethod
    async def create(cls, client_id: str, token: str, login: str) -> Interactor:
        clean = _clean_token(token)
        headers = {
            "Client-Id": client_id,
            "Authorization": f"Bearer {clean}",
        }
        session = aiohttp.ClientSession(headers=headers)
        instance = cls(client_id, token, session)

        await instance.load_user_id(login)
        await instance.load_badges()

        logger.info(f"Interactor created for user {login}")

        return instance

    async def load_user_id(self, login: str) -> str:
        async with self._session.get(f"{HELIX_URL}/users", params={"login": login}) as resp:
            data = await resp.json()
            self._user_id = data["data"][0]["id"]
            return self._user_id

    async def close(self) -> None:
        await self._session.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    # BADGE RESOLVER INTEGRATION

    async def load_badges(self) -> None:
        """Load global and channel badges into cache."""
        async with self._session.get(f"{HELIX_URL}/chat/badges/global") as resp:
            data = await resp.json()
            self._merge_badges(data.get("data", []))

        async with self._session.get(
            f"{HELIX_URL}/chat/badges",
            params={"broadcaster_id": self._user_id},
        ) as resp:
            data = await resp.json()
            self._merge_badges(data.get("data", []))

    def resolve_badges(self, badges: dict[str, str]) -> list[str]:
        """Sync dict lookup for badge URLs, zero network cost."""
        return [
            self._badge_urls[(name, version)]
            for name, version in badges.items()
            if (name, version) in self._badge_urls
        ]

    def _merge_badges(self, badge_sets: list) -> None:
        for badge_set in badge_sets:
            set_id = badge_set["set_id"]
            for version in badge_set["versions"]:
                self._badge_urls[(set_id, version["id"])] = version["image_url_2x"]

    # METHODS

    async def get_subscriber_count(self) -> int:
        async with self._session.get(
            f"{HELIX_URL}/subscriptions",
            params={"broadcaster_id": self._user_id},
        ) as resp:
            data = await resp.json()
            return data["total"]

    async def get_follower_count(self) -> int:
        async with self._session.get(
            f"{HELIX_URL}/channels/followers",
            params={"broadcaster_id": self._user_id},
        ) as resp:
            data = await resp.json()
            return data["total"]

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
        payload = {
            "broadcaster_id": self._user_id,
            "title": title,
            "choices": [{"title": c} for c in choices],
            "duration": duration,
            "channel_points_voting_enabled": channel_points_voting_enabled,
            "channel_points_per_vote": channel_points_per_vote,
            "bits_voting_enabled": bits_voting_enabled,
            "bits_per_vote": bits_per_vote,
        }
        async with self._session.post(f"{HELIX_URL}/polls", json=payload) as resp:
            data = await resp.json()
            return data["data"][0]["id"]
