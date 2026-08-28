from __future__ import annotations

from typing import Self

import aiohttp

from pymear.core.twitch_bot import TwitchBot
from pymear.utils.logger import VerboseLogger

HELIX_URL = "https://api.twitch.tv/helix"


def _clean_token(token: str) -> str:
    """Remove oauth: prefix from token."""
    return token.replace("oauth:", "")


class Interactor:
    """
    Interact with the Twitch Helix API for channel operations including badges, subscriptions, followers, polls, and messaging.
    Use the `create` class method to instantiate with proper initialization sequence.
    """

    def __init__(
        self,
        client_id: str,
        token: str,
        session: aiohttp.ClientSession,
        bot: TwitchBot,
        verbose: bool = False,
    ) -> None:
        self._user_id: str = ""
        self._session = session
        self.client_id = client_id
        self.token = _clean_token(token)
        self._badge_urls: dict[tuple[str, str], str] = {}
        self._bot = bot
        self.logger = VerboseLogger(self.__class__.__name__, verbose)

    @classmethod
    async def create(
        cls,
        client_id: str,
        token: str,
        login: str,
        bot: TwitchBot,
        verbose: bool = False,
    ) -> Interactor:
        """
        Create and initialize an Interactor instance with loaded user ID and badge cache.
        Performs necessary API calls during construction to prepare the instance for use.
        """
        clean = _clean_token(token)
        headers = {
            "Client-Id": client_id,
            "Authorization": f"Bearer {clean}",
        }
        session = aiohttp.ClientSession(headers=headers)
        instance = cls(client_id, token, session, bot, verbose)

        await instance.load_user_id(login)
        await instance.load_badges()

        instance.logger.info(f"Interactor created for user {login}")
        instance.logger.info_v(f"Loaded {len(instance._badge_urls)} badge variants")

        return instance

    async def load_user_id(self, login: str) -> str:
        """
        Fetch and store the Twitch user ID for the given login name.
        Returns the user ID string upon successful resolution.
        """
        self.logger.info_v(f"Resolving user ID for login: {login}")
        async with self._session.get(
            f"{HELIX_URL}/users", params={"login": login}
        ) as resp:
            if resp.status != 200:
                self.logger.error(
                    f"Failed to resolve user ID for {login}: HTTP {resp.status}"
                )
                raise RuntimeError(f"User resolution failed: {resp.status}")
            data = await resp.json()
            if not data.get("data"):
                self.logger.error(f"No user found for login: {login}")
                raise ValueError(f"User not found: {login}")
            self._user_id = data["data"][0]["id"]
            self.logger.info_v(f"Resolved user ID: {self._user_id}")
            return self._user_id

    async def close(self) -> None:
        """Close the aiohttp session and release associated resources."""
        self.logger.info_v("Closing HTTP session")
        await self._session.close()

    async def __aenter__(self) -> Self:
        """Enter async context manager."""
        return self

    async def __aexit__(self, *_) -> None:
        """Exit async context manager and cleanup resources."""
        await self.close()

    # BADGE RESOLVER INTEGRATION

    async def load_badges(self) -> None:
        """
        Load global and channel-specific chat badges into internal cache.
        Merges both badge sets for unified badge resolution during chat operations.
        """
        self.logger.info_v("Loading global badges")
        async with self._session.get(f"{HELIX_URL}/chat/badges/global") as resp:
            if resp.status != 200:
                self.logger.warning(f"Failed to load global badges: HTTP {resp.status}")
            else:
                data = await resp.json()
                self._merge_badges(data.get("data", []))

        self.logger.info_v(f"Loading channel badges for broadcaster {self._user_id}")
        async with self._session.get(
            f"{HELIX_URL}/chat/badges",
            params={"broadcaster_id": self._user_id},
        ) as resp:
            if resp.status != 200:
                self.logger.warning(
                    f"Failed to load channel badges: HTTP {resp.status}"
                )
            else:
                data = await resp.json()
                self._merge_badges(data.get("data", []))

        self.logger.info_v(
            f"Badge cache initialized with {len(self._badge_urls)} entries"
        )

    def resolve_badges(self, badges: dict[str, str]) -> list[str]:
        """
        Resolve badge identifiers to image URLs using cached badge data.
        Returns list of matching badge URLs with zero network cost.
        """
        matched = [
            self._badge_urls[(name, version)]
            for name, version in badges.items()
            if (name, version) in self._badge_urls
        ]
        self.logger.info_v(f"Resolved {len(matched)}/{len(badges)} badges")
        return matched

    def _merge_badges(self, badge_sets: list) -> None:
        """
        Merge badge set data into internal URL mapping dictionary.
        Maps (set_id, version_id) tuples to 2x image URLs for quick lookup.
        """
        count_before = len(self._badge_urls)
        for badge_set in badge_sets:
            set_id = badge_set["set_id"]
            for version in badge_set["versions"]:
                self._badge_urls[(set_id, version["id"])] = version["image_url_2x"]
        self.logger.info_v(
            f"Merged {len(badge_sets)} badge sets ({len(self._badge_urls) - count_before} new entries)"
        )

    # METHODS

    async def get_subscriber_count(self) -> int:
        """
        Fetch total subscriber count for the broadcaster channel.
        Returns the total subscription count from Twitch API.
        """
        self.logger.info_v(f"Fetching subscriber count for broadcaster {self._user_id}")
        async with self._session.get(
            f"{HELIX_URL}/subscriptions",
            params={"broadcaster_id": self._user_id},
        ) as resp:
            if resp.status != 200:
                self.logger.error(f"Failed to fetch subscribers: HTTP {resp.status}")
                raise RuntimeError(f"Subscription fetch failed: {resp.status}")
            data = await resp.json()
            count = data.get("total", 0)
            self.logger.info_v(f"Subscriber count: {count}")
            return count

    async def get_follower_count(self) -> int:
        """
        Fetch total follower count for the broadcaster channel.
        Returns the total follower count from Twitch API.
        """
        self.logger.info_v(f"Fetching follower count for broadcaster {self._user_id}")
        async with self._session.get(
            f"{HELIX_URL}/channels/followers",
            params={"broadcaster_id": self._user_id},
        ) as resp:
            if resp.status != 200:
                self.logger.error(f"Failed to fetch followers: HTTP {resp.status}")
                raise RuntimeError(f"Follower fetch failed: {resp.status}")
            data = await resp.json()
            count = data.get("total", 0)
            self.logger.info_v(f"Follower count: {count}")
            return count

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
        """
        Create a new poll on the broadcaster channel with specified options and voting settings.
        Returns the poll ID for tracking and status checks.
        """
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
        self.logger.info_v(
            f"Creating poll: '{title}' with {len(choices)} choices for {duration}s"
        )
        async with self._session.post(f"{HELIX_URL}/polls", json=payload) as resp:
            if resp.status != 200:
                self.logger.error(f"Failed to create poll: HTTP {resp.status}")
                error_data = await resp.json()
                self.logger.error_v(f"Poll creation error: {error_data}")
                raise RuntimeError(f"Poll creation failed: {resp.status}")
            data = await resp.json()
            poll_id = data["data"][0]["id"]
            self.logger.info_v(f"Poll created successfully: {poll_id}")
            return poll_id

    async def create_message(self, message: str) -> None:
        """
        Send a chat message to the channel via the connected bot.
        Delegates message sending to the underlying TwitchBot instance.
        """
        self.logger.info_v(f"Sending message: '{message[:50]}...'")
        await self._bot.send_message(message)
        self.logger.info_v("Message sent successfully")
