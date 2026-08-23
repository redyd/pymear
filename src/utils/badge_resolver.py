from __future__ import annotations

import aiohttp

HELIX_URL = "https://api.twitch.tv/helix"


def _clean_token(token: str) -> str:
    return token.replace("oauth:", "")


class BadgeResolver:
    """
    Resolves chat badges (name → version) to image URLs.
    - load(): Call once at startup (2 Helix calls – global + channel badges).
    - resolve(): Sync dict lookup, zero network cost on event_message hot path.
    """

    def __init__(self):
        self._urls: dict[tuple[str, str], str] = {}

    async def load(self, client_id: str, token: str, broadcaster_id: str) -> None:
        headers = {
            "Client-Id": client_id,
            "Authorization": f"Bearer {_clean_token(token)}",
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(f"{HELIX_URL}/chat/badges/global") as resp:
                data = await resp.json()
                self._merge(data.get("data", []))

            async with session.get(
                f"{HELIX_URL}/chat/badges",
                params={"broadcaster_id": broadcaster_id},
            ) as resp:
                data = await resp.json()
                self._merge(data.get("data", []))

    def resolve(self, badges: dict[str, str]) -> list[str]:
        return [
            self._urls[(name, version)]
            for name, version in badges.items()
            if (name, version) in self._urls
        ]

    def _merge(self, badge_sets: list) -> None:
        for badge_set in badge_sets:
            set_id = badge_set["set_id"]
            for version in badge_set["versions"]:
                self._urls[(set_id, version["id"])] = version["image_url_2x"]
