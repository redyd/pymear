from __future__ import annotations

import aiohttp

HELIX_URL = "https://api.twitch.tv/helix"


def _clean_token(token: str) -> str:
    return token.replace("oauth:", "")


async def get_user_id(client_id: str, token: str, login: str) -> str:
    headers = {
        "Client-Id": client_id,
        "Authorization": f"Bearer {_clean_token(token)}",
    }
    async with (
        aiohttp.ClientSession(headers=headers) as session,
        session.get(f"{HELIX_URL}/users", params={"login": login}) as resp,
    ):
        data = await resp.json()
        return data["data"][0]["id"]
