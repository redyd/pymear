from __future__ import annotations

import asyncio
from pathlib import Path

from pymear.contracts.events import ChatMessageEvent
from pymear.core.feature_runtime import FeatureRuntime


async def log_message(event: ChatMessageEvent) -> None:
    print(f"{event.user}: {event.text}")

def main() -> None:
    runtime = FeatureRuntime(
        name="chat",
        event_types=[ChatMessageEvent],
        static_dir=Path(__file__).parent,
        on_event=log_message,
    )
    asyncio.run(runtime.run())


if __name__ == "__main__":
    main()
