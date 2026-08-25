from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from pathlib import Path

from pymear.contracts.events import ChatMessageEvent, DeletedMessageEvent
from pymear.core.feature_runtime import FeatureRuntime

logger = logging.getLogger(__name__)


def handling(event: ChatMessageEvent | DeletedMessageEvent) -> dict | None:
    if isinstance(event, DeletedMessageEvent):
        logger.info(f"Deleted message: {event.message_id} {event.user}")
        return {"kind": "delete", "message_id": event.message_id}

    logger.info(f"{event.user}: {event.text}")
    return {"kind": "message", **asdict(event)}


def main() -> None:
    logger.info("Starting chat feature")
    runtime = FeatureRuntime(
        name="chat",
        event_types=[ChatMessageEvent, DeletedMessageEvent],
        static_dir=Path(__file__).parent,
    )
    runtime.add_source("chat", handling)
    asyncio.run(runtime.run())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main()
