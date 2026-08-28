# Pymear

A lightweight framework for building Twitch overlays, integrations and actions quickly, through a local central server that connects to the stream, listens to events, and executes the actions you define.

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

## Table of contents

- [Overview](#overview)
- [How it works](#how-it-works)
- [Creating a feature](#creating-a-feature)
- [Interacting with the Twitch API](#interacting-with-the-twitch-api)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Advanced usage](#advanced-usage)
- [License](#license)

## Overview
Pymear's client side lets you listen to a live SSE stream of hub events (messages, subscriptions, etc.) exposed by the central server, but it also lets you define your own websocket flows per feature and react to them for maximum control.

The core idea of the project is to let you create, modify or stop any flow or integration currently running, without impacting the rest of your features, since each one runs as its own separate process.

## How it works

Each new flow or integration you want to add to your project should be organized as:

- a Python file
- an `index.html` file (and optionally a css/js file)

In your Python file, you create a `Feature` object with a name (used to register it with the proxy under `http://localhost:<port>/<name>`), a list of event types to listen to, and the path to the folder containing your `index.html` and optional css/js files (usually `Path(__file__).parent`).

You can then attach a data source, which registers itself with the proxy under `http://localhost:<port>/<name>/ws/<source_name>`, and pass it two optional methods: `on_message` and `transform`.

## Creating a feature

A simple chat feature can look like this:

```python
# main.py
from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from pathlib import Path

from pymear import ChatMessageEvent, DeletedMessageEvent, Feature


def handling(event: ChatMessageEvent | DeletedMessageEvent) -> dict | None:
    if isinstance(event, DeletedMessageEvent):
        return {"kind": "delete", "message_id": event.message_id}
    return {"kind": "message", **asdict(event)}


def main() -> None:
    feature = Feature(
        name="chat",
        event_types=[ChatMessageEvent, DeletedMessageEvent]
    )
    feature.add_source("chat_ws", transform=handling)
    feature.start()


if __name__ == "__main__":
    main()
```

On the front end, subscribe to the `ws/chat_ws` websocket:

```javascript
// script.js
const chat = document.getElementById("chat");
const MAX_MESSAGES = 50;
const source = new WebSocket("ws/chat_ws");

source.onmessage = (rawEvent) => {
    const event = JSON.parse(rawEvent.data);
    if (event.kind === "delete") {
        removeMessage(event.message_id);
    } else {
        appendMessage(event);
    }
};
```

## Interacting with the Twitch API

You can also interact directly with Twitch through the `Command` class, which lets you make requests to the Twitch API directly.

## Installation

```bash
pip install git+https://github.com/redyd/pymear.git
```

## Quick start

Place a `.env` file at the root of the project and define the required environment variables:

```env
TWITCH_TOKEN=oauth:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWITCH_CLIENT_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWITCH_CHANNEL=your_twitch_channel
TWITCH_PREFIX=!
HUB_PORT=8765
```

Then define your entry point:

```python
# main.py
import pymear

pymear.run()
```

## Advanced usage

If you need more control, instantiate the object directly:

```python
# main.py
import asyncio
from pymear import Pymear

app = Pymear(hub_port=8765, proxy_port=9000)
app.client_id = "..."
app.token = "..."
app.channel = "..."
asyncio.run(app.run())
```

### Consuming the hub's SSE stream directly

The hub exposes every subscribed event as a live SSE stream at `http://localhost:<hub_port>/internal/events` (`8765` by default). This is what `Feature` uses internally to receive hub events, but you can also consume it directly, for example from a script that doesn't need the full `Feature`/proxy machinery:

```python
import json
import aiohttp
import asyncio

async def listen() -> None:
    async with aiohttp.ClientSession() as session:
        async with session.get("http://localhost:8765/internal/events") as response:
            async for raw_line in response.content:
                line = raw_line.decode("utf-8").strip()
                if not line or not line.startswith("data:"):
                    continue
                event = json.loads(line[len("data:"):].strip())
                print(event)

asyncio.run(listen())
```

The stream is one-way: server to client only. There is no way to send messages back through this channel.

## License

MIT — see the [LICENSE](LICENSE) file for details.
