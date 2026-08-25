# pymear

A lightweight Python framework for building live Twitch overlays and integrations. A single hub connects to Twitch (chat, follows, subs, gifted subs, cheers, raids) and re-broadcasts every event over a websocket. Independent "features" (separate Python processes) subscribe to that stream and expose their own small web UI, meant to be added as a browser source in OBS. A routing proxy unifies every feature under a single port, so OBS only ever needs to know one address per feature path.

Repository: https://github.com/redyd/pymear

## Architecture

- **EventExporter** (`pymear.core.event_exporter`): the Twitch bot (built on `twitchio`). Connects to chat, listens to Twitch events, and publishes typed events onto the internal event bus. Runs once, as a long-lived singleton for the whole session.
- **EventBus** (`pymear.core.event_bus`): an in-process, in-memory pub/sub bus. Handlers subscribe to an exact event dataclass type; publishing dispatches to each handler in its own task, so a slow or failing handler never blocks the others.
- **Broadcaster** (`pymear.core.broadcaster`): subscribes to the EventBus and relays every event to connected websocket clients over a single `/ws` endpoint. This is the only bridge between the hub process and the outside world (other processes, browsers).
- **FeatureProxy** (`pymear.core.proxy`): a separate process that unifies all features on a single port. Each feature registers itself over websocket (`/register`) with its name and current port; the proxy then reverse proxies any request under `/<name>/...` to that feature's own local server, SSE streams included.
- **Pymear** (`pymear.server`): the hub's entry point, packaged as a class. Owns the `EventBus`, the `Broadcaster`, the `EventExporter` startup/shutdown, and the `FeatureProxy`. Twitch credentials and the command prefix are set via properties rather than the constructor; if left unset, they're resolved from a `.env` file at startup, and the source (environment vs default) is logged.
- **contracts** (`pymear.contracts`): the typed event dataclasses (`ChatMessageEvent`, `FollowEvent`, `SubscriptionEvent`, `GiftSubscriptionEvent`, `CheerEvent`, `RaidEvent`) plus `encapsulate`/`decapsulate` mappers to convert them to and from JSON for the websocket wire format.
- **FeatureRuntime** (`pymear.core.feature_runtime`): the helper every feature is built on. It connects to the hub as a websocket client, filters events by the type(s) the feature cares about, and registers itself with the `FeatureProxy` so it becomes reachable at `/<name>/` without needing a fixed port. Two distinct ways to react to events:
  - `add_handler(fn)`: a server side reaction to an event (logging, TTS, etc.), with no effect on any SSE stream.
  - `add_source(name, transform)`: opens a dedicated SSE stream at `/events/<name>`. `transform` receives the typed event and returns the payload to send to that stream, or `None` to filter the event out of it. A single feature can expose several independent sources (e.g. `chat` and `subscriptions` on the same process), each with its own set of connected browser clients.
- **utils** (`pymear.utils`): small Helix API helpers: `BadgeResolver` (resolves chat badges to image URLs once at startup) and `get_user_id` (looks up a broadcaster's user id).

Each feature is its own OS process with its own port, picked automatically from the OS's ephemeral range unless you request a specific one. This means you can start, stop, or edit a single feature live without touching the hub, the proxy, or any other feature. The ephemeral port stays an internal detail: from the outside, every feature is reached through the proxy's single port.

Twitch --(IRC/Helix)--> EventExporter --(publish)--> EventBus --(subscribe)--> Broadcaster --(websocket /ws)--> FeatureRuntime (per feature process)
|
(register: name+port)
v
FeatureProxy --(SSE via /<name>/events/<source>)--> browser / OBS


## Installing pymear in your own project

Requires Python 3.11+.

```bash
pip install git+https://github.com/redyd/pymear.git
```

This makes `pymear` importable from anywhere in your project's virtual environment. Your project doesn't need any particular folder layout; a flat `main.py` at the root next to a `features/` folder works just as well as anything nested under `src/`. pymear only cares about being importable, it has no expectation about where your own code lives.

Create a `.env` file in your project if you want Twitch credentials picked up automatically:

TWITCH_CLIENT_ID=your_client_id
TWITCH_TOKEN=your_oauth_token
TWITCH_CHANNEL=your_channel_login
TWITCH_PREFIX=!
HUB_PORT=8765


These are fallbacks only: any credential you set explicitly in code takes priority over the `.env` value.

## Quickstart

The fastest way to start the hub, using `.env` fallbacks or explicit credentials:

```python
import pymear

pymear.run(client_id="...", token="...", channel="...")
```

If you need more control (custom ports, inspecting the bus, running inside an already active event loop), instantiate `Pymear` directly instead:

```python
import asyncio
from pymear import Pymear

app = Pymear(hub_port=8765, proxy_port=9000)
app.client_id = "..."
app.token = "..."
app.channel = "..."

asyncio.run(app.run())
```

Either way, this starts the aiohttp app exposing the websocket relay on `ws://localhost:<hub_port>/ws`, connects the Twitch bot, loads the badge resolver, and starts the `FeatureProxy` on `<proxy_port>`. Start this before any feature; a feature will retry the connection with backoff if the hub or the proxy isn't up yet.

## Example project layout

A project using pymear typically looks like this. Nothing here is enforced by the library itself, it's just a convention that keeps features isolated from each other:

your-project/
├── .env
├── main.py # calls pymear.run() or instantiates Pymear
└── features/
└── chat/
├── main.py
├── index.html
├── script.js
└── style.css


## Running a feature

```bash
cd features/chat
python main.py
```

The feature connects to the hub, registers itself with the proxy, and opens its own local web server on an automatically chosen free port (logged on startup). It's reachable through the proxy at `http://localhost:<proxy_port>/<name>/`, this is the URL to add as a Browser Source in OBS, not the feature's own ephemeral port.

## Creating a new feature

1. Add a new folder under `features/<name>/` with a `main.py`, `index.html`, and `style.css`.
2. In `main.py`, instantiate a `FeatureRuntime`, declare which event type(s) it should receive, and register at least one source:

```python
from pathlib import Path
from pymear.contracts.events import ChatMessageEvent
from pymear.core.feature_runtime import FeatureRuntime

runtime = FeatureRuntime(
    name="chat",
    event_types=[ChatMessageEvent],
    static_dir=Path(__file__).parent,
)

def chat_transform(event: ChatMessageEvent) -> dict | None:
    if event.text.startswith("!"):
        return None
    return {"user": event.user, "text": event.text}

runtime.add_source("chat", chat_transform)
```

3. In `index.html` (or a separate `script.js`), open an `EventSource("events/chat")`, a relative path, so it resolves correctly both behind the proxy (`/<name>/events/chat`) and when the feature is accessed directly on its own port.

## Developing pymear itself

If you're working on the library rather than just consuming it:

```bash
git clone https://github.com/redyd/pymear.git
cd pymear
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

The editable install means any change to pymear's source is reflected immediately in whichever project imports it from this same environment, no reinstall needed.

## Notes

- Each feature is an isolated process: a crash or edit in one never affects the hub, the proxy, or any other feature.
- The websocket protocol (`/ws` and `/register`) is considered internal, used only between the hub, the proxy, and feature processes. Browsers never talk to it directly; they only see SSE from the proxy, which relays it from their own feature's local server.
- `*.egg-info/`, `__pycache__/`, and `.venv/` are build artifacts and should stay out of version control.
