# pymear

A lightweight Python framework for building live Twitch overlays and integrations. A single hub connects to Twitch (chat, follows, subs, gifted subs, cheers, raids) and re-broadcasts every event over a websocket. Independent "features" (separate Python processes) subscribe to that stream and expose their own small web UI, meant to be added as a browser source in OBS. A routing proxy unifies every feature under a single port, so OBS only ever needs to know one address per feature path.

## Architecture

- **EventExporter** (`pymear.core.event_exporter`) — the Twitch bot (built on `twitchio`). Connects to chat, listens to Twitch events, and publishes typed events onto the internal event bus. Runs once, as a long-lived singleton for the whole session.
- **EventBus** (`pymear.core.event_bus`) — an in-process, in-memory pub/sub bus. Handlers subscribe to an exact event dataclass type; publishing dispatches to each handler in its own task, so a slow or failing handler never blocks the others.
- **Broadcaster** (`pymear.core.broadcaster`) — subscribes to the EventBus and relays every event to connected websocket clients over a single `/ws` endpoint. This is the only bridge between the hub process and the outside world (other processes, browsers).
- **FeatureProxy** (`pymear.core.proxy`) — a separate process that unifies all features on a single port. Each feature registers itself over websocket (`/register`) with its name and current port; the proxy then reverse-proxies any request under `/<name>/...` to that feature's own local server, SSE streams included.
- **Pymear** (`pymear.server`) — the hub's entry point, packaged as a class. Owns the `EventBus`, the `Broadcaster`, the `EventExporter` startup/shutdown, and the `FeatureProxy`. Twitch credentials and the command prefix are set via properties rather than the constructor; if left unset, they're resolved from a `.env` file at startup, and the source (environment vs default) is logged.
- **contracts** (`pymear.contracts`) — the typed event dataclasses (`ChatMessageEvent`, `FollowEvent`, `SubscriptionEvent`, `GiftSubscriptionEvent`, `CheerEvent`, `RaidEvent`) plus `encapsulate`/`decapsulate` mappers to convert them to and from JSON for the websocket wire format.
- **FeatureRuntime** (`pymear.core.feature_runtime`) — the helper every feature is built on. It connects to the hub as a websocket client, filters events by the type(s) the feature cares about, and registers itself with the `FeatureProxy` so it becomes reachable at `/<name>/` without needing a fixed port. Two distinct ways to react to events:
  - `add_handler(fn)` — a server-side reaction to an event (logging, TTS, etc.), with no effect on any SSE stream.
  - `add_source(name, transform)` — opens a dedicated SSE stream at `/events/<name>`. `transform` receives the typed event and returns the payload to send to that stream, or `None` to filter the event out of it. A single feature can expose several independent sources (e.g. `chat` and `subscriptions` on the same process), each with its own set of connected browser clients.
- **utils** (`pymear.utils`) — small Helix API helpers: `BadgeResolver` (resolves chat badges to image URLs once at startup) and `get_user_id` (looks up a broadcaster's user id).

Each feature is its own OS process with its own port, picked automatically from the OS's ephemeral range unless you request a specific one. This means you can start, stop, or edit a single feature live without touching the hub, the proxy, or any other feature. The ephemeral port stays an internal detail: from the outside, every feature is reached through the proxy's single port.

Twitch --(IRC/Helix)--> EventExporter --(publish)--> EventBus --(subscribe)--> Broadcaster --(websocket /ws)--> FeatureRuntime (per feature process)
|
(register: name+port)
v
FeatureProxy --(SSE via /<name>/events/<source>)--> browser / OBS


## Project structure

pyproject.toml
requirements.txt
.env
src/
├── pymear/ # the library
│ ├── server.py # Pymear: hub entry point
│ ├── contracts/
│ │ ├── events.py
│ │ └── events_mapper.py
│ ├── core/
│ │ ├── event_bus.py
│ │ ├── event_exporter.py
│ │ ├── broadcaster.py
│ │ ├── feature_runtime.py
│ │ └── proxy.py # FeatureProxy: single-port router
│ └── utils/
│ ├── badge_resolver.py
│ ├── helix_client.py
│ └── speaker.py
└── features/ # your own overlays, one folder each
└── chat/
├── main.py
├── index.html
├── script.js
└── style.css


## Setup

Requires Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Installing the project in editable mode makes `pymear` importable from anywhere in the virtual environment, regardless of which directory you run a script from — this is what lets each feature be launched directly with `python main.py` from its own folder.

Create a `.env` file at the project root if you want credentials picked up automatically:

TWITCH_CLIENT_ID=your_client_id
TWITCH_TOKEN=your_oauth_token
TWITCH_CHANNEL=your_channel_login
TWITCH_PREFIX=!
HUB_PORT=8765


These are fallbacks only: `Pymear` reads them at startup for any property you haven't set explicitly in code. The Twitch token needs the scopes required by whichever Helix calls you use (chat read/write at minimum; add others such as `channel:manage:polls` if a feature needs them).

## Running the hub

From the project root:

```bash
python -m pymear.server
```

This instantiates `Pymear`, starts the aiohttp app exposing the websocket relay on `ws://localhost:<hub_port>/ws`, connects the Twitch bot, loads the badge resolver, and starts the `FeatureProxy` on `<proxy_port>`. Both ports default to `8765` and `9000` and are overridable when instantiating `Pymear` directly instead of using the CLI entry point. Start this before any feature; a feature will retry the connection with backoff if the hub or the proxy isn't up yet.

## Running a feature

```bash
cd src/features/chat
python main.py
```

The feature connects to the hub, registers itself with the proxy, and opens its own local web server on an automatically chosen free port (logged on startup). It's reachable through the proxy at `http://localhost:<proxy_port>/<name>/` — this is the URL to add as a Browser Source in OBS, not the feature's own ephemeral port.

## Creating a new feature

1. Add a new folder under `src/features/<name>/` with a `main.py`, `index.html`, and `style.css`.
2. In `main.py`, instantiate a `FeatureRuntime`, declare which event type(s) it should receive, and register at least one source:

```python
from pathlib import Path
from pymear.contracts.events import ChatMessageEvent
from pymear.core.feature_runtime import FeatureRuntime

runtime = FeatureRuntime[ChatMessageEvent](
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

3. In `index.html` (or a separate `script.js`), open an `EventSource("events/chat")` — a relative path, so it resolves correctly both behind the proxy (`/<name>/events/chat`) and when the feature is accessed directly on its own port.

## Notes

- Each feature is an isolated process: a crash or edit in one never affects the hub, the proxy, or any other feature.
- The websocket protocol (`/ws` and `/register`) is considered internal, used only between the hub, the proxy, and feature processes. Browsers never talk to it directly; they only see SSE from the proxy, which relays it from their own feature's local server.
- `*.egg-info/`, `__pycache__/`, and `.venv/` are build artifacts and should stay out of version control.
