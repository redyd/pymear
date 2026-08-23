# pymear

A lightweight Python framework for building live Twitch overlays and integrations. A single hub connects to Twitch (chat, follows, subs, gifted subs, cheers, raids) and re-broadcasts every event over a websocket. Independent "features" (separate Python processes) subscribe to that stream and expose their own small web UI, meant to be added as a browser source in OBS.

## Architecture

- **EventExporter** (`pymear.core.event_exporter`) — the Twitch bot (built on `twitchio`). Connects to chat, listens to Twitch events, and publishes typed events onto the internal event bus. Runs once, as a long-lived singleton for the whole session.
- **EventBus** (`pymear.core.event_bus`) — an in-process, in-memory pub/sub bus. Handlers subscribe to an exact event dataclass type; publishing dispatches to each handler in its own task, so a slow or failing handler never blocks the others.
- **Broadcaster** (`pymear.core.broadcaster`) — subscribes to the EventBus and relays every event to connected websocket clients over a single `/ws` endpoint. This is the only bridge between the hub process and the outside world (other processes, browsers).
- **contracts** (`pymear.contracts`) — the typed event dataclasses (`ChatMessageEvent`, `FollowEvent`, `SubscriptionEvent`, `GiftSubscriptionEvent`, `CheerEvent`, `RaidEvent`) plus `encapsulate`/`decapsulate` mappers to convert them to and from JSON for the websocket wire format.
- **FeatureRuntime** (`pymear.core.feature_runtime`) — the helper every feature is built on. It connects to the hub as a websocket client, filters events by the type(s) the feature cares about, keeps a small replay buffer for late connections, streams matching events to the browser via Server-Sent Events (`/events`), and serves the feature's static `index.html`/`style.css`. An optional `on_event` callback lets the feature's own script react to each event server-side (logging, triggering TTS, etc.), independently of what gets streamed to the browser.
- **utils** (`pymear.utils`) — small Helix API helpers: `BadgeResolver` (resolves chat badges to image URLs once at startup) and `get_user_id` (looks up a broadcaster's user id).

Each feature is its own OS process with its own port, picked automatically from the OS's ephemeral range unless you request a specific one. This means you can start, stop, or edit a single feature live without touching the hub or any other feature.

```
Twitch  --(IRC/Helix)-->  EventExporter  --(publish)-->  EventBus  --(subscribe)-->  Broadcaster  --(websocket /ws)-->  FeatureRuntime (per feature process)  --(SSE)-->  browser / OBS
```

## Project structure

```
pyproject.toml
requirements.txt
.env
src/
├── pymear/                  # the library
│   ├── server.py            # hub entry point
│   ├── contracts/
│   │   ├── events.py
│   │   └── events_mapper.py
│   ├── core/
│   │   ├── event_bus.py
│   │   ├── event_exporter.py
│   │   ├── broadcaster.py
│   │   └── feature_runtime.py
│   └── utils/
│       ├── badge_resolver.py
│       ├── helix_client.py
│       └── speaker.py
└── features/                 # your own overlays, one folder each
    └── chat/
        ├── main.py
        ├── index.html
        └── style.css
```

## Setup

Requires Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Installing the project in editable mode makes `pymear` importable from anywhere in the virtual environment, regardless of which directory you run a script from — this is what lets each feature be launched directly with `python main.py` from its own folder.

Create a `.env` file at the project root:

```
TWITCH_CLIENT_ID=your_client_id
TWITCH_TOKEN=your_oauth_token
TWITCH_CHANNEL=your_channel_login
HUB_PORT=8765
```

The Twitch token needs the scopes required by whichever Helix calls you use (chat read/write at minimum; add others such as `channel:manage:polls` if a feature needs them).

## Running the hub

From the project root:

```bash
python -m pymear.server
```

This starts the aiohttp server, connects the Twitch bot, loads the badge resolver, and exposes the websocket relay on `ws://localhost:<HUB_PORT>/ws`. Start this before any feature; a feature will retry the connection with backoff if the hub isn't up yet.

## Running a feature

```bash
cd src/features/chat
python main.py
```

The feature connects to the hub, opens its own local web server on an automatically chosen free port (logged on startup), and serves its `index.html` there. Add that URL as a Browser Source in OBS.

## Creating a new feature

1. Add a new folder under `src/features/<name>/` with a `main.py`, `index.html`, and `style.css`.
2. In `main.py`, instantiate a `FeatureRuntime`, declaring which event type(s) it should receive:

```python
from pathlib import Path
from pymear.contracts.events import ChatMessageEvent
from pymear.core.feature_runtime import FeatureRuntime

runtime = FeatureRuntime[ChatMessageEvent](
    name="chat",
    event_types=[ChatMessageEvent],
    static_dir=Path(__file__).parent,
)
```

3. In `index.html`, open an `EventSource("/events")` and render incoming JSON payloads.

## Notes

- Each feature is an isolated process: a crash or edit in one never affects the hub, the bot, or any other feature.
- The websocket protocol (`/ws`) is considered internal, used only between the hub and feature processes. Browsers never talk to it directly; they only see SSE from their own feature's local server.
- `*.egg-info/`, `__pycache__/`, and `.venv/` are build artifacts and should stay out of version control.
