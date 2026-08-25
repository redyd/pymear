
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
