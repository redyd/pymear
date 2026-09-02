(() => {
    const DEFAULTS = {
        minDelay: 500,
        maxDelay: 10000,
        factor: 1.6,
        jitter: 0.25,
    };

    function buildUrl(sourceName) {
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const parts = window.location.pathname.split("/").filter(Boolean);
        const basePath = parts.length > 0 ? `/${parts[0]}` : "";
        const path = `${basePath}/ws/${encodeURIComponent(sourceName)}`;
        return `${protocol}//${window.location.host}${path}`;
    }

    function delayWithJitter(delay, jitter) {
        const spread = delay * jitter;
        return delay - spread + Math.random() * spread * 2;
    }

    window.connectPymearSource = function connectPymearSource(
        sourceName,
        handlers = {},
        options = {},
    ) {
        const config = { ...DEFAULTS, ...options };
        let socket = null;
        let reconnectTimer = null;
        let stopped = false;
        let reconnectDelay = config.minDelay;

        const notifyStatus = (status) => {
            if (typeof handlers.onStatus === "function") {
                handlers.onStatus(status);
            }
        };

        const scheduleReconnect = () => {
            if (stopped || reconnectTimer !== null) {
                return;
            }

            notifyStatus("reconnecting");
            reconnectTimer = window.setTimeout(
                () => {
                    reconnectTimer = null;
                    open();
                },
                delayWithJitter(reconnectDelay, config.jitter),
            );
            reconnectDelay = Math.min(
                reconnectDelay * config.factor,
                config.maxDelay,
            );
        };

        const open = () => {
            if (stopped) {
                return;
            }

            notifyStatus("connecting");
            socket = new WebSocket(buildUrl(sourceName));

            socket.addEventListener("open", (event) => {
                reconnectDelay = config.minDelay;
                notifyStatus("open");
                if (typeof handlers.onOpen === "function") {
                    handlers.onOpen(event);
                }
            });

            socket.addEventListener("message", (event) => {
                if (typeof handlers.onMessage !== "function") {
                    return;
                }

                try {
                    handlers.onMessage(JSON.parse(event.data), event);
                } catch (error) {
                    if (typeof handlers.onError === "function") {
                        handlers.onError(error, event);
                    }
                }
            });

            socket.addEventListener("close", (event) => {
                socket = null;
                notifyStatus("closed");
                if (typeof handlers.onClose === "function") {
                    handlers.onClose(event);
                }
                scheduleReconnect();
            });

            socket.addEventListener("error", (event) => {
                if (typeof handlers.onError === "function") {
                    handlers.onError(event);
                }
            });
        };

        open();

        return {
            close() {
                stopped = true;
                notifyStatus("closed");
                if (reconnectTimer !== null) {
                    window.clearTimeout(reconnectTimer);
                    reconnectTimer = null;
                }
                if (socket !== null) {
                    socket.close();
                    socket = null;
                }
            },
            send(payload) {
                if (socket === null || socket.readyState !== WebSocket.OPEN) {
                    return false;
                }
                socket.send(JSON.stringify(payload));
                return true;
            },
            get readyState() {
                return socket === null ? WebSocket.CLOSED : socket.readyState;
            },
        };
    };
})();
