const chat = document.getElementById("chat");
const MAX_MESSAGES = 50;
const source = new EventSource("events/chat");

source.onmessage = (rawEvent) => {
    const event = JSON.parse(rawEvent.data);
    if (event.kind === "delete") {
        removeMessage(event.message_id);
    } else {
        appendMessage(event);
    }
};

function removeMessage(messageId) {
    const line = chat.querySelector(`[data-message-id="${messageId}"]`);
    if (line) {
        line.remove();
    }
}

function appendMessage(event) {
    const line = document.createElement("div");
    line.className = "message";
    line.dataset.messageId = event.message_id;

    const badges = document.createElement("span");
    badges.className = "badges";
    for (const badgeUrl of event.badges || []) {
        const img = document.createElement("img");
        img.src = badgeUrl;
        badges.appendChild(img);
    }

    const user = document.createElement("span");
    user.className = "user";
    user.textContent = event.user;
    user.style.color = event.color || "#a970ff";

    const text = document.createElement("span");
    text.className = "text";
    text.textContent = event.text;

    line.appendChild(badges);
    line.appendChild(user);
    line.appendChild(document.createTextNode(": "));
    line.appendChild(text);
    chat.appendChild(line);

    while (chat.children.length > MAX_MESSAGES) {
        chat.removeChild(chat.firstChild);
    }
    chat.scrollTop = chat.scrollHeight;
}
