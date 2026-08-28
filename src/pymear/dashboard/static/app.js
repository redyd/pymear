const EVENT_MODULES = [
  {
    type: "chat_message",
    fields: [
      { name: "user", value: "pixelfox" },
      { name: "text", value: "chat vibes only" },
      { name: "color", value: "#3B82F6" },
      { name: "message_id", value: "msg_001" },
    ],
  },
  {
    type: "follow",
    fields: [{ name: "user", value: "newviewer42" }],
  },
  {
    type: "subscription",
    fields: [
      { name: "user", value: "loyalwatcher" },
      { name: "months", value: 6, numeric: true },
      { name: "tier", value: "1000" },
    ],
  },
  {
    type: "gift_subscription",
    fields: [
      { name: "gifter", value: "generous_gary" },
      { name: "recipient", value: "lucky_lee" },
      { name: "gifter_total", value: 3, numeric: true },
      { name: "tier", value: "1000" },
    ],
  },
  {
    type: "cheer",
    fields: [
      { name: "user", value: "bitsdonor" },
      { name: "bits", value: 500, numeric: true },
      { name: "message", value: "great stream" },
    ],
  },
  {
    type: "raid",
    fields: [
      { name: "raider", value: "squadleader" },
      { name: "viewer_count", value: 120, numeric: true },
    ],
  },
  {
    type: "deleted_message",
    fields: [
      { name: "user", value: "troublemaker" },
      { name: "message_id", value: "msg_002" },
    ],
  },
];

const grid = document.getElementById("grid");
const feed = document.getElementById("feed");

function log(text, cls) {
  const line = document.createElement("div");
  line.className = "feed-line" + (cls ? " " + cls : "");
  line.textContent = `[${new Date().toLocaleTimeString()}]  ${text}`;
  feed.prepend(line);
}

function buildCard(module, index) {
  const card = document.createElement("div");
  card.className = "card";
  card.dataset.event = module.type;

  const head = document.createElement("div");
  head.className = "card-head";
  head.innerHTML = `
    <div class="card-head-left">
      <span class="chan-num">CH.${String(index + 1).padStart(2, "0")}</span>
      <h2>${module.type}</h2>
    </div>
  `;
  const clearBtn = document.createElement("button");
  clearBtn.className = "ghost-btn";
  clearBtn.textContent = "clear";
  head.appendChild(clearBtn);

  const body = document.createElement("div");
  body.className = "card-body";

  module.fields.forEach(f => {
    const field = document.createElement("div");
    field.className = "field";
    field.innerHTML = `
      <label>${f.name}</label>
      <input name="${f.name}" type="${f.numeric ? "number" : "text"}" value="${f.value}">
    `;
    body.appendChild(field);
  });

  const fireBtn = document.createElement("button");
  fireBtn.className = "fire-btn";
  fireBtn.textContent = "trigger";
  body.appendChild(fireBtn);

  clearBtn.addEventListener("click", () => {
    module.fields.forEach(f => {
      const input = body.querySelector(`input[name="${f.name}"]`);
      input.value = f.value;
    });
  });

  fireBtn.addEventListener("click", async () => {
    const payload = {};
    module.fields.forEach(f => {
      const input = body.querySelector(`input[name="${f.name}"]`);
      const val = input.value.trim();
      if (val === "") return;
      payload[f.name] = f.numeric ? Number(val) : val;
    });

    fireBtn.classList.remove("err");
    fireBtn.textContent = "sending";

    try {
      const res = await fetch(`/trigger/${module.type}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json();

      if (res.ok) {
        fireBtn.textContent = "trigger";
        log(`${module.type}  ${JSON.stringify(payload)}`, "ok");
      } else {
        fireBtn.classList.add("err");
        fireBtn.textContent = "trigger";
        log(`${module.type}  error: ${data.error}`, "err");
      }
    } catch (e) {
      fireBtn.classList.add("err");
      fireBtn.textContent = "trigger";
      log(`${module.type}  fetch error: ${e.message}`, "err");
    }
  });

  card.appendChild(head);
  card.appendChild(body);
  return card;
}

EVENT_MODULES.forEach((m, i) => grid.appendChild(buildCard(m, i)));

document.getElementById("feed-clear").addEventListener("click", () => {
  feed.innerHTML = "";
});
