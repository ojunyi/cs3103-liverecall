/*
 * Working but deliberately naive LiveRecall browser starter.
 *
 * Provided mechanical behavior:
 *   - opens a WebSocket
 *   - sends CLIENT_HELLO
 *   - visibly appends every received CAPTION_UPDATE
 *   - displays whether an update is provisional/final
 *
 * Graded TODOs:
 *   - replace revisions rather than appending duplicates
 *   - maintain >=60 s of finalized local state
 *   - implement 30 s default recall + 15/30/60 s views
 *   - automatic reconnect + correct resume state
 *   - HISTORY merge/deduplication
 *   - usable final UI
 */

const WS_URL = "ws://127.0.0.1:8765";
const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const recallEl = document.getElementById("recall");

let ws;

// --- Local finalized state (added for the TODOs below) -----------------------
const KEEP_SECONDS = 120;            // keep well over the 60 s the largest view needs
const finals = new Map();            // segment -> final CAPTION_UPDATE
let lastFinal = { segment: 0, revision: 0 };   // resume state; never pruned
 
// Store a final. Returns false if we already hold that segment (duplicate/stale).
function mergeFinal(msg) {
  const segment = Number(msg.segment), revision = Number(msg.revision);
  const held = finals.get(segment);
  if (held && Number(held.revision) >= revision) return false;
  finals.set(segment, msg);
  if (segment > lastFinal.segment ||
      (segment === lastFinal.segment && revision > lastFinal.revision)) {
    lastFinal = { segment, revision };
  }
  const cutoff = Date.now() / 1000 - KEEP_SECONDS;
  for (const [seg, m] of finals) if (Number(m.source_ts) < cutoff) finals.delete(seg);
  return true;
}

function numericClientId() {
  const key = "liverecall-client-id";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

function appendNaively(msg) {
  const row = document.createElement("div");
  row.className = "naive-line";
  const state = msg.final ? "FINAL" : "PROVISIONAL";
  row.textContent = `[${state}] segment ${msg.segment} rev ${msg.revision}: ${msg.text}`;
  liveEl.appendChild(row);
  liveEl.scrollTop = liveEl.scrollHeight;
}

function connect() {
  statusEl.textContent = "connecting";
  ws = new WebSocket(WS_URL);

  ws.addEventListener("open", () => {
    statusEl.textContent = "connected";
    ws.send(JSON.stringify({
      type: "CLIENT_HELLO",
      client_id: numericClientId(),
      session: "L1",
      // TODO: resume latest finalized state
      last_segment: lastFinal.segment,
      last_revision: lastFinal.revision,
      history_seconds: 60
    }));
  });

  ws.addEventListener("message", (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "CAPTION_UPDATE") {
      if (msg.final && !mergeFinal(msg)) return;
      appendNaively(msg);
    } else if (msg.type === "HISTORY") {
      console.log(`HISTORY with ${msg.items?.length ?? 0} item(s)`, msg);
      // TODO: merge history without stale/duplicate final captions.
      for (const item of msg.items ?? []) {
        if (item.final === true && mergeFinal(item)) appendNaively(item);
      }
    }
  });

  ws.addEventListener("close", () => {
    statusEl.textContent = "disconnected";
    // TODO: automatic reconnect after a short delay, using finalized resume state.
    setTimeout(connect, 1000);
  });

  ws.addEventListener("error", () => {
    statusEl.textContent = "connection error";
  });
}

for (const button of document.querySelectorAll("button[data-seconds]")) {
  button.addEventListener("click", () => {
    const seconds = Number(button.dataset.seconds);
    const since = Date.now() / 1000 - seconds;
    const rows = [...finals.values()]
      .filter((m) => Number(m.source_ts) >= since)
      .sort((a, b) => a.segment - b.segment);
    // TODO: show finalized captions from the previous ${seconds} seconds using local state
    recallEl.textContent = `Previous ${seconds} s (${rows.length} finalized caption(s)):\n` +
      rows.map((m) => `[-${Math.round(Date.now() / 1000 - m.source_ts)} s] ${m.text}`).join("\n");
  });
}

connect();
