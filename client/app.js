/* Live and recovered captions share one revision-aware store. */
const WS_URL = `${location.protocol === "https:" ? "wss" : "ws"}://${location.hostname || "127.0.0.1"}:8765`;
const SESSION = "L1";
const RETENTION_SECONDS = 120;
const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const recallEl = document.getElementById("recall");
const recallTitleEl = document.getElementById("recall-title");
const captions = new Map();
const liveRows = new Map();
let lastFinalSegment = 0;
let lastFinalRevision = 0;
let recallSeconds = 30;
let retryDelay = 1000;
let retryTimer;
let ws;

function numericClientId() {
  const key = "liverecall-client-id";
  const fallback = Math.floor(100000 + Math.random() * 900000);
  try {
    const saved = Number(sessionStorage.getItem(key));
    if (Number.isSafeInteger(saved) && saved > 0) return saved;
    sessionStorage.setItem(key, String(fallback));
  } catch {
    // Storage may be disabled when opening this page from disk.
  }
  return fallback;
}
const clientId = numericClientId();

function mergeCaption(item, history = false) {
  if (!item || typeof item !== "object" || item.session !== SESSION ||
      !Number.isSafeInteger(item.segment) || item.segment < 1 ||
      !Number.isSafeInteger(item.revision) || item.revision < 1 ||
      typeof item.final !== "boolean" || typeof item.text !== "string" ||
      !Number.isFinite(item.source_ts) || item.source_ts < 0 ||
      (history && !item.final)) return;
  const previous = captions.get(item.segment);
  // The required source completes segments in order. Once a final has aged
  // out, its delayed provisional revisions must not reappear as live rows.
  if (!previous && !item.final && item.segment <= lastFinalSegment) return;
  if (previous && (item.revision < previous.revision ||
      (previous.final && !item.final) ||
      (item.revision === previous.revision && (previous.final || !item.final)))) return;
  captions.set(item.segment, { ...item, receivedAt: Date.now() / 1000 });
  // Retention must not reset the finalized resume cursor.
  if (item.final && (item.segment > lastFinalSegment ||
      (item.segment === lastFinalSegment && item.revision > lastFinalRevision))) {
    lastFinalSegment = item.segment;
    lastFinalRevision = item.revision;
  }
}

function updateRow(row, caption) {
  row.className = `caption ${caption.final ? "final" : "provisional"}`;
  row.dataset.segment = String(caption.segment);
  row.dataset.revision = String(caption.revision);
  const label = document.createElement("span");
  label.className = "caption-meta";
  label.textContent = `${caption.final ? "Final" : "Provisional"} · Segment ${caption.segment} · Rev ${caption.revision}`;
  const text = document.createElement("span");
  text.className = "caption-text";
  text.textContent = caption.text;
  row.replaceChildren(label, text);
}
function captionRow(caption) {
  const row = document.createElement("div");
  updateRow(row, caption);
  return row;
}
function render() {
  const now = Date.now() / 1000;
  for (const [segment, caption] of captions) {
    const timestamp = caption.final ? caption.source_ts : caption.receivedAt;
    if (timestamp < now - RETENTION_SECONDS) captions.delete(segment);
  }
  const ordered = [...captions.values()].sort((a, b) => a.segment - b.segment);
  const followLive = liveEl.scrollHeight - liveEl.scrollTop - liveEl.clientHeight < 48;
  for (const [segment, row] of liveRows) {
    if (!captions.has(segment)) {
      row.remove();
      liveRows.delete(segment);
    }
  }
  let previousRow = null;
  for (const caption of ordered) {
    let row = liveRows.get(caption.segment);
    if (!row) {
      row = captionRow(caption);
      liveRows.set(caption.segment, row);
    } else if (row.dataset.revision !== String(caption.revision) ||
               row.classList.contains("final") !== caption.final) {
      updateRow(row, caption);
    }
    const nextRow = previousRow ? previousRow.nextSibling : liveEl.firstChild;
    if (nextRow !== row) liveEl.insertBefore(row, nextRow);
    previousRow = row;
  }
  liveEl.classList.toggle("empty", ordered.length === 0);
  if (followLive) liveEl.scrollTop = liveEl.scrollHeight;
  const recalled = ordered.filter(caption => caption.final &&
    caption.source_ts >= now - recallSeconds && caption.source_ts <= now);
  recallTitleEl.textContent = `Previous ${recallSeconds} seconds`;
  recallEl.replaceChildren(...recalled.map(captionRow));
  if (!recalled.length) recallEl.textContent = "No finalized captions in this window yet.";
}
function scheduleReconnect() {
  if (retryTimer) return;
  statusEl.textContent = `Disconnected · reconnecting in ${retryDelay / 1000} s`;
  retryTimer = setTimeout(() => {
    retryTimer = undefined;
    connect();
  }, retryDelay);
  retryDelay = Math.min(retryDelay * 2, 10000);
}
function connect() {
  statusEl.textContent = "Connecting…";
  let socket;
  try {
    socket = new WebSocket(WS_URL);
  } catch {
    scheduleReconnect();
    return;
  }
  ws = socket;
  const timeout = setTimeout(() => {
    if (ws === socket && socket.readyState === WebSocket.CONNECTING) socket.close();
  }, 10000);
  socket.addEventListener("open", () => {
    if (ws !== socket) return;
    clearTimeout(timeout);
    retryDelay = 1000;
    statusEl.textContent = "Connected";
    socket.send(JSON.stringify({
      type: "CLIENT_HELLO", client_id: clientId, session: SESSION,
      last_segment: lastFinalSegment, last_revision: lastFinalRevision,
      history_seconds: 60
    }));
  });
  socket.addEventListener("message", event => {
    if (ws !== socket) return;
    let msg;
    try { msg = JSON.parse(event.data); } catch { return; }
    if (!msg || typeof msg !== "object") return;
    if (msg.type === "CAPTION_UPDATE") {
      mergeCaption(msg);
    } else if (msg.type === "HISTORY" && Array.isArray(msg.items) &&
               (msg.session === undefined || msg.session === SESSION)) {
      for (const item of msg.items) mergeCaption(item, true);
    } else {
      return;
    }
    render();
  });
  socket.addEventListener("close", () => {
    clearTimeout(timeout);
    if (ws === socket) scheduleReconnect();
  });
  socket.addEventListener("error", () => {
    if (ws !== socket) return;
    statusEl.textContent = "Connection error · retrying…";
    socket.close();
  });
}
for (const button of document.querySelectorAll("button[data-seconds]")) {
  button.addEventListener("click", () => {
    recallSeconds = Number(button.dataset.seconds);
    for (const option of document.querySelectorAll("button[data-seconds]")) {
      option.setAttribute("aria-pressed", String(option === button));
    }
    render();
  });
}
// Demo control: drop this tab's socket and let the normal reconnect path resume it.
const DROP_SECONDS = 7;
document.getElementById("drop")?.addEventListener("click", () => {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  const socket = ws;
  ws = undefined; // the existing ws !== socket guards now ignore this socket
  socket.close(4000, "simulated drop");
  retryDelay = DROP_SECONDS * 1000;
  scheduleReconnect();
  statusEl.textContent = `Simulated network drop · reconnecting in ${DROP_SECONDS} s`;
});
render();
setInterval(render, 1000);
connect();
