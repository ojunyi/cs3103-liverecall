/* LiveRecall browser client.
 *
 * One revision-aware store (segment -> newest accepted caption) feeds both the
 * live view and "What did I miss?". Recall is answered from this local store;
 * the server is asked only for finals the client genuinely missed (resume
 * cursor after a disconnect, or recent context on a late join).
 *
 * Optional URL parameters: ?port=8765, ?ws=ws://host:port, ?session=L1
 */
"use strict";

const params = new URLSearchParams(location.search);
const SESSION = params.get("session") || "L1";
const WS_URL = params.get("ws") ||
  `${location.protocol === "https:" ? "wss" : "ws"}://${location.hostname || "127.0.0.1"}:${params.get("port") || "8765"}`;
const RETENTION_SECONDS = 120;        // keeps well over 60 s of finals locally
const RECONNECT_MIN_MS = 500;
const RECONNECT_MAX_MS = 5000;
const SIMULATED_OUTAGE_MS = 10000;

const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const recallPanel = document.getElementById("recall-panel");
const recallEl = document.getElementById("recall");
const recallTitleEl = document.getElementById("recall-title");
const missButton = document.getElementById("what-did-i-miss");
const closeRecallButton = document.getElementById("close-recall");
const simulateButton = document.getElementById("simulate-disconnect");
const windowButtons = [...document.querySelectorAll("button[data-seconds]")];

const captions = new Map();           // segment -> newest accepted caption
const liveRows = new Map();           // segment -> DOM row
let lastFinalSegment = 0;             // resume cursor: newest final held
let lastFinalRevision = 0;
let prunedThrough = 0;                // segments at or below this left local memory
let lecture = null;                   // server lecture number, if announced
let recallSeconds = 30;
let recallOpen = false;
let recallSignature = "";
const offsetSamples = [];             // receive time - source_ts for live updates
let clockOffset = 0;                  // maps source time onto this browser's clock
let ws;
let connected = false;
let connectedOnce = false;
let retryDelay = RECONNECT_MIN_MS;
let retryTimer;
let nextAttemptAt = 0;
let pausedUntil = 0;
let statusNoteUntil = 0;              // when a "recovered N captions" note fades

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

function setStatus(text, state) {
  statusEl.textContent = text;
  statusEl.dataset.state = state;
}

function validCaption(item) {
  return item && typeof item === "object" && item.session === SESSION &&
    Number.isSafeInteger(item.segment) && item.segment >= 1 &&
    Number.isSafeInteger(item.revision) && item.revision >= 1 &&
    typeof item.final === "boolean" && typeof item.text === "string" &&
    Number.isFinite(item.source_ts);
}

/* Accept only a newer revision; a final is terminal. Returns true if stored. */
function mergeCaption(item, fromHistory) {
  if (!validCaption(item) || (fromHistory && !item.final)) return false;
  const previous = captions.get(item.segment);
  if (previous) {
    if (previous.final || item.revision < previous.revision) return false;
    if (item.revision === previous.revision && !item.final) return false;
  } else if (item.segment <= prunedThrough) {
    return false;                     // aged out locally: never resurrect it
  }
  captions.set(item.segment, {
    segment: item.segment, revision: item.revision, final: item.final,
    text: item.text, source_ts: item.source_ts, receivedAt: Date.now() / 1000,
  });
  if (item.final && (item.segment > lastFinalSegment ||
      (item.segment === lastFinalSegment && item.revision > lastFinalRevision))) {
    lastFinalSegment = item.segment;
    lastFinalRevision = item.revision;
  }
  return true;
}

/* Source and browser clocks may differ (e.g. server in a VM). The smallest
 * receive delay seen recently approximates the offset between them. */
function noteLiveDelay(item) {
  offsetSamples.push(Date.now() / 1000 - item.source_ts);
  if (offsetSamples.length > 64) offsetSamples.shift();
  clockOffset = Math.min(...offsetSamples);
}
function spokenAt(caption) {
  return caption.final ? caption.source_ts + clockOffset : caption.receivedAt;
}

function resetLocalState(newLecture) {
  captions.clear();
  for (const row of liveRows.values()) row.remove();
  liveRows.clear();
  lastFinalSegment = lastFinalRevision = 0;
  prunedThrough = 0;
  recallSignature = "";
  lecture = newLecture;
}

function clockLabel(seconds) {
  return new Date(seconds * 1000).toLocaleTimeString([], { hour12: false });
}
function updateRow(row, caption, withTime) {
  row.className = `caption ${caption.final ? "final" : "provisional"}`;
  row.dataset.segment = String(caption.segment);
  row.dataset.revision = String(caption.revision);
  const label = document.createElement("span");
  label.className = "caption-meta";
  const parts = [caption.final ? "Final" : "Provisional (updating)",
                 `Segment ${caption.segment}`, `Rev ${caption.revision}`];
  if (withTime) parts.push(clockLabel(spokenAt(caption)));
  label.textContent = parts.join(" · ");
  const text = document.createElement("span");
  text.className = "caption-text";
  text.textContent = caption.text;
  row.replaceChildren(label, text);
}
function captionRow(caption, withTime) {
  const row = document.createElement("div");
  updateRow(row, caption, withTime);
  return row;
}

function prune(now) {
  for (const [segment, caption] of captions) {
    if (spokenAt(caption) < now - RETENTION_SECONDS) {
      captions.delete(segment);
      prunedThrough = Math.max(prunedThrough, segment);
    }
  }
}

function renderLive(ordered) {
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
      row = captionRow(caption, false);
      liveRows.set(caption.segment, row);
    } else if (row.dataset.revision !== String(caption.revision) ||
               row.classList.contains("final") !== caption.final) {
      updateRow(row, caption, false);   // revision replaces the row in place
    }
    const nextRow = previousRow ? previousRow.nextSibling : liveEl.firstChild;
    if (nextRow !== row) liveEl.insertBefore(row, nextRow);
    previousRow = row;
  }
  liveEl.classList.toggle("empty", ordered.length === 0);
  if (followLive) liveEl.scrollTop = liveEl.scrollHeight;
}

function renderRecall(ordered, now) {
  recallTitleEl.textContent = `What did I miss? Previous ${recallSeconds} seconds`;
  if (!recallOpen) return;
  const recalled = ordered.filter(caption => caption.final &&
    spokenAt(caption) >= now - recallSeconds && spokenAt(caption) <= now + 1);
  const signature = `${recallSeconds}|` + recalled.map(c => `${c.segment}.${c.revision}`).join(",");
  if (signature === recallSignature) return;   // unchanged: keep scroll position
  recallSignature = signature;
  const atBottom = recallEl.scrollHeight - recallEl.scrollTop - recallEl.clientHeight < 48;
  if (recalled.length) {
    recallEl.replaceChildren(...recalled.map(caption => captionRow(caption, true)));
  } else {
    const empty = document.createElement("p");
    empty.className = "hint";
    empty.textContent = "No finalized captions in this window yet.";
    recallEl.replaceChildren(empty);
  }
  if (atBottom) recallEl.scrollTop = recallEl.scrollHeight;
}

function render() {
  const now = Date.now() / 1000;
  prune(now);
  const ordered = [...captions.values()].sort((a, b) => a.segment - b.segment);
  renderLive(ordered);
  renderRecall(ordered, now);
  if (!connected && retryTimer) {
    const seconds = Math.max(0, Math.ceil((nextAttemptAt - Date.now()) / 1000));
    const simulated = pausedUntil > Date.now() ? " (simulated outage)" : "";
    setStatus(`Disconnected${simulated} · reconnecting in ${seconds} s`, "offline");
  } else if (connected && statusNoteUntil && Date.now() > statusNoteUntil) {
    statusNoteUntil = 0;
    setStatus("Connected", "online");
  }
}

function scheduleReconnect() {
  if (retryTimer) return;
  const jitter = 0.75 + Math.random() * 0.5;
  const delay = Math.max(retryDelay * jitter, pausedUntil - Date.now());
  nextAttemptAt = Date.now() + delay;
  retryTimer = setTimeout(() => {
    retryTimer = undefined;
    connect();
  }, delay);
  retryDelay = Math.min(retryDelay * 2, RECONNECT_MAX_MS);
  render();
}

function connect() {
  if (retryTimer) {
    clearTimeout(retryTimer);
    retryTimer = undefined;
  }
  setStatus("Connecting…", "connecting");
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
    connected = true;
    retryDelay = RECONNECT_MIN_MS;
    setStatus("Connected", "online");
    const hello = {
      type: "CLIENT_HELLO", client_id: clientId, session: SESSION,
      last_segment: lastFinalSegment, last_revision: lastFinalRevision,
      history_seconds: 60,
    };
    if (Number.isInteger(lecture)) hello.lecture = lecture;
    socket.send(JSON.stringify(hello));
  });
  socket.addEventListener("message", event => {
    if (ws !== socket) return;
    let msg;
    try { msg = JSON.parse(event.data); } catch { return; }
    if (!msg || typeof msg !== "object") return;
    if (msg.type === "CAPTION_UPDATE") {
      if (mergeCaption(msg, false)) noteLiveDelay(msg);
    } else if (msg.type === "HISTORY" && Array.isArray(msg.items) &&
               (msg.session === undefined || msg.session === SESSION)) {
      if (Number.isInteger(msg.lecture)) {
        if (lecture !== null && msg.lecture !== lecture) resetLocalState(msg.lecture);
        lecture = msg.lecture;
      }
      let added = 0;
      for (const item of msg.items) if (mergeCaption(item, true)) added += 1;
      const plural = added === 1 ? "" : "s";
      const detail = connectedOnce ? `recovered ${added} missed caption${plural}`
                                   : `loaded ${added} recent caption${plural}`;
      setStatus(added ? `Connected · ${detail}` : "Connected", "online");
      statusNoteUntil = Date.now() + 15000;
      connectedOnce = true;
    } else if (msg.type === "SESSION_RESET" && msg.session === SESSION) {
      resetLocalState(Number.isInteger(msg.lecture) ? msg.lecture : null);
      setStatus("Connected · new lecture started", "online");
      statusNoteUntil = Date.now() + 15000;
    } else {
      return;
    }
    render();
  });
  socket.addEventListener("close", () => {
    clearTimeout(timeout);
    if (ws !== socket) return;
    connected = false;
    scheduleReconnect();
  });
  socket.addEventListener("error", () => {
    if (ws !== socket) return;
    socket.close();
  });
}

function openRecall(seconds) {
  recallSeconds = seconds;
  recallOpen = true;
  recallPanel.hidden = false;
  recallSignature = "";
  for (const button of windowButtons) {
    button.setAttribute("aria-pressed", String(Number(button.dataset.seconds) === seconds));
  }
  missButton.setAttribute("aria-expanded", "true");
  render();
}

missButton.addEventListener("click", () => {
  openRecall(30);
  recallEl.scrollTop = recallEl.scrollHeight;
  recallPanel.scrollIntoView({ block: "nearest" });
});
for (const button of windowButtons) {
  button.addEventListener("click", () => openRecall(Number(button.dataset.seconds)));
}
closeRecallButton.addEventListener("click", () => {
  recallOpen = false;
  recallPanel.hidden = true;
  missButton.setAttribute("aria-expanded", "false");
  missButton.focus();
});
simulateButton.addEventListener("click", () => {
  // Demo aid: drop the real connection; the normal reconnect path recovers.
  pausedUntil = Date.now() + SIMULATED_OUTAGE_MS;
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
    ws.close(4000, "simulated outage");
  } else if (retryTimer) {
    clearTimeout(retryTimer);          // already offline: extend the wait
    retryTimer = undefined;
    scheduleReconnect();
  }
});
window.addEventListener("online", () => {
  if (!connected && retryTimer && pausedUntil <= Date.now()) connect();
});

render();
setInterval(render, 1000);
connect();
