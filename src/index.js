import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "url";
import { hostname } from "node:os";
import { server as wisp, logging } from "@mercuryworkshop/wisp-js/server";
import Fastify from "fastify";
import fastifyStatic from "@fastify/static";
import { WebSocketServer, WebSocket } from "ws";

import { scramjetPath } from "@mercuryworkshop/scramjet/path";
import { libcurlPath } from "@mercuryworkshop/libcurl-transport";
import { baremuxPath } from "@mercuryworkshop/bare-mux/node";

const publicPath = fileURLToPath(new URL("../public/", import.meta.url));

logging.set_level(logging.NONE);
Object.assign(wisp.options, {
  allow_udp_streams: false,
  hostname_blacklist: [/example\.com/],
  dns_servers: ["1.1.1.3", "1.0.0.3"],
});

/* =========================
   WFATP CHAT — public room + live DMs + small-file sharing
   ========================= */

const MAX_CHAT_PAYLOAD = 1400 * 1024;
const MAX_FILE_BYTES = 700 * 1024;
const CHAT_HISTORY_LIMIT = 80;
const DM_HISTORY_LIMIT = 60;
const MAX_DM_THREADS = 120;
const MAX_STORED_FILE_BYTES = 12 * 1024 * 1024;
const CHAT_TEXT_LIMIT = 500;
const CHAT_AVATAR_LIMIT = 52000;
const ALLOWED_FILE_TYPES = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "image/gif",
  "application/pdf",
  "text/plain",
]);

const chatWss = new WebSocketServer({ noServer: true, maxPayload: MAX_CHAT_PAYLOAD });
const chatClients = new Map();
const publicHistory = [];
const dmHistory = new Map();

// HTTPS fallback for browsers and networks that cannot keep a WebSocket open.
// Each HTTP visitor has a local WebSocket bridge, so both transports share
// the same chat room, message validation, rate limits, and DM history.
const httpSessions = new Map();
const HTTP_SESSION_TTL = 45000;
const MAX_HTTP_SESSIONS = 200;
const MAX_HTTP_QUEUE_BYTES = 4 * 1024 * 1024;

function cleanUserId(value) {
  const id = String(value || "").trim();
  return /^[A-Za-z0-9_-]{8,64}$/.test(id) ? id : "";
}

function cleanName(value) {
  return (
    String(value || "")
      .replace(/[\\<>\u0000-\u001f\u007f]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 24) || "Guest"
  );
}

function cleanText(value) {
  let text = String(value || "")
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, "")
    .trim()
    .slice(0, CHAT_TEXT_LIMIT);

  text = text.replace(
    /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi,
    "[email removed]"
  );
  text = text.replace(
    /(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?){2}\d{4}/g,
    "[phone number removed]"
  );
  return text;
}

function cleanAvatar(value) {
  const avatar = String(value || "");
  if (!/^data:image\/(?:png|jpeg|webp);base64,[A-Za-z0-9+/=]+$/i.test(avatar)) {
    return "";
  }
  return avatar.length <= CHAT_AVATAR_LIMIT ? avatar : "";
}

function cleanFileName(value) {
  return (
    String(value || "file")
      .replace(/[\\/:*?"<>|\u0000-\u001f\u007f]/g, "_")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 80) || "file"
  );
}

function cleanAttachment(raw) {
  if (!raw || typeof raw !== "object") return null;
  const type = String(raw.type || "").toLowerCase();
  if (!ALLOWED_FILE_TYPES.has(type)) return null;

  const data = String(raw.data || "");
  const prefix = `data:${type};base64,`;
  if (!data.toLowerCase().startsWith(prefix.toLowerCase())) return null;

  const base64 = data.slice(prefix.length);
  if (!base64 || !/^[A-Za-z0-9+/=]+$/.test(base64)) return null;

  let size = 0;
  try {
    size = Buffer.byteLength(base64, "base64");
  } catch {
    return null;
  }
  if (!size || size > MAX_FILE_BYTES) return null;

  return {
    name: cleanFileName(raw.name),
    type,
    size,
    data,
  };
}

function sendJson(ws, packet) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  try {
    ws.send(JSON.stringify(packet));
  } catch {}
}

function uniqueOnlineUsers() {
  const users = new Map();
  for (const client of chatClients.values()) {
    users.set(client.id, {
      id: client.id,
      name: client.name,
      avatar: client.avatar,
    });
  }
  return Array.from(users.values()).sort((a, b) =>
    a.name.localeCompare(b.name, undefined, { sensitivity: "base" })
  );
}

function broadcast(packet) {
  const payload = JSON.stringify(packet);
  for (const ws of chatClients.keys()) {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(payload);
      } catch {}
    }
  }
}

function broadcastPresence() {
  const users = uniqueOnlineUsers();
  broadcast({ type: "presence", online: users.length, users });
}

function socketsForUser(userId) {
  const out = [];
  for (const [ws, client] of chatClients.entries()) {
    if (client.id === userId && ws.readyState === WebSocket.OPEN) out.push(ws);
  }
  return out;
}

function dmKey(a, b) {
  return [String(a), String(b)].sort().join("::");
}

function trimStoredFileData() {
  const withFiles = [];
  for (const message of publicHistory) {
    if (message.attachment && message.attachment.data) withFiles.push(message);
  }
  for (const row of dmHistory.values()) {
    for (const message of row.messages) {
      if (message.attachment && message.attachment.data) withFiles.push(message);
    }
  }
  withFiles.sort((a, b) => Number(b.time || 0) - Number(a.time || 0));
  let kept = 0;
  for (const message of withFiles) {
    const size = Number(message.attachment && message.attachment.size) || 0;
    kept += size;
    if (kept > MAX_STORED_FILE_BYTES && message.attachment) {
      message.attachment = { ...message.attachment, data: "", expired: true };
    }
  }
}

function rememberDm(message) {
  const key = dmKey(message.from, message.to);
  let row = dmHistory.get(key);
  if (!row) {
    row = { messages: [], touched: Date.now() };
    dmHistory.set(key, row);
  }
  row.touched = Date.now();
  row.messages.push(message);
  if (row.messages.length > DM_HISTORY_LIMIT) {
    row.messages.splice(0, row.messages.length - DM_HISTORY_LIMIT);
  }

  if (dmHistory.size > MAX_DM_THREADS) {
    let oldestKey = null;
    let oldest = Infinity;
    for (const [k, v] of dmHistory.entries()) {
      if (v.touched < oldest) {
        oldest = v.touched;
        oldestKey = k;
      }
    }
    if (oldestKey) dmHistory.delete(oldestKey);
  }
}

function rateLimit(client) {
  const now = Date.now();
  client.recentMessages = client.recentMessages.filter((t) => now - t < 10000);
  if (now - client.lastMessageAt < 550 || client.recentMessages.length >= 8) {
    return false;
  }
  client.lastMessageAt = now;
  client.recentMessages.push(now);
  return true;
}

function closeHttpSession(session) {
  if (!session) return;
  if (httpSessions.get(session.id) === session) httpSessions.delete(session.id);
  try { session.ws.close(); } catch {}
}

function startHttpSession(id) {
  const previous = httpSessions.get(id);
  if (previous) closeHttpSession(previous);
  if (httpSessions.size >= MAX_HTTP_SESSIONS) return null;
  const address = fastify.server.address();
  if (!address || !address.port) return null;
  const session = {
    id, lastSeen: Date.now(), packets: [], queuedBytes: 0,
    hello: null, closed: false, onHello: null, ws: null,
  };
  httpSessions.set(id, session);
  const ws = new WebSocket(`ws://127.0.0.1:${address.port}/chat?id=${encodeURIComponent(id)}`);
  session.ws = ws;
  ws.on("message", (raw) => {
    let packet;
    try { packet = JSON.parse(raw.toString("utf8")); } catch { return; }
    if (packet.type === "hello" && !session.hello) {
      session.hello = packet;
      if (session.onHello) session.onHello(packet);
      return;
    }
    const size = Buffer.byteLength(JSON.stringify(packet));
    session.packets.push({ packet, size });
    session.queuedBytes += size;
    while (session.packets.length > 100 || session.queuedBytes > MAX_HTTP_QUEUE_BYTES) {
      const removed = session.packets.shift();
      session.queuedBytes -= removed.size;
    }
  });
  ws.on("close", () => {
    session.closed = true;
    if (session.onHello && !session.hello) session.onHello(null);
    if (httpSessions.get(id) === session) httpSessions.delete(id);
  });
  ws.on("error", () => {});
  return session;
}

function waitForHttpHello(session) {
  if (session.hello) return Promise.resolve(session.hello);
  return new Promise((resolve) => {
    const timer = setTimeout(() => { session.onHello = null; resolve(null); }, 6500);
    session.onHello = (packet) => { clearTimeout(timer); session.onHello = null; resolve(packet); };
  });
}

const httpCleanup = setInterval(() => {
  for (const session of httpSessions.values()) {
    if (Date.now() - session.lastSeen > HTTP_SESSION_TTL) closeHttpSession(session);
  }
}, 12000);
httpCleanup.unref();

chatWss.on("connection", (ws, req) => {
  let requestedId = "";
  try {
    const u = new URL(req.url || "/chat", "http://localhost");
    requestedId = cleanUserId(u.searchParams.get("id"));
  } catch {}

  const client = {
    id: requestedId || randomUUID().replace(/-/g, ""),
    name: "Guest",
    avatar: "",
    lastMessageAt: 0,
    recentMessages: [],
  };
  chatClients.set(ws, client);

  const users = uniqueOnlineUsers();
  sendJson(ws, {
    type: "hello",
    id: client.id,
    history: publicHistory,
    online: users.length,
    users,
    limits: { fileBytes: MAX_FILE_BYTES },
  });
  broadcastPresence();

  ws.on("message", (raw, isBinary) => {
    if (isBinary || raw.length > MAX_CHAT_PAYLOAD) return;

    let packet;
    try {
      packet = JSON.parse(raw.toString("utf8"));
    } catch {
      return;
    }
    if (!packet || typeof packet !== "object") return;

    if (packet.type === "profile") {
      client.name = cleanName(packet.name);
      client.avatar = cleanAvatar(packet.avatar);
      sendJson(ws, { type: "profile", ok: true, id: client.id, name: client.name });
      broadcastPresence();
      return;
    }

    if (packet.type === "dm_history_request") {
      const other = cleanUserId(packet.with);
      if (!other || other === client.id) return;
      const row = dmHistory.get(dmKey(client.id, other));
      sendJson(ws, {
        type: "dm_history",
        with: other,
        messages: row ? row.messages : [],
      });
      return;
    }

    if (packet.type !== "message" && packet.type !== "dm") return;
    if (!rateLimit(client)) {
      sendJson(ws, { type: "error", message: "You're sending messages too quickly." });
      return;
    }

    const text = cleanText(packet.text);
    const attachment = cleanAttachment(packet.attachment);
    if (!text && !attachment) return;

    if (packet.type === "message") {
      const message = {
        type: "message",
        messageId: randomUUID(),
        id: client.id,
        name: client.name,
        avatar: client.avatar,
        text,
        attachment,
        time: Date.now(),
      };
      publicHistory.push(message);
      if (publicHistory.length > CHAT_HISTORY_LIMIT) {
        publicHistory.splice(0, publicHistory.length - CHAT_HISTORY_LIMIT);
      }
      trimStoredFileData();
      broadcast(message);
      return;
    }

    const to = cleanUserId(packet.to);
    if (!to || to === client.id) return;
    const targetSockets = socketsForUser(to);
    if (!targetSockets.length) {
      sendJson(ws, { type: "error", message: "That user is offline right now." });
      return;
    }

    const message = {
      type: "dm",
      messageId: randomUUID(),
      from: client.id,
      to,
      name: client.name,
      avatar: client.avatar,
      text,
      attachment,
      time: Date.now(),
    };
    rememberDm(message);
    trimStoredFileData();

    const recipients = new Set([...targetSockets, ...socketsForUser(client.id)]);
    for (const peer of recipients) sendJson(peer, message);
  });

  ws.on("close", () => {
    chatClients.delete(ws);
    broadcastPresence();
  });
  ws.on("error", () => {});
});

/* =========================
   FASTIFY + SCRAMJET
   ========================= */

const fastify = Fastify({
  serverFactory: (handler) => {
    return createServer()
      .on("request", (req, res) => {
        res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
        res.setHeader("Cross-Origin-Embedder-Policy", "require-corp");
        if ((req.url || "").startsWith("/chat/http/")) {
          res.setHeader("Access-Control-Allow-Origin", "*");
          res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
          res.setHeader("Access-Control-Allow-Headers", "Content-Type");
          res.setHeader("Cache-Control", "no-store, max-age=0");
        }
        handler(req, res);
      })
      .on("upgrade", (req, socket, head) => {
        let pathname = "";
        try {
          pathname = new URL(req.url || "/", "http://localhost").pathname;
        } catch {}

        if (pathname === "/wisp/" || pathname.endsWith("/wisp/")) {
          wisp.routeRequest(req, socket, head);
          return;
        }

        if (pathname === "/chat") {
          chatWss.handleUpgrade(req, socket, head, (ws) => {
            chatWss.emit("connection", ws, req);
          });
          return;
        }

        socket.end();
      });
  },
});

fastify.register(fastifyStatic, {
  root: publicPath,
  decorateReply: true,
});

fastify.register(fastifyStatic, {
  root: scramjetPath,
  prefix: "/scram/",
  decorateReply: false,
});

fastify.register(fastifyStatic, {
  root: libcurlPath,
  prefix: "/libcurl/",
  decorateReply: false,
});

fastify.register(fastifyStatic, {
  root: baremuxPath,
  prefix: "/baremux/",
  decorateReply: false,
});

fastify.get("/chat/status", async () => ({
  ok: true,
  online: uniqueOnlineUsers().length,
  history: publicHistory.length,
  dms: true,
  fileSharing: true,
  maxFileBytes: MAX_FILE_BYTES,
  httpsFallback: true,
}));

fastify.options("/chat/http/*", async (request, reply) => reply.code(204).send());

fastify.get("/chat/http/connect", async (request, reply) => {
  const id = cleanUserId(request.query?.id);
  if (!id) return reply.code(400).send({ ok: false, error: "Invalid chat ID" });
  const session = startHttpSession(id);
  if (!session) return reply.code(503).send({ ok: false, error: "Chat is busy" });
  const hello = await waitForHttpHello(session);
  if (!hello) {
    closeHttpSession(session);
    return reply.code(503).send({ ok: false, error: "Chat server unavailable" });
  }
  return { ok: true, hello };
});

fastify.get("/chat/http/poll", async (request, reply) => {
  const id = cleanUserId(request.query?.id);
  const session = id && httpSessions.get(id);
  if (!session || session.closed || session.ws.readyState !== WebSocket.OPEN) {
    return reply.code(410).send({ ok: false, error: "Chat session expired" });
  }
  session.lastSeen = Date.now();
  const packets = session.packets.map((row) => row.packet);
  session.packets = [];
  session.queuedBytes = 0;
  return { ok: true, packets };
});

fastify.post("/chat/http/send", { bodyLimit: MAX_CHAT_PAYLOAD }, async (request, reply) => {
  const id = cleanUserId(request.query?.id);
  const session = id && httpSessions.get(id);
  if (!session || session.closed || session.ws.readyState !== WebSocket.OPEN) {
    return reply.code(410).send({ ok: false, error: "Chat session expired" });
  }
  let packet;
  try { packet = JSON.parse(String(request.body || "")); }
  catch { return reply.code(400).send({ ok: false, error: "Invalid message" }); }
  if (!packet || typeof packet !== "object" || Array.isArray(packet)) {
    return reply.code(400).send({ ok: false, error: "Invalid message" });
  }
  session.lastSeen = Date.now();
  try { session.ws.send(JSON.stringify(packet)); }
  catch { return reply.code(503).send({ ok: false, error: "Chat connection closed" }); }
  return { ok: true };
});

fastify.setNotFoundHandler((res, reply) => {
  return reply.code(404).type("text/html").sendFile("404.html");
});

fastify.server.on("listening", () => {
  const address = fastify.server.address();
  console.log("Listening on:");
  console.log(`\thttp://localhost:${address.port}`);
  console.log(`\thttp://${hostname()}:${address.port}`);
  console.log(
    `\thttp://${
      address.family === "IPv6" ? `[${address.address}]` : address.address
    }:${address.port}`
  );
});

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

function shutdown() {
  console.log("SIGTERM signal received: closing HTTP server");
  clearInterval(httpCleanup);
  for (const session of httpSessions.values()) closeHttpSession(session);
  try {
    chatWss.close();
  } catch {}
  fastify.close();
  process.exit(0);
}

let port = parseInt(process.env.PORT || "");
if (isNaN(port)) port = 8080;

fastify.listen({
  port,
  host: "0.0.0.0",
});
