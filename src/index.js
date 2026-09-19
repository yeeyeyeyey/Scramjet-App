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
   WFATP ONLINE CHAT
   PUBLIC CHAT + DMs + FILES
   ========================= */

const CHAT_WS_PATH = "/chat";
const MAX_WS_PAYLOAD = 2.5 * 1024 * 1024;
const CHAT_HISTORY_LIMIT = 60;
const CHAT_TEXT_LIMIT = 700;
const CHAT_AVATAR_LIMIT = 80 * 1024;
const MAX_FILE_BYTES = 1.5 * 1024 * 1024;

const ALLOWED_FILE_TYPES = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "image/gif",
  "application/pdf",
  "text/plain",
]);

const chatWss = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_WS_PAYLOAD,
});

const chatClients = new Map();
const chatHistory = [];
const dmHistory = new Map();

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

  if (
    !/^data:image\/(?:png|jpeg|webp);base64,[A-Za-z0-9+/=]+$/i.test(avatar)
  ) {
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

function approximateBase64Bytes(base64) {
  const clean = String(base64 || "").replace(/\s/g, "");
  if (!clean) return 0;

  let padding = 0;
  if (clean.endsWith("==")) padding = 2;
  else if (clean.endsWith("=")) padding = 1;

  return Math.floor((clean.length * 3) / 4) - padding;
}

function cleanAttachment(value) {
  if (!value || typeof value !== "object") return null;

  const name = cleanFileName(value.name);
  const type = String(value.type || "").toLowerCase().trim();
  const dataUrl = String(value.dataUrl || "");

  if (!ALLOWED_FILE_TYPES.has(type)) return null;

  const escapedType = type.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(
    `^data:${escapedType};base64,([A-Za-z0-9+/=]+)$`,
    "i"
  );

  const match = dataUrl.match(re);

  if (!match) return null;

  const bytes = approximateBase64Bytes(match[1]);

  if (!bytes || bytes > MAX_FILE_BYTES) return null;

  return {
    name,
    type,
    size: bytes,
    dataUrl,
  };
}

function sendJson(ws, packet) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;

  try {
    ws.send(JSON.stringify(packet));
  } catch {}
}

function broadcast(packet, exceptWs = null) {
  const payload = JSON.stringify(packet);

  for (const ws of chatClients.keys()) {
    if (ws === exceptWs || ws.readyState !== WebSocket.OPEN) continue;

    try {
      ws.send(payload);
    } catch {}
  }
}

function publicUser(client) {
  return {
    id: client.id,
    name: client.name,
    avatar: client.avatar,
  };
}

function onlineUsers() {
  const users = [];

  for (const client of chatClients.values()) {
    users.push(publicUser(client));
  }

  return users;
}

function broadcastPresence() {
  broadcast({
    type: "presence",
    online: chatClients.size,
    users: onlineUsers(),
  });
}

function findClientById(id) {
  const wanted = String(id || "");

  for (const [ws, client] of chatClients.entries()) {
    if (client.id === wanted) {
      return { ws, client };
    }
  }

  return null;
}

function dmKey(a, b) {
  return [String(a), String(b)].sort().join("::");
}

function addDmHistory(message) {
  const key = dmKey(message.from, message.to);
  const list = dmHistory.get(key) || [];

  list.push(message);

  if (list.length > 50) {
    list.splice(0, list.length - 50);
  }

  dmHistory.set(key, list);
}

function canSendMessage(client) {
  const now = Date.now();

  client.recentMessages = client.recentMessages.filter(
    (time) => now - time < 10000
  );

  if (
    now - client.lastMessageAt < 500 ||
    client.recentMessages.length >= 10
  ) {
    return false;
  }

  client.lastMessageAt = now;
  client.recentMessages.push(now);

  return true;
}

chatWss.on("connection", (ws) => {
  const client = {
    id: randomUUID(),
    name: "Guest",
    avatar: "",
    lastMessageAt: 0,
    recentMessages: [],
  };

  chatClients.set(ws, client);

  sendJson(ws, {
    type: "hello",
    id: client.id,
    history: chatHistory,
    online: chatClients.size,
    users: onlineUsers(),
  });

  broadcastPresence();

  ws.on("message", (raw, isBinary) => {
    if (isBinary || raw.length > MAX_WS_PAYLOAD) return;

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

      sendJson(ws, {
        type: "profile",
        ok: true,
        id: client.id,
        name: client.name,
        avatar: client.avatar,
      });

      broadcastPresence();
      return;
    }

    if (packet.type === "dm-history") {
      const otherId = String(packet.with || "");

      if (!otherId) return;

      sendJson(ws, {
        type: "dm-history",
        with: otherId,
        messages: dmHistory.get(dmKey(client.id, otherId)) || [],
      });

      return;
    }

    if (packet.type === "message") {
      if (!canSendMessage(client)) {
        sendJson(ws, {
          type: "error",
          message: "You're sending messages too quickly.",
        });
        return;
      }

      const text = cleanText(packet.text);
      const attachment = cleanAttachment(packet.attachment);

      if (!text && !attachment) return;

      if (packet.attachment && !attachment) {
        sendJson(ws, {
          type: "error",
          message: "That file type or size isn't allowed.",
        });
        return;
      }

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

      chatHistory.push(message);

      if (chatHistory.length > CHAT_HISTORY_LIMIT) {
        chatHistory.splice(
          0,
          chatHistory.length - CHAT_HISTORY_LIMIT
        );
      }

      broadcast(message);
      return;
    }

    if (packet.type === "dm") {
      if (!canSendMessage(client)) {
        sendJson(ws, {
          type: "error",
          message: "You're sending messages too quickly.",
        });
        return;
      }

      const to = String(packet.to || "");
      const target = findClientById(to);

      if (!target) {
        sendJson(ws, {
          type: "error",
          message: "That user is no longer online.",
        });
        return;
      }

      if (target.client.id === client.id) return;

      const text = cleanText(packet.text);
      const attachment = cleanAttachment(packet.attachment);

      if (!text && !attachment) return;

      if (packet.attachment && !attachment) {
        sendJson(ws, {
          type: "error",
          message: "That file type or size isn't allowed.",
        });
        return;
      }

      const message = {
        type: "dm",
        messageId: randomUUID(),
        from: client.id,
        to: target.client.id,
        name: client.name,
        avatar: client.avatar,
        text,
        attachment,
        time: Date.now(),
      };

      addDmHistory(message);

      sendJson(ws, message);
      sendJson(target.ws, message);

      return;
    }
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
        res.setHeader(
          "Cross-Origin-Opener-Policy",
          "same-origin"
        );

        res.setHeader(
          "Cross-Origin-Embedder-Policy",
          "require-corp"
        );

        handler(req, res);
      })

      .on("upgrade", (req, socket, head) => {
        let pathname = "";

        try {
          pathname = new URL(
            req.url || "/",
            "http://localhost"
          ).pathname;
        } catch {}

        if (
          pathname === "/wisp/" ||
          pathname.endsWith("/wisp/")
        ) {
          wisp.routeRequest(req, socket, head);
          return;
        }

        if (pathname === CHAT_WS_PATH) {
          chatWss.handleUpgrade(
            req,
            socket,
            head,
            (ws) => {
              chatWss.emit("connection", ws, req);
            }
          );

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
  online: chatClients.size,
  history: chatHistory.length,
  features: {
    publicChat: true,
    dms: true,
    fileSharing: true,
  },
}));

fastify.setNotFoundHandler((res, reply) => {
  return reply
    .code(404)
    .type("text/html")
    .sendFile("404.html");
});

fastify.server.on("listening", () => {
  const address = fastify.server.address();

  console.log("Listening on:");
  console.log(`\thttp://localhost:${address.port}`);
  console.log(`\thttp://${hostname()}:${address.port}`);

  console.log(
    `\thttp://${
      address.family === "IPv6"
        ? `[${address.address}]`
        : address.address
    }:${address.port}`
  );
});

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

function shutdown() {
  console.log(
    "SIGTERM signal received: closing HTTP server"
  );

  try {
    chatWss.close();
  } catch {}

  fastify.close();
  process.exit(0);
}

let port = parseInt(process.env.PORT || "");

if (isNaN(port)) {
  port = 8080;
}

fastify.listen({
  port,
  host: "0.0.0.0",
});
