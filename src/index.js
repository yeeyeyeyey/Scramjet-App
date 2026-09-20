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

const MAX_CHAT_PAYLOAD = 1.5 * 1024 * 1024;
const chatWss = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_CHAT_PAYLOAD,
});

const chatClients = new Map();
const chatHistory = [];
const CHAT_HISTORY_LIMIT = 80;
const CHAT_TEXT_LIMIT = 500;
const CHAT_AVATAR_LIMIT = 52000;
const CHAT_FILE_DATA_LIMIT = 1_050_000;

const ALLOWED_FILE_TYPES = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "image/gif",
  "text/plain",
  "application/pdf",
]);

function cleanName(value) {
  return String(value || "")
    .replace(/[\\<>\u0000-\u001f\u007f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 24) || "Guest";
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
  return String(value || "file")
    .replace(/[\\/\u0000-\u001f\u007f]/g, "_")
    .trim()
    .slice(0, 80) || "file";
}

function cleanAttachment(value) {
  if (!value || typeof value !== "object") return null;

  const type = String(value.type || "").toLowerCase();
  const data = String(value.data || "");

  if (!ALLOWED_FILE_TYPES.has(type)) return null;
  if (!data.startsWith(`data:${type};base64,`)) return null;
  if (data.length > CHAT_FILE_DATA_LIMIT) return null;

  if (!/^[A-Za-z0-9+/=]+$/.test(data.slice(data.indexOf(",") + 1))) {
    return null;
  }

  return {
    name: cleanFileName(value.name),
    type,
    data,
  };
}

function sendJson(ws, packet) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;

  try {
    ws.send(JSON.stringify(packet));
  } catch {}
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

function publicUsers() {
  return Array.from(chatClients.values()).map((client) => ({
    id: client.id,
    name: client.name,
    avatar: client.avatar,
  }));
}

function broadcastPresence() {
  broadcast({
    type: "presence",
    online: chatClients.size,
    users: publicUsers(),
  });
}

function findSocketByClientId(id) {
  for (const [ws, client] of chatClients.entries()) {
    if (client.id === id) return ws;
  }

  return null;
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
    users: publicUsers(),
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

      sendJson(ws, {
        type: "profile",
        ok: true,
        name: client.name,
      });

      broadcastPresence();
      return;
    }

    if (packet.type !== "message") return;

    const now = Date.now();

    client.recentMessages = client.recentMessages.filter(
      (time) => now - time < 10000
    );

    if (
      now - client.lastMessageAt < 550 ||
      client.recentMessages.length >= 8
    ) {
      sendJson(ws, {
        type: "error",
        message: "You're sending messages too quickly.",
      });
      return;
    }

    const text = cleanText(packet.text);
    const attachment = cleanAttachment(packet.attachment);

    if (!text && !attachment) {
      if (packet.attachment) {
        sendJson(ws, {
          type: "error",
          message: "That file type or size is not allowed.",
        });
      }
      return;
    }

    client.lastMessageAt = now;
    client.recentMessages.push(now);

    const to = String(packet.to || "").trim();

    const message = {
      type: "message",
      scope: to ? "dm" : "general",
      id: client.id,
      name: client.name,
      avatar: client.avatar,
      text,
      time: now,
      ...(attachment ? { attachment } : {}),
      ...(to ? { to } : {}),
    };

    if (to) {
      if (to === client.id) {
        sendJson(ws, {
          type: "error",
          message: "You can't DM yourself.",
        });
        return;
      }

      const targetWs = findSocketByClientId(to);

      if (!targetWs || targetWs.readyState !== WebSocket.OPEN) {
        sendJson(ws, {
          type: "error",
          message: "That user is no longer online.",
        });
        return;
      }

      // Private messages go only to their recipient and sender.
      sendJson(targetWs, message);
      sendJson(ws, message);
      return;
    }

    // Public text history only. Files and DMs are live-only.
    if (!attachment) {
      chatHistory.push(message);

      if (chatHistory.length > CHAT_HISTORY_LIMIT) {
        chatHistory.splice(
          0,
          chatHistory.length - CHAT_HISTORY_LIMIT
        );
      }
    }

    broadcast(message);
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

        if (pathname === "/chat") {
          chatWss.handleUpgrade(req, socket, head, (upgraded) => {
            chatWss.emit("connection", upgraded, req);
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
  online: chatClients.size,
  history: chatHistory.length,
  dms: true,
  files: true,
}));

fastify.setNotFoundHandler((res, reply) =>
  reply.code(404).type("text/html").sendFile("404.html")
);

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
  console.log("SIGTERM signal received: closing HTTP server");

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
