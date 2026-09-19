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
   ========================= */

const chatWss = new WebSocketServer({
  noServer: true,
  maxPayload: 96 * 1024,
});

const chatClients = new Map();
const chatHistory = [];

const CHAT_HISTORY_LIMIT = 80;
const CHAT_TEXT_LIMIT = 500;
const CHAT_AVATAR_LIMIT = 52000;

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

function broadcastPresence() {
  broadcast({
    type: "presence",
    online: chatClients.size,
  });
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
  });

  broadcastPresence();

  ws.on("message", (raw, isBinary) => {
    if (isBinary || raw.length > 96 * 1024) return;

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

    if (!text) return;

    client.lastMessageAt = now;
    client.recentMessages.push(now);

    const message = {
      type: "message",
      id: client.id,
      name: client.name,
      avatar: client.avatar,
      text,
      time: now,
    };

    chatHistory.push(message);

    if (chatHistory.length > CHAT_HISTORY_LIMIT) {
      chatHistory.splice(
        0,
        chatHistory.length - CHAT_HISTORY_LIMIT
      );
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

        if (pathname === "/chat") {
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
