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

/* =========================================================
   WFATP CHAT
   Public chat + live DMs + small-file sharing
   ========================================================= */

const MAX_SOCKET_PAYLOAD = 2 * 1024 * 1024;
const MAX_FILE_BYTES = 1024 * 1024;
const MAX_MESSAGE_LENGTH = 500;
const MAX_NAME_LENGTH = 24;
const MAX_AVATAR_LENGTH = 70000;
const MAX_FILE_NAME_LENGTH = 80;
const CHAT_HISTORY_LIMIT = 80;

const ALLOWED_FILE_TYPES = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "image/gif",
  "text/plain",
  "application/pdf",
]);

const chatWss = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_SOCKET_PAYLOAD,
});

const chatClients = new Map();
const chatHistory = [];

// Older WebSocket clients keep their original packet format. The HTTPS
// sessions below use the packet format expected by the current index.html.
const httpClients = new Map();
const chatSessions = new Map();
const httpEvents = [];
let httpCursor = 0;
let httpEventBytes = 0;
const allowedChatOrigins = new Set(
  (process.env.CHAT_ALLOWED_ORIGINS || "")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean)
);

function validChatOrigin(req) {
  const origin = req.headers.origin;
  if (!origin) return true;
  if (allowedChatOrigins.size) return allowedChatOrigins.has(origin);
  return origin === "null" || /^https?:\/\/[^/]+$/i.test(origin);
}

/* =========================================================
   CHAT HELPERS
   ========================================================= */

function cleanName(value) {
  return (
    String(value || "")
      .replace(/[\\<>\u0000-\u001f\u007f]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, MAX_NAME_LENGTH) || "Guest"
  );
}

function cleanText(value) {
  let text = String(value || "")
    .replace(
      /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g,
      ""
    )
    .trim()
    .slice(0, MAX_MESSAGE_LENGTH);

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
    !/^data:image\/(?:png|jpeg|webp);base64,[A-Za-z0-9+/=]+$/i.test(
      avatar
    )
  ) {
    return "";
  }

  if (avatar.length > MAX_AVATAR_LENGTH) {
    return "";
  }

  return avatar;
}

function cleanFileName(value) {
  return (
    String(value || "file")
      .replace(/[\\/:*?"<>|\u0000-\u001f]/g, "_")
      .trim()
      .slice(0, MAX_FILE_NAME_LENGTH) || "file"
  );
}

function estimateBase64Bytes(base64) {
  const clean = String(base64 || "").replace(/\s/g, "");

  if (!clean) return 0;

  let padding = 0;

  if (clean.endsWith("==")) padding = 2;
  else if (clean.endsWith("=")) padding = 1;

  return Math.floor((clean.length * 3) / 4) - padding;
}

function cleanFile(file) {
  if (!file || typeof file !== "object") {
    return null;
  }

  const name = cleanFileName(file.name);
  const mime = String(file.mime || "").toLowerCase().trim();
  const data = String(file.data || "");

  if (!ALLOWED_FILE_TYPES.has(mime)) {
    return null;
  }

  const escapedMime = mime.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  const match = data.match(
    new RegExp(
      `^data:${escapedMime};base64,([A-Za-z0-9+/=]+)$`,
      "i"
    )
  );

  if (!match) {
    return null;
  }

  const bytes = estimateBase64Bytes(match[1]);

  if (!bytes || bytes > MAX_FILE_BYTES) {
    return null;
  }

  return {
    name,
    mime,
    data,
    size: bytes,
  };
}

function sendJson(ws, packet) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    return;
  }

  try {
    ws.send(JSON.stringify(packet));
  } catch {}
}

function modernPacket(packet) {
  if (!packet || typeof packet !== "object") return null;
  if (packet.type === "presence") return packet;
  if (packet.type === "message" && packet.user) {
    return {
      type: "message", mid: packet.messageId, id: packet.user.id,
      name: packet.user.name, avatar: packet.user.avatar,
      text: packet.text, time: packet.time, scope: "general",
    };
  }
  if (packet.type === "dm") {
    return {
      type: "message", mid: packet.messageId, id: packet.from.id,
      name: packet.from.name, avatar: packet.from.avatar,
      to: packet.to.id, text: packet.text, time: packet.time, scope: "dm",
    };
  }
  if (packet.type === "file" || packet.type === "dm-file") {
    return {
      type: "message", mid: packet.messageId, id: packet.from.id,
      name: packet.from.name, avatar: packet.from.avatar,
      ...(packet.to ? { to: packet.to.id } : {}),
      scope: packet.to ? "dm" : "general", text: packet.text || "",
      attachment: {
        name: packet.file.name, type: packet.file.mime, data: packet.file.data,
      },
      time: packet.time,
    };
  }
  return null;
}

function pushHttpEvent(legacyPacket, recipientIds = null) {
  const packet = modernPacket(legacyPacket);
  if (!packet) return;
  const record = {
    seq: ++httpCursor, packet, recipientIds,
    bytes: JSON.stringify(packet).length,
  };
  httpEvents.push(record);
  httpEventBytes += record.bytes;
  while (httpEvents.length > 250 || httpEventBytes > 8 * 1024 * 1024) {
    httpEventBytes -= httpEvents.shift().bytes;
  }
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

  pushHttpEvent(packet);
}

function publicClientInfo(client) {
  return {
    id: client.id,
    name: client.name,
    avatar: client.avatar,
  };
}

function onlineUsers() {
  return Array.from(chatClients.values()).concat(Array.from(httpClients.values()))
    .map(publicClientInfo);
}

function broadcastPresence() {
  broadcast({
    type: "presence",
    online: onlineUsers().length,
    users: onlineUsers(),
  });
}

function findSocketById(id) {
  const wanted = String(id || "");

  for (const [ws, client] of chatClients.entries()) {
    if (client.id === wanted) {
      return {
        ws,
        client,
      };
    }
  }

  const client = httpClients.get(wanted);
  if (client) return { ws: null, client };

  return null;
}

function canSend(client) {
  const now = Date.now();

  client.recentMessages =
    client.recentMessages.filter(
      (time) => now - time < 10000
    );

  if (now - client.lastMessageAt < 500) {
    return false;
  }

  if (client.recentMessages.length >= 10) {
    return false;
  }

  client.lastMessageAt = now;
  client.recentMessages.push(now);

  return true;
}

function addPublicHistory(packet) {
  chatHistory.push(packet);

  if (chatHistory.length > CHAT_HISTORY_LIMIT) {
    chatHistory.splice(
      0,
      chatHistory.length - CHAT_HISTORY_LIMIT
    );
  }
}

/* =========================================================
   CHAT SOCKET
   ========================================================= */

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
    profile: publicClientInfo(client),
    history: chatHistory,
    online: onlineUsers().length,
    users: onlineUsers(),
    limits: {
      messageLength: MAX_MESSAGE_LENGTH,
      fileBytes: MAX_FILE_BYTES,
      fileTypes: Array.from(ALLOWED_FILE_TYPES),
    },
  });

  broadcastPresence();

  ws.on("message", (raw, isBinary) => {
    if (isBinary) {
      return;
    }

    if (raw.length > MAX_SOCKET_PAYLOAD) {
      sendJson(ws, {
        type: "error",
        message: "That upload is too large.",
      });

      return;
    }

    let packet;

    try {
      packet = JSON.parse(raw.toString("utf8"));
    } catch {
      return;
    }

    if (!packet || typeof packet !== "object") {
      return;
    }

    /* -------------------------
       PROFILE
       ------------------------- */

    if (packet.type === "profile") {
      client.name = cleanName(packet.name);
      client.avatar = cleanAvatar(packet.avatar);

      sendJson(ws, {
        type: "profile",
        ok: true,
        profile: publicClientInfo(client),
      });

      broadcastPresence();
      return;
    }

    /* -------------------------
       CURRENT INDEX.HTML: DM / ATTACHMENT IN ONE PACKET
       Legacy clients still use the branches below unchanged.
       ------------------------- */

    if (packet.type === "message" && (packet.to || packet.attachment)) {
      if (!canSend(client)) {
        sendJson(ws, { type: "error", message: "You're sending messages too quickly." });
        return;
      }

      const text = cleanText(packet.text);
      const file = packet.attachment ? cleanFile({
        name: packet.attachment.name,
        mime: packet.attachment.type,
        data: packet.attachment.data,
      }) : null;

      if (packet.attachment && !file) {
        sendJson(ws, { type: "error", message: "Unsupported attachment or file too large." });
        return;
      }
      if (!text && !file) {
        sendJson(ws, { type: "error", message: "Write a message or attach a file." });
        return;
      }

      const target = packet.to ? findSocketById(packet.to) : null;
      if (packet.to && (!target || target.client.id === client.id)) {
        sendJson(ws, { type: "error", message: "That user is not available for a DM." });
        return;
      }

      const outgoing = {
        type: file ? (target ? "dm-file" : "file") : "dm",
        messageId: randomUUID(),
        from: publicClientInfo(client),
        ...(target ? { to: publicClientInfo(target.client) } : {}),
        ...(file ? { file } : {}),
        text,
        time: Date.now(),
      };

      if (!target) {
        broadcast(outgoing);
      } else {
        if (target.ws) sendJson(target.ws, outgoing);
        else pushHttpEvent(outgoing, [target.client.id]);
        if (target.ws !== ws) sendJson(ws, outgoing);
      }
      return;
    }

    /* -------------------------
       PUBLIC MESSAGE
       ------------------------- */

    if (packet.type === "message") {
      if (!canSend(client)) {
        sendJson(ws, {
          type: "error",
          message: "You're sending messages too quickly.",
        });

        return;
      }

      const text = cleanText(packet.text);

      if (!text) {
        return;
      }

      const message = {
        type: "message",
        messageId: randomUUID(),
        user: publicClientInfo(client),
        text,
        time: Date.now(),
      };

      addPublicHistory(message);
      broadcast(message);

      return;
    }

    /* -------------------------
       DIRECT MESSAGE
       ------------------------- */

    if (packet.type === "dm") {
      if (!canSend(client)) {
        sendJson(ws, {
          type: "error",
          message: "You're sending messages too quickly.",
        });

        return;
      }

      const target = findSocketById(packet.to);
      const text = cleanText(packet.text);

      if (!target) {
        sendJson(ws, {
          type: "error",
          message: "That user is no longer online.",
        });

        return;
      }

      if (!text) {
        return;
      }

      const dm = {
        type: "dm",
        messageId: randomUUID(),

        from: publicClientInfo(client),
        to: publicClientInfo(target.client),

        text,
        time: Date.now(),
      };

      if (target.ws) sendJson(target.ws, dm);
      else pushHttpEvent(dm, [target.client.id]);

      if (target.ws !== ws) {
        sendJson(ws, dm);
      }

      return;
    }

    /* -------------------------
       FILE
       Can be public or DM
       ------------------------- */

    if (packet.type === "file") {
      if (!canSend(client)) {
        sendJson(ws, {
          type: "error",
          message: "You're sending files too quickly.",
        });

        return;
      }

      const file = cleanFile(packet.file);

      if (!file) {
        sendJson(ws, {
          type: "error",
          message:
            "That file isn't supported. Use PNG, JPG, WEBP, GIF, TXT, or PDF under 1 MB.",
        });

        return;
      }

      const targetId = String(packet.to || "").trim();

      const filePacket = {
        type: targetId ? "dm-file" : "file",
        messageId: randomUUID(),
        from: publicClientInfo(client),
        file,
        time: Date.now(),
      };

      /* DM FILE */

      if (targetId) {
        const target = findSocketById(targetId);

        if (!target) {
          sendJson(ws, {
            type: "error",
            message: "That user is no longer online.",
          });

          return;
        }

        filePacket.to =
          publicClientInfo(target.client);

        if (target.ws) sendJson(target.ws, filePacket);
        else pushHttpEvent(filePacket, [target.client.id]);

        if (target.ws !== ws) {
          sendJson(ws, filePacket);
        }

        return;
      }

      /* PUBLIC FILE */

      broadcast(filePacket);

      return;
    }

    /* -------------------------
       USER LIST REFRESH
       ------------------------- */

    if (packet.type === "users") {
      sendJson(ws, {
        type: "presence",
        online: onlineUsers().length,
        users: onlineUsers(),
      });

      return;
    }

    /* -------------------------
       PING
       ------------------------- */

    if (packet.type === "ping") {
      sendJson(ws, {
        type: "pong",
        time: Date.now(),
      });
    }
  });

  ws.on("close", () => {
    chatClients.delete(ws);
    broadcastPresence();
  });

  ws.on("error", () => {});
});

/* =========================================================
   FASTIFY + SCRAMJET
   ========================================================= */

const fastify = Fastify({
  bodyLimit: MAX_SOCKET_PAYLOAD,
  serverFactory: (handler) => {
    return createServer()
      .on("request", (req, res) => {
        if (!req.url.startsWith("/chat")) {
          res.setHeader(
            "Cross-Origin-Opener-Policy",
            "same-origin"
          );

          res.setHeader(
            "Cross-Origin-Embedder-Policy",
            "require-corp"
          );
        }

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

        /* WISP */

        if (
          pathname === "/wisp/" ||
          pathname.endsWith("/wisp/")
        ) {
          wisp.routeRequest(req, socket, head);
          return;
        }

        /* WFATP CHAT */

        if (
          pathname === "/chat" ||
          pathname === "/chat/"
        ) {
          if (!validChatOrigin(req)) {
            socket.destroy();
            return;
          }
          chatWss.handleUpgrade(
            req,
            socket,
            head,
            (ws) => {
              chatWss.emit(
                "connection",
                ws,
                req
              );
            }
          );

          return;
        }

        socket.destroy();
      });
  },
});

fastify.addHook("onRequest", (request, reply, done) => {
  if (!request.url.startsWith("/chat")) {
    done();
    return;
  }

  if (!validChatOrigin(request.raw)) {
    reply.code(403).send({ error: "Origin not allowed" });
    return;
  }

  if (request.headers.origin) {
    reply.header("Access-Control-Allow-Origin", request.headers.origin);
    reply.header("Vary", "Origin");
  }
  reply.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  reply.header("Access-Control-Allow-Headers", "Content-Type");
  reply.header("Cache-Control", "no-store");

  if (request.method === "OPTIONS") {
    reply.code(204).send();
    return;
  }
  done();
});

/* =========================================================
   STATIC ROUTES
   ========================================================= */

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

/* =========================================================
   HTTPS CHAT FOR IPAD
   The original WebSocket protocol above stays intact. These
   endpoints translate its messages for the current HTML page.
   ========================================================= */

function newHttpClient() {
  const client = {
    id: randomUUID(),
    token: randomUUID(),
    name: "Guest",
    avatar: "",
    lastMessageAt: 0,
    recentMessages: [],
    seen: Date.now(),
  };

  httpClients.set(client.id, client);
  chatSessions.set(client.token, client);
  return client;
}

function removeHttpClient(client) {
  if (!client || !httpClients.delete(client.id)) return;
  chatSessions.delete(client.token);
  broadcastPresence();
}

function httpClientFor(request) {
  const token = String(
    (request.query && request.query.token) ||
    (request.body && request.body.token) ||
    ""
  );
  const client = chatSessions.get(token);
  if (client) client.seen = Date.now();
  return client;
}

function httpMessage(client, packet) {
  if (!packet || typeof packet !== "object") {
    return { type: "error", message: "Invalid request." };
  }

  if (packet.type === "profile") {
    client.name = cleanName(packet.name);
    client.avatar = cleanAvatar(packet.avatar);
    broadcastPresence();
    return {
      type: "profile", ok: true, profile: publicClientInfo(client),
    };
  }

  if (packet.type === "ping") return { type: "pong", time: Date.now() };
  if (packet.type === "users") {
    return {
      type: "presence", online: onlineUsers().length,
      users: onlineUsers(),
    };
  }

  if (packet.type !== "message") {
    return { type: "error", message: "Unknown request." };
  }

  if (!canSend(client)) {
    return { type: "error", message: "You're sending messages too quickly." };
  }

  const text = cleanText(packet.text);
  const file = packet.attachment ? cleanFile({
    name: packet.attachment.name,
    mime: packet.attachment.type,
    data: packet.attachment.data,
  }) : null;

  if (packet.attachment && !file) {
    return { type: "error", message: "Unsupported attachment or file too large." };
  }
  if (!text && !file) {
    return { type: "error", message: "Write a message or attach a file." };
  }

  const target = packet.to ? findSocketById(packet.to) : null;
  if (packet.to && (!target || target.client.id === client.id)) {
    return { type: "error", message: "That user is not available for a DM." };
  }

  let outgoing;
  if (file) {
    outgoing = {
      type: target ? "dm-file" : "file",
      messageId: randomUUID(),
      from: publicClientInfo(client),
      ...(target ? { to: publicClientInfo(target.client) } : {}),
      file,
      text,
      time: Date.now(),
    };
  } else if (target) {
    outgoing = {
      type: "dm",
      messageId: randomUUID(),
      from: publicClientInfo(client),
      to: publicClientInfo(target.client),
      text,
      time: Date.now(),
    };
  } else {
    outgoing = {
      type: "message",
      messageId: randomUUID(),
      user: publicClientInfo(client),
      text,
      time: Date.now(),
    };
    addPublicHistory(outgoing);
  }

  if (target) {
    if (target.ws) sendJson(target.ws, outgoing);
    const ids = target.ws ? [client.id] : [client.id, target.client.id];
    pushHttpEvent(outgoing, ids);
  } else {
    broadcast(outgoing);
  }

  return modernPacket(outgoing);
}

fastify.get("/chat/health", async () => ({ ok: true }));

fastify.post("/chat/session", async () => {
  const client = newHttpClient();
  const packet = {
    type: "hello",
    id: client.id,
    users: onlineUsers(),
    online: onlineUsers().length,
    history: chatHistory.map(modernPacket).filter(Boolean),
  };
  broadcastPresence();
  return { token: client.token, cursor: httpCursor, packet };
});

fastify.get("/chat/events", async (request, reply) => {
  const client = httpClientFor(request);
  if (!client) return reply.code(401).send({ error: "Session expired." });
  const after = Math.max(0, Number(request.query.after) || 0);
  return {
    cursor: httpCursor,
    events: httpEvents
      .filter((event) => event.seq > after &&
        (!event.recipientIds || event.recipientIds.includes(client.id)))
      .map((event) => event.packet),
  };
});

fastify.post("/chat/send", async (request, reply) => {
  const client = httpClientFor(request);
  if (!client) return reply.code(401).send({ error: "Session expired." });
  const packet = httpMessage(client, request.body);
  if (packet.type === "error") return reply.code(400).send(packet);
  return { packet };
});

fastify.post("/chat/leave", async (request) => {
  removeHttpClient(httpClientFor(request));
  return { ok: true };
});

setInterval(() => {
  const now = Date.now();
  for (const client of httpClients.values()) {
    if (now - client.seen > 45000) removeHttpClient(client);
  }
}, 15000).unref();

/* =========================================================
   CHAT STATUS
   ========================================================= */

fastify.get("/chat/status", async () => {
  return {
    ok: true,
    online: onlineUsers().length,
    history: chatHistory.length,
    features: {
      publicChat: true,
      directMessages: true,
      fileSharing: true,
      httpsFallback: true,
    },
  };
});

/* =========================================================
   404
   ========================================================= */

fastify.setNotFoundHandler((request, reply) => {
  return reply
    .code(404)
    .type("text/html")
    .sendFile("404.html");
});

/* =========================================================
   STARTUP
   ========================================================= */

fastify.server.on("listening", () => {
  const address = fastify.server.address();

  console.log("WFATP server listening on:");

  console.log(
    `http://localhost:${address.port}`
  );

  console.log(
    `http://${hostname()}:${address.port}`
  );

  console.log(
    `http://${
      address.family === "IPv6"
        ? `[${address.address}]`
        : address.address
    }:${address.port}`
  );
});

/* =========================================================
   SHUTDOWN
   ========================================================= */

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

function shutdown() {
  console.log("Closing WFATP server...");

  for (const ws of chatClients.keys()) {
    try {
      ws.close();
    } catch {}
  }

  try {
    chatWss.close();
  } catch {}

  fastify
    .close()
    .finally(() => {
      process.exit(0);
    });
}

/* =========================================================
   LISTEN
   ========================================================= */

let port = parseInt(process.env.PORT || "");

if (Number.isNaN(port)) {
  port = 8080;
}

fastify.listen({
  port,
  host: "0.0.0.0",
});
