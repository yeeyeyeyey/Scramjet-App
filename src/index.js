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

function publicClientInfo(client) {
  return {
    id: client.id,
    name: client.name,
    avatar: client.avatar,
  };
}

function onlineUsers() {
  return Array.from(chatClients.values()).map(publicClientInfo);
}

function broadcastPresence() {
  broadcast({
    type: "presence",
    online: chatClients.size,
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
    online: chatClients.size,
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

      sendJson(target.ws, dm);

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

        sendJson(target.ws, filePacket);

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
        online: chatClients.size,
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
   CHAT STATUS
   ========================================================= */

fastify.get("/chat/status", async () => {
  return {
    ok: true,
    online: chatClients.size,
    history: chatHistory.length,
    features: {
      publicChat: true,
      directMessages: true,
      fileSharing: true,
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
