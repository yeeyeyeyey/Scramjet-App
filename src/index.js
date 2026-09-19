import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "url";
import { hostname } from "node:os";

import Fastify from "fastify";
import fastifyStatic from "@fastify/static";
import { WebSocketServer, WebSocket } from "ws";

import { server as wisp, logging } from "@mercuryworkshop/wisp-js/server";
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
   Public chat + DMs + profile pictures + safe file sharing
   ========================================================= */

const MAX_FILE_BYTES = 2 * 1024 * 1024; // 2 MB
const MAX_AVATAR_BYTES = 350 * 1024;
const MAX_MESSAGE_LENGTH = 750;
const HISTORY_LIMIT = 100;

const ALLOWED_FILE_TYPES = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "image/gif",
  "text/plain",
  "application/pdf",
  "audio/mpeg",
  "audio/wav",
  "audio/ogg",
]);

const chatWss = new WebSocketServer({
  noServer: true,

  // Slightly above the 2 MB file limit because JSON/base64 adds size.
  maxPayload: 4 * 1024 * 1024,
});

const clients = new Map();
const publicHistory = [];

/* --------------------------
   Cleaning / validation
   -------------------------- */

function cleanName(value) {
  return (
    String(value || "")
      .replace(/[<>{}\\]/g, "")
      .replace(/[\u0000-\u001f\u007f]/g, "")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 24) || "Guest"
  );
}

function cleanText(value) {
  return String(value || "")
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, "")
    .trim()
    .slice(0, MAX_MESSAGE_LENGTH);
}

function cleanFileName(value) {
  let name = String(value || "file")
    .replace(/[\/\\:*?"<>|]/g, "_")
    .replace(/[\u0000-\u001f\u007f]/g, "")
    .trim()
    .slice(0, 80);

  return name || "file";
}

function estimateBase64Bytes(base64) {
  const clean = String(base64 || "").replace(/\s/g, "");

  if (!clean) return 0;

  let padding = 0;

  if (clean.endsWith("==")) padding = 2;
  else if (clean.endsWith("=")) padding = 1;

  return Math.floor((clean.length * 3) / 4) - padding;
}

function cleanAvatar(value) {
  const data = String(value || "");

  const match = data.match(
    /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/=]+)$/i
  );

  if (!match) return "";

  const bytes = estimateBase64Bytes(match[2]);

  if (bytes <= 0 || bytes > MAX_AVATAR_BYTES) {
    return "";
  }

  return data;
}

function cleanFile(file) {
  if (!file || typeof file !== "object") {
    return null;
  }

  const name = cleanFileName(file.name);
  const type = String(file.type || "").toLowerCase().trim();
  const data = String(file.data || "");

  if (!ALLOWED_FILE_TYPES.has(type)) {
    return null;
  }

  const escapedType = type.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  const pattern = new RegExp(
    "^data:" + escapedType + ";base64,([A-Za-z0-9+/=]+)$",
    "i"
  );

  const match = data.match(pattern);

  if (!match) {
    return null;
  }

  const size = estimateBase64Bytes(match[1]);

  if (size <= 0 || size > MAX_FILE_BYTES) {
    return null;
  }

  return {
    name,
    type,
    size,
    data,
  };
}

/* --------------------------
   Socket helpers
   -------------------------- */

function send(ws, packet) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;

  try {
    ws.send(JSON.stringify(packet));
  } catch {}
}

function broadcast(packet, except = null) {
  const json = JSON.stringify(packet);

  for (const ws of clients.keys()) {
    if (ws === except) continue;
    if (ws.readyState !== WebSocket.OPEN) continue;

    try {
      ws.send(json);
    } catch {}
  }
}

function getPublicUser(client) {
  return {
    id: client.id,
    name: client.name,
    avatar: client.avatar,
  };
}

function getOnlineUsers() {
  return [...clients.values()].map(getPublicUser);
}

function broadcastUsers() {
  broadcast({
    type: "users",
    users: getOnlineUsers(),
    online: clients.size,
  });
}

function findSocketById(id) {
  const wanted = String(id || "");

  for (const [ws, client] of clients.entries()) {
    if (client.id === wanted) {
      return ws;
    }
  }

  return null;
}

function canSend(client) {
  const now = Date.now();

  client.sendTimes = client.sendTimes.filter(
    (time) => now - time < 10000
  );

  if (now - client.lastMessageAt < 400) {
    return false;
  }

  if (client.sendTimes.length >= 10) {
    return false;
  }

  client.lastMessageAt = now;
  client.sendTimes.push(now);

  return true;
}

/* --------------------------
   Chat WebSocket
   -------------------------- */

chatWss.on("connection", (ws) => {
  const client = {
    id: randomUUID(),
    name: "Guest",
    avatar: "",
    lastMessageAt: 0,
    sendTimes: [],
  };

  clients.set(ws, client);

  send(ws, {
    type: "hello",
    id: client.id,
    history: publicHistory,
    users: getOnlineUsers(),
    online: clients.size,
  });

  broadcastUsers();

  ws.on("message", (raw, isBinary) => {
    if (isBinary) return;

    let packet;

    try {
      packet = JSON.parse(raw.toString("utf8"));
    } catch {
      send(ws, {
        type: "error",
        message: "Invalid chat packet.",
      });

      return;
    }

    if (!packet || typeof packet !== "object") {
      return;
    }

    /* =========================
       PROFILE
       ========================= */

    if (packet.type === "profile") {
      client.name = cleanName(packet.name);

      if (packet.avatar !== undefined) {
        client.avatar = cleanAvatar(packet.avatar);
      }

      send(ws, {
        type: "profile",
        ok: true,
        id: client.id,
        name: client.name,
        avatar: client.avatar,
      });

      broadcastUsers();
      return;
    }

    /* =========================
       PUBLIC MESSAGE
       ========================= */

    if (packet.type === "message") {
      if (!canSend(client)) {
        send(ws, {
          type: "error",
          message: "You're sending messages too quickly.",
        });

        return;
      }

      const text = cleanText(packet.text);
      const file = packet.file ? cleanFile(packet.file) : null;

      if (packet.file && !file) {
        send(ws, {
          type: "error",
          message:
            "That file isn't supported or is larger than 2 MB.",
        });

        return;
      }

      if (!text && !file) return;

      const message = {
        type: "message",
        messageId: randomUUID(),

        sender: {
          id: client.id,
          name: client.name,
          avatar: client.avatar,
        },

        text,
        file,
        time: Date.now(),
      };

      /*
        Don't permanently hold file data in server memory.
        Normal text messages go into recent history.
      */

      if (!file) {
        publicHistory.push(message);

        if (publicHistory.length > HISTORY_LIMIT) {
          publicHistory.splice(
            0,
            publicHistory.length - HISTORY_LIMIT
          );
        }
      }

      broadcast(message);
      return;
    }

    /* =========================
       DIRECT MESSAGE
       ========================= */

    if (packet.type === "dm") {
      if (!canSend(client)) {
        send(ws, {
          type: "error",
          message: "You're sending messages too quickly.",
        });

        return;
      }

      const targetId = String(packet.to || "");
      const targetSocket = findSocketById(targetId);

      if (!targetSocket) {
        send(ws, {
          type: "error",
          message: "That user is no longer online.",
        });

        return;
      }

      const targetClient = clients.get(targetSocket);

      const text = cleanText(packet.text);
      const file = packet.file ? cleanFile(packet.file) : null;

      if (packet.file && !file) {
        send(ws, {
          type: "error",
          message:
            "That file isn't supported or is larger than 2 MB.",
        });

        return;
      }

      if (!text && !file) return;

      const dm = {
        type: "dm",
        messageId: randomUUID(),

        from: {
          id: client.id,
          name: client.name,
          avatar: client.avatar,
        },

        to: {
          id: targetClient.id,
          name: targetClient.name,
          avatar: targetClient.avatar,
        },

        text,
        file,
        time: Date.now(),
      };

      /*
        Send DM only to sender + receiver.
        Nobody else receives it.
      */

      send(targetSocket, dm);

      if (targetSocket !== ws) {
        send(ws, dm);
      }

      return;
    }

    /* =========================
       USER LIST REQUEST
       ========================= */

    if (packet.type === "get-users") {
      send(ws, {
        type: "users",
        users: getOnlineUsers(),
        online: clients.size,
      });

      return;
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
    broadcastUsers();
  });

  ws.on("error", () => {});
});

/* =========================================================
   FASTIFY + SCRAMJET
   ========================================================= */

const fastify = Fastify({
  serverFactory: (handler) => {
    const server = createServer();

    server.on("request", (req, res) => {
      res.setHeader(
        "Cross-Origin-Opener-Policy",
        "same-origin"
      );

      res.setHeader(
        "Cross-Origin-Embedder-Policy",
        "require-corp"
      );

      handler(req, res);
    });

    server.on("upgrade", (req, socket, head) => {
      let pathname = "";

      try {
        pathname = new URL(
          req.url || "/",
          "http://localhost"
        ).pathname;
      } catch {}

      /* Scramjet / Wisp */

      if (
        pathname === "/wisp/" ||
        pathname.endsWith("/wisp/")
      ) {
        wisp.routeRequest(req, socket, head);
        return;
      }

      /* WFATP chat */

      if (
        pathname === "/chat" ||
        pathname === "/chat/"
      ) {
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

      socket.destroy();
    });

    return server;
  },
});

/* =========================================================
   STATIC FILES
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
    online: clients.size,
    history: publicHistory.length,

    features: {
      publicChat: true,
      directMessages: true,
      profiles: true,
      avatars: true,
      fileSharing: true,
    },

    maxFileSize: MAX_FILE_BYTES,
  };
});

/* =========================================================
   404
   ========================================================= */

fastify.setNotFoundHandler((req, reply) => {
  return reply
    .code(404)
    .type("text/html")
    .sendFile("404.html");
});

/* =========================================================
   START SERVER
   ========================================================= */

fastify.server.on("listening", () => {
  const address = fastify.server.address();

  console.log("Listening on:");

  console.log(
    `\thttp://localhost:${address.port}`
  );

  console.log(
    `\thttp://${hostname()}:${address.port}`
  );

  console.log(
    `\thttp://${
      address.family === "IPv6"
        ? `[${address.address}]`
        : address.address
    }:${address.port}`
  );

  console.log("WFATP chat enabled.");
});

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

function shutdown() {
  console.log("Closing WFATP server...");

  for (const ws of clients.keys()) {
    try {
      ws.close();
    } catch {}
  }

  try {
    chatWss.close();
  } catch {}

  fastify.close().finally(() => {
    process.exit(0);
  });
}

/* =========================================================
   PORT
   ========================================================= */

let port = parseInt(process.env.PORT || "", 10);

if (Number.isNaN(port)) {
  port = 8080;
}

fastify.listen({
  port,
  host: "0.0.0.0",
});
