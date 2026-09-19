import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
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
   ========================================================= */

const CHAT_HISTORY_LIMIT = 80;
const DM_HISTORY_LIMIT = 60;

const CHAT_TEXT_LIMIT = 700;
const CHAT_NAME_LIMIT = 24;

const AVATAR_MAX_BYTES = 32 * 1024;
const FILE_MAX_BYTES = 768 * 1024;

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

  // Enough for a ~768 KB base64 attachment plus JSON overhead.
  maxPayload: 1.25 * 1024 * 1024,
});

const chatClients = new Map();

const chatHistory = [];
const dmHistory = new Map();

/* =========================================================
   HELPERS
   ========================================================= */

function cleanName(value) {
  const name = String(value || "")
    .replace(/[\\/<>\u0000-\u001f\u007f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, CHAT_NAME_LIMIT);

  return name || "Guest";
}

function cleanText(value) {
  return String(value || "")
    .replace(
      /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g,
      ""
    )
    .trim()
    .slice(0, CHAT_TEXT_LIMIT);
}

function safeFileName(value) {
  let name = String(value || "file")
    .replace(/[\\/:*?"<>|\u0000-\u001f]/g, "_")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);

  if (!name) name = "file";

  return name;
}

function dataUrlByteLength(dataUrl) {
  try {
    const comma = dataUrl.indexOf(",");

    if (comma === -1) return 0;

    const base64 = dataUrl.slice(comma + 1);

    return Buffer.from(base64, "base64").length;
  } catch {
    return 0;
  }
}

function cleanAvatar(value) {
  const avatar = String(value || "");

  const match = avatar.match(
    /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/=\r\n]+)$/i
  );

  if (!match) return "";

  if (dataUrlByteLength(avatar) > AVATAR_MAX_BYTES) {
    return "";
  }

  return avatar;
}

function cleanAttachment(value) {
  if (!value || typeof value !== "object") {
    return null;
  }

  const name = safeFileName(value.name);
  const data = String(value.data || "");

  const match = data.match(
    /^data:([^;,]+);base64,([A-Za-z0-9+/=\r\n]+)$/i
  );

  if (!match) {
    return null;
  }

  const type = String(match[1] || "")
    .toLowerCase()
    .trim();

  if (!ALLOWED_FILE_TYPES.has(type)) {
    return null;
  }

  const bytes = dataUrlByteLength(data);

  if (!bytes || bytes > FILE_MAX_BYTES) {
    return null;
  }

  return {
    name,
    type,
    size: bytes,
    data,
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
  let payload;

  try {
    payload = JSON.stringify(packet);
  } catch {
    return;
  }

  for (const ws of chatClients.keys()) {
    if (ws.readyState !== WebSocket.OPEN) {
      continue;
    }

    try {
      ws.send(payload);
    } catch {}
  }
}

function getPublicUsers() {
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
    users: getPublicUsers(),
  });
}

function findSocketById(id) {
  const wanted = String(id || "");

  for (const [ws, client] of chatClients.entries()) {
    if (client.id === wanted) {
      return ws;
    }
  }

  return null;
}

function findClientById(id) {
  const wanted = String(id || "");

  for (const client of chatClients.values()) {
    if (client.id === wanted) {
      return client;
    }
  }

  return null;
}

function dmKey(a, b) {
  return [String(a), String(b)]
    .sort()
    .join(":");
}

function addDmHistory(message) {
  // We intentionally don't save actual file bytes into history.
  // Files are live/temporary so the server doesn't eat tons of RAM.
  const stored = {
    type: "dm",
    id: message.id,
    fromId: message.fromId,
    toId: message.toId,
    name: message.name,
    text: message.text,
    time: message.time,
  };

  if (message.attachment) {
    stored.attachmentExpired = true;
    stored.attachmentName = message.attachment.name;
    stored.attachmentType = message.attachment.type;
  }

  const key = dmKey(
    message.fromId,
    message.toId
  );

  if (!dmHistory.has(key)) {
    dmHistory.set(key, []);
  }

  const history = dmHistory.get(key);

  history.push(stored);

  if (history.length > DM_HISTORY_LIMIT) {
    history.splice(
      0,
      history.length - DM_HISTORY_LIMIT
    );
  }
}

function addPublicHistory(message) {
  const stored = {
    type: "message",
    id: message.id,
    name: message.name,
    avatar: message.avatar,
    text: message.text,
    time: message.time,
  };

  if (message.attachment) {
    stored.attachmentExpired = true;
    stored.attachmentName = message.attachment.name;
    stored.attachmentType = message.attachment.type;
  }

  chatHistory.push(stored);

  if (chatHistory.length > CHAT_HISTORY_LIMIT) {
    chatHistory.splice(
      0,
      chatHistory.length - CHAT_HISTORY_LIMIT
    );
  }
}

function canSendMessage(client) {
  const now = Date.now();

  client.recentMessages =
    client.recentMessages.filter(
      (time) => now - time < 10000
    );

  if (now - client.lastMessageAt < 450) {
    return false;
  }

  if (client.recentMessages.length >= 10) {
    return false;
  }

  client.lastMessageAt = now;
  client.recentMessages.push(now);

  return true;
}

/* =========================================================
   CHAT CONNECTIONS
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

    history: chatHistory,

    online: chatClients.size,
    users: getPublicUsers(),

    limits: {
      text: CHAT_TEXT_LIMIT,
      fileBytes: FILE_MAX_BYTES,
      avatarBytes: AVATAR_MAX_BYTES,
    },
  });

  broadcastPresence();

  ws.on("message", (raw, isBinary) => {
    if (isBinary) {
      return;
    }

    if (raw.length > 1.25 * 1024 * 1024) {
      return;
    }

    let packet;

    try {
      packet = JSON.parse(
        raw.toString("utf8")
      );
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

      if (packet.avatar !== undefined) {
        client.avatar = cleanAvatar(
          packet.avatar
        );
      }

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

    /* -------------------------
       REQUEST DM HISTORY
       ------------------------- */

    if (packet.type === "dm_history") {
      const targetId = String(
        packet.targetId || ""
      );

      if (!targetId) {
        return;
      }

      const key = dmKey(
        client.id,
        targetId
      );

      sendJson(ws, {
        type: "dm_history",

        targetId,

        messages:
          dmHistory.get(key) || [],
      });

      return;
    }

    /* -------------------------
       PUBLIC MESSAGE
       ------------------------- */

    if (packet.type === "message") {
      if (!canSendMessage(client)) {
        sendJson(ws, {
          type: "error",
          message:
            "You're sending messages too quickly.",
        });

        return;
      }

      const text = cleanText(packet.text);

      const attachment =
        cleanAttachment(packet.attachment);

      if (!text && !attachment) {
        sendJson(ws, {
          type: "error",
          message:
            "Message was empty or the file was not supported.",
        });

        return;
      }

      if (
        packet.attachment &&
        !attachment
      ) {
        sendJson(ws, {
          type: "error",
          message:
            "That file type is not supported or the file is too large.",
        });

        return;
      }

      const message = {
        type: "message",

        id: randomUUID(),

        userId: client.id,

        name: client.name,
        avatar: client.avatar,

        text,

        attachment,

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
      if (!canSendMessage(client)) {
        sendJson(ws, {
          type: "error",
          message:
            "You're sending messages too quickly.",
        });

        return;
      }

      const targetId = String(
        packet.targetId || ""
      );

      if (!targetId) {
        return;
      }

      if (targetId === client.id) {
        sendJson(ws, {
          type: "error",
          message:
            "You can't DM yourself.",
        });

        return;
      }

      const targetSocket =
        findSocketById(targetId);

      const targetClient =
        findClientById(targetId);

      if (
        !targetSocket ||
        !targetClient
      ) {
        sendJson(ws, {
          type: "error",
          message:
            "That user is no longer online.",
        });

        return;
      }

      const text = cleanText(packet.text);

      const attachment =
        cleanAttachment(packet.attachment);

      if (!text && !attachment) {
        sendJson(ws, {
          type: "error",
          message:
            "Message was empty or the file was not supported.",
        });

        return;
      }

      if (
        packet.attachment &&
        !attachment
      ) {
        sendJson(ws, {
          type: "error",
          message:
            "That file type is not supported or the file is too large.",
        });

        return;
      }

      const message = {
        type: "dm",

        id: randomUUID(),

        fromId: client.id,
        toId: targetClient.id,

        name: client.name,
        avatar: client.avatar,

        text,

        attachment,

        time: Date.now(),
      };

      addDmHistory(message);

      // Send to recipient.
      sendJson(
        targetSocket,
        message
      );

      // Echo back to sender so the sender's UI updates too.
      sendJson(
        ws,
        message
      );

      return;
    }
  });

  ws.on("close", () => {
    chatClients.delete(ws);

    broadcastPresence();
  });

  ws.on("error", () => {});
});

/* =========================================================
   FASTIFY / SCRAMJET
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

      .on(
        "upgrade",
        (req, socket, head) => {
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
            wisp.routeRequest(
              req,
              socket,
              head
            );

            return;
          }

          /* WFATP Chat */

          if (pathname === "/chat") {
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

          socket.end();
        }
      );
  },
});

/* =========================================================
   STATIC FILES
   ========================================================= */

fastify.register(
  fastifyStatic,
  {
    root: publicPath,
    decorateReply: true,
  }
);

fastify.register(
  fastifyStatic,
  {
    root: scramjetPath,
    prefix: "/scram/",
    decorateReply: false,
  }
);

fastify.register(
  fastifyStatic,
  {
    root: libcurlPath,
    prefix: "/libcurl/",
    decorateReply: false,
  }
);

fastify.register(
  fastifyStatic,
  {
    root: baremuxPath,
    prefix: "/baremux/",
    decorateReply: false,
  }
);

/* =========================================================
   CHAT STATUS
   ========================================================= */

fastify.get(
  "/chat/status",
  async () => {
    return {
      ok: true,

      online: chatClients.size,

      history:
        chatHistory.length,

      features: {
        publicChat: true,
        directMessages: true,
        fileSharing: true,
        profiles: true,
      },

      fileSharing: {
        maxBytes: FILE_MAX_BYTES,

        types: Array.from(
          ALLOWED_FILE_TYPES
        ),
      },
    };
  }
);

/* =========================================================
   404
   ========================================================= */

fastify.setNotFoundHandler(
  (request, reply) => {
    return reply
      .code(404)
      .type("text/html")
      .sendFile("404.html");
  }
);

/* =========================================================
   START
   ========================================================= */

fastify.server.on(
  "listening",
  () => {
    const address =
      fastify.server.address();

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

    console.log(
      `WFATP Chat: /chat`
    );

    console.log(
      `WFATP Chat Status: /chat/status`
    );
  }
);

/* =========================================================
   SHUTDOWN
   ========================================================= */

process.on(
  "SIGINT",
  shutdown
);

process.on(
  "SIGTERM",
  shutdown
);

function shutdown() {
  console.log(
    "Closing WFATP server..."
  );

  try {
    chatWss.close();
  } catch {}

  fastify.close();

  process.exit(0);
}

/* =========================================================
   PORT
   ========================================================= */

let port = parseInt(
  process.env.PORT || "",
  10
);

if (Number.isNaN(port)) {
  port = 8080;
}

fastify.listen({
  port,
  host: "0.0.0.0",
});
