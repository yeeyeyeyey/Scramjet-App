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
   Public chat + DMs + files
   ========================= */

const CHAT_HISTORY_LIMIT = 80;
const CHAT_TEXT_LIMIT = 500;
const CHAT_AVATAR_LIMIT = 52000;
const CHAT_FILE_MAX_BYTES = 4 * 1024 * 1024;
const CHAT_SOCKET_MAX_BYTES = 7 * 1024 * 1024;

const ALLOWED_FILE_MIMES = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "image/gif",
  "application/pdf",
  "text/plain",
  "audio/mpeg",
  "audio/wav",
  "audio/ogg",
  "video/mp4",
  "video/webm",
]);

const chatWss = new WebSocketServer({
  noServer: true,
  maxPayload: CHAT_SOCKET_MAX_BYTES,
});

const chatClients = new Map();
const chatHistory = [];

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

function parseFileData(dataUrl) {
  const value = String(dataUrl || "");
  const match = value.match(
    /^data:([^;,]+);base64,([A-Za-z0-9+/=]+)$/i
  );

  if (!match) return null;

  const mime = String(match[1] || "").toLowerCase();
  const base64 = match[2] || "";

  if (!ALLOWED_FILE_MIMES.has(mime)) return null;

  let size = 0;

  try {
    size = Buffer.byteLength(base64, "base64");
  } catch {
    return null;
  }

  if (!size || size > CHAT_FILE_MAX_BYTES) return null;

  return {
    mime,
    size,
    data: `data:${mime};base64,${base64}`,
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

function getClientById(id) {
  const wanted = String(id || "");

  for (const [ws, client] of chatClients.entries()) {
    if (client.id === wanted) {
      return { ws, client };
    }
  }

  return null;
}

function publicClient(client) {
  return {
    id: client.id,
    name: client.name,
    avatar: client.avatar,
  };
}

function getOnlineUsers() {
  return Array.from(chatClients.values()).map(publicClient);
}

function broadcastPresence() {
  broadcast({
    type: "presence",
    online: chatClients.size,
    users: getOnlineUsers(),
  });
}

function canSendText(client) {
  const now = Date.now();

  client.recentMessages = client.recentMessages.filter(
    (time) => now - time < 10000
  );

  if (
    now - client.lastMessageAt < 550 ||
    client.recentMessages.length >= 8
  ) {
    return false;
  }

  client.lastMessageAt = now;
  client.recentMessages.push(now);

  return true;
}

function canSendFile(client) {
  const now = Date.now();

  client.recentFiles = client.recentFiles.filter(
    (time) => now - time < 60000
  );

  if (
    now - client.lastFileAt < 4000 ||
    client.recentFiles.length >= 4
  ) {
    return false;
  }

  client.lastFileAt = now;
  client.recentFiles.push(now);

  return true;
}

chatWss.on("connection", (ws) => {
  const client = {
    id: randomUUID(),
    name: "Guest",
    avatar: "",
    lastMessageAt: 0,
    recentMessages: [],
    lastFileAt: 0,
    recentFiles: [],
  };

  chatClients.set(ws, client);

  sendJson(ws, {
    type: "hello",
    id: client.id,
    history: chatHistory,
    online: chatClients.size,
    users: getOnlineUsers(),

    limits: {
      text: CHAT_TEXT_LIMIT,
      fileBytes: CHAT_FILE_MAX_BYTES,
    },
  });

  broadcastPresence();

  ws.on("message", (raw, isBinary) => {
    if (isBinary || raw.length > CHAT_SOCKET_MAX_BYTES) {
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

    /* PROFILE */

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

    /* PUBLIC MESSAGE / DM */

    if (
      packet.type === "message" ||
      packet.type === "dm"
    ) {
      if (!canSendText(client)) {
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

      const isDm =
        packet.type === "dm" ||
        packet.scope === "dm";

      const now = Date.now();

      if (isDm) {
        const target = getClientById(packet.to);

        if (!target) {
          sendJson(ws, {
            type: "error",
            message: "That user is no longer online.",
          });

          return;
        }

        const message = {
          type: "message",
          scope: "dm",

          id: client.id,
          from: client.id,
          to: target.client.id,

          name: client.name,
          avatar: client.avatar,

          text,
          time: now,
        };

        sendJson(ws, message);

        if (target.ws !== ws) {
          sendJson(target.ws, message);
        }

        return;
      }

      const message = {
        type: "message",
        scope: "public",

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
      return;
    }

    /* FILE SHARING */

    if (packet.type === "file") {
      if (!canSendFile(client)) {
        sendJson(ws, {
          type: "error",
          message: "You're sending files too quickly.",
        });

        return;
      }

      const parsed = parseFileData(packet.data);

      if (!parsed) {
        sendJson(ws, {
          type: "error",
          message:
            "That file type is not allowed or the file is over 4 MB.",
        });

        return;
      }

      const isDm =
        packet.scope === "dm" ||
        Boolean(packet.to);

      const now = Date.now();

      const fileMessage = {
        type: "file",
        scope: isDm ? "dm" : "public",

        id: client.id,
        from: client.id,

        name: client.name,
        avatar: client.avatar,

        time: now,

        file: {
          name: cleanFileName(packet.name),
          mime: parsed.mime,
          size: parsed.size,
          data: parsed.data,
        },
      };

      if (isDm) {
        const target = getClientById(packet.to);

        if (!target) {
          sendJson(ws, {
            type: "error",
            message: "That user is no longer online.",
          });

          return;
        }

        fileMessage.to = target.client.id;

        sendJson(ws, fileMessage);

        if (target.ws !== ws) {
          sendJson(target.ws, fileMessage);
        }

        return;
      }

      /*
        Files are live-only.
        They are NOT stored in history so Railway memory
        doesn't get filled with base64 files.
      */

      broadcast(fileMessage);
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
          wisp.routeRequest(
            req,
            socket,
            head
          );

          return;
        }

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
      });
  },
});

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

fastify.get(
  "/chat/status",
  async () => ({
    ok: true,

    online: chatClients.size,
    history: chatHistory.length,

    users: getOnlineUsers().map(
      (user) => ({
        id: user.id,
        name: user.name,
      })
    ),

    features: {
      dms: true,
      files: true,
      maxFileBytes: CHAT_FILE_MAX_BYTES,
    },
  })
);

fastify.setNotFoundHandler(
  (res, reply) => {
    return reply
      .code(404)
      .type("text/html")
      .sendFile("404.html");
  }
);

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
  }
);

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

let port = parseInt(
  process.env.PORT || ""
);

if (isNaN(port)) {
  port = 8080;
}

fastify.listen({
  port,
  host: "0.0.0.0",
});
