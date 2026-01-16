import express from "express";
import cors from "cors";
import { io, Socket } from "socket.io-client";

type TLQuote = {
  type: "Quote";
  instrument: string;
  routeId: string;
  timestamp: string;
  bid: string;
  ask: string;
  [k: string]: unknown;
};

const app = express();
app.use(cors());
app.use(express.json());

const PORT = Number(process.env.PORT ?? 3000);

// TradeLocker BrandSocket config
const TL_HOST = process.env.TL_HOST ?? "https://api-dev.tradelocker.com";
const TL_NAMESPACE = "/brand-socket";
const TL_PATH = process.env.TL_PATH ?? "/brand-api/socket.io";
const TL_TYPE = process.env.TL_TYPE ?? "DEMO"; // docs use type=LIVE/DEMO in examples :contentReference[oaicite:1]{index=1}
const TL_BRAND_API_KEY = process.env.TL_BRAND_API_KEY;

if (!TL_BRAND_API_KEY) throw new Error("Missing env TL_BRAND_API_KEY");

const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN; // optional auth for your SSE endpoint

function authed(req: express.Request): boolean {
  if (!INTERNAL_TOKEN) return true;
  const got = req.header("authorization")?.replace(/^Bearer\s+/i, "");
  return got === INTERNAL_TOKEN;
}

const socket: Socket = io(`${TL_HOST}${TL_NAMESPACE}`, {
  path: TL_PATH,
  transports: ["websocket"],
  query: { type: TL_TYPE },
  extraHeaders: { "brand-api-key": TL_BRAND_API_KEY },
});

socket.on("connect", () => console.log("[brandsocket] connected", socket.id));
socket.on("disconnect", (r) => console.log("[brandsocket] disconnected", r));
socket.on("error", (e) => console.log("[brandsocket] error", e));

// BrandSocket can emit connection-status messages (including invalid API key) :contentReference[oaicite:2]{index=2}
socket.on("connection", (msg) => console.log("[brandsocket] connection msg", msg));

/**
 * subscription accounting: instrument -> count
 * First subscriber => SUBSCRIBE, last unsub => UNSUBSCRIBE :contentReference[oaicite:3]{index=3}
 */
const instrumentCounts = new Map<string, number>();

function norm(s: string) {
  return s.trim().toUpperCase();
}

function subscribeInstrument(raw: string) {
  const instrument = norm(raw);
  const prev = instrumentCounts.get(instrument) ?? 0;
  instrumentCounts.set(instrument, prev + 1);
  if (prev === 0) {
    socket.emit("subscriptions", { action: "SUBSCRIBE", instrument }); // :contentReference[oaicite:4]{index=4}
    console.log("[brandsocket] SUBSCRIBE", instrument);
  }
}

function unsubscribeInstrument(raw: string) {
  const instrument = norm(raw);
  const prev = instrumentCounts.get(instrument) ?? 0;
  const next = Math.max(0, prev - 1);
  if (prev > 0 && next === 0) {
    socket.emit("subscriptions", { action: "UNSUBSCRIBE", instrument }); // :contentReference[oaicite:5]{index=5}
    instrumentCounts.delete(instrument);
    console.log("[brandsocket] UNSUBSCRIBE", instrument);
  } else {
    instrumentCounts.set(instrument, next);
  }
}

// SSE client registry
type SSEClient = {
  id: string;
  res: express.Response;
  instruments: Set<string>;
};

const clients = new Map<string, SSEClient>();

function sseWrite(res: express.Response, event: string, data: unknown) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

// Quotes arrive on "subscriptions" with payload like {type:"Quote", instrument, routeId, timestamp, bid, ask} :contentReference[oaicite:6]{index=6}
socket.on("subscriptions", (msg: unknown) => {
  const q = msg as Partial<TLQuote>;
  if (!q || q.type !== "Quote" || !q.instrument) return;

  const instrument = norm(q.instrument);

  for (const c of clients.values()) {
    if (c.instruments.has(instrument)) {
      sseWrite(c.res, "quote", q);
    }
  }
});

app.get("/health", (_req, res) => res.json({ ok: true }));

/**
 * SSE endpoint:
 * GET /sse?instruments=EURUSD,GBPUSD
 * Optional auth: Authorization: Bearer <INTERNAL_TOKEN>
 */
app.get("/sse", (req, res) => {
  if (!authed(req)) return res.status(401).json({ error: "unauthorized" });

  const instrumentsParam = String(req.query.instruments ?? "");
  const instruments = instrumentsParam
    .split(",")
    .map(norm)
    .filter(Boolean);

  if (instruments.length === 0) {
    return res.status(400).json({ error: "Provide ?instruments=EURUSD,GBPUSD" });
  }

  // SSE headers
  res.status(200);
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  const id = crypto.randomUUID();
  const set = new Set(instruments);

  // Track
  const client: SSEClient = { id, res, instruments: set };
  clients.set(id, client);

  // Subscribe to instruments on BrandSocket
  for (const inst of set) subscribeInstrument(inst);

  // Keepalive ping (prevents some proxies from killing the stream)
  const keepalive = setInterval(() => {
    sseWrite(res, "ping", { t: new Date().toISOString() });
  }, 15000);

  // Cleanup on disconnect
  req.on("close", () => {
    clearInterval(keepalive);
    clients.delete(id);
    for (const inst of set) unsubscribeInstrument(inst);
  });

  // Initial ack
  sseWrite(res, "ready", { id, instruments });
});

app.listen(PORT, () => console.log(`listening on :${PORT}`));
