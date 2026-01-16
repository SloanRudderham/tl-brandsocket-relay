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
const TL_TYPE = process.env.TL_TYPE ?? "DEMO";
const TL_BRAND_API_KEY = process.env.TL_BRAND_API_KEY;

if (!TL_BRAND_API_KEY) throw new Error("Missing env TL_BRAND_API_KEY");

const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN;

// ---- simple structured logger ----
function log(...args: any[]) {
  console.log(new Date().toISOString(), ...args);
}

// Helpful to see env at boot (without leaking secrets)
log("[boot] starting", {
  TL_HOST,
  TL_NAMESPACE,
  TL_PATH,
  TL_TYPE,
  hasBrandKey: Boolean(TL_BRAND_API_KEY),
  hasInternalToken: Boolean(INTERNAL_TOKEN),
});

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

socket.on("connect", () => log("[brandsocket] connected", socket.id));
socket.on("disconnect", (r) => log("[brandsocket] disconnected", r));
socket.on("connect_error", (e) =>
  log("[brandsocket] connect_error", { message: (e as any)?.message, e })
);
socket.on("error", (e) => log("[brandsocket] error", e));
socket.on("connection", (msg) => log("[brandsocket] connection msg", msg));

// subscription accounting: instrument -> count
const instrumentCounts = new Map<string, number>();

function norm(s: string) {
  return s.trim().toUpperCase();
}

function subscribeInstrument(raw: string) {
  const instrument = norm(raw);
  const prev = instrumentCounts.get(instrument) ?? 0;
  instrumentCounts.set(instrument, prev + 1);

  if (prev === 0) {
    socket.emit("subscriptions", { action: "SUBSCRIBE", instrument });
    log("[brandsocket] SUBSCRIBE", { instrument, count: prev + 1 });
  } else {
    log("[brandsocket] subscribe refcount++", { instrument, count: prev + 1 });
  }
}

function unsubscribeInstrument(raw: string) {
  const instrument = norm(raw);
  const prev = instrumentCounts.get(instrument) ?? 0;
  const next = Math.max(0, prev - 1);

  if (prev > 0 && next === 0) {
    socket.emit("subscriptions", { action: "UNSUBSCRIBE", instrument });
    instrumentCounts.delete(instrument);
    log("[brandsocket] UNSUBSCRIBE", { instrument });
  } else {
    instrumentCounts.set(instrument, next);
    log("[brandsocket] subscribe refcount--", { instrument, count: next });
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

// Diagnostics: log every inbound "subscriptions" message (rate-limited)
let subMsgCount = 0;
let subMsgLastLog = 0;

function shouldLogSubMsg() {
  const now = Date.now();
  // log first 20 messages, then at most 1 per second
  if (subMsgCount < 20) return true;
  if (now - subMsgLastLog > 1000) return true;
  return false;
}

// Quotes arrive on "subscriptions" with payload like {type:"Quote", instrument, routeId, timestamp, bid, ask}
socket.on("subscriptions", (msg: unknown) => {
  subMsgCount++;
  if (shouldLogSubMsg()) {
    subMsgLastLog = Date.now();
    log("[brandsocket] subscriptions raw", msg);
  }

  const q = msg as Partial<TLQuote>;
  if (!q || !q.instrument) return;

  // If payload differs, this will show up in the raw log above
  if (q.type !== "Quote") return;

  const instrument = norm(q.instrument);
  let forwarded = 0;

  for (const c of clients.values()) {
    if (c.instruments.has(instrument)) {
      sseWrite(c.res, "quote", q);
      forwarded++;
    }
  }

  if (forwarded > 0 && shouldLogSubMsg()) {
    log("[relay] forwarded quote", { instrument, forwarded, routeId: q.routeId, bid: q.bid, ask: q.ask });
  }
});

// Optional: log stream messages (can be very noisy; rate-limited)
let streamCount = 0;
let streamLastLog = 0;
socket.on("stream", (data: unknown) => {
  streamCount++;
  const now = Date.now();
  if (streamCount <= 5 || now - streamLastLog > 2000) {
    streamLastLog = now;
    log("[brandsocket] stream sample", data);
  }
});

app.get("/health", (_req, res) =>
  res.json({
    ok: true,
    socketConnected: socket.connected,
    clients: clients.size,
    subscribedInstruments: Array.from(instrumentCounts.keys()),
  })
);

app.get("/sse", (req, res) => {
  const ok = authed(req);
  log("[http] /sse", {
    ok,
    ip: req.headers["x-forwarded-for"] ?? req.socket.remoteAddress,
    ua: req.headers["user-agent"],
    q: req.originalUrl,
  });

  if (!ok) return res.status(401).json({ error: "unauthorized" });

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

  clients.set(id, { id, res, instruments: set });
  log("[sse] client connected", { id, instruments: Array.from(set), clients: clients.size });

  for (const inst of set) subscribeInstrument(inst);

  const keepalive = setInterval(() => {
    sseWrite(res, "ping", { t: new Date().toISOString() });
  }, 15000);

  req.on("close", () => {
    clearInterval(keepalive);
    clients.delete(id);
    for (const inst of set) unsubscribeInstrument(inst);
    log("[sse] client closed", { id, clients: clients.size });
  });

  sseWrite(res, "ready", { id, instruments });
});

app.listen(PORT, () => log(`listening on :${PORT}`));
