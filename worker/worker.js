// Cloudflare Worker edge proxy for the Go MTProto streaming backend on Koyeb.
//
// Routes:
//   GET|HEAD /dl/<message_id>?hash=<hex hmac>[&inline=1]  -> verified, streamed from backend
//   GET      /health                                      -> worker liveness
//
// Secrets (wrangler secret put): HMAC_SECRET, WORKER_SECRET
// Var (wrangler.toml):           BACKEND_URL
//
// The body is piped straight through (no arrayBuffer/blob), so CPU time per
// request stays far below the free-tier limit and memory use stays tiny.

const encoder = new TextEncoder();
let hmacKeyPromise = null;

function getKey(env) {
  if (!hmacKeyPromise) {
    hmacKeyPromise = crypto.subtle.importKey(
      "raw",
      encoder.encode(env.HMAC_SECRET),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["verify"]
    );
  }
  return hmacKeyPromise;
}

function hexToBytes(hex) {
  if (typeof hex !== "string" || !/^[0-9a-fA-F]{64}$/.test(hex)) return null;
  const out = new Uint8Array(32);
  for (let i = 0; i < 32; i++) out[i] = parseInt(hex.substr(i * 2, 2), 16);
  return out;
}

// crypto.subtle.verify performs a constant-time comparison.
async function validSignature(env, id, hash) {
  const sig = hexToBytes(hash);
  if (!sig) return false;
  const key = await getKey(env);
  return crypto.subtle.verify("HMAC", key, sig, encoder.encode(id));
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const PASS_THROUGH_REQUEST_HEADERS = ["range", "if-range", "if-none-match"];
const PASS_THROUGH_RESPONSE_HEADERS = [
  "content-type",
  "content-length",
  "content-range",
  "accept-ranges",
  "etag",
  "content-disposition",
  "cache-control",
  "retry-after",
];

const CORS_HEADERS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, HEAD, OPTIONS",
  "access-control-allow-headers": "Range, If-Range, If-None-Match",
  "access-control-expose-headers":
    "Content-Length, Content-Range, Accept-Ranges, ETag, Content-Disposition",
};

function text(body, status, extra = {}) {
  return new Response(body, {
    status,
    headers: { "content-type": "text/plain; charset=utf-8", ...CORS_HEADERS, ...extra },
  });
}

// One cold-start retry: Koyeb answers 502/503 while the instance wakes from sleep.
async function fetchBackend(url, init) {
  let resp = null;
  try {
    resp = await fetch(url, init);
  } catch (_) {
    resp = null;
  }
  if (resp === null || resp.status === 502 || resp.status === 503) {
    if (resp && resp.body) await resp.body.cancel();
    await sleep(1500);
    resp = await fetch(url, init);
  }
  return resp;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (url.pathname === "/health") {
      return new Response('{"status":"ok"}', {
        headers: { "content-type": "application/json", ...CORS_HEADERS },
      });
    }

    const m = url.pathname.match(/^\/dl\/(\d{1,12})\/?$/);
    if (!m) return text("Not Found", 404);

    if (request.method !== "GET" && request.method !== "HEAD") {
      return text("Method Not Allowed", 405, { allow: "GET, HEAD, OPTIONS" });
    }

    const id = m[1];
    if (!(await validSignature(env, id, url.searchParams.get("hash")))) {
      return text("Forbidden", 403);
    }

    const backendUrl =
      `${env.BACKEND_URL.replace(/\/+$/, "")}/stream/${id}` +
      (url.searchParams.get("inline") === "1" ? "?inline=1" : "");

    const headers = new Headers({ "X-Worker-Secret": env.WORKER_SECRET });
    for (const name of PASS_THROUGH_REQUEST_HEADERS) {
      const v = request.headers.get(name);
      if (v) headers.set(name, v);
    }

    let upstream;
    try {
      upstream = await fetchBackend(backendUrl, { method: request.method, headers });
    } catch (_) {
      return text("Backend unavailable", 502);
    }

    const out = new Headers(CORS_HEADERS);
    for (const name of PASS_THROUGH_RESPONSE_HEADERS) {
      const v = upstream.headers.get(name);
      if (v) out.set(name, v);
    }

    // Zero-copy: the backend body stream is handed to the client as-is.
    return new Response(upstream.body, { status: upstream.status, headers: out });
  },

  // Optional cron keep-alive so Koyeb does not scale to zero after an hour idle.
  async scheduled(event, env, ctx) {
    ctx.waitUntil(
      fetch(`${env.BACKEND_URL.replace(/\/+$/, "")}/health`).then((r) => r.body?.cancel()).catch(() => {})
    );
  },
};
