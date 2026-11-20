# Telegram File-to-Link Bot (Go gotd/td + Koyeb + Cloudflare Worker)

## Structure

```
tg-filelink/
├── backend/
│   ├── Dockerfile    multi-stage, distroless, GOMEMLIMIT=400MiB
│   ├── go.mod
│   ├── config.go     env loader
│   ├── pool.go       multi-token gotd client pool (least-busy picker, DC media pools)
│   ├── stream.go     TTL metadata cache, Range parser, aligned GetFile streamer, /health
│   └── main.go       primary bot handler, HTTP server, graceful shutdown
└── worker/
    ├── worker.js     HMAC check, zero-copy proxy, cold-start retry, cron ping
    └── wrangler.toml
```

Flow: user sends file to the primary bot -> bot copies it to `BIN_CHANNEL` -> bot replies with
`WORKER_URL/dl/<msg_id>?hash=<hmac>` -> Worker verifies the HMAC and proxies to
`KOYEB/stream/<msg_id>` with `X-Worker-Secret` -> backend streams `upload.GetFile` chunks.

## Environment variables (Koyeb)

| Name | Required | Notes |
|---|---|---|
| `API_ID`, `API_HASH` | yes | from my.telegram.org |
| `BOT_TOKEN` | yes | primary bot (runs the update dispatcher) |
| `MULTI_TOKENS` | no | extra bot tokens, comma-separated (7-14 recommended; total capped at 15) |
| `BIN_CHANNEL` | yes | private channel id, e.g. `-1001234567890` |
| `WORKER_URL` | yes | `https://tg-file-proxy.<you>.workers.dev` |
| `HMAC_SECRET` | yes | signs links; same value in the Worker |
| `WORKER_SECRET` | yes | `X-Worker-Secret` value; same value in the Worker |
| `ALLOWED_USERS` | no | comma-separated user ids; empty = anyone |
| `PORT` | no | default `8000` |
| `META_TTL_MINUTES` | no | default `30` |

Generate secrets with `openssl rand -hex 32`.

## Prerequisites

1. Create every bot with @BotFather.
2. Create a **private channel**, add **all** bots as **admins** (the primary needs post rights;
   workers need read access). Get its id (`-100...`).

## Deploy backend on Koyeb (Frankfurt)

1. Push this folder to a GitHub repo.
2. Koyeb dashboard -> **Create Service** -> **GitHub** -> pick the repo.
3. Builder: **Dockerfile**. Dockerfile location: `backend/Dockerfile`. Work directory: `backend`.
4. Instance: **Free**. Region: **Frankfurt (fra)**.
5. Exposed port `8000`, protocol HTTP, path `/`.
6. Health check: HTTP, port `8000`, path `/health`.
7. Add the environment variables above (mark tokens and secrets as secrets).
8. Deploy. Note the public URL, e.g. `https://<app>-<org>.koyeb.app`.

## Deploy Worker on Cloudflare

```bash
cd worker
npm i -g wrangler
wrangler login
# edit BACKEND_URL in wrangler.toml to your Koyeb URL
wrangler secret put HMAC_SECRET      # same value as Koyeb
wrangler secret put WORKER_SECRET    # same value as Koyeb
wrangler deploy
```

Then set `WORKER_URL` on Koyeb to the deployed Worker URL and redeploy the service.

## Notes

- Not compiled in the authoring environment (no Go toolchain / module proxy access). The Dockerfile
  runs `go mod tidy` to resolve the latest `gotd/td`; if it reports an API mismatch, run
  `go build ./...` locally and adjust the call named in the error.
- Each gotd client uses roughly 10-20 MB; with 15 tokens watch Koyeb's memory graph and lower the count if needed.
- The cron keep-alive stops Koyeb from scaling to zero; remove the `[triggers]` block to allow sleeping.
