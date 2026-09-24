# 🚌 SG Bus Arrivals — a Muse connector

Real-time Singapore bus arrival times as an MCP server, powered by the official
**LTA DataMall API**. Ask in plain language:

> "When is the next bus 96 at stop 83139?"
> "Which buses stop at Buona Vista?"
> "Is the next bus 151 wheelchair accessible?"
> "What buses are near me?"

Built for submission to the **Meta Muse connector directory** (muse.ai/platform).

## How it works

```
Muse ──MCP (streamable HTTP)──▶ this server ──HTTPS──▶ LTA DataMall API
                                      │                        ▲
                                      └─ forwards YOUR key ────┘
```

### Auth model (per-user keys, with a demo fallback)

This connector uses **API keys** auth:

- Each end user ideally brings **their own free LTA DataMall API key** (from
  <https://datamall.lta.gov.sg> — sign up, free).
- The user saves the key in Muse as the `x-lta-account-key` request header
  (alias `accountkey` also accepted). Own keys are **unlimited**.
- For try-before-you-commit, the server can be configured with a **demo key**
  via the `LTA_DEMO_ACCOUNT_KEY` env var (the developer's own key, shared
  quota). Callers without their own key fall back to it, rate-limited to
  **10 calls per client IP per day** (in-memory). When the budget is exhausted
  they get a friendly error pointing at the free key signup.
- The server **never stores** any key — yours or the demo one — and never logs keys.

### Tools (all read-only)

| Tool | What it does |
|---|---|
| `bus_arrivals(bus_stop_code)` | Next 3 buses per service at a 5-digit stop code: minutes until arrival, crowding in plain words (seats / standing / limited standing), wheelchair accessibility, single/double deck. |
| `find_bus_stops(query)` | Case-insensitive search over stop names and roads. The full stop list (~5k stops) is lazy-loaded from LTA on first call (paginated `$skip`) and cached in memory with a 24h TTL. |
| `nearby_bus_stops(latitude, longitude, max_results=5, radius_m=500)` | Stops near a location (haversine distance), nearest first, with distance in metres. Enables a "buses near me" flow: resolve the user's location → this tool → `bus_arrivals`. |
| `bus_route(service_no)` | Full ordered route for a service (e.g. `106`, `106A`): every direction with all stops in order — sequence number, 5-digit stop code, name and road — plus operator and weekday first/last bus from the origin stop. The full route dataset (~30k records) is lazy-loaded from LTA on first call (paginated `$skip`, fetched in small concurrent batches) and cached in memory with a 24h TTL; stop names come from the stop cache, so no extra LTA calls. |

Bus stop codes are validated as exactly 5 digits. LTA calls have a 10s timeout
with 2 retries (exponential backoff), per-call latency logging, and
human-readable errors — agents surface these directly to users.

### Static pages

The same app serves three pages (needed for directory submission):

- `/` — product landing: what it is, example prompts, key signup link, connect instructions
- `/privacy` — plain language: keys forwarded per-request to LTA only, nothing stored, no analytics
- `/terms` — as-is, not affiliated with LTA or Meta

## Local dev

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn server:app --reload
# → http://127.0.0.1:8000/        (landing page)
# → http://127.0.0.1:8000/mcp     (MCP endpoint)
```

No LTA key is needed to start the server or list tool schemas — only actual
bus queries need a key.

## Deploy

### Fly.io

```bash
fly launch --no-deploy        # accepts the existing fly.toml
fly deploy
fly open                      # your public URL, e.g. https://sg-bus-mcp.fly.dev
```

Optional — enable the demo fallback: `fly secrets set LTA_DEMO_ACCOUNT_KEY=<your-key>`
(use your own DataMall key; the shared quota is yours to mind).

Region is `sin` (closest to LTA's API). Machines scale to zero when idle.

### Render

1. Push this repo to GitHub.
2. In Render: **New → Blueprint** and point it at the repo (it picks up `render.yaml`).
3. Done — Render builds the Dockerfile and gives you a public URL.

Optional — enable the demo fallback by adding an `LTA_DEMO_ACCOUNT_KEY`
environment variable (marked secret) in the Render dashboard.

Region is `singapore`; Render injects `$PORT`, which the Dockerfile already listens on.

## Connect from Muse (custom connector)

1. In Muse: **Connectors → Add custom connector**.
2. MCP server URL: `https://<your-deployed-host>/mcp`
3. API key: either save your free LTA DataMall key as the `x-lta-account-key`
   request header — **or skip it** and try the shared demo key (10 free tries
   a day, no signup).
4. Ask: *"When is the next bus 96 at stop 83139?"*

## Submission checklist (muse.ai/platform)

- [x] Public HTTPS endpoint serving the MCP server at `/mcp`
- [x] Product landing page at `/` with example prompts and key signup link
- [x] `/privacy` and `/terms` pages
- [x] Auth documented: per-user LTA keys via `x-lta-account-key` header, nothing stored server-side
- [x] Demo fallback documented (developer's shared key, 10 tries/day/IP, own key for unlimited use)
- [x] Read-only tools (no writes, no spending, no side effects)
- [x] Human-readable error messages for every failure mode
- [x] Health check: `GET /` returns 200
- [ ] Live end-to-end test with a real LTA key (needs a key — see below)
- [ ] Privacy/terms reviewed by a human before submission

> ⚠️ **Not yet tested live:** the code paths that call LTA (`bus_arrivals` with a
> valid key, and the first `find_bus_stops` cache load) have not been exercised
> against the real API — no LTA key was available during development. Run one
> manual `bus_arrivals` call with a real key before submitting.

## License

MIT — see [LICENSE](LICENSE). Not affiliated with LTA or Meta.
