# 🚌🚇 SG Bus + Train — a Muse connector

Real-time Singapore bus arrival times **and MRT/LRT service info** as an MCP
server, powered by the official **LTA DataMall API**. Ask in plain language:

> "When is the next bus 96 at stop 83139?"
> "Which buses stop at Buona Vista?"
> "Is the next bus 151 wheelchair accessible?"
> "What buses are near me?"
> "Is the MRT disrupted right now?"
> "How crowded is Dhoby Ghaut station?"
> "Which stations are on the Circle Line?"

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
| `bus_arrivals(bus_stop_code)` | Next 3 buses per service at a 5-digit stop code: minutes until arrival, crowding in plain words (seats / standing / limited standing), wheelchair accessibility, single/double deck. Each bus is tagged with its terminating stop, resolved from LTA's per-vehicle destination code. |
| `find_bus_stops(query)` | Case-insensitive search over stop names and roads. Served from the bundled stop list (see below) — instant, no LTA call. |
| `nearby_bus_stops(latitude, longitude, max_results=5, radius_m=500)` | Stops near a location (haversine distance), nearest first, with distance in metres. Served from the bundled stop list. Enables a "buses near me" flow: resolve the user's location → this tool → `bus_arrivals`. |
| `bus_route(service_no)` | Full ordered route for a service (e.g. `106`, `106A`): every direction with all stops in order — each tagged with its destination (`[→ Shenton Way Ter]`; sequence numbers restart per direction), 5-digit stop code, name and road — plus operator and weekday first/last bus from the origin stop. Served from the bundled dataset (see below), so it's instant even on a cold start. |
| `train_alerts()` | Live MRT/LRT service status: any disrupted lines with affected stations, directions, free bridging buses / MRT shuttles and LTA's advisory message — or confirmation that all lines run normally. Updated ad hoc by LTA. |
| `station_crowding(train_line, station="")` | Live platform crowding (`low` / `moderate` / `high`) for every station on a line, e.g. `station_crowding("CCL")`; optionally filter to one station by code or name. Line names work too (`"Circle Line"`). LTA updates ~every 10 min. |
| `station_crowd_forecast(train_line, station="", hours=4)` | Crowd forecast in 30-minute slots for the next `hours` (default 4, max 12), stations in line order. LTA refreshes ~once a day. |
| `train_stations(line="", query="")` | The bundled MRT/LRT station map: codes, names, coordinates and line order for all 9 lines (188 stations). Filter by line or search by name/code — no API key needed, always instant. |
| `dump_static_data(dataset, skip=0)` | Maintenance: one raw 500-record page of an LTA static dataset (`bus_stops`, `bus_routes`, `bus_services`) as JSON. Used by `scripts/refresh_static_data.py` to rebuild the bundled files — not for everyday questions. |

Bus stop codes are validated as exactly 5 digits. LTA calls have a 10s timeout
with 2 retries (exponential backoff), per-call latency logging, and
human-readable errors — agents surface these directly to users.

### Bundled static data (no cold starts)

Reference data that changes rarely lives in `data/*.json`, committed to the
repo, loaded from disk on first use and kept in memory afterwards:

| File | Contents | Source |
|---|---|---|
| `data/bus_stops.json` | ~5k bus stops: code, name, road, coordinates | LTA DataMall `/BusStops` |
| `data/bus_routes.json` | ~30k route records: service, operator, direction, ordered stops, first/last bus | LTA DataMall `/BusRoutes` |
| `data/mrt_stations.json` | 188 MRT/LRT stations: codes, names, coordinates, line topology (9 lines) | [ayaka14732/singapore-hdb-map](https://github.com/ayaka14732/singapore-hdb-map) (sourced from LTA DataMall, the LTA system map and data.gov.sg under the Singapore Open Data Licence) |

This means `bus_route`, `find_bus_stops`, `nearby_bus_stops` and
`train_stations` answer instantly — even the very first call after a deploy —
with zero LTA traffic. Only genuinely live data (arrivals, alerts, crowding)
hits the LTA API per request. If a bundled file is ever missing, the server
falls back to a live LTA fetch so a partial checkout still works.

Tool outputs note the snapshot date (e.g. *"Route data bundled 2026-09-25"*),
so agents can see the vintage. To refresh the bus datasets:

```bash
python3 scripts/refresh_static_data.py   # uses the sg-bus CLI + stored credential
git add data/bus_stops.json data/bus_routes.json && git commit -m "Refresh bus datasets"
```

`scripts/build_train_stations.py` rebuilds the station map from its upstream
source (re-run when new stations open).

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
