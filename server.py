"""
SG Bus Arrivals — MCP server for real-time Singapore bus arrival times.

Powered by the LTA DataMall API (https://datamall.lta.gov.sg).

Auth model
---------
Each end user ideally brings their OWN free LTA DataMall API key. The key
travels in the incoming MCP request headers (`x-lta-account-key`, or
`accountkey`) and is forwarded per-request to LTA as the `AccountKey` header.
Own keys are unlimited.

For try-before-you-commit, the server may also be configured with a demo key
via the LTA_DEMO_ACCOUNT_KEY env var. Callers without their own key fall back
to the demo key, rate-limited to 10 calls per client IP per day. The server
never stores end-user keys and never logs any key.

Exposed tools (all read-only):
    bus_arrivals(bus_stop_code)                 — next 3 buses per service at a stop
    find_bus_stops(query)                      — search bus stops by name / road
    nearby_bus_stops(latitude, longitude, ...) — bus stops near a location
    bus_route(service_no)                      — full ordered route for a service
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import HTMLResponse

# ---------------------------------------------------------------- constants

SGT = ZoneInfo("Asia/Singapore")

LTA_BASE_URL = "https://datamall2.mytransport.sg/ltaodataservice"
LTA_TIMEOUT_SECONDS = 10.0
LTA_MAX_RETRIES = 2            # total attempts = 1 + LTA_MAX_RETRIES
STOP_PAGE_SIZE = 500           # DataMall returns at most 500 records per page
BUS_STOP_CACHE_TTL = timedelta(hours=24)
MAX_SEARCH_RESULTS = 20
DEMO_BUDGET_PER_DAY = 10  # keyless calls per client IP per day via the demo key

LOAD_WORDS = {
    "SEA": "seats available",
    "SDA": "standing available",
    "LSD": "limited standing",
}
BUS_TYPE_WORDS = {
    "SD": "single deck",
    "DD": "double deck",
}

logger = logging.getLogger("sg-bus-mcp")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

mcp = FastMCP("sg-bus-arrivals")

# The MCP SDK's DNS-rebinding protection defaults to localhost-only hosts,
# which 421s every request behind Fly's edge proxy. Allow our public
# hostname (plus loopback for local dev/test) explicitly instead.
_PUBLIC_HOST = os.environ.get("PUBLIC_HOST", "sg-bus-mcp.fly.dev")
mcp.settings.transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=True,
    allowed_hosts=[
        _PUBLIC_HOST,
        f"{_PUBLIC_HOST}:*",
        "127.0.0.1:*",
        "localhost:*",
        "[::1]:*",
    ],
    allowed_origins=[f"https://{_PUBLIC_HOST}"],
)


# ------------------------------------------------------------------ auth

MISSING_KEY_MESSAGE = (
    "No LTA DataMall API key was provided, and this server has no demo key "
    "configured. To use SG Bus Arrivals you need a free key from "
    "https://datamall.lta.gov.sg — add it to this connector as the "
    "'x-lta-account-key' request header, then try again."
)

DEMO_EXHAUSTED_MESSAGE = (
    "You've used all 10 free demo tries for today from this address. The demo "
    "key is a shared one just for trying things out — grab your own free LTA "
    "DataMall key at https://datamall.lta.gov.sg and add it to this connector "
    "as the 'x-lta-account-key' request header for unlimited use."
)

# client IP -> [yyyy-mm-dd, calls used that day]; guards the shared demo key
_demo_usage: dict[str, list] = {}
_demo_lock = asyncio.Lock()


def _client_ip(ctx: Context) -> str:
    request = getattr(ctx.request_context, "request", None)
    if request is None:  # defensive: older SDKs exposed it directly
        request = getattr(ctx, "request", None)
    client = getattr(request, "client", None) if request is not None else None
    return getattr(client, "host", None) or "unknown"


async def _resolve_api_key(ctx: Context) -> tuple[str, bool, int | None]:
    """Decide which LTA key to use for this call.

    Returns (api_key, is_demo, demo_remaining). A caller-supplied header key
    always wins and is unlimited. Otherwise falls back to the optional
    LTA_DEMO_ACCOUNT_KEY env var, rate-limited per client IP per day.
    Raises a human-readable error when no key is available or the demo
    budget is exhausted.
    """
    request = getattr(ctx.request_context, "request", None)
    if request is None:
        request = getattr(ctx, "request", None)
    headers = request.headers if request is not None else {}

    for name in ("x-lta-account-key", "accountkey"):
        value = headers.get(name)
        if value and value.strip():
            return value.strip(), False, None

    demo_key = os.environ.get("LTA_DEMO_ACCOUNT_KEY", "").strip()
    if not demo_key:
        raise ValueError(MISSING_KEY_MESSAGE)

    ip = _client_ip(ctx)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with _demo_lock:
        if len(_demo_usage) > 5000:  # prune stale entries
            for stale in [k for k, v in _demo_usage.items() if v[0] != today]:
                del _demo_usage[stale]
        day, count = _demo_usage.get(ip, (today, 0))
        if day != today:
            day, count = today, 0
        if count >= DEMO_BUDGET_PER_DAY:
            raise ValueError(DEMO_EXHAUSTED_MESSAGE)
        _demo_usage[ip] = [day, count + 1]
        remaining = DEMO_BUDGET_PER_DAY - (count + 1)
    logger.info("demo key used by %s (%d/%d today)", ip, count + 1, DEMO_BUDGET_PER_DAY)
    return demo_key, True, remaining


def _demo_note(remaining: int | None) -> str:
    return (
        f"\n\n—\nYou're trying this with the shared demo key "
        f"({remaining} of {DEMO_BUDGET_PER_DAY} free tries left today). "
        "For unlimited use, get your own free key at https://datamall.lta.gov.sg "
        "and add it as the 'x-lta-account-key' request header."
    )


# ------------------------------------------------------------ LTA client

async def _lta_get(api_key: str, path: str, params: dict[str, Any], demo: bool = False) -> Any:
    """GET against the DataMall API with timeouts, retries and latency logs.

    Raises human-readable exceptions (agents surface these to the user).
    `demo` switches auth-error wording to the shared demo key.
    """
    url = f"{LTA_BASE_URL}{path}"
    last_error = "unknown error"

    for attempt in range(LTA_MAX_RETRIES + 1):
        started = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=LTA_TIMEOUT_SECONDS, trust_env=False) as client:
                resp = await client.get(
                    url,
                    headers={"AccountKey": api_key, "Accept": "application/json"},
                    params=params,
                )
            latency_ms = (time.perf_counter() - started) * 1000
            logger.info(
                "LTA GET %s -> %s in %.0f ms (attempt %d)",
                path, resp.status_code, latency_ms, attempt + 1,
            )
            if resp.status_code == 401:
                if demo:
                    raise ValueError(
                        "The shared demo key was rejected by LTA (401) — it may "
                        "have expired or hit its quota. Grab your own free key at "
                        "https://datamall.lta.gov.sg and add it to this connector "
                        "as the 'x-lta-account-key' request header."
                    )
                raise ValueError(
                    "LTA rejected your API key (401). Double-check the key you "
                    "saved for this connector — it may be a typo, or the key may "
                    "have been revoked on the DataMall site."
                )
            if resp.status_code == 403:
                if demo:
                    raise ValueError(
                        "LTA refused the shared demo key (403) — it may be "
                        "rate-limited right now. Grab your own free key at "
                        "https://datamall.lta.gov.sg and add it to this connector "
                        "as the 'x-lta-account-key' request header."
                    )
                raise ValueError(
                    "LTA refused the request (403). Your API key may not have "
                    "access to this dataset, or LTA may be rate-limiting it."
                )
            resp.raise_for_status()
            return resp.json()
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last_error = str(exc) or type(exc).__name__
            logger.warning(
                "LTA GET %s failed on attempt %d: %s", path, attempt + 1, exc
            )
            if attempt < LTA_MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)  # 1s, then 2s backoff
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"The LTA DataMall API returned an unexpected error "
                f"({exc.response.status_code}). It's probably a temporary hiccup — "
                f"please try again in a moment."
            ) from exc

    raise RuntimeError(
        f"Couldn't reach the LTA DataMall API after {LTA_MAX_RETRIES + 1} tries "
        f"({last_error}). The service may be down or your network may be "
        f"blocking it — please try again shortly."
    )


# ------------------------------------------------------------------ tools

def _describe_bus(
    bus: dict[str, Any], now: datetime, destination: str = ""
) -> str:
    eta_raw = (bus.get("EstimatedArrival") or "").strip()
    if not eta_raw:
        return "not currently in service"

    minutes = _minutes_until(eta_raw, now)
    if minutes is None:
        return "arrival time unavailable"
    when = "arriving now" if minutes < 1 else f"{minutes} min"
    if destination:
        when += f" → {destination}"

    load = LOAD_WORDS.get((bus.get("Load") or "").upper(), "crowding unknown")
    wab = "wheelchair accessible" if (bus.get("Feature") or "").upper() == "WAB" else "not wheelchair accessible"
    deck = BUS_TYPE_WORDS.get((bus.get("Type") or "").upper(), "")

    parts = [when, load, wab]
    if deck:
        parts.append(deck)
    return ", ".join(parts)


def _describe_service(
    svc: dict[str, Any], now: datetime, dest_names: dict[str, str]
) -> str:
    """One service line, with direction when LTA reports it.

    Every arriving bus carries OriginCode/DestinationCode (vehicle level).
    When all buses on the service share one destination it goes on the
    service line ("- 74 (SBST) → Buona Vista Ter: ..."). When they differ
    (e.g. short-working trips) each bus is annotated individually. Loop
    services (origin == destination) get a "(loop)" marker.
    """
    slots = [svc.get(name) or {} for name in ("NextBus", "NextBus2", "NextBus3")]
    active = [b for b in slots if (b.get("EstimatedArrival") or "").strip()]
    head = f"- {svc.get('ServiceNo', '?')} ({svc.get('Operator', '?')})"

    uniq = list(
        dict.fromkeys(
            (b.get("DestinationCode") or "").strip() for b in active
        )
    )
    uniq = [code for code in uniq if code]

    if len(uniq) == 1:
        dest = dest_names.get(uniq[0], uniq[0])
        origin = (active[0].get("OriginCode") or "").strip()
        loop = " (loop)" if origin and origin == uniq[0] else ""
        buses = " | ".join(_describe_bus(b, now) for b in slots)
        return f"{head} → {dest}{loop}: {buses}"

    # Mixed destinations, or none reported: annotate each bus on its own.
    parts = []
    for bus in slots:
        code = (bus.get("DestinationCode") or "").strip()
        dest = ""
        if code and (bus.get("EstimatedArrival") or "").strip():
            dest = dest_names.get(code, code)
        parts.append(_describe_bus(bus, now, destination=dest))
    return f"{head}: {' | '.join(parts)}"


def _minutes_until(eta_iso: str, now: datetime) -> Optional[int]:
    try:
        eta = datetime.fromisoformat(eta_iso)
    except ValueError:
        return None
    if eta.tzinfo is None:
        eta = eta.replace(tzinfo=SGT)
    return max(0, round((eta - now).total_seconds() / 60))


@mcp.tool()
async def bus_arrivals(ctx: Context, bus_stop_code: str) -> str:
    """Real-time arrival times for every bus service at a Singapore bus stop.

    Give the 5-digit bus stop code (printed on the stop's sign, e.g. "83139").
    Returns each service with its next three buses: minutes until arrival,
    crowding in plain words, wheelchair accessibility, single/double deck,
    and the direction each bus is heading (its terminating stop, resolved
    from LTA's per-bus destination data — e.g. "→ Buona Vista Ter").
    """
    code = (bus_stop_code or "").strip()
    if not re.fullmatch(r"\d{5}", code):
        return (
            f"'{bus_stop_code}' doesn't look like a valid bus stop code. "
            "Singapore bus stop codes are exactly 5 digits — you'll find yours "
            "printed on the sign at the stop (for example 83139). If you know the "
            "stop's name but not its code, use find_bus_stops; if you know where "
            "you are, use nearby_bus_stops."
        )

    try:
        api_key, is_demo, demo_remaining = await _resolve_api_key(ctx)
    except (ValueError, RuntimeError) as exc:
        return str(exc)  # missing key / demo exhausted — message already explains

    try:
        data = await _lta_get(api_key, "/v3/BusArrival", {"BusStopCode": code}, demo=is_demo)
    except (ValueError, RuntimeError) as exc:
        text = str(exc)
        return text + _demo_note(demo_remaining) if is_demo else text

    services = data.get("Services") or []
    if not services:
        text = (
            f"No buses are currently serving stop {code}. The stop may not exist, "
            "or no services are on the road right now (for example late at night)."
        )
        return text + _demo_note(demo_remaining) if is_demo else text

    # Every bus carries a DestinationCode (vehicle level). Collect them all so
    # stop names can be resolved in one pass through the (cached) stop list.
    dest_codes: set[str] = set()
    for svc in services:
        for slot in ("NextBus", "NextBus2", "NextBus3"):
            bus = svc.get(slot) or {}
            if not (bus.get("EstimatedArrival") or "").strip():
                continue
            dest = (bus.get("DestinationCode") or "").strip()
            if dest:
                dest_codes.add(dest)

    dest_names: dict[str, str] = {}
    if dest_codes:
        try:
            stops = await _get_all_bus_stops(api_key, demo=is_demo)
            by_code = {
                str(s.get("BusStopCode")): str(s.get("Description") or "")
                for s in stops
            }
            dest_names = {c: by_code.get(c) or c for c in dest_codes}
        except (ValueError, RuntimeError):
            # Name resolution must never break arrivals; fall back to codes.
            logger.warning(
                "bus_arrivals: stop-name lookup failed, using raw codes"
            )
            dest_names = {c: c for c in dest_codes}

    now = datetime.now(SGT)
    lines = [f"Bus stop {code} — {len(services)} service(s):"]
    for svc in services:
        lines.append(_describe_service(svc, now, dest_names))
    text = "\n".join(lines)
    if is_demo:
        text += _demo_note(demo_remaining)
    return text


# ------------------------------------------------------- bus stop search

_bus_stops: Optional[list[dict[str, Any]]] = None
_bus_stops_fetched_at: Optional[datetime] = None
_bus_stops_lock = asyncio.Lock()


async def _get_all_bus_stops(api_key: str, demo: bool = False) -> list[dict[str, Any]]:
    """Fetch and cache the full bus stop list (paginated, 500/page, 24h TTL)."""
    global _bus_stops, _bus_stops_fetched_at

    now = datetime.now(timezone.utc)
    if (
        _bus_stops is not None
        and _bus_stops_fetched_at is not None
        and now - _bus_stops_fetched_at < BUS_STOP_CACHE_TTL
    ):
        return _bus_stops

    async with _bus_stops_lock:
        # Re-check inside the lock (another coroutine may have refreshed it).
        now = datetime.now(timezone.utc)
        if (
            _bus_stops is not None
            and _bus_stops_fetched_at is not None
            and now - _bus_stops_fetched_at < BUS_STOP_CACHE_TTL
        ):
            return _bus_stops

        logger.info("Refreshing bus stop cache from LTA DataMall")
        stops: list[dict[str, Any]] = []
        skip = 0
        while True:
            data = await _lta_get(api_key, "/BusStops", {"$skip": skip}, demo=demo)
            page = data.get("value") or []
            stops.extend(page)
            if len(page) < STOP_PAGE_SIZE:
                break
            skip += STOP_PAGE_SIZE

        _bus_stops = stops
        _bus_stops_fetched_at = now
        logger.info("Bus stop cache refreshed: %d stops", len(stops))
        return stops


@mcp.tool()
async def find_bus_stops(ctx: Context, query: str) -> str:
    """Search Singapore bus stops by name or road (case-insensitive).

    Give part of a stop name or road, e.g. "buona vista" or "orchard rd".
    Returns matching stops with their 5-digit codes, which you can then pass
    to bus_arrivals. The full stop list is cached for 24 hours so repeat
    searches are fast.
    """
    q = (query or "").strip().lower()
    if len(q) < 2:
        return "Please give at least 2 characters to search by, e.g. 'clementi' or 'orchard'."

    try:
        api_key, is_demo, demo_remaining = await _resolve_api_key(ctx)
    except (ValueError, RuntimeError) as exc:
        return str(exc)  # missing key / demo exhausted — message already explains

    try:
        stops = await _get_all_bus_stops(api_key, demo=is_demo)
    except (ValueError, RuntimeError) as exc:
        text = str(exc)
        return text + _demo_note(demo_remaining) if is_demo else text

    matches = [
        s for s in stops
        if q in str(s.get("Description", "")).lower()
        or q in str(s.get("RoadName", "")).lower()
    ]
    if not matches:
        text = (
            f"No bus stops matched '{query}'. Try a shorter fragment of the stop "
            "name or road, e.g. 'buona' instead of 'buona vista mrt'."
        )
        return text + _demo_note(demo_remaining) if is_demo else text

    shown = matches[:MAX_SEARCH_RESULTS]
    lines = [f"{len(matches)} stop(s) matched '{query}':"]
    for s in shown:
        lines.append(
            f"- {s.get('BusStopCode', '?')}: {s.get('Description', '?')} "
            f"({s.get('RoadName', '?')})"
        )
    if len(matches) > MAX_SEARCH_RESULTS:
        lines.append(f"...and {len(matches) - MAX_SEARCH_RESULTS} more — try a more specific search.")
    text = "\n".join(lines)
    if is_demo:
        text += _demo_note(demo_remaining)
    return text


# ------------------------------------------------------ nearby bus stops

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres between two lat/lng points."""
    radius = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


@mcp.tool()
async def nearby_bus_stops(
    ctx: Context,
    latitude: float,
    longitude: float,
    max_results: int = 5,
    radius_m: int = 500,
) -> str:
    """Find bus stops near a location, nearest first.

    Give a latitude/longitude (e.g. from the user's device location) and get
    the closest stops within radius_m metres, each with its 5-digit code,
    name, road, and distance in metres. Pass a code from the results to
    bus_arrivals for live timings. max_results caps at 20; radius_m at 2000.
    """
    try:
        lat, lng = float(latitude), float(longitude)
    except (TypeError, ValueError):
        return (
            "I need a valid latitude and longitude to search nearby, e.g. "
            "latitude 1.306 and longitude 103.77 for the Buona Vista area."
        )
    if not -90 <= lat <= 90:
        return (
            f"{latitude} isn't a valid latitude — it must be between -90 and 90. "
            "Singapore sits around latitude 1.3."
        )
    if not -180 <= lng <= 180:
        return (
            f"{longitude} isn't a valid longitude — it must be between -180 and 180. "
            "Singapore sits around longitude 103.8."
        )
    try:
        limit = max(1, min(int(max_results), 20))
    except (TypeError, ValueError):
        limit = 5
    try:
        radius = max(50, min(int(radius_m), 2000))
    except (TypeError, ValueError):
        radius = 500

    try:
        api_key, is_demo, demo_remaining = await _resolve_api_key(ctx)
    except (ValueError, RuntimeError) as exc:
        return str(exc)  # missing key / demo exhausted — message already explains

    try:
        stops = await _get_all_bus_stops(api_key, demo=is_demo)
    except (ValueError, RuntimeError) as exc:
        text = str(exc)
        return text + _demo_note(demo_remaining) if is_demo else text

    scored: list[tuple[float, dict]] = []
    for s in stops:
        try:
            slat, slng = float(s.get("Latitude")), float(s.get("Longitude"))
        except (TypeError, ValueError):
            continue
        dist = _haversine_m(lat, lng, slat, slng)
        if dist <= radius:
            scored.append((dist, s))
    scored.sort(key=lambda t: t[0])

    if not scored:
        text = (
            f"No bus stops found within {radius} m of ({lat}, {lng}). "
            "Try widening the search radius."
        )
        return text + _demo_note(demo_remaining) if is_demo else text

    lines = [f"{len(scored)} stop(s) within {radius} m of ({lat}, {lng}), nearest first:"]
    for dist, s in scored[:limit]:
        lines.append(
            f"- {s.get('BusStopCode', '?')}: {s.get('Description', '?')} "
            f"({s.get('RoadName', '?')}) — {int(round(dist))} m away"
        )
    text = "\n".join(lines)
    if is_demo:
        text += _demo_note(demo_remaining)
    return text


# ------------------------------------------------------- bus routes

BUS_ROUTE_PAGE_BATCH = 5  # concurrent $skip pages per round when warming the route cache

_bus_routes: Optional[dict[str, dict[int, dict[str, Any]]]] = None
_bus_routes_fetched_at: Optional[datetime] = None
_bus_routes_lock = asyncio.Lock()


def _hhmm(raw: Any) -> str:
    """'2352' -> '23:52'; anything unexpected comes back untouched."""
    t = str(raw or "").strip()
    if len(t) == 4 and t.isdigit():
        return f"{t[:2]}:{t[2:]}"
    return t


async def _get_all_bus_routes(api_key: str, demo: bool = False) -> dict[str, dict[int, dict[str, Any]]]:
    """Fetch and cache the full bus route dataset (paginated, 24h TTL).

    Returns {service_no: {direction: {"operator", "first_bus", "last_bus",
    "stops"}}} with stops sorted by StopSequence and enriched with names and
    roads from the stop cache. Pages are fetched in small concurrent batches
    because the dataset spans ~60 pages of 500 records; the 24h cache means
    this warmup happens rarely.
    """
    global _bus_routes, _bus_routes_fetched_at

    now = datetime.now(timezone.utc)
    if (
        _bus_routes is not None
        and _bus_routes_fetched_at is not None
        and now - _bus_routes_fetched_at < BUS_STOP_CACHE_TTL
    ):
        return _bus_routes

    async with _bus_routes_lock:
        # Re-check inside the lock (another coroutine may have refreshed it).
        now = datetime.now(timezone.utc)
        if (
            _bus_routes is not None
            and _bus_routes_fetched_at is not None
            and now - _bus_routes_fetched_at < BUS_STOP_CACHE_TTL
        ):
            return _bus_routes

        logger.info("Refreshing bus route cache from LTA DataMall")

        async def _page(skip: int) -> tuple[int, list[dict[str, Any]]]:
            data = await _lta_get(api_key, "/BusRoutes", {"$skip": skip}, demo=demo)
            return skip, data.get("value") or []

        records: list[dict[str, Any]] = []
        skip = 0
        while True:
            pages = await asyncio.gather(
                *(_page(skip + i * STOP_PAGE_SIZE) for i in range(BUS_ROUTE_PAGE_BATCH))
            )
            pages.sort(key=lambda p: p[0])
            last = False
            for _, page in pages:
                records.extend(page)
                if len(page) < STOP_PAGE_SIZE:
                    last = True
                    break
            if last:
                break
            skip += BUS_ROUTE_PAGE_BATCH * STOP_PAGE_SIZE

        logger.info("Bus route cache: %d records", len(records))

        # Stop names/roads come from the (already cached) stop list — no extra LTA calls.
        stops = await _get_all_bus_stops(api_key, demo=demo)
        stop_info = {
            str(s.get("BusStopCode")): (
                str(s.get("Description") or "").strip(),
                str(s.get("RoadName") or "").strip(),
            )
            for s in stops
        }

        index: dict[str, dict[int, dict[str, Any]]] = {}
        for r in records:
            svc = str(r.get("ServiceNo") or "").strip().upper()
            if not svc:
                continue
            try:
                direction = int(r.get("Direction"))
            except (TypeError, ValueError):
                continue
            if direction not in (1, 2):
                continue
            code = str(r.get("BusStopCode") or "").strip()
            desc, road = stop_info.get(code, ("", ""))
            seq = r.get("StopSequence")
            bucket = index.setdefault(svc, {}).setdefault(
                direction,
                {"operator": str(r.get("Operator") or "").strip(), "stops": []},
            )
            bucket["stops"].append(
                {
                    "sequence": seq if isinstance(seq, int) else 0,
                    "bus_stop_code": code,
                    "description": desc or code,
                    "road": road,
                    # First/last bus times are per stop in LTA's data; the
                    # origin stop's times become the direction's headline.
                    "wd_first": _hhmm(r.get("WD_FirstBus")),
                    "wd_last": _hhmm(r.get("WD_LastBus")),
                }
            )

        for svc_dirs in index.values():
            for bucket in svc_dirs.values():
                ordered = sorted(bucket["stops"], key=lambda e: e["sequence"])
                origin = ordered[0] if ordered else {}
                bucket["first_bus"] = origin.get("wd_first", "")
                bucket["last_bus"] = origin.get("wd_last", "")
                bucket["stops"] = [
                    {k: e[k] for k in ("sequence", "bus_stop_code", "description", "road")}
                    for e in ordered
                ]

        _bus_routes = index
        _bus_routes_fetched_at = now
        logger.info("Bus route cache refreshed: %d services", len(index))
        return index


@mcp.tool()
async def bus_route(ctx: Context, service_no: str) -> str:
    """Full ordered route for a Singapore bus service.

    Give the service number as printed on the bus, e.g. "106", "106A" or
    "97e". Returns every direction with all stops in order — sequence
    number, 5-digit stop code, stop name and road — plus the operator and
    the weekday first/last bus from the origin stop. Every stop line is
    tagged with its destination (e.g. [→ Shenton Way Ter]); sequence
    numbers restart at 1 for each direction, so always read the tag, not
    just the stop code. The full route dataset is cached for 24 hours so
    repeat lookups are fast (the first call warms the cache and takes a
    little longer).
    """
    svc = (service_no or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{1,5}", svc):
        return (
            f"'{service_no}' doesn't look like a bus service number. Give the "
            "number as printed on the bus, e.g. '106', '106A' or '97e'."
        )

    try:
        api_key, is_demo, demo_remaining = await _resolve_api_key(ctx)
    except (ValueError, RuntimeError) as exc:
        return str(exc)  # missing key / demo exhausted — message already explains

    try:
        routes = await _get_all_bus_routes(api_key, demo=is_demo)
    except (ValueError, RuntimeError) as exc:
        text = str(exc)
        return text + _demo_note(demo_remaining) if is_demo else text

    directions = routes.get(svc)
    if not directions:
        text = (
            f"I couldn't find a route for service '{service_no}'. Double-check "
            "the service number — it should be the number printed on the bus, "
            "e.g. '106' or '106A'."
        )
        return text + _demo_note(demo_remaining) if is_demo else text

    operator = next(iter(directions.values()))["operator"]
    dir_word = "direction" if len(directions) == 1 else "directions"
    lines = [f"{svc} — {operator} ({len(directions)} {dir_word}):"]
    lines.append(
        "How to read: every stop line starts with its destination tag, e.g. "
        "[→ Shenton Way Ter]. Sequence numbers restart at 1 for each "
        "direction — a stop code can appear under more than one direction, "
        "so always check the tag."
    )
    for direction in sorted(directions):
        bucket = directions[direction]
        stops = bucket["stops"]
        terminus = stops[-1]["description"] if stops else "?"
        loop = (
            " (loop)"
            if stops and stops[0]["bus_stop_code"] == stops[-1]["bus_stop_code"]
            else ""
        )
        lines.append(f"\nDirection {direction} → {terminus}{loop} ({len(stops)} stops):")
        if bucket.get("first_bus") or bucket.get("last_bus"):
            lines.append(
                f"Weekday first bus {bucket['first_bus'] or '?'} from origin, "
                f"last bus {bucket['last_bus'] or '?'}."
            )
        for i, s in enumerate(stops, 1):
            road = f" ({s['road']})" if s["road"] else ""
            lines.append(
                f"[→ {terminus}{loop}] {i}. {s['bus_stop_code']} — "
                f"{s['description']}{road}"
            )

    text = "\n".join(lines)
    if is_demo:
        text += _demo_note(demo_remaining)
    return text


# ------------------------------------------------------- maintenance

_STATIC_DATASETS = {
    "bus_stops": "/BusStops",
    "bus_routes": "/BusRoutes",
    "bus_services": "/BusServices",
}


@mcp.tool()
async def dump_static_data(ctx: Context, dataset: str, skip: int = 0) -> str:
    """Maintenance: return one raw page (500 records) of an LTA static dataset.

    Used by scripts/refresh_data.py to rebuild the bundled data/*.json files.
    Not meant for everyday questions — it returns raw JSON, not friendly text.
    dataset is one of: bus_stops, bus_routes, bus_services. skip pages through
    the dataset in 500-record steps (0, 500, 1000, ...).
    """
    dataset = (dataset or "").strip().lower()
    if dataset not in _STATIC_DATASETS:
        return (
            f"Unknown dataset {dataset!r}. "
            f"Choose one of: {', '.join(sorted(_STATIC_DATASETS))}."
        )
    if skip < 0:
        skip = 0

    try:
        api_key, is_demo, demo_remaining = await _resolve_api_key(ctx)
    except (ValueError, RuntimeError) as exc:
        return str(exc)  # missing key / demo exhausted — message already explains

    try:
        data = await _lta_get(
            api_key, _STATIC_DATASETS[dataset], {"$skip": skip}, demo=is_demo
        )
    except (ValueError, RuntimeError) as exc:
        text = str(exc)
        return text + _demo_note(demo_remaining) if is_demo else text

    records = data.get("value") or []
    return json.dumps({"skip": skip, "count": len(records), "records": records})


# ------------------------------------------------------------- web pages

_PAGE_STYLE = """
  body { font-family: -apple-system, "Segoe UI", Roboto, sans-serif; max-width: 680px;
         margin: 3rem auto; padding: 0 1.5rem; color: #1a1a1a; line-height: 1.6; }
  h1 { font-size: 1.8rem; } h2 { font-size: 1.2rem; margin-top: 2rem; }
  code { background: #f1f3f5; padding: 0.15rem 0.4rem; border-radius: 4px; font-size: 0.9em; }
  .card { background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px;
          padding: 1rem 1.25rem; margin: 1rem 0; }
  a { color: #1971c2; } footer { margin-top: 3rem; font-size: 0.85rem; color: #868e96; }
"""

LANDING_HTML = f"""<!doctype html><html><head><meta charset="utf-8">
<title>SG Bus Arrivals — Muse connector</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{_PAGE_STYLE}</style></head><body>
<h1>🚌 SG Bus Arrivals</h1>
<p>A Muse connector for <strong>real-time Singapore bus arrival times</strong>,
powered by the official LTA DataMall API. Ask in plain language — it answers with
minutes until arrival, crowding, wheelchair access and single/double deck.</p>

<h2>Try saying</h2>
<div class="card">
"When is the next bus 96 at stop 83139?"<br>
"Which buses stop at Buona Vista?"<br>
"Is the next bus 151 wheelchair accessible?"<br>
"What buses are near me?"
</div>

<h2>Get connected (2 minutes)</h2>
<ol>
<li>Get a <strong>free</strong> LTA DataMall API key at
  <a href="https://datamall.lta.gov.sg">datamall.lta.gov.sg</a>.</li>
<li>In Muse, open <strong>Connectors → Add custom connector</strong>.</li>
<li>Paste this server's address with <code>/mcp</code> appended as the MCP URL.</li>
<li>When prompted for your key, save it as the
  <code>x-lta-account-key</code> request header.</li>
</ol>
<p>Your key is sent to LTA with each request and never stored by this server —
see the <a href="/privacy">privacy policy</a>.</p>

<h2>No key yet? Try the demo</h2>
<p>Just connect <strong>without</strong> adding any key — your first 10 tries each
day go through on a <strong>shared demo key</strong>, on the house. It's the
developer's own key with a shared quota, meant for trying things out. For real,
unlimited use, grab your own free key above — it takes two minutes.</p>

<footer><a href="/privacy">Privacy</a> · <a href="/terms">Terms</a> ·
Not affiliated with LTA or Meta.</footer>
</body></html>"""

PRIVACY_HTML = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Privacy — SG Bus Arrivals</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{_PAGE_STYLE}</style></head><body>
<h1>Privacy policy</h1>
<p>SG Bus Arrivals is a simple relay between you and the LTA DataMall API.
In plain language:</p>
<ul>
<li><strong>Your LTA API key</strong> arrives in each request's headers and is
forwarded to LTA's servers to fetch bus data. It is <strong>never stored</strong>
on disk or in a database, never logged, and never sent anywhere except LTA.</li>
<li><strong>Bus stop queries</strong> are forwarded to LTA to answer your
request. We keep no history of what you searched for.</li>
<li><strong>Demo key:</strong> if you connect without your own key, the server
may forward its own demo key to LTA on your behalf (rate-limited to 10 tries
per day per address). Like any other key here, it is forwarded per-request
only — never stored, never logged.</li>
<li><strong>No analytics, no cookies, no tracking.</strong> The only logging is
anonymous server telemetry (request latency, error counts) used to keep the
service healthy.</li>
<li>We never sell, share, or disclose any data to third parties.</li>
</ul>
<p>Questions: open an issue on the project's GitHub repository.</p>
<footer><a href="/">Home</a> · <a href="/terms">Terms</a></footer>
</body></html>"""

TERMS_HTML = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Terms — SG Bus Arrivals</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>{_PAGE_STYLE}</style></head><body>
<h1>Terms of use</h1>
<ul>
<li>SG Bus Arrivals is provided <strong>as-is</strong>, without warranties of any
kind. Arrival times come from the LTA DataMall API and may be delayed,
incomplete, or unavailable (e.g. outside operating hours).</li>
<li>This is a community project and is <strong>not affiliated with, endorsed by,
or sponsored by</strong> the Land Transport Authority (LTA) or Meta.</li>
<li>You are responsible for your own LTA DataMall API key and for complying with
LTA's DataMall terms of use.</li>
<li>We may rate-limit abusive traffic to protect the service.</li>
</ul>
<footer><a href="/">Home</a> · <a href="/privacy">Privacy</a></footer>
</body></html>"""


@mcp.custom_route("/", methods=["GET"])
async def _index(request: Request) -> HTMLResponse:
    return HTMLResponse(LANDING_HTML)


@mcp.custom_route("/privacy", methods=["GET"])
async def _privacy(request: Request) -> HTMLResponse:
    return HTMLResponse(PRIVACY_HTML)


@mcp.custom_route("/terms", methods=["GET"])
async def _terms(request: Request) -> HTMLResponse:
    return HTMLResponse(TERMS_HTML)


# ------------------------------------------------------- app composition

# One Starlette app: the MCP streamable-HTTP endpoint (/mcp) plus the three
# static pages registered above. Registering them as custom routes keeps the
# session manager's lifespan intact — mounting this app inside another
# Starlette app would skip that lifespan and break the MCP endpoint.
app = mcp.streamable_http_app()
