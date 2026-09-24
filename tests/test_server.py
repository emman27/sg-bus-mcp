"""Top-level behavioral tests for SG Bus Arrivals.

These test what the connector *does*, not how: tool contracts, input
validation, key handling, the demo budget, and the web pages. All LTA
network calls are stubbed out — nothing here touches the real API.
"""
import asyncio
from datetime import datetime, timedelta

import pytest
from starlette.testclient import TestClient

import server
from server import SGT
from conftest import FakeCtx

HEADER_KEY = {"x-lta-account-key": "user-key-123"}


def run(coro):
    return asyncio.run(coro)


# ------------------------------------------------------------------ tools


def test_exactly_three_tools_exposed():
    tools = run(server.mcp.list_tools())
    assert sorted(t.name for t in tools) == [
        "bus_arrivals",
        "find_bus_stops",
        "nearby_bus_stops",
    ]


# ---------------------------------------------------------- bus_arrivals


def test_bus_arrivals_rejects_bad_stop_codes():
    for bad in ["abc", "1234", "123456", "12a45", ""]:
        text = run(server.bus_arrivals(FakeCtx(HEADER_KEY), bad))
        assert "5 digits" in text, bad


def test_bus_arrivals_without_any_key_explains_how_to_get_one(monkeypatch):
    monkeypatch.delenv("LTA_DEMO_ACCOUNT_KEY", raising=False)
    text = run(server.bus_arrivals(FakeCtx(), "83139"))
    assert "datamall.lta.gov.sg" in text
    assert "x-lta-account-key" in text


def _arrival_payload():
    now = datetime.now(SGT)
    return {
        "Services": [
            {
                "ServiceNo": "96",
                "Operator": "SBST",
                "NextBus": {
                    "EstimatedArrival": (now + timedelta(minutes=6)).isoformat(),
                    "Load": "SEA",
                    "Feature": "WAB",
                    "Type": "DD",
                },
                "NextBus2": {
                    "EstimatedArrival": (now + timedelta(minutes=14)).isoformat(),
                    "Load": "SDA",
                    "Feature": "",
                    "Type": "SD",
                },
                "NextBus3": {},
            }
        ]
    }


def test_bus_arrivals_reports_times_crowding_accessibility_and_deck(monkeypatch):
    async def fake_lta(api_key, path, params, demo=False):
        assert params == {"BusStopCode": "83139"}
        return _arrival_payload()

    monkeypatch.setattr(server, "_lta_get", fake_lta)
    text = run(server.bus_arrivals(FakeCtx(HEADER_KEY), "83139"))
    assert "96" in text and "SBST" in text
    assert "6 min" in text and "14 min" in text
    assert "seats available" in text
    assert "standing available" in text
    assert "wheelchair accessible" in text
    assert "double deck" in text and "single deck" in text


def test_bus_arrivals_with_no_services_says_so_plainly(monkeypatch):
    async def fake_lta(api_key, path, params, demo=False):
        return {"Services": []}

    monkeypatch.setattr(server, "_lta_get", fake_lta)
    text = run(server.bus_arrivals(FakeCtx(HEADER_KEY), "83139"))
    assert "No buses are currently serving stop 83139" in text


# -------------------------------------------------------- find_bus_stops


def test_find_bus_stops_rejects_tiny_queries():
    text = run(server.find_bus_stops(FakeCtx(HEADER_KEY), "a"))
    assert "at least 2 characters" in text


def _canned_stops():
    return [
        {"BusStopCode": "11111", "Description": "Buona Vista",
         "RoadName": "North Buona Vista Rd", "Latitude": 1.306, "Longitude": 103.782},
        {"BusStopCode": "22222", "Description": "Clementi Int",
         "RoadName": "Clementi Rd", "Latitude": 1.31, "Longitude": 103.79},
    ]


def test_find_bus_stops_matches_name_and_road_case_insensitively(monkeypatch):
    async def fake_stops(api_key, demo=False):
        return _canned_stops()

    monkeypatch.setattr(server, "_get_all_bus_stops", fake_stops)
    for query in ["buona", "BUONA", "clementi rd"]:
        text = run(server.find_bus_stops(FakeCtx(HEADER_KEY), query))
        assert "11111" in text or "22222" in text, query


def test_find_bus_stops_no_match_suggests_shorter_search(monkeypatch):
    async def fake_stops(api_key, demo=False):
        return _canned_stops()

    monkeypatch.setattr(server, "_get_all_bus_stops", fake_stops)
    text = run(server.find_bus_stops(FakeCtx(HEADER_KEY), "zzzznotreal"))
    assert "No bus stops matched" in text


# ------------------------------------------------------ nearby_bus_stops


def test_nearby_rejects_impossible_coordinates():
    text = run(server.nearby_bus_stops(FakeCtx(HEADER_KEY), 999, 103.8))
    assert "isn't a valid latitude" in text
    text = run(server.nearby_bus_stops(FakeCtx(HEADER_KEY), 1.3, 999))
    assert "isn't a valid longitude" in text


def test_nearby_returns_closest_first_with_distances(monkeypatch):
    async def fake_stops(api_key, demo=False):
        return _canned_stops()

    monkeypatch.setattr(server, "_get_all_bus_stops", fake_stops)
    text = run(server.nearby_bus_stops(FakeCtx(HEADER_KEY), 1.306, 103.782, radius_m=1500))
    near_pos = text.index("11111")
    far_pos = text.index("22222")
    assert near_pos < far_pos  # nearest first
    assert "0 m away" in text  # standing right at the near stop


def test_haversine_sanity_one_degree_of_latitude():
    # ~111.2 km per degree of latitude; guards against unit mix-ups.
    metres = server._haversine_m(1.0, 103.8, 2.0, 103.8)
    assert 110_000 < metres < 112_000


# ------------------------------------------------------------ key logic


def test_caller_supplied_key_always_wins_over_demo(monkeypatch):
    monkeypatch.setenv("LTA_DEMO_ACCOUNT_KEY", "demo-key")
    key, is_demo, remaining = run(server._resolve_api_key(FakeCtx(HEADER_KEY)))
    assert (key, is_demo, remaining) == ("user-key-123", False, None)


def test_demo_budget_allows_ten_calls_then_says_so_nicely(monkeypatch):
    monkeypatch.setenv("LTA_DEMO_ACCOUNT_KEY", "demo-key")
    ctx = FakeCtx()  # no caller key -> demo path
    for _ in range(10):
        key, is_demo, _ = run(server._resolve_api_key(ctx))
        assert (key, is_demo) == ("demo-key", True)
    with pytest.raises(ValueError, match="10 free demo tries"):
        run(server._resolve_api_key(ctx))


# -------------------------------------------------------------- web pages


@pytest.fixture(scope="module")
def client():
    return TestClient(server.app)


def test_landing_page_loads(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "SG Bus Arrivals" in r.text
    assert "/privacy" in r.text


def test_privacy_page_loads(client):
    r = client.get("/privacy")
    assert r.status_code == 200
    assert "never stored" in r.text


def test_terms_page_loads(client):
    r = client.get("/terms")
    assert r.status_code == 200
    assert "as-is" in r.text
