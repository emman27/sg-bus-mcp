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


def test_exactly_nine_tools_exposed():
    tools = run(server.mcp.list_tools())
    assert sorted(t.name for t in tools) == [
        "bus_arrivals",
        "bus_route",
        "dump_static_data",
        "find_bus_stops",
        "nearby_bus_stops",
        "station_crowd_forecast",
        "station_crowding",
        "train_alerts",
        "train_stations",
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


def _arrival_payload_with_direction():
    now = datetime.now(SGT)
    return {
        "Services": [
            {
                "ServiceNo": "74",
                "Operator": "SBST",
                "NextBus": {
                    "EstimatedArrival": (now + timedelta(minutes=5)).isoformat(),
                    "OriginCode": "64009",
                    "DestinationCode": "77131",
                    "Load": "SEA",
                    "Feature": "WAB",
                    "Type": "DD",
                },
                "NextBus2": {
                    "EstimatedArrival": (now + timedelta(minutes=20)).isoformat(),
                    "OriginCode": "64009",
                    "DestinationCode": "77131",
                    "Load": "SDA",
                    "Feature": "",
                    "Type": "SD",
                },
                "NextBus3": {},
            },
            {
                "ServiceNo": "15",
                "Operator": "GAS",
                "NextBus": {
                    "EstimatedArrival": (now + timedelta(minutes=3)).isoformat(),
                    "OriginCode": "77009",
                    "DestinationCode": "77009",
                    "Load": "SEA",
                    "Feature": "WAB",
                    "Type": "SD",
                },
                "NextBus2": {},
                "NextBus3": {},
            },
        ]
    }


def _canned_stops_with_terminals():
    return [
        {"BusStopCode": "77131", "Description": "Buona Vista Ter",
         "RoadName": "North Buona Vista Rd", "Latitude": 1.306, "Longitude": 103.782},
        {"BusStopCode": "77009", "Description": "Marine Parade Rd",
         "RoadName": "Marine Parade Rd", "Latitude": 1.31, "Longitude": 103.91},
    ]


def test_bus_arrivals_shows_direction_from_destination_code(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)  # empty: force LTA fallback
    async def fake_lta(api_key, path, params, demo=False):
        return _arrival_payload_with_direction()

    async def fake_stops(api_key, demo=False):
        return _canned_stops_with_terminals()

    monkeypatch.setattr(server, "_lta_get", fake_lta)
    monkeypatch.setattr(server, "_get_all_bus_stops", fake_stops)
    text = run(server.bus_arrivals(FakeCtx(HEADER_KEY), "19099"))
    assert "→ Buona Vista Ter" in text
    assert "(loop)" in text  # service 15: origin == destination


def test_bus_arrivals_mixed_destinations_annotated_per_bus(monkeypatch):
    now = datetime.now(SGT)

    async def fake_lta(api_key, path, params, demo=False):
        return {
            "Services": [
                {
                    "ServiceNo": "74",
                    "Operator": "SBST",
                    "NextBus": {
                        "EstimatedArrival": (now + timedelta(minutes=5)).isoformat(),
                        "OriginCode": "64009",
                        "DestinationCode": "77131",
                        "Load": "SEA",
                        "Feature": "WAB",
                        "Type": "DD",
                    },
                    "NextBus2": {
                        "EstimatedArrival": (now + timedelta(minutes=20)).isoformat(),
                        "OriginCode": "64009",
                        "DestinationCode": "64009",
                        "Load": "SDA",
                        "Feature": "",
                        "Type": "SD",
                    },
                    "NextBus3": {},
                }
            ]
        }

    async def fake_stops(api_key, demo=False):
        return _canned_stops_with_terminals()

    monkeypatch.setattr(server, "_lta_get", fake_lta)
    monkeypatch.setattr(server, "_get_all_bus_stops", fake_stops)
    text = run(server.bus_arrivals(FakeCtx(HEADER_KEY), "19099"))
    # Destinations differ bus-to-bus, so each bus carries its own arrow.
    assert "5 min → Buona Vista Ter" in text
    assert "20 min → 64009" in text  # unknown code falls back to the code


def test_bus_arrivals_header_shows_requested_stop_code(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)  # empty: force LTA fallback
    async def fake_lta(api_key, path, params, demo=False):
        assert params == {"BusStopCode": "19099"}
        return _arrival_payload_with_direction()

    async def fake_stops(api_key, demo=False):
        return _canned_stops_with_terminals()

    monkeypatch.setattr(server, "_lta_get", fake_lta)
    monkeypatch.setattr(server, "_get_all_bus_stops", fake_stops)
    text = run(server.bus_arrivals(FakeCtx(HEADER_KEY), "19099"))
    assert text.startswith("Bus stop 19099 —")


def test_bus_arrivals_without_destination_codes_has_no_arrows(monkeypatch):
    async def fake_lta(api_key, path, params, demo=False):
        return _arrival_payload()  # no OriginCode/DestinationCode at all

    monkeypatch.setattr(server, "_lta_get", fake_lta)
    text = run(server.bus_arrivals(FakeCtx(HEADER_KEY), "83139"))
    assert "→" not in text
    assert "6 min" in text  # arrivals themselves still work


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


def test_find_bus_stops_matches_name_and_road_case_insensitively(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)  # empty: force LTA fallback
    async def fake_stops(api_key, demo=False):
        return _canned_stops()

    monkeypatch.setattr(server, "_get_all_bus_stops", fake_stops)
    for query in ["buona", "BUONA", "clementi rd"]:
        text = run(server.find_bus_stops(FakeCtx(HEADER_KEY), query))
        assert "11111" in text or "22222" in text, query


def test_find_bus_stops_no_match_suggests_shorter_search(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)  # empty: force LTA fallback
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


def test_nearby_returns_closest_first_with_distances(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)  # empty: force LTA fallback
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


# ---------------------------------------------------------- bus_route


def _fake_route_pages(monkeypatch, tmp_path):
    """Stub LTA with 6 route records + 3 stops; shrink the page size so the
    paginated fetch is exercised without 500-record fixtures.

    Points DATA_DIR at an empty tmp dir so the bundled data files can't leak
    into the test — the LTA fallback path is what's under test here.
    """
    routes = [
        # Direction 1, deliberately out of sequence order: A -> B -> C
        {"ServiceNo": "106", "Operator": "TT", "Direction": 1, "StopSequence": 2,
         "BusStopCode": "22222", "WD_FirstBus": "0535", "WD_LastBus": "2335"},
        {"ServiceNo": "106", "Operator": "TT", "Direction": 1, "StopSequence": 1,
         "BusStopCode": "11111", "WD_FirstBus": "0530", "WD_LastBus": "2330"},
        {"ServiceNo": "106", "Operator": "TT", "Direction": 1, "StopSequence": 3,
         "BusStopCode": "33333", "WD_FirstBus": "0540", "WD_LastBus": "2340"},
        # Direction 2: C -> B -> A
        {"ServiceNo": "106", "Operator": "TT", "Direction": 2, "StopSequence": 1,
         "BusStopCode": "33333", "WD_FirstBus": "0545", "WD_LastBus": "2345"},
        {"ServiceNo": "106", "Operator": "TT", "Direction": 2, "StopSequence": 2,
         "BusStopCode": "22222", "WD_FirstBus": "0550", "WD_LastBus": "2350"},
        {"ServiceNo": "106", "Operator": "TT", "Direction": 2, "StopSequence": 3,
         "BusStopCode": "11111", "WD_FirstBus": "0555", "WD_LastBus": "2355"},
    ]
    stops = [
        {"BusStopCode": "11111", "Description": "Stop A", "RoadName": "Road A"},
        {"BusStopCode": "22222", "Description": "Stop B", "RoadName": "Road B"},
        {"BusStopCode": "33333", "Description": "Stop C", "RoadName": "Road C"},
    ]

    async def fake_lta(api_key, path, params, demo=False):
        assert path == "/BusRoutes", path
        skip = params.get("$skip", 0)
        return {"value": routes[skip:skip + server.STOP_PAGE_SIZE]}

    async def fake_stops(api_key, demo=False):
        return stops

    monkeypatch.setattr(server, "_lta_get", fake_lta)
    monkeypatch.setattr(server, "_get_all_bus_stops", fake_stops)
    monkeypatch.setattr(server, "_bus_routes", None)
    monkeypatch.setattr(server, "STOP_PAGE_SIZE", 2)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)


def test_hhmm_formats_lta_times():
    assert server._hhmm("2352") == "23:52"
    assert server._hhmm("0530") == "05:30"
    assert server._hhmm("") == ""
    assert server._hhmm(None) == ""
    assert server._hhmm("nope") == "nope"


def test_bus_route_returns_both_directions_in_stop_order(monkeypatch, tmp_path):
    _fake_route_pages(monkeypatch, tmp_path)
    text = run(server.bus_route(FakeCtx(HEADER_KEY), "106"))

    assert "106 — TT (2 directions):" in text
    assert "Direction 1 → Stop C (3 stops):" in text
    assert "Direction 2 → Stop A (3 stops):" in text

    # Every stop line carries its destination tag, so a stop code read on
    # its own can't be misattributed to the wrong direction.
    assert "destination tag" in text
    assert "[→ Stop C] 1. 11111 — Stop A (Road A)" in text
    assert "[→ Stop C] 3. 33333 — Stop C (Road C)" in text
    assert "[→ Stop A] 1. 33333 — Stop C (Road C)" in text
    assert "[→ Stop A] 3. 11111 — Stop A (Road A)" in text

    # Direction 1 sorted by StopSequence despite the shuffled input.
    d1 = text.split("Direction 1")[1].split("Direction 2")[0]
    assert d1.index("11111") < d1.index("22222") < d1.index("33333")
    assert "Stop A (Road A)" in d1

    # Weekday first/last bus come from the origin stop, formatted as HH:MM.
    assert "Weekday first bus 05:30 from origin, last bus 23:30." in text


def test_bus_route_normalises_service_number(monkeypatch, tmp_path):
    _fake_route_pages(monkeypatch, tmp_path)
    text = run(server.bus_route(FakeCtx(HEADER_KEY), " 106 "))
    assert "106 — TT" in text


def test_bus_route_unknown_service(monkeypatch, tmp_path):
    _fake_route_pages(monkeypatch, tmp_path)
    text = run(server.bus_route(FakeCtx(HEADER_KEY), "999"))
    assert "couldn't find a route for service '999'" in text


def test_bus_route_rejects_garbage_input():
    for bad in ["", "!!", "abc def", "123456"]:
        text = run(server.bus_route(FakeCtx(HEADER_KEY), bad))
        assert "doesn't look like a bus service number" in text, bad


def test_bus_route_caches_across_calls(monkeypatch, tmp_path):
    _fake_route_pages(monkeypatch, tmp_path)
    calls = {"n": 0}
    inner = server._lta_get

    async def counting(api_key, path, params, demo=False):
        calls["n"] += 1
        return await inner(api_key, path, params, demo=demo)

    monkeypatch.setattr(server, "_lta_get", counting)
    run(server.bus_route(FakeCtx(HEADER_KEY), "106"))
    assert calls["n"] > 0
    run(server.bus_route(FakeCtx(HEADER_KEY), "106"))
    run(server.bus_route(FakeCtx(HEADER_KEY), "999"))  # cache hit, miss on lookup
    assert calls["n"] > 0
    # Second and third calls served entirely from cache: no new LTA hits.
    first_total = calls["n"]
    run(server.bus_route(FakeCtx(HEADER_KEY), "106"))
    assert calls["n"] == first_total


# --------------------------------------------- bundled static data files


def _write_data_file(tmp_path, name, doc):
    (tmp_path / name).write_text(
        __import__("json").dumps(doc), encoding="utf-8"
    )


def _bundled_bus_files(tmp_path):
    """Minimal bus_stops.json + bus_routes.json in a tmp DATA_DIR."""
    _write_data_file(tmp_path, "bus_stops.json", {
        "generated_at": "2026-09-25",
        "records": [
            {"BusStopCode": "11111", "Description": "Stop A",
             "RoadName": "Road A", "Latitude": 1.3, "Longitude": 103.8},
            {"BusStopCode": "22222", "Description": "Stop B",
             "RoadName": "Road B", "Latitude": 1.31, "Longitude": 103.81},
        ],
    })
    _write_data_file(tmp_path, "bus_routes.json", {
        "generated_at": "2026-09-25",
        "records": [
            {"ServiceNo": "106", "Operator": "TT", "Direction": 1,
             "StopSequence": 1, "BusStopCode": "11111",
             "WD_FirstBus": "0530", "WD_LastBus": "2330"},
            {"ServiceNo": "106", "Operator": "TT", "Direction": 1,
             "StopSequence": 2, "BusStopCode": "22222",
             "WD_FirstBus": "0535", "WD_LastBus": "2335"},
        ],
    })


def _no_lta(monkeypatch):
    async def boom(api_key, path, params, demo=False):
        raise AssertionError(f"LTA should not be called (got {path})")
    monkeypatch.setattr(server, "_lta_get", boom)


def test_bus_stops_served_from_bundled_file(monkeypatch, tmp_path):
    _bundled_bus_files(tmp_path)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    _no_lta(monkeypatch)
    stops = run(server._get_all_bus_stops("key"))
    assert [s["BusStopCode"] for s in stops] == ["11111", "22222"]
    # Second call hits the in-memory copy, still no LTA.
    assert run(server._get_all_bus_stops("key")) == stops


def test_bus_route_served_from_bundled_file_with_vintage(monkeypatch, tmp_path):
    _bundled_bus_files(tmp_path)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    _no_lta(monkeypatch)
    text = run(server.bus_route(FakeCtx(HEADER_KEY), "106"))
    assert "106 — TT (1 direction):" in text
    assert "[→ Stop B] 1. 11111 — Stop A (Road A)" in text
    assert "Route data bundled 2026-09-25" in text


def test_bus_stops_fall_back_to_lta_when_no_file(monkeypatch, tmp_path):
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)  # empty: no files

    async def fake_lta(api_key, path, params, demo=False):
        assert path == "/BusStops"
        return {"value": [
            {"BusStopCode": "99999", "Description": "Fallback Stop",
             "RoadName": "Road Z"},
        ]}

    monkeypatch.setattr(server, "_lta_get", fake_lta)
    stops = run(server._get_all_bus_stops("key"))
    assert stops[0]["Description"] == "Fallback Stop"


# ------------------------------------------------------- dump_static_data


def test_dump_static_data_returns_raw_page(monkeypatch):
    async def fake_lta(api_key, path, params, demo=False):
        assert path == "/BusRoutes"
        assert params == {"$skip": 500}
        return {"value": [{"ServiceNo": "106"}]}

    monkeypatch.setattr(server, "_lta_get", fake_lta)
    text = run(server.dump_static_data(FakeCtx(HEADER_KEY), "bus_routes", 500))
    doc = __import__("json").loads(text)
    assert doc["skip"] == 500
    assert doc["records"] == [{"ServiceNo": "106"}]


def test_dump_static_data_rejects_unknown_dataset():
    text = run(server.dump_static_data(FakeCtx(HEADER_KEY), "bus_fares", 0))
    assert "Unknown dataset" in text
    assert "bus_stops" in text


# ------------------------------------------------------------------ trains


def _mini_station_map(tmp_path):
    _write_data_file(tmp_path, "mrt_stations.json", {
        "generated_at": "2026-09-25",
        "lines": {
            "NSL": {"name": "North-South Line", "color": "#d42e12",
                    "codes": ["NS1", "NS2"],
                    "segments": [["NS1", "NS2"]]},
            "CCL": {"name": "Circle Line", "color": "#fa9e0d",
                    "codes": ["CC1"],
                    "segments": [["CC1"]]},
        },
        "stations": [
            {"name": "Jurong East", "codes": ["NS1"], "lines": ["NSL"],
             "lat": 1.3332, "lon": 103.7424},
            {"name": "Bukit Batok", "codes": ["NS2"], "lines": ["NSL"],
             "lat": 1.3489, "lon": 103.7496},
            {"name": "Dhoby Ghaut", "codes": ["CC1", "NE6", "NS24"],
             "lines": ["CCL", "NEL", "NSL"], "lat": 1.2986, "lon": 103.8451},
        ],
    })


def test_normalize_train_line_aliases():
    assert server._normalize_train_line("NSL") == "NSL"
    assert server._normalize_train_line("ns") == "NSL"
    assert server._normalize_train_line("North South Line") == "NSL"
    assert server._normalize_train_line("Circle Line") == "CCL"
    assert server._normalize_train_line("Downtown") == "DTL"
    assert server._normalize_train_line("Thomson-East Coast") == "TEL"
    assert server._normalize_train_line("Bukit Panjang LRT") == "BPL"
    assert server._normalize_train_line("Sengkang") == "SLRT"
    assert server._normalize_train_line("Punggol LRT") == "PLRT"
    assert server._normalize_train_line("bogus") is None
    assert server._normalize_train_line("") is None


def _pcd_realtime(monkeypatch, rows):
    async def fake_lta(api_key, path, params, demo=False):
        assert path == "/PCDRealTime"
        return {"value": rows}
    monkeypatch.setattr(server, "_lta_get", fake_lta)


def test_station_crowding_enriches_names_and_plain_words(monkeypatch, tmp_path):
    _mini_station_map(tmp_path)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    _pcd_realtime(monkeypatch, [
        {"Station": "NS1", "StartTime": "2026-09-25T09:40:00+08:00",
         "EndTime": "2026-09-25T09:50:00+08:00", "CrowdLevel": "l"},
        {"Station": "NS2", "StartTime": "2026-09-25T09:40:00+08:00",
         "EndTime": "2026-09-25T09:50:00+08:00", "CrowdLevel": "h"},
    ])
    text = run(server.station_crowding(FakeCtx(HEADER_KEY), "North South Line"))
    assert "NSL platform crowding" in text
    assert "9:40am–9:50am SGT" in text
    assert "- NS1 Jurong East: low" in text
    assert "- NS2 Bukit Batok: high" in text


def test_station_crowding_filters_by_station(monkeypatch, tmp_path):
    _mini_station_map(tmp_path)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    _pcd_realtime(monkeypatch, [
        {"Station": "NS1", "CrowdLevel": "l"},
        {"Station": "NS2", "CrowdLevel": "m"},
    ])
    text = run(server.station_crowding(FakeCtx(HEADER_KEY), "NSL", "bukit batok"))
    assert "NS2 Bukit Batok: moderate" in text
    assert "NS1" not in text


def test_station_crowding_rejects_unknown_line():
    text = run(server.station_crowding(FakeCtx(HEADER_KEY), "Hogwarts Express"))
    assert "Unknown train line" in text
    assert "NSL" in text


def test_station_crowding_unknown_station_says_so(monkeypatch, tmp_path):
    _mini_station_map(tmp_path)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    _pcd_realtime(monkeypatch, [{"Station": "NS1", "CrowdLevel": "l"}])
    text = run(server.station_crowding(FakeCtx(HEADER_KEY), "NSL", "zzz"))
    assert "No stations matching 'zzz'" in text


def test_station_crowd_forecast_groups_slots_in_line_order(monkeypatch, tmp_path):
    _mini_station_map(tmp_path)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)

    async def fake_lta(api_key, path, params, demo=False):
        assert path == "/PCDForecast"
        return {"value": [
            # Deliberately shuffled: NS2 row sorts after NS1 via line order.
            {"Station": "NS2", "Start": "2026-09-25T10:00:00+08:00",
             "CrowdLevel": "m"},
            {"Station": "NS1", "Start": "2026-09-25T10:00:00+08:00",
             "CrowdLevel": "l"},
            {"Station": "NS1", "Start": "2026-09-25T10:30:00+08:00",
             "CrowdLevel": "m"},
            # Yesterday's slot is outside the window and must be dropped.
            {"Station": "NS1", "Start": "2026-09-24T10:00:00+08:00",
             "CrowdLevel": "h"},
        ]}

    monkeypatch.setattr(server, "_lta_get", fake_lta)

    class _FrozenDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 25, 9, 45, tzinfo=server.SGT)

    monkeypatch.setattr(server, "datetime", _FrozenDT)
    text = run(server.station_crowd_forecast(FakeCtx(HEADER_KEY), "NSL", hours=4))
    assert "NSL crowd forecast" in text
    assert "- NS1 Jurong East: 10:00am low · 10:30am moderate" in text
    assert "- NS2 Bukit Batok: 10:00am moderate" in text
    assert text.index("NS1 Jurong East") < text.index("NS2 Bukit Batok")


def test_train_alerts_all_clear(monkeypatch):
    async def fake_lta(api_key, path, params, demo=False):
        assert path == "/TrainServiceAlerts"
        return {"value": []}
    monkeypatch.setattr(server, "_lta_get", fake_lta)
    text = run(server.train_alerts(FakeCtx(HEADER_KEY)))
    assert "running normally" in text


def test_train_alerts_reports_disruption(monkeypatch):
    async def fake_lta(api_key, path, params, demo=False):
        return {"value": [
            {"Status": "1", "Line": "NSL", "Message": "Minor delay"},
            {"Status": "2", "Line": "EWL", "Direction": "Towards Pasir Ris",
             "Stations": "EW13-EW16",
             "FreePublicBus": "Yes, at affected stations",
             "FreeMRTShuttle": "Yes", "MRTShuttleDirection": "Both",
             "Message": "Track fault near City Hall"},
        ]}
    monkeypatch.setattr(server, "_lta_get", fake_lta)
    text = run(server.train_alerts(FakeCtx(HEADER_KEY)))
    assert "⚠️ EWL — disrupted" in text
    assert "Towards Pasir Ris" in text
    assert "EW13-EW16" in text
    assert "Free bridging buses: Yes, at affected stations" in text
    assert "Track fault near City Hall" in text
    # Status-1 rows are not disruptions and stay quiet.
    assert "Minor delay" not in text


def test_train_stations_lists_line_from_map(monkeypatch, tmp_path):
    _mini_station_map(tmp_path)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    text = run(server.train_stations(FakeCtx(), "nsl"))
    assert "NSL — North-South Line (2 stations):" in text
    assert "NS1 · Jurong East · 1.3332, 103.7424" in text
    assert "NS2 · Bukit Batok · 1.3489, 103.7496" in text


def test_train_stations_searches_by_name_and_code(monkeypatch, tmp_path):
    _mini_station_map(tmp_path)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    text = run(server.train_stations(FakeCtx(), "", "dhoby"))
    assert "CC1/NE6/NS24 · Dhoby Ghaut (CCL, NEL, NSL)" in text
    text = run(server.train_stations(FakeCtx(), "", "ns2"))
    assert "NS2 · Bukit Batok" in text
    text = run(server.train_stations(FakeCtx(), "", "zzz"))
    assert "No stations matching 'zzz'" in text


def test_train_stations_rejects_unknown_line():
    text = run(server.train_stations(FakeCtx(), "Hogwarts Express"))
    assert "Unknown train line" in text


# ------------------------------------------------- keyless static lookups


def test_static_tools_need_no_key_when_bundled(monkeypatch, tmp_path):
    """find_bus_stops / nearby / bus_route serve from disk with zero auth."""
    _bundled_bus_files(tmp_path)
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)

    async def boom(api_key, path, params, demo=False):
        raise AssertionError(f"LTA should not be called (got {path})")
    monkeypatch.setattr(server, "_lta_get", boom)
    # Also fail loudly if any key resolution is attempted.
    async def no_key(ctx):
        raise AssertionError("_resolve_api_key should not be called")
    monkeypatch.setattr(server, "_resolve_api_key", no_key)

    no_auth = FakeCtx()  # no headers, no demo key in env
    text = run(server.find_bus_stops(no_auth, "stop a"))
    assert "11111" in text
    text = run(server.nearby_bus_stops(no_auth, 1.3, 103.8, 5, 500))
    assert "11111" in text
    text = run(server.bus_route(no_auth, "106"))
    assert "106 — TT (1 direction):" in text


def test_static_tools_ask_for_key_only_when_no_bundled_file(monkeypatch, tmp_path):
    """Without data files, the tools fall back to LTA and need a key."""
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)  # empty: no files
    text = run(server.find_bus_stops(FakeCtx(), "stop"))
    assert "LTA DataMall API key" in text or "API key" in text
    text = run(server.bus_route(FakeCtx(), "106"))
    assert "LTA DataMall API key" in text or "API key" in text
