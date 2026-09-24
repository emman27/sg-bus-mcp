"""Shared fixtures for the SG Bus Arrivals test suite."""
import sys
from pathlib import Path

import pytest

# Make `import server` work when pytest runs from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import server  # noqa: E402


class _FakeClient:
    host = "1.2.3.4"


class _FakeRequest:
    client = _FakeClient()

    def __init__(self, headers=None):
        self.headers = headers or {}


class _FakeRequestContext:
    def __init__(self, headers=None):
        self.request = _FakeRequest(headers)


class FakeCtx:
    """Minimal stand-in for FastMCP's Context (only what the tools touch)."""

    def __init__(self, headers=None):
        self.request_context = _FakeRequestContext(headers)


@pytest.fixture(autouse=True)
def _clean_demo_state():
    """Every test starts with a fresh demo-key budget and no demo key set."""
    server._demo_usage.clear()
    yield
    server._demo_usage.clear()


@pytest.fixture(autouse=True)
def _clean_static_data_state():
    """Every test starts with empty static-data caches (disk + memory)."""
    server._data_files.clear()
    server._bus_stops = None
    server._bus_stops_fetched_at = None
    server._bundled_routes = None
    server._bundled_vintage = None
    server._live_routes = None
    server._live_routes_fetched_at = None
    server._train_network = None
    yield
    server._data_files.clear()
    server._bus_stops = None
    server._bus_stops_fetched_at = None
    server._bundled_routes = None
    server._bundled_vintage = None
    server._live_routes = None
    server._live_routes_fetched_at = None
    server._train_network = None
