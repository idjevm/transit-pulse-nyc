"""Tests for weather observations and crowd surge state ingestion and agent grounding."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import agents
from dashboard import state as state_mod
from dashboard.state import DashboardState


class _Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, secs):
        self.t += secs


def test_weather_ingestion_and_snapshot(monkeypatch):
    clock = _Clock(1_000_000.0)
    monkeypatch.setattr(state_mod, "_now", clock)

    st = DashboardState()
    # Ingest Open-Meteo weather payload
    st.update_weather({
        "current": {
            "temperature_2m": 18.5,
            "precipitation": 0.2,
            "weather_code": 61,
            "wind_speed_10m": 14.0,
        }
    })

    snap = st.snapshot()
    w = snap.get("weather")
    assert w is not None
    assert w["temp_c"] == 18.5
    assert w["temp_f"] == 65.3
    assert w["condition"] == "Light Rain"
    assert w["precip_mm"] == 0.2

    # Agent grounding includes weather
    grounding = agents._summarize_state(snap)
    assert "WEATHER: Light Rain" in grounding
    assert "18.5°C" in grounding


def test_passenger_surges_ingestion_and_snapshot(monkeypatch):
    clock = _Clock(1_000_000.0)
    monkeypatch.setattr(state_mod, "_now", clock)

    st = DashboardState()
    # Ingest Datagen crowd surge events
    st.update_passenger_surge({
        "station_id": "127",
        "station_name": "Times Sq-42 St",
        "line": "1",
        "taps_per_minute": 1850,
        "crowd_level": "SURGE",
    })
    st.update_passenger_surge({
        "station_id": "R16",
        "station_name": "Herald Sq",
        "line": "N",
        "taps_per_minute": 850,
        "crowd_level": "NORMAL",
    })

    snap = st.snapshot()
    surges = snap.get("crowd_surges", [])
    # Only SURGE and HIGH should be in crowd_surges list
    assert len(surges) == 1
    assert surges[0]["station_name"] == "Times Sq-42 St"
    assert surges[0]["crowd_level"] == "SURGE"
    assert snap["counts"]["crowd_surges"] == 1

    # Agent grounding includes surge details
    grounding = agents._summarize_state(snap)
    assert "STATION PASSENGER SURGES" in grounding
    assert "Times Sq-42 St" in grounding


def test_weather_and_surge_ttl_expiry(monkeypatch):
    clock = _Clock(1_000_000.0)
    monkeypatch.setattr(state_mod, "_now", clock)

    st = DashboardState()
    st.update_weather({"current": {"temperature_2m": 20.0, "weather_code": 0}})
    st.update_passenger_surge({"station_id": "127", "station_name": "Times Sq", "crowd_level": "SURGE"})

    assert st.snapshot()["weather"] is not None
    assert len(st.snapshot()["crowd_surges"]) == 1

    # Advance 200s (surges expire after 180s)
    clock.advance(200)
    snap = st.snapshot()
    assert len(snap["crowd_surges"]) == 0
    assert snap["weather"] is not None  # weather TTL is 1800s

    # Advance past 1800s
    clock.advance(1700)
    assert st.snapshot()["weather"] is None

