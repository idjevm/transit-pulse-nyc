"""Tests for the forecast + judge-triggered simulation state (PR: disruption
simulator & forecast).

DashboardState has no native-SDK dependency, so it imports directly. Time-based
TTL/expiry is tested by monkeypatching the module clock (dashboard.state._now).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import state as state_mod  # noqa: E402
from dashboard.state import DashboardState  # noqa: E402


class _Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, secs):
        self.t += secs


def _forecast(**over):
    base = {
        "route_id": "N", "direction": "S", "stop_id": "R16", "stop_name": "Times Sq-42 St",
        "stop_lat": 40.7557, "stop_lon": -73.987, "curr_trip": "T1",
        "forecast_type": "PREDICTED_BUNCHING", "headway_seconds": 160, "predicted_headway": 70,
    }
    base.update(over)
    return base


# ---- forecast ingestion ----

def test_update_forecast_dedup_keeps_latest():
    st = DashboardState()
    st.update_forecast(_forecast(predicted_headway=70))
    st.update_forecast(_forecast(predicted_headway=55))  # same (route,dir,stop) key
    forecasts = st.snapshot()["forecasts"]
    assert len(forecasts) == 1
    assert forecasts[0]["predicted_headway"] == 55
    assert forecasts[0]["forecast_type"] == "PREDICTED_BUNCHING"


def test_update_forecast_ignores_stable_and_missing():
    st = DashboardState()
    st.update_forecast(_forecast(forecast_type="STABLE"))
    st.update_forecast(_forecast(route_id="", forecast_type="PREDICTED_GAP"))
    st.update_forecast(_forecast(stop_id="", forecast_type="PREDICTED_GAP"))
    assert st.snapshot()["forecasts"] == []


def test_forecast_ttl_eviction(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(state_mod, "_now", clock)
    st = DashboardState()
    st.update_forecast(_forecast())
    assert len(st.snapshot()["forecasts"]) == 1
    clock.advance(state_mod.FORECAST_TTL_SEC + 1)
    assert st.snapshot()["forecasts"] == []
    assert st._forecasts == {}  # evicted from the backing dict, not just filtered


# ---- judge-triggered simulation ----

def test_inject_bunching_surfaces_labeled_records():
    st = DashboardState()
    result = st.inject_simulation("bunching")
    assert result["scenario"] == "bunching"
    snap = st.snapshot()

    assert len(snap["alerts"]) == 1
    assert snap["alerts"][0]["simulated"] is True
    assert snap["alerts"][0]["alert_type"] == "BUNCHING"
    assert snap["alerts"][0]["lat"] and snap["alerts"][0]["lon"]

    assert snap["recommendations"][0]["simulated"] is True
    assert snap["recommendations"][0]["action"] == "HOLD TRAIN"

    assert snap["forecasts"][0]["simulated"] is True
    assert snap["forecasts"][0]["forecast_type"] == "PREDICTED_BUNCHING"

    assert "_seen" not in snap["alerts"][0]  # internal field must not leak out


def test_inject_gap_scenario():
    st = DashboardState()
    st.inject_simulation("gap")
    snap = st.snapshot()
    assert snap["recommendations"][0]["action"] == "GAP FILL"
    assert snap["forecasts"][0]["forecast_type"] == "PREDICTED_GAP"
    assert snap["alerts"][0]["alert_type"] == "GAP"


def test_unknown_scenario_falls_back_to_bunching():
    st = DashboardState()
    st.inject_simulation("nonsense")
    assert st.snapshot()["alerts"][0]["alert_type"] == "BUNCHING"


def test_simulated_alerts_count_in_stats():
    st = DashboardState()
    st.inject_simulation("bunching")
    assert st.snapshot()["counts"]["alerts"] == 1


def test_clear_simulation_removes_records():
    st = DashboardState()
    st.inject_simulation("bunching")
    st.clear_simulation()
    snap = st.snapshot()
    assert snap["alerts"] == []
    assert snap["recommendations"] == []
    assert snap["forecasts"] == []


def test_simulation_ttl_expiry(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(state_mod, "_now", clock)
    st = DashboardState()
    st.inject_simulation("bunching")
    assert len(st.snapshot()["alerts"]) == 1
    clock.advance(state_mod.SIM_TTL_SEC + 1)
    snap = st.snapshot()
    assert snap["alerts"] == []
    assert snap["forecasts"] == []


def test_simulation_does_not_touch_live_records():
    st = DashboardState()
    st.add_alert({"route_id": "6", "alert_type": "GAP", "headway_seconds": 1000, "curr_trip": "L1"})
    st.inject_simulation("bunching")
    snap = st.snapshot()
    # one simulated (first) + one real, real one unlabeled
    assert len(snap["alerts"]) == 2
    assert snap["alerts"][0]["simulated"] is True
    assert snap["alerts"][1].get("simulated") is None
