"""Tests for arrival estimates ingestion, diverse route selection, and ETA calculations."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import state as state_mod
from dashboard.state import DashboardState


class _Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, secs):
        self.t += secs


def _arrival(route="1", stop="101N", stop_name="South Ferry", trip="T1", eta=60, epoch=None):
    return {
        "route_id": route,
        "direction": "N",
        "stop_id": stop,
        "stop_name": stop_name,
        "trip_id": trip,
        "eta_seconds": eta,
        "arrival_epoch": epoch,
    }


def test_update_arrival_ingestion_and_snapshot(monkeypatch):
    clock = _Clock(1_000_000.0)
    monkeypatch.setattr(state_mod, "_now", clock)

    st = DashboardState()
    # Ingest arrivals across multiple routes with diverse ETAs
    st.update_arrival(_arrival(route="1", stop="101", stop_name="South Ferry", trip="T1-1", eta=10))
    st.update_arrival(_arrival(route="1", stop="102", stop_name="Rector St", trip="T1-1", eta=90))
    st.update_arrival(_arrival(route="A", stop="A01", stop_name="Inwood", trip="TA-1", eta=25))
    st.update_arrival(_arrival(route="A", stop="A02", stop_name="190 St", trip="TA-1", eta=180))
    st.update_arrival(_arrival(route="7", stop="701", stop_name="Flushing", trip="T7-1", eta=300))

    snap = st.snapshot()
    arrivals = snap["arrivals"]
    assert len(arrivals) == 5

    # Check ETAs are sorted ascending
    etas = [a["eta_seconds"] for a in arrivals]
    assert etas == sorted(etas)

    # Routes are represented
    routes = {a["route_id"] for a in arrivals}
    assert routes == {"1", "A", "7"}


def test_arrival_diverse_route_selection(monkeypatch):
    clock = _Clock(1_000_000.0)
    monkeypatch.setattr(state_mod, "_now", clock)

    st = DashboardState()
    # Ingest 15 arrivals for Route 1 all at low ETAs
    for i in range(15):
        st.update_arrival(_arrival(route="1", stop=f"1{i:02d}", trip=f"T1-{i}", eta=i * 2))

    # And 1 arrival each for routes 2, 3, 4, 5, 7, A, B, C, D, E, F, G, J, L, M, N, Q, R with higher ETAs
    other_routes = ["2", "3", "4", "5", "7", "A", "B", "C", "D", "E", "F", "G", "J", "L", "M", "N", "Q", "R"]
    for i, r in enumerate(other_routes):
        st.update_arrival(_arrival(route=r, stop=f"{r}01", trip=f"T{r}-1", eta=100 + i * 30))

    snap = st.snapshot()
    arrivals = snap["arrivals"]

    # Route 1 should not crowd out all other routes (capped at 6)
    r1_count = sum(1 for a in arrivals if a["route_id"] == "1")
    assert r1_count <= 6

    # Multiple distinct routes are present in snapshot
    distinct_routes = {a["route_id"] for a in arrivals}
    assert len(distinct_routes) > 10


def test_arrival_ttl_eviction(monkeypatch):
    clock = _Clock(1_000_000.0)
    monkeypatch.setattr(state_mod, "_now", clock)

    st = DashboardState()
    st.update_arrival(_arrival(route="1", stop="101", trip="T1", eta=50))
    assert len(st.snapshot()["arrivals"]) == 1

    # Advance beyond ARRIVAL_TTL_SEC (120s)
    clock.advance(130)
    assert len(st.snapshot()["arrivals"]) == 0
