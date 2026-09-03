"""Thread-safe in-memory snapshot of the subway for the dashboard.

The Kafka consumer thread writes records in; the websocket reads snapshots out.
All access is guarded by a single lock. Stale trains/arrivals are pruned on
snapshot so a train that stops reporting drops off the map instead of lingering.
"""

from __future__ import annotations

import threading
import time
from collections import deque

TRAIN_TTL_SEC = 180          # drop a train not seen in this long
ARRIVAL_TTL_SEC = 120        # drop a prediction not refreshed in this long
MAX_ALERTS = 60
MAX_RECS = 60
MAX_TRAINS_OUT = 1500       # subway (~700) + a healthy slice of the ~2.7k buses
MAX_ARRIVALS_OUT = 40


def _now() -> float:
    return time.time()


class DashboardState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._trains: dict[str, dict] = {}                 # trip_id -> record
        self._arrivals: dict[tuple[str, str], dict] = {}   # (trip_id, stop_id) -> record
        self._alerts: deque[dict] = deque(maxlen=MAX_ALERTS)
        self._recs: deque[dict] = deque(maxlen=MAX_RECS)
        self._connection_error: str | None = None
        self._last_record_ts: float = 0.0

    # ---- writers (consumer thread) ----
    def update_vehicle(self, v: dict) -> None:
        trip_id = v.get("trip_id")
        if not trip_id:
            return
        with self._lock:
            self._trains[f"subway:{trip_id}"] = {
                "trip_id": trip_id,
                "mode": "subway",
                "route_id": v.get("route_id", ""),
                "route_short": v.get("route_id", ""),
                "direction": v.get("direction", ""),
                "stop_id": v.get("stop_id", ""),
                "stop_name": v.get("stop_name", ""),
                "lat": v.get("stop_lat", 0.0),
                "lon": v.get("stop_lon", 0.0),
                "bearing": -1.0,
                "status": v.get("current_status", ""),
                "_seen": _now(),
            }
            self._last_record_ts = _now()

    def update_bus(self, v: dict) -> None:
        trip_id = v.get("trip_id")
        if not trip_id:
            return
        with self._lock:
            self._trains[f"bus:{trip_id}"] = {
                "trip_id": trip_id,
                "mode": "bus",
                "route_id": v.get("route_id", ""),
                "route_short": v.get("route_short") or v.get("route_id", ""),
                "direction": v.get("direction", ""),
                "stop_id": v.get("stop_id", ""),
                "stop_name": "",
                "lat": v.get("vehicle_lat", 0.0),
                "lon": v.get("vehicle_lon", 0.0),
                "bearing": float(v.get("bearing") if v.get("bearing") is not None else -1.0),
                "status": v.get("current_status", ""),
                "_seen": _now(),
            }
            self._last_record_ts = _now()

    def update_arrival(self, a: dict) -> None:
        trip_id = a.get("trip_id")
        stop_id = a.get("stop_id")
        if not trip_id or not stop_id:
            return
        with self._lock:
            self._arrivals[(trip_id, stop_id)] = {
                "route_id": a.get("route_id", ""),
                "direction": a.get("direction", ""),
                "stop_id": stop_id,
                "stop_name": a.get("stop_name", ""),
                "trip_id": trip_id,
                "arrival_epoch": int(a.get("arrival_epoch") or 0),
                "_seen": _now(),
            }
            self._last_record_ts = _now()

    def add_alert(self, a: dict) -> None:
        with self._lock:
            self._alerts.append({
                "route_id": a.get("route_id", ""),
                "direction": a.get("direction", ""),
                "stop_id": a.get("stop_id", ""),
                "stop_name": a.get("stop_name", ""),
                "alert_type": a.get("alert_type", ""),
                "severity": a.get("severity", ""),
                "headway_seconds": int(a.get("headway_seconds") or 0),
                "prev_trip": a.get("prev_trip", ""),
                "curr_trip": a.get("curr_trip", ""),
                "ts": int(_now() * 1000),
            })
            self._last_record_ts = _now()

    def add_recommendation(self, d: dict) -> None:
        with self._lock:
            self._recs.append({
                "route_id": d.get("route_id", ""),
                "direction": d.get("direction", ""),
                "stop_id": d.get("stop_id", ""),
                "stop_name": d.get("stop_name", ""),
                "alert_type": d.get("alert_type", ""),
                "action": d.get("action", ""),
                "dispatcher_note": d.get("dispatcher_note", ""),
                "rider_message": d.get("rider_message", ""),
                "ts": int(_now() * 1000),
            })
            self._last_record_ts = _now()

    # ---- error tracking ----
    def record_error(self, code: str, detail: str) -> None:
        with self._lock:
            self._connection_error = f"{code}: {detail}"

    def clear_error(self) -> None:
        with self._lock:
            self._connection_error = None

    # ---- reader (websocket) ----
    def snapshot(self) -> dict:
        now = _now()
        with self._lock:
            trains = [
                {k: v[k] for k in ("trip_id", "mode", "route_id", "route_short", "direction",
                                   "stop_id", "stop_name", "lat", "lon", "bearing", "status")}
                for v in self._trains.values()
                if now - v["_seen"] <= TRAIN_TTL_SEC
            ]
            arrivals = []
            for a in self._arrivals.values():
                if now - a["_seen"] > ARRIVAL_TTL_SEC:
                    continue
                eta = a["arrival_epoch"] - int(now)
                if eta < -30 or eta > 1800:
                    continue
                arrivals.append({
                    "route_id": a["route_id"], "direction": a["direction"],
                    "stop_id": a["stop_id"], "stop_name": a["stop_name"],
                    "trip_id": a["trip_id"], "eta_seconds": eta,
                })
            arrivals.sort(key=lambda x: x["eta_seconds"])
            alerts = list(self._alerts)[::-1]
            recs = list(self._recs)[::-1]
            n_subway = sum(1 for t in trains if t["mode"] == "subway")
            n_bus = sum(1 for t in trains if t["mode"] == "bus")
            routes_live = len({t["route_short"] for t in trains if t["route_short"]})
            live = (now - self._last_record_ts) < 30 if self._last_record_ts else False
            connection_error = self._connection_error

        return {
            "live": live,
            "connection_error": connection_error,
            "updated_ts": int(now * 1000),
            "counts": {
                "trains": n_subway,
                "buses": n_bus,
                "routes": routes_live,
                "alerts": len(alerts),
            },
            "trains": trains[:MAX_TRAINS_OUT],
            "arrivals": arrivals[:MAX_ARRIVALS_OUT],
            "alerts": alerts,
            "recommendations": recs,
        }
