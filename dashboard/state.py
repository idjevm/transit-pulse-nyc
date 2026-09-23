"""Thread-safe in-memory snapshot of the subway for the dashboard.

The Kafka consumer thread writes records in; the websocket reads snapshots out.
All access is guarded by a single lock. On each snapshot, entries whose last
update is older than their TTL are both excluded from the output AND deleted
from the backing dicts, so a train that stops reporting drops off the map and
its memory is reclaimed instead of growing unbounded over a service day.
"""

from __future__ import annotations

import threading
import time
from collections import deque

TRAIN_TTL_SEC = 180          # drop a train not seen in this long
ARRIVAL_TTL_SEC = 120        # drop a prediction not refreshed in this long
FORECAST_TTL_SEC = 150       # drop a headway forecast not refreshed in this long
SIM_TTL_SEC = 180            # judge-triggered simulated items auto-expire after this
MAX_ALERTS = 60
MAX_RECS = 60
MAX_TRAINS_OUT = 1500       # subway (~700) + a healthy slice of the ~2.7k buses
MAX_ARRIVALS_OUT = 40
MAX_FORECASTS_OUT = 40


def _now() -> float:
    return time.time()


# Judge-triggered demo disruptions. Each injects a clearly SIMULATED alert +
# dispatcher recommendation + predictive forecast at a real NYC stop (with real
# coordinates so the map lights up), letting a presenter trigger the full pipeline
# view on demand instead of waiting for live conditions to produce one. These are
# labeled simulated=True end to end and never mix into the real Kafka-fed records.
_SIM_SCENARIOS = {
    "bunching": {
        "label": "N/Q/R bunching at Times Sq-42 St",
        "route_id": "N", "direction": "S", "stop_id": "R16",
        "stop_name": "Times Sq-42 St", "stop_lat": 40.7557, "stop_lon": -73.9870,
        "alert_type": "BUNCHING", "severity": "high",
        "headway_seconds": 72, "predicted_headway": 55,
        "action": "HOLD TRAIN",
        "dispatcher_note": "Two southbound N trains are 72s apart and closing; hold the "
                           "trailing train ~90s at 49 St to restore spacing.",
        "rider_message": "Heads up: N/Q/R trains are bunching at Times Sq. The next one is "
                         "crowded and close behind another; a short wait gets you a roomier ride.",
    },
    "gap": {
        "label": "A train service gap at 125 St",
        "route_id": "A", "direction": "N", "stop_id": "A15",
        "stop_name": "125 St", "stop_lat": 40.8110, "stop_lon": -73.9526,
        "alert_type": "GAP", "severity": "high",
        "headway_seconds": 1320, "predicted_headway": 1500,
        "action": "GAP FILL",
        "dispatcher_note": "22-minute gap opening northbound on the A at 125 St; put the "
                           "next available train in service or short-turn to fill it.",
        "rider_message": "The next uptown A is running about 22 minutes out. If you can, the "
                         "C or the 2/3 nearby may get you moving sooner.",
    },
}


class DashboardState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._trains: dict[str, dict] = {}                 # trip_id -> record
        self._arrivals: dict[tuple[str, str], dict] = {}   # (trip_id, stop_id) -> record
        self._forecasts: dict[tuple[str, str, str], dict] = {}  # (route,dir,stop) -> record
        self._alerts: deque[dict] = deque(maxlen=MAX_ALERTS)
        self._recs: deque[dict] = deque(maxlen=MAX_RECS)
        self._sim: dict[str, list] = {"alerts": [], "recs": [], "forecasts": []}
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

    def update_forecast(self, f: dict) -> None:
        """A predictive headway forecast (flink/08). Keyed by (route, dir, stop) so
        the newest forecast for a stop replaces the prior one. Only actionable
        predictions are kept; STABLE rows carry no signal to surface."""
        route_id = f.get("route_id")
        stop_id = f.get("stop_id")
        forecast_type = f.get("forecast_type", "")
        if not route_id or not stop_id:
            return
        if forecast_type not in ("PREDICTED_BUNCHING", "PREDICTED_GAP"):
            return
        with self._lock:
            self._forecasts[(route_id, f.get("direction", ""), stop_id)] = {
                "route_id": route_id,
                "direction": f.get("direction", ""),
                "stop_id": stop_id,
                "stop_name": f.get("stop_name", ""),
                "lat": f.get("stop_lat", 0.0),
                "lon": f.get("stop_lon", 0.0),
                "curr_trip": f.get("curr_trip", ""),
                "forecast_type": forecast_type,
                "headway_seconds": int(f.get("headway_seconds") or 0),
                "predicted_headway": int(f.get("predicted_headway") or 0),
                "_seen": _now(),
            }
            self._last_record_ts = _now()

    # ---- judge-triggered simulation (demo only) ----
    def inject_simulation(self, scenario: str) -> dict:
        """Inject a clearly-labeled SIMULATED disruption (alert + recommendation +
        forecast) so a presenter can light up the full pipeline on demand. Items
        auto-expire after SIM_TTL_SEC and never touch the live Kafka-fed records."""
        scen = (scenario or "bunching").strip().lower()
        spec = _SIM_SCENARIOS.get(scen, _SIM_SCENARIOS["bunching"])
        now = _now()
        ts = int(now * 1000)
        curr_trip = f"SIM-{scen}-{ts}"
        alert = {
            "route_id": spec["route_id"], "direction": spec["direction"],
            "stop_id": spec["stop_id"], "stop_name": spec["stop_name"],
            "lat": spec["stop_lat"], "lon": spec["stop_lon"],
            "alert_type": spec["alert_type"], "severity": spec["severity"],
            "headway_seconds": spec["headway_seconds"],
            "prev_trip": f"{curr_trip}-prev", "curr_trip": curr_trip,
            "simulated": True, "ts": ts, "_seen": now,
        }
        rec = {
            "route_id": spec["route_id"], "direction": spec["direction"],
            "stop_id": spec["stop_id"], "stop_name": spec["stop_name"],
            "alert_type": spec["alert_type"], "action": spec["action"],
            "dispatcher_note": spec["dispatcher_note"], "rider_message": spec["rider_message"],
            "simulated": True, "ts": ts, "_seen": now,
        }
        forecast = {
            "route_id": spec["route_id"], "direction": spec["direction"],
            "stop_id": spec["stop_id"], "stop_name": spec["stop_name"],
            "lat": spec["stop_lat"], "lon": spec["stop_lon"], "curr_trip": curr_trip,
            "forecast_type": "PREDICTED_BUNCHING" if spec["alert_type"] == "BUNCHING" else "PREDICTED_GAP",
            "headway_seconds": spec["headway_seconds"], "predicted_headway": spec["predicted_headway"],
            "simulated": True, "_seen": now,
        }
        with self._lock:
            self._sim["alerts"].append(alert)
            self._sim["recs"].append(rec)
            self._sim["forecasts"].append(forecast)
        return {"scenario": scen, "label": spec["label"]}

    def clear_simulation(self) -> None:
        with self._lock:
            self._sim = {"alerts": [], "recs": [], "forecasts": []}

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
            # Evict expired entries from the backing dicts so they don't grow
            # unbounded over a service day; TTL is by last-seen, independent of
            # the eta-window filtering applied to the output above.
            for k in [k for k, v in self._trains.items() if now - v["_seen"] > TRAIN_TTL_SEC]:
                del self._trains[k]
            for k in [k for k, a in self._arrivals.items() if now - a["_seen"] > ARRIVAL_TTL_SEC]:
                del self._arrivals[k]
            forecasts = [
                {k: f[k] for k in ("route_id", "direction", "stop_id", "stop_name",
                                   "lat", "lon", "curr_trip", "forecast_type",
                                   "headway_seconds", "predicted_headway")}
                for f in self._forecasts.values()
                if now - f["_seen"] <= FORECAST_TTL_SEC
            ]
            for k in [k for k, f in self._forecasts.items() if now - f["_seen"] > FORECAST_TTL_SEC]:
                del self._forecasts[k]

            # Judge-triggered simulation: expire, then surface first (newest on top).
            for kind in ("alerts", "recs", "forecasts"):
                self._sim[kind] = [x for x in self._sim[kind] if now - x["_seen"] <= SIM_TTL_SEC]
            sim = {kind: [{k: v for k, v in x.items() if k != "_seen"} for x in self._sim[kind]]
                   for kind in ("alerts", "recs", "forecasts")}

            alerts = sim["alerts"] + list(self._alerts)[::-1]
            recs = sim["recs"] + list(self._recs)[::-1]
            forecasts = sim["forecasts"] + forecasts
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
            "forecasts": forecasts[:MAX_FORECASTS_OUT],
        }
