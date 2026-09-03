"""Static GTFS stop lookup: stop_id (or base id) -> name, lat, lon.

The GTFS-Realtime feed carries stop_ids like "221N" but no names or coordinates.
Static GTFS stops.txt provides those. Run scripts/fetch_static_gtfs.py to download
it to data/stops.txt; this module loads it lazily. If the file is absent, lookups
return empty name and 0.0 coordinates (the pipeline and dashboard still work; the
map just has fewer plotted trains).
"""

import csv
import os

_DATA = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "stops.txt")

_STOPS: dict[str, tuple[str, float, float]] = {}
_LOADED = False


def _load() -> None:
    global _LOADED
    _LOADED = True
    if not os.path.exists(_DATA):
        return
    with open(_DATA, encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            sid = row.get("stop_id", "").strip()
            if not sid:
                continue
            try:
                lat = float(row.get("stop_lat") or 0.0)
                lon = float(row.get("stop_lon") or 0.0)
            except ValueError:
                lat = lon = 0.0
            _STOPS[sid] = (row.get("stop_name", "").strip(), lat, lon)


def base_id(stop_id: str) -> str:
    """Strip the N/S direction suffix: '221N' -> '221'."""
    if stop_id and stop_id[-1] in ("N", "S"):
        return stop_id[:-1]
    return stop_id


def lookup(stop_id: str) -> tuple[str, float, float]:
    """Return (stop_name, lat, lon) for a GTFS-RT stop_id, or ('', 0.0, 0.0)."""
    if not _LOADED:
        _load()
    if stop_id in _STOPS:
        return _STOPS[stop_id]
    b = base_id(stop_id)
    if b in _STOPS:
        return _STOPS[b]
    return ("", 0.0, 0.0)
