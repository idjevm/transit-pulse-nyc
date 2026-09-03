"""Build route-line geometry (GeoJSON) from MTA static GTFS.

high-precision maps draw the actual route SHAPE as a colored line, then move
vehicle icons along it. GTFS-Realtime has no geometry, so we derive it once from
static GTFS shapes.txt + trips.txt + routes.txt and serve it to the map.

For each route we keep its longest shape (the trunk of the line) so the map stays
readable instead of drawing every branch/variant. Output is a GeoJSON
FeatureCollection of LineStrings, one per route, with {route_id, route_short,
mode, color} properties.

  python scripts/build_shapes.py            # subway (default)

Writes data/subway_shapes.geojson . The dashboard's /api/shapes endpoint serves
every data/*_shapes.geojson it finds.
"""

import csv
import io
import json
import os
import sys
import zipfile
from collections import defaultdict

import requests

SUBWAY_GTFS = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


def _read_csv(zf: zipfile.ZipFile, name: str):
    with zf.open(name) as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8-sig")
        yield from csv.DictReader(text)


def build(url: str, mode: str, out_name: str) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"Downloading {url} ...")
    resp = requests.get(url, timeout=180)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        names = set(zf.namelist())
        for req in ("shapes.txt", "trips.txt", "routes.txt"):
            if req not in names:
                print(f"{req} not in archive", file=sys.stderr)
                sys.exit(1)

        route_color = {}
        for r in _read_csv(zf, "routes.txt"):
            rid = (r.get("route_id") or "").strip()
            color = (r.get("route_color") or "").strip()
            if rid:
                route_color[rid] = f"#{color}" if color else "#888888"

        shape_route = {}
        for t in _read_csv(zf, "trips.txt"):
            sid = (t.get("shape_id") or "").strip()
            rid = (t.get("route_id") or "").strip()
            if sid and rid:
                shape_route.setdefault(sid, rid)

        pts = defaultdict(list)  # shape_id -> [(seq, lat, lon)]
        for s in _read_csv(zf, "shapes.txt"):
            sid = (s.get("shape_id") or "").strip()
            if not sid:
                continue
            try:
                seq = int(s.get("shape_pt_sequence") or 0)
                lat = float(s.get("shape_pt_lat"))
                lon = float(s.get("shape_pt_lon"))
            except (TypeError, ValueError):
                continue
            pts[sid].append((seq, lat, lon))

    # Keep the longest shape per route (the line's trunk).
    longest: dict[str, str] = {}
    for sid, pt in pts.items():
        rid = shape_route.get(sid)
        if not rid:
            continue
        if rid not in longest or len(pt) > len(pts[longest[rid]]):
            longest[rid] = sid

    features = []
    for rid, sid in sorted(longest.items()):
        coords = [[lon, lat] for _, lat, lon in sorted(pts[sid])]
        if len(coords) < 2:
            continue
        features.append({
            "type": "Feature",
            "properties": {
                "route_id": rid,
                "route_short": rid,
                "mode": mode,
                "color": route_color.get(rid, "#888888"),
            },
            "geometry": {"type": "LineString", "coordinates": coords},
        })

    out_path = os.path.join(OUT_DIR, out_name)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump({"type": "FeatureCollection", "features": features}, fh)
    print(f"Wrote {out_path} ({len(features)} route lines).")


def main() -> None:
    build(SUBWAY_GTFS, "subway", "subway_shapes.geojson")


if __name__ == "__main__":
    main()
