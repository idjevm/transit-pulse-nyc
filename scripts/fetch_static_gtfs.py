"""Download MTA static GTFS and extract stops.txt to data/stops.txt.

stops.txt maps stop_id -> stop_name, stop_lat, stop_lon, which the producer uses
to enrich records so the dashboard map can plot trains and show station names.
Run: python scripts/fetch_static_gtfs.py
"""

import io
import os
import sys
import zipfile

import requests

GTFS_URL = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
OUT_FILE = os.path.join(OUT_DIR, "stops.txt")


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    print(f"Downloading {GTFS_URL} ...")
    try:
        resp = requests.get(GTFS_URL, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        sys.exit(1)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        if "stops.txt" not in zf.namelist():
            print("stops.txt not found in archive", file=sys.stderr)
            sys.exit(1)
        with zf.open("stops.txt") as src, open(OUT_FILE, "wb") as dst:
            dst.write(src.read())

    line_count = sum(1 for _ in open(OUT_FILE, encoding="utf-8-sig")) - 1
    print(f"Wrote {OUT_FILE} ({line_count} stops).")


if __name__ == "__main__":
    main()
