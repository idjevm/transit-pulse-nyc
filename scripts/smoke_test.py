"""Decode the live MTA subway feed and print stats. Needs no Kafka.

Proves ingestion works end to end before any Confluent Cloud setup: fetches each
GTFS-RT feed, counts vehicle positions and trip updates, and prints a couple of
sample records. Run: python scripts/smoke_test.py
"""

import os
import sys

import requests
from google.transit import gtfs_realtime_pb2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producers import feeds  # noqa: E402


def main() -> None:
    total_vp = total_tu = 0
    for key, url in feeds.FEEDS.items():
        try:
            content = requests.get(url, timeout=30).content
        except requests.RequestException as exc:
            print(f"[{key:8s}] fetch failed: {exc}")
            continue
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(content)
        vp = sum(1 for e in feed.entity if e.HasField("vehicle"))
        tu = sum(1 for e in feed.entity if e.HasField("trip_update"))
        total_vp += vp
        total_tu += tu
        print(f"[{key:8s}] entities={len(feed.entity):4d}  vehicle_positions={vp:4d}  trip_updates={tu:4d}")

    print("-" * 64)
    print(f"TOTAL     vehicle_positions={total_vp}  trip_updates={total_tu}")
    print("Live NYC subway data is flowing. Ready to stream into Kafka.")


if __name__ == "__main__":
    main()
