"""MTA Bus Time (OneBusAway NYC) GTFS-Realtime feed URLs.

The vehicle-positions feed carries real GPS for every bus (position.latitude,
.longitude, .bearing) — which is what lets the map draw ~2.7k buses at their true
location and rotate the icon to heading (the high-precision live view), unlike the
subway which snaps to station coordinates.

A `?key=...` may be appended (free from https://register.developer.obanyc.com) but
the public feed currently serves data without one. Docs:
https://bustime.mta.info/wiki/Developers/GTFSRt
"""

# System-wide GTFS-RT endpoints (all NYC boroughs + MTA Bus Company in one feed).
BUS_VEHICLE_POSITIONS = "https://gtfsrt.prod.obanyc.com/vehiclePositions"
BUS_TRIP_UPDATES = "https://gtfsrt.prod.obanyc.com/tripUpdates"
BUS_ALERTS = "https://gtfsrt.prod.obanyc.com/alerts"


def vehicle_positions_url(api_key: str = "") -> str:
    """The key is optional at the HTTP layer today; append it when provided."""
    return f"{BUS_VEHICLE_POSITIONS}?key={api_key}" if api_key else BUS_VEHICLE_POSITIONS


def route_short(route_id: str) -> str:
    """'MTA NYCT_M15' / 'MTABC_Q39' -> 'M15' / 'Q39'.

    Bus route_ids are namespaced by agency; riders know the short name only.
    """
    if not route_id:
        return ""
    return route_id.split("_", 1)[-1].strip()


def agency_of(vehicle_id: str) -> str:
    """'MTA NYCT_9771' -> 'MTA NYCT', 'MTABC_1234' -> 'MTABC'.

    In the OneBusAway NYC feed the agency prefix is on the vehicle id; the
    trip's route_id is already the rider-facing short name (e.g. 'M15').
    """
    if not vehicle_id or "_" not in vehicle_id:
        return ""
    return vehicle_id.split("_", 1)[0].strip()


def agency_selected(vehicle_id: str, selection: str) -> bool:
    """True if this bus's agency passes the BUS_AGENCIES filter ('all' = any)."""
    if not selection or selection.strip().lower() == "all":
        return True
    wanted = {a.strip().upper() for a in selection.split(",") if a.strip()}
    return agency_of(vehicle_id).upper() in wanted
