"""MTA subway GTFS-Realtime feed URLs.

No API key is required for the NYC subway real-time feeds (as of 2023). Each URL
returns a GTFS-RT FeedMessage protobuf covering a group of lines.
"""

FEEDS = {
    "123456S": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "ACE":     "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "BDFM":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "G":       "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "JZ":      "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "NQRW":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "L":       "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "SIR":     "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
}


def resolve(selection: str) -> dict[str, str]:
    """Resolve a comma-separated selection (or 'all') to {key: url}."""
    if not selection or selection.strip().lower() == "all":
        return dict(FEEDS)
    keys = [k.strip() for k in selection.split(",") if k.strip()]
    chosen = {k: FEEDS[k] for k in keys if k in FEEDS}
    return chosen or dict(FEEDS)
