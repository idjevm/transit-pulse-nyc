"""Interactive Claude agents for the MTA dashboard.

Three agents, all backed by the Anthropic API (Claude) as a side service — this is
separate from the in-Flink Bedrock model that powers the streaming dispatcher.
Each one is grounded in the LIVE dashboard snapshot (trains, buses, arrivals,
headway alerts, and the Flink dispatcher's own recommendations) so its answers
reflect the real system state, not a static prompt.

  - rider_advisor   : "what should I watch for going from X to Y right now?"
  - operator_insight: "how do we improve / what will get worse?" over the fleet
  - route_designer  : proposes a new bus route (waypoints + rationale) to draw

If ANTHROPIC_API_KEY is unset or the SDK errors, every entry point returns a
structured error the frontend can show instead of raising.
"""

from __future__ import annotations

import json
import logging
import os
from collections import Counter

log = logging.getLogger(__name__)

MODEL = os.environ.get("DISPATCHER_MODEL", "claude-haiku-4-5-20251001")
_MAX_LISTED = 40  # cap how many rows of each kind we feed the model


def _client():
    """Lazily build an Anthropic client. Returns None if unavailable."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None
    try:
        from anthropic import Anthropic

        return Anthropic(api_key=key)
    except Exception as exc:  # SDK missing / import failure
        log.warning("anthropic client unavailable: %s", exc)
        return None


# --------------------------------------------------------------------------- #
# Grounding: turn the live snapshot into a compact text context for the model.
# --------------------------------------------------------------------------- #

def _summarize_state(snap: dict) -> str:
    counts = snap.get("counts", {})
    trains = snap.get("trains", [])
    alerts = snap.get("alerts", [])
    recs = snap.get("recommendations", [])
    arrivals = snap.get("arrivals", [])

    subway_routes = Counter(
        t["route_short"] for t in trains if t.get("mode") == "subway" and t.get("route_short")
    )
    bus_routes = Counter(
        t["route_short"] for t in trains if t.get("mode") == "bus" and t.get("route_short")
    )

    lines = []
    lines.append(
        f"LIVE FLEET: {counts.get('trains', 0)} subway trains, "
        f"{counts.get('buses', 0)} buses, {counts.get('routes', 0)} active routes, "
        f"{counts.get('alerts', 0)} headway alerts. "
        f"data_live={snap.get('live')}"
    )
    if subway_routes:
        top = ", ".join(f"{r}({n})" for r, n in subway_routes.most_common(15))
        lines.append(f"SUBWAY LINES RUNNING: {top}")
    if bus_routes:
        top = ", ".join(f"{r}({n})" for r, n in bus_routes.most_common(20))
        lines.append(f"BUS ROUTES RUNNING: {top}")

    if alerts:
        lines.append("HEADWAY ALERTS (route/type/severity @ stop, headway secs):")
        for a in alerts[:_MAX_LISTED]:
            lines.append(
                f"  - {a.get('route_id')} {a.get('alert_type')} "
                f"[{a.get('severity')}] @ {a.get('stop_name') or a.get('stop_id')} "
                f"({a.get('headway_seconds')}s)"
            )

    if recs:
        lines.append("FLINK DISPATCHER RECOMMENDATIONS (already issued by the streaming agent):")
        for d in recs[:_MAX_LISTED]:
            lines.append(
                f"  - {d.get('route_id')}: {d.get('action')} — {d.get('dispatcher_note')}"
            )

    if arrivals:
        lines.append("NEXT ARRIVALS (route @ stop in N sec):")
        for a in arrivals[:_MAX_LISTED]:
            lines.append(
                f"  - {a.get('route_id')} @ {a.get('stop_name') or a.get('stop_id')} "
                f"in {a.get('eta_seconds')}s"
            )

    return "\n".join(lines)


def _ask(system: str, user: str, max_tokens: int = 900) -> tuple[str | None, str | None]:
    """Single-shot Claude call. Returns (text, error)."""
    client = _client()
    if client is None:
        return None, (
            "Claude is not configured. Set ANTHROPIC_API_KEY (and install the "
            "`anthropic` package) to enable the interactive agents."
        )
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
        return text.strip(), None
    except Exception as exc:
        log.exception("claude call failed")
        return None, f"Claude request failed: {exc}"


# --------------------------------------------------------------------------- #
# Agent 1 — Rider trip advisor
# --------------------------------------------------------------------------- #

RIDER_SYSTEM = (
    "You are the MTA Rider Advisor for a real-time NYC transit app. A rider tells "
    "you where they are going; you give short, practical, calm guidance for RIGHT "
    "NOW, grounded strictly in the live system state provided. Call out relevant "
    "bunching/gap alerts, which lines or bus routes touch their trip, and what to "
    "watch for (delays, long waits, crowding, alternatives). If the live data does "
    "not cover their route, say so plainly and give general NYC transit advice. "
    "Never invent alerts or delays that are not in the data. Be concise: a few "
    "short paragraphs or bullets, no preamble."
)


def rider_advisor(snap: dict, origin: str, destination: str, question: str = "") -> dict:
    ctx = _summarize_state(snap)
    parts = []
    if origin or destination:
        parts.append(f"Trip: from '{origin or '?'}' to '{destination or '?'}'.")
    if question:
        parts.append(f"Rider asks: {question}")
    parts.append("\nLIVE SYSTEM STATE:\n" + ctx)
    text, err = _ask(RIDER_SYSTEM, "\n".join(parts))
    if err:
        return {"ok": False, "error": err}
    return {"ok": True, "answer": text}


# --------------------------------------------------------------------------- #
# Agent 2 — Operator prediction / insight
# --------------------------------------------------------------------------- #

OPERATOR_SYSTEM = (
    "You are the MTA Operations Analyst for a real-time control center. You advise "
    "operators on how to improve service and what is likely to degrade next, using "
    "the live fleet state, active headway alerts, and the streaming dispatcher's "
    "recommendations. Identify the routes/corridors most at risk (bunching or "
    "gaps), predict where problems will spread if unaddressed, and recommend "
    "concrete operational actions (hold, short-turn, gap train, add/space service). "
    "Ground every claim in the provided data and prioritize by severity. Structure: "
    "1) Current risk summary, 2) Predicted next problems, 3) Recommended actions. "
    "Be specific and concise."
)


def operator_insight(snap: dict, question: str = "") -> dict:
    ctx = _summarize_state(snap)
    ask = question or (
        "Assess current service health, predict what will degrade next, and "
        "recommend the highest-impact operational actions."
    )
    user = f"Operator asks: {ask}\n\nLIVE SYSTEM STATE:\n{ctx}"
    text, err = _ask(OPERATOR_SYSTEM, user, max_tokens=1100)
    if err:
        return {"ok": False, "error": err}
    return {"ok": True, "answer": text}


# --------------------------------------------------------------------------- #
# Agent 3 — New bus route designer (stretch)
# --------------------------------------------------------------------------- #

ROUTE_DESIGNER_SYSTEM = (
    "You are a transit network planner for NYC. Given an origin and destination "
    "(and the live fleet state), propose ONE new or improved bus route that would "
    "serve the corridor efficiently. Consider where service is thin, where alerts "
    "show recurring bunching/gaps, and realistic NYC street geography (avenues and "
    "cross streets). Output STRICT JSON only, no markdown fences, matching:\n"
    '{"name": "<short route name>", '
    '"waypoints": [[lat, lon], ...],  // 4-10 points tracing the route through NYC, '
    'ordered origin->destination, using real approximate NYC coordinates '
    '(lat ~40.5-40.9, lon ~ -74.05 to -73.7), '
    '"stops": ["<major stop/landmark>", ...], '
    '"rationale": "<2-4 sentences: why this route, what demand/gaps it addresses>", '
    '"connections": ["<subway/bus lines it connects>", ...]}\n'
    "Return only that JSON object."
)


def route_designer(snap: dict, origin: str, destination: str, constraints: str = "") -> dict:
    ctx = _summarize_state(snap)
    parts = [f"Design a bus route from '{origin or '?'}' to '{destination or '?'}'."]
    if constraints:
        parts.append(f"Constraints/goals: {constraints}")
    parts.append("\nLIVE SYSTEM STATE (for demand/gap signals):\n" + ctx)
    text, err = _ask(ROUTE_DESIGNER_SYSTEM, "\n".join(parts), max_tokens=1100)
    if err:
        return {"ok": False, "error": err}

    proposal = _parse_route_json(text)
    if proposal is None:
        return {"ok": False, "error": "Could not parse a route proposal.", "raw": text}
    proposal["geojson"] = _waypoints_to_geojson(proposal)
    return {"ok": True, "proposal": proposal}


def _parse_route_json(text: str) -> dict | None:
    if not text:
        return None
    # tolerate accidental ```json fences
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`")
        t = t[t.find("{") : t.rfind("}") + 1]
    else:
        start, end = t.find("{"), t.rfind("}")
        if start == -1 or end == -1:
            return None
        t = t[start : end + 1]
    try:
        obj = json.loads(t)
    except ValueError:
        return None
    wp = obj.get("waypoints")
    if not isinstance(wp, list) or len(wp) < 2:
        return None
    clean = []
    for p in wp:
        try:
            lat, lon = float(p[0]), float(p[1])
        except (TypeError, ValueError, IndexError):
            continue
        if 40.3 <= lat <= 41.1 and -74.3 <= lon <= -73.6:
            clean.append([lat, lon])
    if len(clean) < 2:
        return None
    obj["waypoints"] = clean
    return obj


def _waypoints_to_geojson(proposal: dict) -> dict:
    # GeoJSON is [lon, lat]; waypoints are [lat, lon].
    coords = [[lon, lat] for lat, lon in proposal["waypoints"]]
    return {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": coords},
        "properties": {
            "name": proposal.get("name", "Proposed route"),
            "kind": "proposed_route",
        },
    }
