"""Interactive transit copilots for the MTA dashboard.

Three copilots, each grounded in the LIVE dashboard snapshot (trains, buses,
arrivals, headway alerts, and the Flink dispatcher's own recommendations) so its
answers reflect the real system state, not a static prompt. They use Google
Gemini when a Google API key (GEMINI_API_KEY / GOOGLEAI_API_KEY /
GOOGLE_API_KEY) is set, and fall back to Anthropic Claude otherwise; Gemini is
optional, not the hard default. This is a side service, separate from the
in-Flink model that powers the streaming dispatcher.

  - rider_advisor   : "what should I watch for going from X to Y right now?"
  - operator_insight: "how do we improve / what will get worse?" over the fleet
  - route_designer  : proposes a new bus route (waypoints + rationale) to draw

If no LLM is configured (no Google or Anthropic key) or the SDK errors, every
entry point returns a structured error the frontend can show instead of raising.
"""

from __future__ import annotations

import json
import logging
import os
from collections import Counter

log = logging.getLogger(__name__)

MODEL = os.environ.get("DISPATCHER_MODEL", "claude-haiku-4-5-20251001")
_MAX_LISTED = 40  # cap how many rows of each kind we feed the model

# Gemini's reasoning models (2.5-pro/flash) spend output tokens on internal
# thinking before writing a word, so a low cap yields empty/truncated text. Give
# every Gemini call generous headroom regardless of the per-agent cap, and steer
# a chunk of the budget away from thinking when the SDK supports it.
GEMINI_MIN_OUTPUT_TOKENS = 8192
GEMINI_THINKING_BUDGET = 2048


def _gemini_client():
    """Lazily build a Google Gemini client. Returns None if unavailable."""
    key = (
        os.environ.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLEAI_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
    )
    if not key:
        return None
    try:
        from google import genai
        return genai.Client(api_key=key)
    except Exception as exc:
        log.warning("gemini client unavailable: %s", exc)
        return None


def _anthropic_client():
    """Lazily build an Anthropic client. Returns None if unavailable."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None
    try:
        from anthropic import Anthropic
        return Anthropic(api_key=key)
    except Exception as exc:
        log.warning("anthropic client unavailable: %s", exc)
        return None


def _has_google_key() -> bool:
    return bool(
        os.environ.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLEAI_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
    )


def _gemini_model_id() -> str:
    return os.environ.get("GEMINI_MODEL_ID", "gemini-pro-latest")


def _claude_model_id() -> str:
    return os.environ.get("DISPATCHER_MODEL", "claude-haiku-4-5-20251001")


def _pretty_model(provider: str, model: str) -> str:
    """A short, human label for the model tag in the UI (e.g. 'Gemini 2.5 Pro')."""
    if not model:
        return "not configured"
    m = model.lower()
    if provider == "google":
        base = m.replace("models/", "").replace("gemini-", "").replace("-", " ").strip()
        return "Gemini " + " ".join(w.capitalize() for w in base.split())
    if provider == "anthropic":
        for fam in ("opus", "sonnet", "haiku"):
            if fam in m:
                nums = [t for t in m.replace("claude-", "").split("-") if t.isdigit() and len(t) <= 2]
                ver = ".".join(nums[:2])
                return f"Claude {fam.capitalize()} {ver}".strip()
        return "Claude"
    return model


def active_model() -> dict:
    """Report the provider + model the interactive agents will actually use, by
    the same precedence _ask() applies (Gemini if a Google key + SDK are present,
    else Claude). Lets the UI label itself truthfully instead of hardcoding a
    provider that may not be the one answering."""
    provider, _client, model = _resolve_backend()
    return {"provider": provider, "model": model, "label": _pretty_model(provider or "", model or "")}


def _resolve_backend() -> tuple[str | None, object | None, str | None]:
    """Pick the LLM backend: Gemini if a Google key and SDK are available, else
    Claude. Returns (provider, client, model) or (None, None, None)."""
    if _has_google_key():
        g = _gemini_client()
        if g is not None:
            return "google", g, _gemini_model_id()
    a = _anthropic_client()
    if a is not None:
        return "anthropic", a, _claude_model_id()
    return None, None, None


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


def _finish_reason(resp) -> str:
    try:
        fr = resp.candidates[0].finish_reason
        return getattr(fr, "name", None) or str(fr)
    except Exception:  # noqa: BLE001
        return ""


def _extract_gemini_text(resp) -> str:
    """resp.text is None when a reasoning model emits only thinking and no answer;
    fall back to walking the candidate parts so a partial answer still shows."""
    t = getattr(resp, "text", None)
    if t:
        return t.strip()
    out = []
    try:
        for cand in resp.candidates or []:
            for part in (cand.content.parts or []):
                if getattr(part, "text", None):
                    out.append(part.text)
    except Exception:  # noqa: BLE001
        pass
    return "".join(out).strip()


def _gemini_config(system: str, max_tokens: int):
    """Build a GenerateContentConfig with a generous output cap and, when the
    installed SDK supports it, a bounded thinking budget so tokens go to the
    answer instead of being spent entirely on hidden reasoning."""
    from google.genai import types
    eff_tokens = max(max_tokens, GEMINI_MIN_OUTPUT_TOKENS)
    kwargs = {"system_instruction": system, "max_output_tokens": eff_tokens}
    if hasattr(types, "ThinkingConfig"):
        try:
            return types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(thinking_budget=GEMINI_THINKING_BUDGET),
                **kwargs,
            )
        except (TypeError, ValueError):
            pass  # older SDK / model without a thinking budget — fall through
    return types.GenerateContentConfig(**kwargs)


def _ask(system: str, user: str, max_tokens: int = 1800) -> tuple[str | None, str | None, str | None]:
    """Single-shot LLM call. Uses Gemini if configured, else Anthropic. Returns
    (text, error, model_label) — model_label names whoever actually answered so
    the UI can label itself truthfully."""
    provider, client, model = _resolve_backend()
    label = _pretty_model(provider or "", model or "")

    if provider == "google":
        try:
            resp = client.models.generate_content(
                model=model,
                contents=user,
                config=_gemini_config(system, max_tokens),
            )
            text = _extract_gemini_text(resp)
            if not text:
                reason = _finish_reason(resp)
                if reason == "MAX_TOKENS":
                    return None, (
                        "The model used its entire output budget on reasoning before answering. "
                        "Try a lighter model (set GEMINI_MODEL_ID=gemini-2.0-flash) or ask a shorter question."
                    ), label
                return None, f"Model returned no text (finish_reason={reason or 'unknown'}).", label
            return text, None, label
        except Exception as exc:  # noqa: BLE001
            log.exception("gemini call failed")
            return None, f"Gemini request failed: {exc}", label

    if provider == "anthropic":
        try:
            msg = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
            text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text").strip()
            if not text:
                return None, "Model returned no text.", label
            return text, None, label
        except Exception as exc:  # noqa: BLE001
            log.exception("claude call failed")
            return None, f"Claude request failed: {exc}", label

    return None, (
        "AI model is not configured. Set GEMINI_API_KEY (or GOOGLEAI_API_KEY), or ANTHROPIC_API_KEY, "
        "in your environment / .env to enable the interactive transit agents."
    ), None


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
    text, err, model = _ask(RIDER_SYSTEM, "\n".join(parts))
    if err:
        return {"ok": False, "error": err, "model": model}
    return {"ok": True, "answer": text, "model": model}


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
    text, err, model = _ask(OPERATOR_SYSTEM, user, max_tokens=2200)
    if err:
        return {"ok": False, "error": err, "model": model}
    return {"ok": True, "answer": text, "model": model}


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
    text, err, model = _ask(ROUTE_DESIGNER_SYSTEM, "\n".join(parts), max_tokens=4096)
    if err:
        return {"ok": False, "error": err, "model": model}

    proposal = _parse_route_json(text)
    if proposal is None:
        return {"ok": False, "error": "Could not parse a route proposal.", "raw": text, "model": model}
    proposal["geojson"] = _waypoints_to_geojson(proposal)
    return {"ok": True, "proposal": proposal, "model": model}


def _parse_route_json(text: str) -> dict | None:
    if not text:
        return None
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        obj = json.loads(text[start : end + 1])
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
