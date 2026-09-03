"""FastAPI app for the MTA dashboard.

Serves the static frontend and pushes DashboardState.snapshot() over a websocket
at ~4 Hz. A background thread runs the Kafka consumer for the app's lifetime.
Mirrors the F1 pitwall server (no-cache static, /healthz, ws snapshot loop).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from dashboard import agents
from dashboard.consumer import run_consumer
from dashboard.state import DashboardState

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

STATIC_DIR = Path(__file__).parent / "static"
DATA_DIR = Path(__file__).parent.parent / "data"
PUSH_INTERVAL_SEC = 0.25
NO_CACHE = {"Cache-Control": "no-cache, no-store, must-revalidate"}


def _load_shapes() -> dict:
    """Merge every data/*_shapes.geojson into one FeatureCollection.

    Empty (no geometry built yet) is fine — the map just draws no route lines.
    """
    features: list = []
    if DATA_DIR.exists():
        for path in sorted(DATA_DIR.glob("*_shapes.geojson")):
            try:
                fc = json.loads(path.read_text(encoding="utf-8"))
                features.extend(fc.get("features", []))
            except (ValueError, OSError):
                continue
    return {"type": "FeatureCollection", "features": features}


class NoCacheStaticFiles(StaticFiles):
    def is_not_modified(self, response_headers, request_headers) -> bool:
        return False

    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        response.headers.update(NO_CACHE)
        return response


def create_app() -> FastAPI:
    state = DashboardState()
    stop = threading.Event()
    shapes = _load_shapes()  # built once at startup; served to the map
    app = FastAPI(title="Transit Pulse NYC")

    @app.on_event("startup")
    async def _startup() -> None:
        thread = threading.Thread(target=run_consumer, args=(state, stop), daemon=True)
        thread.start()
        app.state.consumer_thread = thread

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        stop.set()

    app.mount("/static", NoCacheStaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html", headers=NO_CACHE)

    @app.get("/api/shapes")
    async def api_shapes() -> JSONResponse:
        return JSONResponse(shapes, headers={"Cache-Control": "public, max-age=3600"})

    @app.post("/api/advisor")
    async def api_advisor(req: Request) -> JSONResponse:
        body = await req.json()
        result = agents.rider_advisor(
            state.snapshot(),
            origin=(body.get("origin") or "").strip(),
            destination=(body.get("destination") or "").strip(),
            question=(body.get("question") or "").strip(),
        )
        return JSONResponse(result, headers=NO_CACHE)

    @app.post("/api/operator")
    async def api_operator(req: Request) -> JSONResponse:
        body = await req.json()
        result = agents.operator_insight(
            state.snapshot(),
            question=(body.get("question") or "").strip(),
        )
        return JSONResponse(result, headers=NO_CACHE)

    @app.post("/api/route-designer")
    async def api_route_designer(req: Request) -> JSONResponse:
        body = await req.json()
        result = agents.route_designer(
            state.snapshot(),
            origin=(body.get("origin") or "").strip(),
            destination=(body.get("destination") or "").strip(),
            constraints=(body.get("constraints") or "").strip(),
        )
        return JSONResponse(result, headers=NO_CACHE)

    @app.get("/healthz")
    async def healthz() -> dict:
        snap = state.snapshot()
        return {
            "status": "ok",
            "live": snap["live"],
            "counts": snap["counts"],
            "connection_error": snap["connection_error"],
        }

    @app.websocket("/ws")
    async def ws(socket: WebSocket) -> None:
        await socket.accept()
        try:
            while True:
                payload = json.dumps(state.snapshot(), separators=(",", ":"), default=str)
                await socket.send_text(payload)
                await asyncio.sleep(PUSH_INTERVAL_SEC)
        except WebSocketDisconnect:
            pass
        except (RuntimeError, ConnectionError):
            with contextlib.suppress(Exception):
                await socket.close()

    return app
