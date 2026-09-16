"""Dashboard entrypoint.

Run with:  uvicorn dashboard.app:app --port 8000
Then open http://localhost:8000
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard.server import create_app  # noqa: E402

app = create_app()


def main() -> None:
    import uvicorn

    # The /api/* copilot endpoints are unauthenticated and hit paid LLM APIs;
    # bind to loopback by default so they aren't exposed on all interfaces.
    # Intended for local use; override HOST only behind your own access control.
    host = os.environ.get("HOST", "127.0.0.1")
    uvicorn.run(app, host=host, port=int(os.environ.get("PORT", "8000")))


if __name__ == "__main__":
    main()
