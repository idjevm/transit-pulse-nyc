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

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))


if __name__ == "__main__":
    main()
