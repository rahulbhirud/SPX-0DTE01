"""
dashboard.py
────────────
Simple web dashboard for SPX streamer.
Displays current SPX price, latest RSI, and a scrollable reversal signal log.

Reads from dashboard_state.json written by spx_stream.py.

Usage:
    python dashboard.py              # http://localhost:5050
    python dashboard.py --port 8080  # custom port
"""

import argparse
import json
import logging
import os
import threading
import webbrowser

from flask import Flask, jsonify, render_template

app = Flask(__name__, template_folder="templates")

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_state.json")


def _read_state() -> dict:
    """Read the latest state from the JSON file written by spx_stream.py."""
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            "price": "—",
            "rsi": None,
            "rsi_period": 9,
            "timestamp": "",
            "updated_at": "Waiting for data…",
            "signals": [],
        }


@app.route("/")
def index():
    state = _read_state()
    return render_template("dashboard.html", state=state)


@app.route("/api/state")
def api_state():
    """JSON endpoint for AJAX polling."""
    return jsonify(_read_state())


def main():
    parser = argparse.ArgumentParser(description="SPX Dashboard")
    parser.add_argument("--port", type=int, default=5050)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()

    url = f"http://localhost:{args.port}"
    print(f"Dashboard running at {url}")

    # Auto-open browser after a short delay to let Flask start
    threading.Timer(1.5, lambda: webbrowser.open(url)).start()

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
