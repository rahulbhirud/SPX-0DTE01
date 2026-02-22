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
import sys
import threading
import webbrowser

from flask import Flask, jsonify, render_template, request

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


# ── Trade execution endpoints ─────────────────────────────────

def _get_trader():
    """Lazily build an OptionsTrader using the same Config/TokenManager
    that the streamer uses.  Cached on the Flask app object."""
    if not hasattr(app, "_trader"):
        from spx_stream import Config, TokenManager, setup_logging
        from options_trader import OptionsTrader

        config_path = os.environ.get("SPX_CONFIG", "config.yaml")
        cfg = Config(config_path)
        log = setup_logging(cfg)
        token_mgr = TokenManager(cfg, log)
        app._trader = OptionsTrader(cfg, token_mgr, log)
    return app._trader


@app.route("/api/open_call_credit", methods=["POST"])
def api_open_call_credit():
    """Open a call credit spread (max premium from options_data_config.json)."""
    qty = request.json.get("quantity", 1) if request.is_json else 1
    try:
        trader = _get_trader()
        resp = trader.open_call_credit_spread(quantity=int(qty))
        if resp is None:
            return jsonify({"success": False, "message": "No valid call credit spread available or order failed. Check logs."}), 400
        return jsonify({"success": True, "message": "Call credit spread order submitted.", "order": resp})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route("/api/open_put_credit", methods=["POST"])
def api_open_put_credit():
    """Open a put credit spread (max premium from options_data_config.json)."""
    qty = request.json.get("quantity", 1) if request.is_json else 1
    try:
        trader = _get_trader()
        resp = trader.open_put_credit_spread(quantity=int(qty))
        if resp is None:
            return jsonify({"success": False, "message": "No valid put credit spread available or order failed. Check logs."}), 400
        return jsonify({"success": True, "message": "Put credit spread order submitted.", "order": resp})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


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
