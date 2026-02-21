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
import traceback
import webbrowser

from flask import Flask, jsonify, render_template, request

# Lazy imports for spread trading (only needed when buttons are clicked)
_trader = None  # singleton OptionSpreadTrader, initialised on first use

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


# ── Spread Trading Endpoints ──────────────────────────────────

def _get_trader():
    """Lazily create and cache the OptionSpreadTrader singleton."""
    global _trader
    if _trader is None:
        from spx_stream import Config, TokenManager
        from option_spread_trader import OptionSpreadTrader

        cfg = Config()
        logger = logging.getLogger("spx_stream")
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(message)s"))
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
        token_mgr = TokenManager(cfg, logger)
        _trader = OptionSpreadTrader(cfg, token_mgr, logger)
    return _trader


@app.route("/api/open_call_spread", methods=["POST"])
def api_open_call_spread():
    """Open a bear-call credit spread based on current SPX price."""
    try:
        trader = _get_trader()
        result = trader.open_credit_call_spread()
        if result is None:
            # dry-run or no candidates
            state = _read_state()
            return jsonify({
                "ok": True,
                "message": "Call credit spread scan complete (dry-run or no candidates).",
                "price": state.get("price", "—"),
            })
        return jsonify({"ok": True, "message": "Call credit spread order placed.", "order": result})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/open_put_spread", methods=["POST"])
def api_open_put_spread():
    """Open a bull-put credit spread based on current SPX price."""
    try:
        trader = _get_trader()
        result = trader.open_credit_put_spread()
        if result is None:
            state = _read_state()
            return jsonify({
                "ok": True,
                "message": "Put credit spread scan complete (dry-run or no candidates).",
                "price": state.get("price", "—"),
            })
        return jsonify({"ok": True, "message": "Put credit spread order placed.", "order": result})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


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
