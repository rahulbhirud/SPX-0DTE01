"""
dashboard.py
────────────
Simple web dashboard for SPX streamer.
Displays current SPX price and latest RSI.

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


def _get_order_tracker():
    """Lazily build an OrderTracker using the same shared config/token setup."""
    if not hasattr(app, "_order_tracker"):
        from spx_stream import Config, TokenManager, setup_logging
        from order_tracker import OrderTracker

        config_path = os.environ.get("SPX_CONFIG", "config.yaml")
        cfg = Config(config_path)
        log = setup_logging(cfg)
        token_mgr = TokenManager(cfg, log)
        app._order_tracker = OrderTracker(cfg, token_mgr, log)
    return app._order_tracker


def _get_position_tracker():
    """Lazily build a PositionTracker using the same shared config/token setup."""
    if not hasattr(app, "_position_tracker"):
        from spx_stream import Config, TokenManager, setup_logging
        from position_tracker import PositionTracker

        config_path = os.environ.get("SPX_CONFIG", "config.yaml")
        cfg = Config(config_path)
        log = setup_logging(cfg)
        token_mgr = TokenManager(cfg, log)
        app._position_tracker = PositionTracker(cfg, token_mgr, log)
    return app._position_tracker


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


@app.route("/api/open_orders", methods=["GET"])
def api_open_orders():
    """Return all currently open orders for the configured account."""
    try:
        tracker = _get_order_tracker()
        orders = tracker.get_open_orders()
        return jsonify({"success": True, "orders": orders})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route("/api/open_positions", methods=["GET"])
def api_open_positions():
    """Return open credit spreads with live P&L."""
    try:
        tracker = _get_position_tracker()
        spreads = tracker.get_open_spreads()
        result = []
        for s in spreads:
            result.append({
                "spread_type": s.spread_type,
                "short_strike": s.short_strike,
                "long_strike": s.long_strike,
                "contracts": s.contracts,
                "short_symbol": s.short_symbol,
                "long_symbol": s.long_symbol,
                "short_delta": s.short_delta,
                "credit_received": s.credit_received,
                "current_cost": s.current_cost,
                "open_pnl": s.open_pnl,
            })
        return jsonify({"success": True, "spreads": result})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@app.route("/api/cancel_order", methods=["POST"])
def api_cancel_order():
    """Cancel an order by OrderID."""
    if not request.is_json:
        return jsonify({"success": False, "message": "JSON body required."}), 400

    order_id = str(request.json.get("order_id", "")).strip()
    if not order_id:
        return jsonify({"success": False, "message": "order_id is required."}), 400

    try:
        tracker = _get_order_tracker()
        resp = tracker.cancel_order(order_id)
        return jsonify({"success": True, "message": f"Cancel request sent for order {order_id}.", "result": resp})
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
