"""
options_trader.py
─────────────────
Reads credit-spread candidates from ``options_data_config.json``,
selects the spread with the highest net premium, and submits an
opening order to the TradeStation order-execution API.

Usage (standalone):
    python options_trader.py --side call --qty 2
    python options_trader.py --side put  --qty 2

Programmatic:
    from options_trader import OptionsTrader
    trader = OptionsTrader(cfg, token_mgr, log)
    trader.open_call_credit_spread()           # uses config default
    trader.open_put_credit_spread(quantity=3)  # explicit override
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from datetime import date


# ══════════════════════════════════════════════════════════════
# Helper
# ══════════════════════════════════════════════════════════════

def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _build_occ_symbol(root: str, expiration: str, side: str, strike: float) -> str:
    """Construct an OCC option symbol from spread metadata.

    Format: ``ROOT YYMMDDC/PSTRIKE``
    Example: ``SPXW 260210C7040``

    *root* is cleaned: leading ``$`` and trailing ``.X`` / ``.`` are stripped.
    *expiration* should be an ISO date string (``YYYY-MM-DD``).
    """
    clean = root.lstrip("$").split(".")[0].upper()
    exp_date = date.fromisoformat(expiration)
    exp_str = exp_date.strftime("%y%m%d")
    cp = "C" if side.upper().startswith("C") else "P"
    strike_str = f"{strike:g}"
    return f"{clean} {exp_str}{cp}{strike_str}"


# ══════════════════════════════════════════════════════════════
# Options Trader
# ══════════════════════════════════════════════════════════════

class OptionsTrader:

    def __init__(self, cfg, token_mgr, logger: logging.Logger):
        self.cfg = cfg
        self.token_mgr = token_mgr
        self.log = logger

    def _get_leg_market(self, symbol: str) -> dict:
        """Fetch latest bid/ask for a given option symbol using PositionTracker."""
        try:
            from position_tracker import PositionTracker
        except ImportError:
            self.log.error("PositionTracker import failed.")
            return {"bid": 0.0, "ask": 0.0}
        tracker = PositionTracker(self.cfg, self.token_mgr, self.log)
        try:
            spreads = tracker.get_open_spreads()
            for spread in spreads:
                if spread.short_symbol == symbol:
                    return {"bid": spread.short_leg.bid, "ask": spread.short_leg.ask}
                if spread.long_symbol == symbol:
                    return {"bid": spread.long_leg.bid, "ask": spread.long_leg.ask}
        except Exception as exc:
            self.log.error(f"Failed to get market for {symbol}: {exc}")
        return {"bid": 0.0, "ask": 0.0}

    DATA_FILE = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "json", "options_data_config.json"
    )

    def _load_spreads(self, spread_key: str) -> List[Dict[str, Any]]:
        """Load a list of spread candidates from the JSON data file.

        If any candidate is missing ``short_symbol`` / ``long_symbol``,
        the symbols are constructed from the spread metadata (underlying,
        expiration, strike, and spread type) so orders can still be placed.
        """
        try:
            with open(self.DATA_FILE, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            self.log.error("Spread data file not found: %s", self.DATA_FILE)
            return []
        except Exception as exc:
            self.log.error("Failed to read %s: %s", self.DATA_FILE, exc)
            return []

        spreads = data.get(spread_key, [])
        if not isinstance(spreads, list):
            self.log.error("Invalid format for key '%s' in %s", spread_key, self.DATA_FILE)
            return []

        # Fill in missing symbols from metadata
        underlying = data.get("symbol", "")
        expiration = data.get("expiration", "")
        if underlying and expiration:
            for s in spreads:
                spread_type = s.get("spread_type", "")
                # Determine side from spread type
                if "call" in spread_type:
                    side = "Call"
                elif "put" in spread_type:
                    side = "Put"
                else:
                    continue

                short_sym = (s.get("short_symbol") or "").strip()
                long_sym = (s.get("long_symbol") or "").strip()

                if not short_sym and s.get("short_strike"):
                    s["short_symbol"] = _build_occ_symbol(
                        underlying, expiration, side, s["short_strike"]
                    )
                    self.log.debug(
                        "Built short symbol: %s (strike=%.1f)",
                        s["short_symbol"], s["short_strike"],
                    )
                if not long_sym and s.get("long_strike"):
                    s["long_symbol"] = _build_occ_symbol(
                        underlying, expiration, side, s["long_strike"]
                    )
                    self.log.debug(
                        "Built long symbol: %s (strike=%.1f)",
                        s["long_symbol"], s["long_strike"],
                    )

        return spreads

    def _pick_max_premium(self, spreads: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Return the spread with the highest ``net_credit``.

        Spreads missing a valid ``short_symbol`` / ``long_symbol`` or
        having zero/negative premium are skipped.
        """
        valid: List[Dict[str, Any]] = []
        for s in spreads:
            short_sym = (s.get("short_symbol") or "").strip()
            long_sym  = (s.get("long_symbol") or "").strip()
            premium   = _safe_float(s.get("net_credit"))
            if short_sym and long_sym and premium > 0:
                valid.append(s)

        if not valid:
            return None
        return max(valid, key=lambda s: _safe_float(s.get("net_credit")))

    # ──────────────────────────────────────────────────────────
    # Order submission
    # ──────────────────────────────────────────────────────────

    def _build_order_payload(self, spread: Dict[str, Any], quantity: int) -> Dict[str, Any]:
        """Build a TradeStation V3 order payload for a credit spread.

        Key TradeStation V3 requirements for multi-leg credit spreads:
                * ``OrderType`` must be one of: ``Limit``, ``StopMarket``,
                    ``Market``, or ``StopLimit``.
                * For opening credit spreads, ``OrderType`` is ``"Limit"``.
            * ``LimitPrice`` is the desired net credit as a *negative* string.
        * ``Route`` is omitted — index-option spreads are routed
          automatically by the exchange.
        """
        short_symbol = spread["short_symbol"]
        long_symbol  = spread["long_symbol"]
        net_credit   = round(_safe_float(spread.get("net_credit")), 2)
        limit_price  = -abs(net_credit)

        payload: Dict[str, Any] = {
            "AccountID": self.cfg.account_id,
            "OrderType": "Limit",
            "LimitPrice": f"{limit_price:.2f}",
            "TimeInForce": {"Duration": "GTC"},
            "Legs": [
                {
                    "Symbol": short_symbol,
                    "Quantity": str(quantity),
                    "TradeAction": "SELLTOOPEN",
                },
                {
                    "Symbol": long_symbol,
                    "Quantity": str(quantity),
                    "TradeAction": "BUYTOOPEN",
                },
            ],
        }
        return payload

    @staticmethod
    def _extract_resp_body(resp) -> str:
        """Best-effort extraction of a response body as text."""
        if resp is None:
            return "<no response>"
        try:
            return resp.text or resp.content.decode("utf-8", errors="replace")
        except Exception:
            try:
                return resp.content.decode("utf-8", errors="replace")
            except Exception:
                return "<unreadable>"

    def _confirm_order(self, payload: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
        """POST to ``/orderexecution/orderconfirm`` to validate before placing.

        The TradeStation V3 ``orderconfirm`` endpoint accepts the same
        order object as ``/orderexecution/orders`` (no ``"Orders"`` wrapper).

        Returns the confirmation body (which includes estimated cost,
        warnings, etc.).  Raises on HTTP errors so the caller can
        capture the *detailed* rejection reason.
        """
        url = f"{self.cfg.base_url}/orderexecution/orderconfirm"
        self.log.debug("Confirming order via %s", url)
        self.log.debug("Confirm payload: %s", json.dumps(payload, indent=2))
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        if not resp.ok:
            body = self._extract_resp_body(resp)
            self.log.error(
                "Order confirmation rejected (HTTP %d): %s", resp.status_code, body,
            )
            resp.raise_for_status()
        confirm_body = resp.json()
        self.log.debug("Order confirmation response: %s", json.dumps(confirm_body, indent=2))
        return confirm_body

    def _submit_open_order(self, spread: Dict[str, Any], quantity: int) -> Dict[str, Any]:
        """Validate then POST a two-leg credit-spread opening order."""
        payload = self._build_order_payload(spread, quantity)

        url = f"{self.cfg.base_url}/orderexecution/orders"
        token = self.token_mgr.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        short_symbol = spread["short_symbol"]
        long_symbol  = spread["long_symbol"]
        net_credit   = round(_safe_float(spread.get("net_credit")), 2)

        self.log.info(
            "Submitting open order | short=%s  long=%s  qty=%d  credit=%.2f",
            short_symbol, long_symbol, quantity, net_credit,
        )
        self.log.debug("Order payload: %s", json.dumps(payload, indent=2))

        # Step 1 — confirm (validates symbols, buying power, etc.)
        try:
            self._confirm_order(payload, headers)
        except requests.exceptions.HTTPError:
            # Already logged in _confirm_order; re-raise so caller sees it.
            raise

        # Step 2 — place the order
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        if not resp.ok:
            body = self._extract_resp_body(resp)
            self.log.error(
                "Order placement rejected (HTTP %d): %s", resp.status_code, body,
            )
            resp.raise_for_status()
        return resp.json()

    def _log_order_status(self, label: str, body: Dict[str, Any]) -> None:
        """Extract and log order status fields from the API response."""
        # TradeStation may nest the result inside an "Orders" array
        src: Dict[str, Any] = body
        orders = body.get("Orders")
        if isinstance(orders, list) and orders:
            first = orders[0]
            if isinstance(first, dict):
                src = first

        order_id = (
            src.get("OrderID") or src.get("orderID")
            or src.get("OrderId") or src.get("orderId")
        )
        status = (
            src.get("Status") or src.get("status")
            or src.get("OrderStatus") or src.get("orderStatus")
        )
        message = (
            src.get("Message") or src.get("message")
            or src.get("Error") or src.get("error")
        )

        self.log.info(
            "%s order response | order_id=%s  status=%s  message=%s",
            label, order_id, status, message,
        )

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────

    def open_call_credit_spread(self, quantity: int | None = None) -> Optional[Dict[str, Any]]:
        """Select the call credit spread with the highest premium from
        ``options_data_config.json`` and submit an opening order.

        Returns the API response dict on success, or ``None`` on failure.
        """
        if quantity is None:
            quantity = self.cfg.options_default_quantity

        spreads = self._load_spreads("credit_call_spreads")
        if not spreads:
            self.log.warning("No call credit spreads found in %s", self.DATA_FILE)
            return None

        selected = self._pick_max_premium(spreads)
        if selected is None:
            self.log.error(
                "No valid call credit spread (need symbols + positive premium) in %s",
                self.DATA_FILE,
            )
            return None

        self.log.info(
            "Selected call credit spread | short_strike=%.1f  long_strike=%.1f  "
            "width=%s  net_credit=%.2f",
            selected["short_strike"], selected["long_strike"],
            selected["width"], _safe_float(selected.get("net_credit")),
        )

        try:
            resp_json = self._submit_open_order(selected, quantity)
            self._log_order_status("CALL CREDIT", resp_json)
            return resp_json
        except requests.exceptions.HTTPError as exc:
            body = self._extract_resp_body(getattr(exc, "response", None))
            self.log.error("Call credit order HTTP error: %s | body=%s", exc, body)
        except requests.RequestException as exc:
            self.log.error("Call credit order request failed: %s", exc)
        except Exception as exc:
            self.log.exception("Call credit order unexpected error: %s", exc)
        return None


    def close_credit_spread(
        self,
        short_symbol: str,
        long_symbol: str,
        quantity: int,
        limit_price: float,
    ) -> Optional[Dict[str, Any]]:
        """Submit a closing order for an open credit spread.

        If the long leg bid < $0.05, submit an OSO order:
        - BUY TO CLOSE the short leg at ask
        - Once filled, SELL TO CLOSE the long leg at bid
        Otherwise, submit a regular two-leg close order.
        """
        # Fetch latest market for both legs
        short_mkt = self._get_leg_market(short_symbol)
        long_mkt = self._get_leg_market(long_symbol)
        long_bid = long_mkt.get("bid", 0.0)
        short_ask = short_mkt.get("ask", 0.0)

        if long_bid < 0.05:
            # Submit OSO order
            payload = {
                "AccountID": self.cfg.account_id,
                "OrderType": "Limit",
                "LimitPrice": f"{short_ask:.2f}",
                "TimeInForce": {"Duration": "GTC"},
                "Legs": [
                    {
                        "Symbol": short_symbol,
                        "Quantity": str(quantity),
                        "TradeAction": "BUYTOCLOSE",
                    }
                ],
                "OrderStrategyType": "OSO",
                "ChildOrders": [
                    {
                        "OrderType": "Limit",
                        "LimitPrice": f"{long_bid:.2f}",
                        "TimeInForce": {"Duration": "GTC"},
                        "Legs": [
                            {
                                "Symbol": long_symbol,
                                "Quantity": str(quantity),
                                "TradeAction": "SELLTOCLOSE",
                            }
                        ],
                    }
                ],
            }
            order_type = "OSO"
        else:
            # Regular two-leg close order
            payload = {
                "AccountID": self.cfg.account_id,
                "OrderType": "Limit",
                "LimitPrice": f"{abs(limit_price):.2f}",
                "TimeInForce": {"Duration": "GTC"},
                "Legs": [
                    {
                        "Symbol": short_symbol,
                        "Quantity": str(quantity),
                        "TradeAction": "BUYTOCLOSE",
                    },
                    {
                        "Symbol": long_symbol,
                        "Quantity": str(quantity),
                        "TradeAction": "SELLTOCLOSE",
                    },
                ],
            }
            order_type = "REGULAR"

        url = f"{self.cfg.base_url}/orderexecution/orders"
        token = self.token_mgr.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        self.log.info(
            f"Submitting {order_type} close order | short=%s  long=%s  qty=%d  limit=%.2f",
            short_symbol, long_symbol, quantity, limit_price,
        )
        self.log.debug("Close payload: %s", json.dumps(payload, indent=2))

        try:
            # Step 1 — confirm
            self._confirm_order(payload, headers)
            # Step 2 — place
            resp = requests.post(url, headers=headers, json=payload, timeout=20)
            if not resp.ok:
                body = self._extract_resp_body(resp)
                self.log.error(
                    "Close order rejected (HTTP %d): %s", resp.status_code, body,
                )
                resp.raise_for_status()
            resp_json = resp.json()
            self._log_order_status("CLOSE SPREAD", resp_json)
            return resp_json
        except requests.exceptions.HTTPError as exc:
            body = self._extract_resp_body(getattr(exc, "response", None))
            self.log.error("Close order HTTP error: %s | body=%s", exc, body)
        except requests.RequestException as exc:
            self.log.error("Close order request failed: %s", exc)
        except Exception as exc:
            self.log.exception("Close order unexpected error: %s", exc)
        return None

    def open_put_credit_spread(self, quantity: int | None = None) -> Optional[Dict[str, Any]]:
        """Select the put credit spread with the highest premium from
        ``options_data_config.json`` and submit an opening order.

        Returns the API response dict on success, or ``None`` on failure.
        """
        if quantity is None:
            quantity = self.cfg.options_default_quantity

        spreads = self._load_spreads("credit_put_spreads")
        if not spreads:
            self.log.warning("No put credit spreads found in %s", self.DATA_FILE)
            return None

        selected = self._pick_max_premium(spreads)
        if selected is None:
            self.log.error(
                "No valid put credit spread (need symbols + positive premium) in %s",
                self.DATA_FILE,
            )
            return None

        self.log.info(
            "Selected put credit spread | short_strike=%.1f  long_strike=%.1f  "
            "width=%s  net_credit=%.2f",
            selected["short_strike"], selected["long_strike"],
            selected["width"], _safe_float(selected.get("net_credit")),
        )

        try:
            resp_json = self._submit_open_order(selected, quantity)
            self._log_order_status("PUT CREDIT", resp_json)
            return resp_json
        except requests.exceptions.HTTPError as exc:
            body = self._extract_resp_body(getattr(exc, "response", None))
            self.log.error("Put credit order HTTP error: %s | body=%s", exc, body)
        except requests.RequestException as exc:
            self.log.error("Put credit order request failed: %s", exc)
        except Exception as exc:
            self.log.exception("Put credit order unexpected error: %s", exc)
        return None


# ══════════════════════════════════════════════════════════════
# Standalone entry point
# ══════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Open call/put credit spreads from options_data_config.json"
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--side", choices=["call", "put"], required=True,
                        help="Which spread to open: call or put credit")
    parser.add_argument("--qty", type=int, default=None,
                        help="Number of contracts (default: from config.yaml)")
    args = parser.parse_args()

    if not Path(args.config).exists():
        raise SystemExit(f"Config not found: {args.config}")

    # Reuse Config, TokenManager, and logging from spx_stream
    from spx_stream import Config, TokenManager, setup_logging

    cfg = Config(args.config)
    log = setup_logging(cfg)
    token_mgr = TokenManager(cfg, log)

    trader = OptionsTrader(cfg, token_mgr, log)

    if args.side == "call":
        trader.open_call_credit_spread(quantity=args.qty)
    else:
        trader.open_put_credit_spread(quantity=args.qty)


if __name__ == "__main__":
    main()
