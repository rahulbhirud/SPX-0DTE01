"""
option_spread_trader.py
───────────────────────
0DTE Credit Spread Trader for SPX options via TradeStation API.

Scans the option chain for credit spreads that satisfy configurable
delta, premium, and width constraints, then places orders (or logs
dry-run results).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional

import requests

if TYPE_CHECKING:
    from spx_stream import Config, TokenManager


# ══════════════════════════════════════════════════════════════
# Data Structures
# ══════════════════════════════════════════════════════════════

@dataclass
class SpreadCandidate:
    """Represents a viable credit spread found in the chain."""
    width: int
    short_strike: float
    long_strike: float
    short_symbol: str
    long_symbol: str
    short_delta: float
    long_delta: float
    net_credit: float          # bid(short) − ask(long)
    short_bid: float
    long_ask: float
    expiration: str


# ══════════════════════════════════════════════════════════════
# 0DTE Credit Spread Trader
# ══════════════════════════════════════════════════════════════

class OptionSpreadTrader:
    """
    Scans the SPX 0DTE option chain for credit spreads that satisfy:
      • short-leg |delta| < configured max_delta  (default 10)
      • net credit ≤ configured max_premium       (default $0.60)
      • spread widths of 5, 10, 20 points

    Provides two public methods:
      • open_credit_call_spread()  — bear call spread (sell call, buy higher call)
      • open_credit_put_spread()   — bull put spread  (sell put,  buy lower  put)
    """

    def __init__(self, cfg: Config, token_mgr: TokenManager, logger: logging.Logger):
        self.cfg = cfg
        self.token_mgr = token_mgr
        self.log = logger

    # ──────────────────────────────────────────────────────────
    # Option Chain Helpers
    # ──────────────────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token_mgr.get_access_token()}",
            "Content-Type": "application/json",
        }

    def _today_expiration(self) -> str:
        """Return today's date as YYYY-MM-DD for 0DTE filtering."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _fetch_option_chain(self, option_type: str) -> List[dict]:
        """
        Fetch the option chain from TradeStation for the given type.

        Args:
            option_type: "Call" or "Put"

        Returns:
            List of option leg dicts with Greeks, bid/ask, strike, etc.
        """
        expiration = self._today_expiration()
        underlying = self.cfg.spread_underlying
        url = (
            f"{self.cfg.base_url}/marketdata/options/chains/{underlying}"
            f"?expiration={expiration}"
            f"&optionType={option_type}"
            f"&strikeProximity=50"       # strikes within 50 pts of underlying
            f"&enableGreeks=true"
        )
        self.log.info("Fetching %s option chain for %s exp=%s…",
                       option_type, underlying, expiration)

        r = requests.get(url, headers=self._headers(), timeout=15)
        r.raise_for_status()
        data = r.json()

        # TradeStation returns {"Options": [...]} or {"Expirations": [{"Options": [...]}]}
        options = data.get("Options", [])
        if not options:
            for exp_group in data.get("Expirations", []):
                options.extend(exp_group.get("Strikes", []))
        return options

    def _parse_legs(self, options: List[dict]) -> List[dict]:
        """
        Normalise raw chain data into a flat list of dicts with keys:
        symbol, strike, delta, bid, ask, expiration.
        """
        legs = []
        for opt in options:
            try:
                leg = {
                    "symbol":     opt.get("Symbol", ""),
                    "strike":     float(opt.get("StrikePrice", 0)),
                    "delta":      abs(float(opt.get("Greeks", {}).get("Delta", 999))),
                    "bid":        float(opt.get("Bid", 0)),
                    "ask":        float(opt.get("Ask", 0)),
                    "expiration": opt.get("Expiration", ""),
                }
                legs.append(leg)
            except (TypeError, ValueError):
                continue
        # Sort by strike ascending
        legs.sort(key=lambda x: x["strike"])
        return legs

    # ──────────────────────────────────────────────────────────
    # Spread Scanning
    # ──────────────────────────────────────────────────────────

    def _find_credit_call_spreads(self) -> List[SpreadCandidate]:
        """
        Bear call spread: SELL a lower-strike call, BUY a higher-strike call.
        Short leg delta must be < max_delta.  Net credit ≤ max_premium.
        Scans all configured widths (5, 10, 20).
        """
        options = self._fetch_option_chain("Call")
        legs = self._parse_legs(options)
        if not legs:
            self.log.warning("No call options returned from chain.")
            return []

        strikes_map: Dict[float, dict] = {l["strike"]: l for l in legs}
        max_delta   = self.cfg.spread_max_delta / 100.0   # convert to decimal
        max_premium = self.cfg.spread_max_premium
        widths      = self.cfg.spread_widths
        candidates  = []

        for short_leg in legs:
            # Short leg must have low delta (far OTM)
            if short_leg["delta"] >= max_delta:
                continue
            if short_leg["bid"] <= 0:
                continue

            for w in widths:
                long_strike = short_leg["strike"] + w
                long_leg = strikes_map.get(long_strike)
                if not long_leg:
                    continue

                net_credit = round(short_leg["bid"] - long_leg["ask"], 2)
                if net_credit <= 0 or net_credit > max_premium:
                    continue

                candidates.append(SpreadCandidate(
                    width=w,
                    short_strike=short_leg["strike"],
                    long_strike=long_strike,
                    short_symbol=short_leg["symbol"],
                    long_symbol=long_leg["symbol"],
                    short_delta=short_leg["delta"],
                    long_delta=long_leg["delta"],
                    net_credit=net_credit,
                    short_bid=short_leg["bid"],
                    long_ask=long_leg["ask"],
                    expiration=short_leg["expiration"],
                ))

        # Sort: prefer highest credit first
        candidates.sort(key=lambda c: c.net_credit, reverse=True)
        return candidates

    def _find_credit_put_spreads(self) -> List[SpreadCandidate]:
        """
        Bull put spread: SELL a higher-strike put, BUY a lower-strike put.
        Short leg delta must be < max_delta.  Net credit ≤ max_premium.
        Scans all configured widths (5, 10, 20).
        """
        options = self._fetch_option_chain("Put")
        legs = self._parse_legs(options)
        if not legs:
            self.log.warning("No put options returned from chain.")
            return []

        strikes_map: Dict[float, dict] = {l["strike"]: l for l in legs}
        max_delta   = self.cfg.spread_max_delta / 100.0
        max_premium = self.cfg.spread_max_premium
        widths      = self.cfg.spread_widths
        candidates  = []

        for short_leg in legs:
            if short_leg["delta"] >= max_delta:
                continue
            if short_leg["bid"] <= 0:
                continue

            for w in widths:
                long_strike = short_leg["strike"] - w
                long_leg = strikes_map.get(long_strike)
                if not long_leg:
                    continue

                net_credit = round(short_leg["bid"] - long_leg["ask"], 2)
                if net_credit <= 0 or net_credit > max_premium:
                    continue

                candidates.append(SpreadCandidate(
                    width=w,
                    short_strike=short_leg["strike"],
                    long_strike=long_strike,
                    short_symbol=short_leg["symbol"],
                    long_symbol=long_leg["symbol"],
                    short_delta=short_leg["delta"],
                    long_delta=long_leg["delta"],
                    net_credit=net_credit,
                    short_bid=short_leg["bid"],
                    long_ask=long_leg["ask"],
                    expiration=short_leg["expiration"],
                ))

        candidates.sort(key=lambda c: c.net_credit, reverse=True)
        return candidates

    # ──────────────────────────────────────────────────────────
    # Order Placement
    # ──────────────────────────────────────────────────────────

    def _place_spread_order(self, candidate: SpreadCandidate, spread_type: str) -> Optional[dict]:
        """
        Place a credit spread order via TradeStation Order API.

        Args:
            candidate: The SpreadCandidate to execute.
            spread_type: "CALL" or "PUT" (for logging).

        Returns:
            Order response dict, or None if dry_run.
        """
        B = "═" * 74
        self.log.warning(B)
        self.log.warning(
            "  💰  %s CREDIT SPREAD  |  %d-wide  |  credit=$%.2f",
            spread_type, candidate.width, candidate.net_credit,
        )
        self.log.warning(
            "  SELL %s  strike=%.0f  delta=%.4f  bid=$%.2f",
            candidate.short_symbol, candidate.short_strike,
            candidate.short_delta, candidate.short_bid,
        )
        self.log.warning(
            "  BUY  %s  strike=%.0f  delta=%.4f  ask=$%.2f",
            candidate.long_symbol, candidate.long_strike,
            candidate.long_delta, candidate.long_ask,
        )
        self.log.warning(
            "  exp=%s  qty=%d  max_risk=$%.2f",
            candidate.expiration, self.cfg.spread_quantity,
            (candidate.width - candidate.net_credit) * 100,
        )
        self.log.warning(B)

        if self.cfg.spread_dry_run:
            self.log.info("🔒 DRY RUN — order NOT placed. Set spreads.dry_run=false to trade.")
            return None

        order_payload = {
            "AccountID": self.cfg.account_id,
            "Symbol": candidate.short_symbol,
            "Quantity": str(self.cfg.spread_quantity),
            "OrderType": self.cfg.spread_order_type,
            "TradeAction": "SELLTOOPEN",
            "TimeInForce": {"Duration": self.cfg.spread_time_in_force},
            "LimitPrice": str(candidate.net_credit),
            "Legs": [
                {
                    "Symbol": candidate.short_symbol,
                    "Quantity": str(self.cfg.spread_quantity),
                    "TradeAction": "SELLTOOPEN",
                },
                {
                    "Symbol": candidate.long_symbol,
                    "Quantity": str(self.cfg.spread_quantity),
                    "TradeAction": "BUYTOOPEN",
                },
            ],
            "OrderConfirmId": "",   # TradeStation may require confirm flow
        }

        url = f"{self.cfg.base_url}/orderexecution/orders"
        self.log.info("Placing %s credit spread order…", spread_type)

        r = requests.post(url, headers=self._headers(),
                          json=order_payload, timeout=15)
        r.raise_for_status()
        resp = r.json()
        self.log.info("Order response: %s", json.dumps(resp, indent=2))
        return resp

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────

    def open_credit_call_spread(self) -> Optional[dict]:
        """
        Open a bear call credit spread (0DTE):
          • Fetch call chain for today's expiration
          • Find short call with |delta| < max_delta
          • Pair with long call at short_strike + width
          • Net credit ≤ max_premium ($0.60)
          • Widths scanned: 5, 10, 20

        Returns the order response, or None if no candidate found / dry run.
        """
        self.log.info("━" * 60)
        self.log.info("Scanning for CALL credit spreads  (delta < %s, premium ≤ $%.2f)…",
                       self.cfg.spread_max_delta, self.cfg.spread_max_premium)
        try:
            candidates = self._find_credit_call_spreads()
        except requests.exceptions.RequestException as e:
            self.log.error("Failed to fetch call chain: %s", e)
            return None

        if not candidates:
            self.log.info("No qualifying call credit spreads found.")
            return None

        self.log.info("Found %d candidate(s). Best:", len(candidates))
        best = candidates[0]
        return self._place_spread_order(best, "CALL")

    def open_credit_put_spread(self) -> Optional[dict]:
        """
        Open a bull put credit spread (0DTE):
          • Fetch put chain for today's expiration
          • Find short put with |delta| < max_delta
          • Pair with long put at short_strike − width
          • Net credit ≤ max_premium ($0.60)
          • Widths scanned: 5, 10, 20

        Returns the order response, or None if no candidate found / dry run.
        """
        self.log.info("━" * 60)
        self.log.info("Scanning for PUT credit spreads  (delta < %s, premium ≤ $%.2f)…",
                       self.cfg.spread_max_delta, self.cfg.spread_max_premium)
        try:
            candidates = self._find_credit_put_spreads()
        except requests.exceptions.RequestException as e:
            self.log.error("Failed to fetch put chain: %s", e)
            return None

        if not candidates:
            self.log.info("No qualifying put credit spreads found.")
            return None

        self.log.info("Found %d candidate(s). Best:", len(candidates))
        best = candidates[0]
        return self._place_spread_order(best, "PUT")
