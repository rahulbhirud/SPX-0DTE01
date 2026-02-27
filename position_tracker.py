"""
position_tracker.py
───────────────────
Fetches open positions from the TradeStation brokerage API, groups
them into credit spreads (put credit / call credit), computes open
P&L per spread, and provides a background scheduler that logs spread
status every 5 seconds.

Spread matching logic:
    Legs are grouped by ``OpenedDateTime`` (truncated to the minute) and
    option type (Call / Put).  Within each group the SELL leg is the
    short side and the BUY leg is the long side.

Usage (background thread inside spx_stream.py):
    tracker = PositionTracker(cfg, token_mgr, log)
    tracker.start()       # daemon thread, logs every 5 s
    tracker.stop()

Programmatic:
    spreads = tracker.get_open_spreads()
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

_EST = ZoneInfo("America/New_York")


# ══════════════════════════════════════════════════════════════
# Data Structures
# ══════════════════════════════════════════════════════════════

@dataclass
class PositionLeg:
    """A single option position leg parsed from the API response."""
    symbol: str
    strike: float
    option_type: str       # "CALL" | "PUT"
    quantity: int           # positive = long, negative = short
    trade_action: str       # "BUY" | "SELL" (inferred from qty sign)
    avg_price: float        # average fill price
    last_price: float       # current mark / last
    bid: float              # current bid price
    ask: float              # current ask price
    market_value: float
    opened_dt: str          # truncated timestamp used for grouping
    delta: float
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass
class CreditSpread:
    """Matched two-leg credit spread with computed P&L."""
    spread_type: str         # "Put Credit" | "Call Credit"
    short_strike: float
    long_strike: float
    contracts: int           # number of spread units
    short_symbol: str
    long_symbol: str
    short_delta: float
    credit_received: float   # net premium collected per spread
    current_cost: float      # net cost to close now (short_ask − long_bid)
    open_pnl: float          # credit_received − current_cost  (per spread × contracts)
    short_leg: PositionLeg
    long_leg: PositionLeg


# ══════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════

def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _truncate_time(ts: str) -> str:
    """Truncate an ISO timestamp to the minute for grouping.

    Handles common formats:
        ``2026-02-22T14:36:50Z``  ``2026-02-22T14:36:50.123Z``
    Returns ``2026-02-22T14:36`` (minute precision).
    """
    ts = ts.strip()
    # Take first 16 chars → "2026-02-22T14:36"
    if len(ts) >= 16 and "T" in ts:
        return ts[:16]
    return ts


def _parse_option_type_from_symbol(symbol: str) -> str:
    """Extract 'CALL' or 'PUT' from OCC-style symbol.

    Example:  ``SPXW 260223P6780`` → ``PUT``
    """
    import re
    m = re.search(r"\d{6}([CP])", symbol, re.IGNORECASE)
    if m:
        return "CALL" if m.group(1).upper() == "C" else "PUT"
    return ""


def _parse_strike_from_symbol(symbol: str) -> float:
    """Extract the strike price from an OCC-style symbol."""
    import re
    m = re.search(r"\d{6}[CP](\d+(?:\.\d+)?)", symbol, re.IGNORECASE)
    if m:
        return _safe_float(m.group(1))
    return 0.0


# ══════════════════════════════════════════════════════════════
# Position Tracker
# ══════════════════════════════════════════════════════════════

class PositionTracker:
    """Fetches open positions, groups into credit spreads, and
    provides a background scheduler that logs spread status."""

    POLL_INTERVAL = 5  # seconds

    def __init__(self, cfg, token_mgr, logger: logging.Logger):
        self.cfg = cfg
        self.token_mgr = token_mgr
        self.log = logger
        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ──────────────────────────────────────────────────────────
    # API: fetch raw positions
    # ──────────────────────────────────────────────────────────

    def _fetch_positions(self) -> List[Dict[str, Any]]:
        """GET open positions from brokerage endpoint."""
        url = f"{self.cfg.base_url}/brokerage/accounts/{self.cfg.account_id}/positions"
        token = self.token_mgr.get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        self.log.debug("Fetching positions from %s", url)
        resp = requests.get(url, headers=headers, timeout=20)
        if not resp.ok:
            self.log.error(
                "Positions request failed (HTTP %d): %s",
                resp.status_code,
                self._extract_resp_body(resp),
            )
            resp.raise_for_status()

        body = resp.json()
        # API may wrap in "Positions", "positions", or similar
        for key in ("Positions", "positions", "Items", "items", "Data", "data"):
            value = body.get(key)
            if isinstance(value, list):
                return value
        return []

    # ──────────────────────────────────────────────────────────
    # Parse positions into leg objects
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _parse_position(pos: Dict[str, Any]) -> Optional[PositionLeg]:
        """Convert a raw position dict into a PositionLeg."""
        symbol = (pos.get("Symbol") or pos.get("symbol") or "").strip()
        if not symbol:
            return None

        # Quantity: negative → short
        qty = _safe_int(
            pos.get("Quantity") or pos.get("quantity")
            or pos.get("LongShort") or pos.get("OpenQuantity")
        )

        # Determine direction
        long_short = (pos.get("LongShort") or pos.get("longShort") or "").upper()
        if long_short == "SHORT" and qty > 0:
            qty = -qty
        elif long_short == "LONG" and qty < 0:
            qty = abs(qty)
        trade_action = "SELL" if qty < 0 else "BUY"

        # Strike & option type from explicit fields, falling back to symbol parsing
        strike = _safe_float(pos.get("StrikePrice") or pos.get("strikePrice"))
        if not strike:
            strike = _parse_strike_from_symbol(symbol)

        option_type = (
            pos.get("OptionType") or pos.get("optionType") or ""
        ).upper()
        if not option_type:
            option_type = _parse_option_type_from_symbol(symbol)

        avg_price = _safe_float(
            pos.get("AveragePrice") or pos.get("averagePrice")
            or pos.get("TotalCost") or pos.get("totalCost")
        )
        last_price = _safe_float(
            pos.get("Last") or pos.get("last")
            or pos.get("LastPrice") or pos.get("lastPrice")
            or pos.get("MarketValue") or pos.get("marketValue")
        )
        bid = _safe_float(
            pos.get("Bid") or pos.get("bid")
            or pos.get("BidPrice") or pos.get("bidPrice")
        )
        ask = _safe_float(
            pos.get("Ask") or pos.get("ask")
            or pos.get("AskPrice") or pos.get("askPrice")
        )
        market_value = _safe_float(
            pos.get("MarketValue") or pos.get("marketValue")
        )

        # Opened timestamp for grouping
        opened_dt = (
            pos.get("OpenedDateTime") or pos.get("openedDateTime")
            or pos.get("Timestamp") or pos.get("timestamp")
            or ""
        )
        opened_dt = _truncate_time(str(opened_dt))

        delta = _safe_float(
            pos.get("Delta") or pos.get("delta")
            or pos.get("OptionDelta") or pos.get("optionDelta")
        )

        return PositionLeg(
            symbol=symbol,
            strike=strike,
            option_type=option_type,
            quantity=qty,
            trade_action=trade_action,
            avg_price=avg_price,
            last_price=last_price,
            bid=bid,
            ask=ask,
            market_value=market_value,
            opened_dt=opened_dt,
            delta=delta,
            raw=pos,
        )

    # ──────────────────────────────────────────────────────────
    # Spread matching
    # ──────────────────────────────────────────────────────────

    def _match_spreads(self, legs: List[PositionLeg]) -> List[CreditSpread]:
        """Group legs into credit spreads by opened time + option type.

        Within each group, the short leg (SELL / qty < 0) is paired with
        the long leg (BUY / qty > 0) to form a spread.
        """
        from collections import defaultdict
        groups: Dict[str, List[PositionLeg]] = defaultdict(list)

        for leg in legs:
            if not leg.option_type:
                continue
            key = f"{leg.opened_dt}|{leg.option_type}"
            groups[key].append(leg)

        spreads: List[CreditSpread] = []
        for key, group_legs in groups.items():
            short_legs = [l for l in group_legs if l.quantity < 0]
            long_legs  = [l for l in group_legs if l.quantity > 0]

            if not short_legs or not long_legs:
                continue

            short = short_legs[0]
            long  = long_legs[0]

            contracts = min(abs(short.quantity), abs(long.quantity))

            # Spread type
            if short.option_type == "PUT":
                spread_type = "Put Credit"
            elif short.option_type == "CALL":
                spread_type = "Call Credit"
            else:
                spread_type = "Credit Spread"

            # P&L calculation
            # Credit received = what we sold for − what we bought for (per unit)
            credit_received = abs(short.avg_price) - abs(long.avg_price)
            # Current cost to close:
            #   BUY TO CLOSE the short at the ASK (pay more → fills quickly)
            #   SELL TO CLOSE the long at the BID (receive less → fills quickly)
            #   cost_to_close = short_ask − long_bid
            # Fall back to last_price when bid/ask are unavailable (zero).
            short_close = abs(short.ask) if short.ask > 0 else abs(short.last_price)
            long_close  = abs(long.bid)  if long.bid  > 0 else abs(long.last_price)
            current_cost = short_close - long_close
            # Open P&L per spread × contracts
            open_pnl = (credit_received - current_cost) * contracts * 100

            spreads.append(CreditSpread(
                spread_type=spread_type,
                short_strike=short.strike,
                long_strike=long.strike,
                contracts=contracts,
                short_symbol=short.symbol,
                long_symbol=long.symbol,
                short_delta=short.delta,
                credit_received=round(credit_received, 4),
                current_cost=round(current_cost, 4),
                open_pnl=round(open_pnl, 2),
                short_leg=short,
                long_leg=long,
            ))

        return spreads

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────

    def get_open_spreads(self) -> List[CreditSpread]:
        """Fetch positions, match into spreads, and return the list."""
        raw_positions = self._fetch_positions()
        if not raw_positions:
            self.log.debug("No positions returned from API.")
            return []

        legs = []
        for pos in raw_positions:
            leg = self._parse_position(pos)
            if leg:
                legs.append(leg)

        if not legs:
            self.log.debug("No option legs found in positions.")
            return []

        spreads = self._match_spreads(legs)
        self.log.debug("Matched %d spreads from %d legs.", len(spreads), len(legs))
        return spreads

    # ──────────────────────────────────────────────────────────
    # Scheduler loop
    # ──────────────────────────────────────────────────────────

    def _log_spreads(self, spreads: List[CreditSpread]) -> None:
        """Log each spread's key metrics."""
        if not spreads:
            self.log.info("Position tracker: no open spreads.")
            return

        for i, s in enumerate(spreads, 1):
            self.log.info(
                "Spread #%d | %s  %g/%g  |  contracts=%d  |  "
                "credit=%.2f  cost_to_close=%.2f  open_P&L=$%.2f  |  "
                "short_delta=%.4f",
                i,
                s.spread_type,
                s.short_strike,
                s.long_strike,
                s.contracts,
                s.credit_received,
                s.current_cost,
                s.open_pnl,
                s.short_delta,
            )

    def monitor_and_close_profitable(
        self,
        trader,
        profit_target_pct: float = 40.0,
    ) -> None:
        """Check all open spreads and close any whose profit >= target %.

        Profit % is calculated as:
            ``(credit_received - current_cost) / credit_received * 100``

        When the threshold is met the spread is closed via
        ``trader.close_credit_spread()``.
        """
        try:
            spreads = self.get_open_spreads()
        except Exception as exc:
            self.log.error("Position monitor: failed to fetch spreads: %s", exc)
            return

        if not spreads:
            return

        for s in spreads:
            if s.credit_received <= 0:
                continue  # Can't compute profit %

            profit_pct = ((s.credit_received - s.current_cost) / s.credit_received) * 100

            self.log.debug(
                "Profit check | %s %g/%g | credit=%.2f  cost=%.2f  profit=%.1f%%  target=%.0f%%",
                s.spread_type, s.short_strike, s.long_strike,
                s.credit_received, s.current_cost, profit_pct, profit_target_pct,
            )

            if profit_pct >= profit_target_pct:
                self.log.info(
                    "\U0001f4b0 Profit target hit! %s %g/%g | profit=%.1f%% >= %.0f%% | "
                    "Submitting close order…",
                    s.spread_type, s.short_strike, s.long_strike,
                    profit_pct, profit_target_pct,
                )
                try:
                    result = trader.close_credit_spread(
                        short_symbol=s.short_symbol,
                        long_symbol=s.long_symbol,
                        quantity=s.contracts,
                        limit_price=s.current_cost,
                    )
                    if result:
                        self.log.info(
                            "Close order submitted for %s %g/%g.",
                            s.spread_type, s.short_strike, s.long_strike,
                        )
                    else:
                        self.log.warning(
                            "Close order returned None for %s %g/%g.",
                            s.spread_type, s.short_strike, s.long_strike,
                        )
                except Exception as exc:
                    self.log.error(
                        "Failed to close %s %g/%g: %s",
                        s.spread_type, s.short_strike, s.long_strike, exc,
                    )

    def _run_loop(self) -> None:
        """Background loop — polls positions every POLL_INTERVAL seconds."""
        self.log.info(
            "Position tracker scheduler started (interval=%ds).",
            self.POLL_INTERVAL,
        )

        while self._running:
            try:
                spreads = self.get_open_spreads()
                self._log_spreads(spreads)
            except requests.exceptions.HTTPError as exc:
                self.log.error("Position tracker HTTP error: %s", exc)
                if exc.response is not None and exc.response.status_code == 401:
                    self.log.info("Token may have expired — will retry next cycle.")
            except requests.exceptions.RequestException as exc:
                self.log.error("Position tracker connection error: %s", exc)
            except Exception as exc:
                self.log.exception("Position tracker error: %s", exc)

            # Interruptible sleep
            for _ in range(self.POLL_INTERVAL * 10):
                if not self._running:
                    break
                time.sleep(0.1)

        self.log.info("Position tracker scheduler stopped.")

    def start(self) -> None:
        """Start the scheduler in a daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, name="position-tracker", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the scheduler to stop."""
        self._running = False

    # ──────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _extract_resp_body(resp) -> str:
        if resp is None:
            return "<no response>"
        try:
            return resp.text or resp.content.decode("utf-8", errors="replace")
        except Exception:
            try:
                return resp.content.decode("utf-8", errors="replace")
            except Exception:
                return "<unreadable>"
