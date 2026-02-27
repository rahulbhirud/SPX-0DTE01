"""
options_chain_scheduler.py
──────────────────────────
Periodically fetches SPX options-chain data via the TradeStation streaming
API, identifies credit-spread candidates (bear-call and bull-put) with
|delta| < configured max and net premium in a target range, and persists
the filtered list to ``options_data_config.json``.

The scheduler can run:
  • As a background thread inside ``spx_stream.py`` (primary mode)
    • Standalone:  python options_chain_scheduler.py [--config yaml/config.yaml]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

# ══════════════════════════════════════════════════════════════
# Data Structures
# ══════════════════════════════════════════════════════════════

@dataclass
class OptionLeg:
    """Parsed option contract from the streaming chain."""
    symbol: str
    strike: float
    side: str          # "Call" | "Put"
    bid: float
    ask: float
    last: float
    delta: float       # absolute
    gamma: float
    iv: float
    volume: float
    open_interest: float


@dataclass
class SpreadCandidate:
    """A single vertical credit-spread candidate."""
    spread_type: str         # "bear_call" | "bull_put"
    short_strike: float
    long_strike: float
    width: int
    short_symbol: str
    long_symbol: str
    short_delta: float
    long_delta: float
    net_credit: float        # short_bid − long_ask
    short_bid: float
    long_ask: float
    max_risk: float          # (width − net_credit) per contract


# ══════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════

_EST = ZoneInfo("America/New_York")


def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _resolve_trading_day(target: str | date | None = None) -> date:
    """Return *target* as a ``date``, defaulting to today (ET).

    If the resolved date falls on a weekend, roll forward to Monday.
    """
    if target is None:
        d = datetime.now(_EST).date()
    elif isinstance(target, str):
        d = date.fromisoformat(target)
    else:
        d = target

    # Roll weekends → Monday
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _build_occ_symbol(root: str, expiration: date, side: str, strike: float) -> str:
    """Construct an OCC option symbol from its components.

    Format: ``ROOT YYMMDDC/PSTRIKE``
    Example: ``SPXW 260210C7040``

    *root* is cleaned: leading ``$`` and trailing ``.X`` / ``.`` are stripped.
    """
    clean = root.lstrip("$").split(".")[0].upper()
    exp_str = expiration.strftime("%y%m%d")
    cp = "C" if side.upper().startswith("C") else "P"
    strike_str = f"{strike:g}"
    return f"{clean} {exp_str}{cp}{strike_str}"


def _now_est() -> datetime:
    return datetime.now(_EST)


# ══════════════════════════════════════════════════════════════
# Options Chain Scheduler
# ══════════════════════════════════════════════════════════════

class OptionsChainScheduler:
    """
    Periodically streams the options chain for a given underlying,
    identifies credit-spread candidates filtered by delta and premium,
    and saves the results to ``options_data_config.json``.
    """

    SAVE_FILE = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "json", "options_data_config.json"
    )

    # ──────────────────────────────────────────────────────────
    # Construction
    # ──────────────────────────────────────────────────────────

    def __init__(self, cfg, token_mgr, logger: logging.Logger):
        self.cfg = cfg
        self.token_mgr = token_mgr
        self.log = logger

        # Pull tunables from config
        self._underlying: str       = cfg.options_underlying
        self._price_symbol: str     = cfg.symbol               # e.g. "$SPX.X"
        self._interval: int         = cfg.options_fetch_interval
        self._strike_proximity: int = cfg.options_strike_proximity
        self._widths: List[int]     = cfg.options_spread_widths
        self._max_delta: float      = cfg.options_max_delta
        self._min_premium: float    = cfg.options_min_premium
        self._max_premium: float    = cfg.options_max_premium

        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ──────────────────────────────────────────────────────────
    # Current price helper
    # ──────────────────────────────────────────────────────────

    def _get_current_price(self) -> Optional[float]:
        """Read latest SPX price – first from dashboard state, then API."""
        # Fast path: read from the state file the streamer already writes
        state_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "json", "dashboard_state.json"
        )
        try:
            with open(state_file) as f:
                state = json.load(f)
            price = _safe_float(state.get("price"), 0.0)
            if price > 0:
                self.log.debug("Current price from dashboard state: %.2f", price)
                return price
        except FileNotFoundError:
            self.log.debug("dashboard_state.json not found — trying API fallback.")
        except Exception as exc:
            self.log.debug("Could not read dashboard state: %s", exc)

        # Fallback: TradeStation quotes endpoint
        try:
            sym = requests.utils.quote(self._price_symbol, safe="")
            url = f"{self.cfg.base_url}/marketdata/quotes/{sym}"
            token = self.token_mgr.get_access_token()
            self.log.debug("Fetching price from API: %s", url)
            resp = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            resp.raise_for_status()
            quotes = resp.json().get("Quotes", [])
            if quotes:
                price = _safe_float(quotes[0].get("Last"))
                if price and price > 0:
                    self.log.debug("Current price from API: %.2f", price)
                    return price
                self.log.warning("API returned quote but Last price is invalid: %s", quotes[0].get("Last"))
            else:
                self.log.warning("API returned no quotes for %s", self._price_symbol)
        except Exception as exc:
            self.log.warning("Could not fetch current price via API: %s", exc)

        return None

    # ──────────────────────────────────────────────────────────
    # Streaming options-chain fetch
    # ──────────────────────────────────────────────────────────

    def _fetch_options_chain(
        self, trading_day: date
    ) -> Dict[str, Dict[float, OptionLeg]]:
        """
        Open a streaming connection, consume the initial snapshot, and
        return ``{"calls": {strike: OptionLeg, …}, "puts": {…}}``.

        Snapshot-complete detection mirrors the reference implementation:
        count unique (call + put) strike entries; when the count stops
        growing, the snapshot is done.
        """
        sym = requests.utils.quote(self._underlying, safe="")
        exp = trading_day.strftime("%Y-%m-%d")
        url = (
            f"{self.cfg.base_url}/marketdata/stream/options/chains/{sym}"
            f"?expiration={exp}"
            f"&strikeProximity={self._strike_proximity}"
            f"&optionType=ALL"
        )

        token = self.token_mgr.get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        calls: Dict[float, OptionLeg] = {}
        puts:  Dict[float, OptionLeg] = {}
        max_count = 0
        stagnant_updates = 0
        max_stagnant_updates = 30

        self.log.debug(
            "Options chain fetch: %s  exp=%s  proximity=%d  url=%s",
            self._underlying, exp, self._strike_proximity, url,
        )

        try:
            with requests.get(url, headers=headers, stream=True, timeout=(15, 40)) as resp:
                resp.raise_for_status()

                for raw_line in resp.iter_lines():
                    if self._thread is not None and not self._running:
                        break
                    if not raw_line:
                        continue

                    chunk = raw_line.decode("utf-8").strip()
                    if chunk.startswith("END") or chunk.startswith("ERROR"):
                        self.log.warning("Stream signal: %s", chunk)
                        break

                    try:
                        data = json.loads(chunk)
                    except json.JSONDecodeError:
                        continue

                    # Skip heartbeats / empty objects
                    if "Heartbeat" in data or not data:
                        # Still count toward staleness so we don't
                        # spin forever on heartbeats after the snapshot.
                        if max_count > 0:
                            stagnant_updates += 1
                            if stagnant_updates >= max_stagnant_updates:
                                break
                        continue

                    # Must have Side + Strikes to be an option entry
                    side = data.get("Side")
                    strikes_arr = data.get("Strikes")
                    if not side or not strikes_arr:
                        if max_count > 0:
                            stagnant_updates += 1
                            if stagnant_updates >= max_stagnant_updates:
                                break
                        continue

                    strike = _safe_float(strikes_arr[0])
                    if strike <= 0:
                        continue

                    raw_symbol = (data.get("Symbol") or "").strip()
                    if not raw_symbol:
                        raw_symbol = _build_occ_symbol(
                            self._underlying, trading_day, side, strike
                        )

                    leg = OptionLeg(
                        symbol=raw_symbol,
                        strike=strike,
                        side=side,
                        bid=_safe_float(data.get("Bid")),
                        ask=_safe_float(data.get("Ask")),
                        last=_safe_float(data.get("Last")),
                        delta=abs(_safe_float(data.get("Delta"))),
                        gamma=_safe_float(data.get("Gamma")),
                        iv=_safe_float(data.get("ImpliedVolatility")),
                        volume=_safe_float(data.get("Volume")),
                        open_interest=_safe_float(data.get("DailyOpenInterest")),
                    )

                    if side == "Call":
                        calls[strike] = leg
                    elif side == "Put":
                        puts[strike] = leg

                    # Snapshot-complete detection:
                    # New unique strike → reset counter; otherwise
                    # increment and break once we've seen enough
                    # non-new messages in a row.

                    current_count = len(calls) + len(puts)
                    if(len(calls) >self._strike_proximity and len(puts) > self._strike_proximity):
                        self.log.warning("Aborting options chain fetch: too many strikes received (calls=%d, puts=%d)", len(calls), len(puts))
                        break
                    # if current_count > max_count:
                    #     max_count = current_count
                    #     stagnant_updates = 0
                    # else:
                    #     stagnant_updates += 1
                    #     if stagnant_updates >= max_stagnant_updates:
                    #         break

        except requests.exceptions.HTTPError as exc:
            self.log.error("Options chain HTTP error: %s (url=%s)", exc, url)
            raise
        except requests.exceptions.Timeout:
            self.log.warning("Options chain request timed out for %s", url)
        except requests.exceptions.RequestException as exc:
            self.log.error("Options chain connection error: %s", exc)
            raise

        self.log.debug(
            "Options chain received: %d calls, %d puts", len(calls), len(puts)
        )
        return {"calls": calls, "puts": puts}

    # ──────────────────────────────────────────────────────────
    # Spread identification
    # ──────────────────────────────────────────────────────────

    def _find_credit_call_spreads(
        self, calls: Dict[float, OptionLeg], current_price: float
    ) -> List[SpreadCandidate]:
        """
        Bear-call credit spread: sell OTM call + buy higher-strike call.

        Filters:
          • short strike > current_price  (OTM)
          • short |delta| < max_delta
          • min_premium ≤ net_credit ≤ max_premium
        """
        candidates: List[SpreadCandidate] = []
        sorted_strikes = sorted(calls.keys())

        for short_strike in sorted_strikes:
            short = calls[short_strike]

            # Must be OTM
            if short_strike <= current_price:
                continue
            # Delta filter
            if short.delta >= self._max_delta:
                continue
            # Must have a tradeable bid
            if short.bid <= 0:
                continue

            for width in self._widths:
                long_strike = short_strike + width
                long = calls.get(long_strike)
                if long is None:
                    continue

                net_credit = round(short.bid - long.ask, 2)
                if net_credit < self._min_premium or net_credit > self._max_premium:
                    continue

                candidates.append(SpreadCandidate(
                    spread_type="bear_call",
                    short_strike=short_strike,
                    long_strike=long_strike,
                    width=width,
                    short_symbol=short.symbol,
                    long_symbol=long.symbol,
                    short_delta=round(short.delta, 4),
                    long_delta=round(long.delta, 4),
                    net_credit=net_credit,
                    short_bid=short.bid,
                    long_ask=long.ask,
                    max_risk=round((width - net_credit) * 100, 2),
                ))

        # Best credit first
        candidates.sort(key=lambda c: c.net_credit, reverse=True)
        return candidates

    def _find_credit_put_spreads(
        self, puts: Dict[float, OptionLeg], current_price: float
    ) -> List[SpreadCandidate]:
        """
        Bull-put credit spread: sell OTM put + buy lower-strike put.

        Filters:
          • short strike < current_price  (OTM)
          • short |delta| < max_delta
          • min_premium ≤ net_credit ≤ max_premium
        """
        candidates: List[SpreadCandidate] = []
        sorted_strikes = sorted(puts.keys(), reverse=True)

        for short_strike in sorted_strikes:
            short = puts[short_strike]

            if short_strike >= current_price:
                continue
            if short.delta >= self._max_delta:
                continue
            if short.bid <= 0:
                continue

            for width in self._widths:
                long_strike = short_strike - width
                long = puts.get(long_strike)
                if long is None:
                    continue

                net_credit = round(short.bid - long.ask, 2)
                if net_credit < self._min_premium or net_credit > self._max_premium:
                    continue

                candidates.append(SpreadCandidate(
                    spread_type="bull_put",
                    short_strike=short_strike,
                    long_strike=long_strike,
                    width=width,
                    short_symbol=short.symbol,
                    long_symbol=long.symbol,
                    short_delta=round(short.delta, 4),
                    long_delta=round(long.delta, 4),
                    net_credit=net_credit,
                    short_bid=short.bid,
                    long_ask=long.ask,
                    max_risk=round((width - net_credit) * 100, 2),
                ))

        candidates.sort(key=lambda c: c.net_credit, reverse=True)
        return candidates

    # ──────────────────────────────────────────────────────────
    # Main fetch-and-save cycle
    # ──────────────────────────────────────────────────────────

    def get_and_save_data(
        self,
        symbol: str | None = None,
        oDTE: bool = True,
        trading_day: str | date | None = None,
    ) -> dict:
        """
        Single cycle: fetch the options chain, identify filtered credit
        spreads, and persist to ``options_data_config.json``.

        Args:
            symbol:      Override underlying (default: config value)
            oDTE:        If True use today's expiration, else next weekly
            trading_day: Explicit trading day (date or ISO string)

        Returns:
            dict with keys: symbol, current_price, expiration, timestamp,
            credit_call_spreads, credit_put_spreads
        """
        if symbol:
            self._underlying = symbol.strip().upper()

        trading_day_date = _resolve_trading_day(trading_day)
        expiration = trading_day_date if oDTE else trading_day_date

        current_price = self._get_current_price()
        if current_price is None or current_price <= 0:
            self.log.warning(
                "Cannot determine current price — skipping cycle. "
                "(Is the bar-chart streamer running and writing dashboard_state.json?)"
            )
            return {}

        chain = self._fetch_options_chain(expiration)
        calls = chain["calls"]
        puts  = chain["puts"]

        if not calls and not puts:
            self.log.warning(
                "Empty options chain for %s exp=%s — skipping cycle. "
                "(Market may be closed or symbol may be invalid.)",
                self._underlying, expiration,
            )
            return {}

        call_spreads = self._find_credit_call_spreads(calls, current_price)
        put_spreads  = self._find_credit_put_spreads(puts, current_price)

        now = _now_est()
        result = {
            "symbol": self._underlying,
            "current_price": current_price,
            "expiration": expiration.strftime("%Y-%m-%d"),
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "filter_criteria": {
                "max_delta": self._max_delta,
                "min_premium": self._min_premium,
                "max_premium": self._max_premium,
                "spread_widths": self._widths,
            },
            "credit_call_spreads": [asdict(s) for s in call_spreads],
            "credit_put_spreads":  [asdict(s) for s in put_spreads],
        }

        # Atomic write
        try:
            tmp = self.SAVE_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(result, f, indent=2)
            os.replace(tmp, self.SAVE_FILE)
            self.log.info(
                "Options spreads saved → %d call, %d put  (SPX %.2f, exp %s)",
                len(call_spreads), len(put_spreads),
                current_price, expiration,
            )
        except Exception as exc:
            self.log.error("Failed to write %s: %s", self.SAVE_FILE, exc)

        return result

    # ──────────────────────────────────────────────────────────
    # Scheduler loop
    # ──────────────────────────────────────────────────────────

    def _run_loop(self):
        """Background loop — runs get_and_save_data every ``_interval`` seconds."""
        self.log.info(
            "Options chain scheduler started: %s  interval=%ds  "
            "delta<%.2f  premium=$%.2f–$%.2f  widths=%s",
            self._underlying, self._interval,
            self._max_delta, self._min_premium, self._max_premium,
            self._widths,
        )

        # Wait up to 30 seconds for a valid price to appear
        # (gives the bar-chart streamer time to write dashboard_state.json)
        waited = 0
        while self._running and waited < 30:
            price = self._get_current_price()
            if price is not None and price > 0:
                self.log.info("Options scheduler: initial price acquired (%.2f)", price)
                break
            time.sleep(2)
            waited += 2
            if waited % 10 == 0:
                self.log.info("Options scheduler: waiting for price data… (%ds)", waited)

        cycle = 0
        while self._running:
            cycle += 1
            self.log.debug("Options scheduler cycle #%d starting…", cycle)
            try:
                result = self.get_and_save_data()
                if not result:
                    self.log.debug("Cycle #%d: no data produced (see warnings above).", cycle)
            except requests.exceptions.HTTPError as exc:
                self.log.error("Options chain HTTP error: %s", exc)
                if exc.response is not None and exc.response.status_code == 401:
                    self.log.info("Token may have expired — will retry.")
            except requests.exceptions.RequestException as exc:
                self.log.error("Options chain connection error: %s", exc)
            except Exception as exc:
                self.log.exception("Options chain scheduler error: %s", exc)

            # Interruptible sleep
            for _ in range(self._interval * 10):
                if not self._running:
                    break
                time.sleep(0.1)

        self.log.info("Options chain scheduler stopped.")

    def start(self):
        """Start the scheduler in a daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, name="options-chain-scheduler", daemon=True
        )
        self._thread.start()

    def stop(self):
        """Signal the scheduler to stop."""
        self._running = False


# ══════════════════════════════════════════════════════════════
# Standalone entry point
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="SPX Options Chain Scheduler — streams chain and filters credit spreads"
    )
    parser.add_argument("--config", default="yaml/config.yaml")
    args = parser.parse_args()

    if not Path(args.config).exists():
        print(f"[ERROR] Config not found: {args.config}")
        sys.exit(1)

    # Reuse Config & TokenManager from spx_stream
    from spx_stream import Config, TokenManager, setup_logging

    cfg = Config(args.config)
    log = setup_logging(cfg)

    log.info("═" * 60)
    log.info("  Options Chain Scheduler (standalone)")
    log.info("  Underlying : %s", cfg.options_underlying)
    log.info("  Interval   : %ds", cfg.options_fetch_interval)
    log.info("  Delta      : < %.2f", cfg.options_max_delta)
    log.info("  Premium    : $%.2f – $%.2f", cfg.options_min_premium, cfg.options_max_premium)
    log.info("  Widths     : %s", cfg.options_spread_widths)
    log.info("═" * 60)

    token_mgr = TokenManager(cfg, log)
    scheduler = OptionsChainScheduler(cfg, token_mgr, log)

    try:
        scheduler._running = True
        scheduler._run_loop()
    except KeyboardInterrupt:
        log.info("Shutting down…")
        scheduler.stop()


if __name__ == "__main__":
    main()
