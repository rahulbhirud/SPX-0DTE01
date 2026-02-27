"""
backtest.py
───────────
Standalone SPX back-testing module.
Fetches historical 5-minute bars from the TradeStation REST API,
calculates RSI at each candle close, and writes results to a CSV.

Usage:
    python backtest.py --years 1          # back-test last 1 year
    python backtest.py --years 2          # back-test last 2 years
    python backtest.py --years 5 --period 14   # RSI(14) over 5 years
    python backtest.py --years 1 --symbol '$SPX.X' --output my_results.csv

Requirements:
    pip install requests PyYAML
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse

import requests
import yaml

# ══════════════════════════════════════════════════════════════
# Constants
# ══════════════════════════════════════════════════════════════

EST = timezone(timedelta(hours=-5))

# TradeStation allows a max of ~57,600 bars in a single barchart
# request.  For 5-min bars during regular hours (~78/day),
# 57,600 bars ≈ 738 trading days ≈ ~2.9 years.
# We paginate by date windows to support arbitrary durations.
MAX_BARS_PER_REQUEST = 57_600

CONFIG_PATH = "yaml/config.yaml"
TOKEN_PATH = "./json/ts_token.json"

LOG = logging.getLogger("backtest")


# ══════════════════════════════════════════════════════════════
# Logging (standalone)
# ══════════════════════════════════════════════════════════════

def _setup_logging(level: str = "INFO") -> None:
    """Configure console logging with EST timestamps."""

    class _ESTFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, tz=EST)
            return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")

    fmt = _ESTFormatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(fmt)
    LOG.setLevel(getattr(logging, level.upper(), logging.INFO))
    LOG.addHandler(handler)


# ══════════════════════════════════════════════════════════════
# Config Loader (self-contained copy)
# ══════════════════════════════════════════════════════════════

class BacktestConfig:
    """Reads config.yaml – only the fields this module needs."""

    def __init__(self, path: str = CONFIG_PATH):
        with open(path, "r") as f:
            self._raw = yaml.safe_load(f)

    @property
    def client_id(self) -> str:
        return self._raw["credentials"]["client_id"]

    @property
    def client_secret(self) -> str:
        return self._raw["credentials"]["client_secret"]

    @property
    def redirect_uri(self) -> str:
        return self._raw["credentials"]["redirect_uri"]

    @property
    def refresh_token(self) -> Optional[str]:
        return self._raw["credentials"].get("refresh_token")

    @property
    def auth_url(self) -> str:
        return self._raw["endpoints"]["auth_url"]

    @property
    def token_url(self) -> str:
        return self._raw["endpoints"]["token_url"]

    @property
    def base_url(self) -> str:
        return self._raw["endpoints"]["base_url"]

    @property
    def token_path(self) -> str:
        return self._raw.get("token", {}).get("storage_path", TOKEN_PATH)


# ══════════════════════════════════════════════════════════════
# OAuth 2.0 Token Manager (self-contained copy)
# ══════════════════════════════════════════════════════════════

class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    auth_code: Optional[str] = None

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        _OAuthCallbackHandler.auth_code = params.get("code", [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"<h2>Authorization successful! You can close this tab.</h2>")

    def log_message(self, *args):
        pass  # silence default HTTP log


class BacktestTokenManager:
    """Handles TradeStation OAuth2 – refresh / auth-code flow."""

    def __init__(self, cfg: BacktestConfig):
        self.cfg = cfg
        self._token: dict = {}
        self._load_token()

    # ── persistence ───────────────────────────────────────────

    def _load_token(self):
        p = Path(self.cfg.token_path)
        if p.exists():
            with open(p) as f:
                self._token = json.load(f)
            LOG.info("Loaded saved token from %s", p)
        if not self._token.get("refresh_token") and self.cfg.refresh_token:
            LOG.info("Using refresh_token from config.yaml")
            self._token["refresh_token"] = self.cfg.refresh_token

    def _save_token(self):
        with open(self.cfg.token_path, "w") as f:
            json.dump(self._token, f, indent=2)

    # ── validation ────────────────────────────────────────────

    def _is_valid(self) -> bool:
        has_token = bool(self._token.get("access_token"))
        expires_at = self._token.get("expires_at", 0)
        return has_token and time.time() < expires_at - 60

    # ── auth flows ────────────────────────────────────────────

    def _authorize(self) -> str:
        params = {
            "response_type": "code",
            "client_id": self.cfg.client_id,
            "redirect_uri": self.cfg.redirect_uri,
            "audience": "https://api.tradestation.com",
            "scope": "openid profile MarketData ReadAccount Trade Matrix offline_access",
        }
        webbrowser.open(f"{self.cfg.auth_url}?{urlencode(params)}")
        port = int(urlparse(self.cfg.redirect_uri).port or 3000)
        server = HTTPServer(("localhost", port), _OAuthCallbackHandler)
        LOG.info("Waiting for OAuth callback on port %d …", port)
        while _OAuthCallbackHandler.auth_code is None:
            server.handle_request()
        code = _OAuthCallbackHandler.auth_code
        _OAuthCallbackHandler.auth_code = None
        return code

    def _exchange_code(self, code: str):
        r = requests.post(self.cfg.token_url, data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.cfg.redirect_uri,
            "client_id": self.cfg.client_id,
            "client_secret": self.cfg.client_secret,
        }, timeout=15)
        r.raise_for_status()
        self._store(r.json())

    def _refresh(self):
        rt = self._token.get("refresh_token")
        if not rt:
            raise RuntimeError("No refresh_token available")
        r = requests.post(self.cfg.token_url, data={
            "grant_type": "refresh_token",
            "refresh_token": rt,
            "client_id": self.cfg.client_id,
            "client_secret": self.cfg.client_secret,
        }, timeout=15)
        r.raise_for_status()
        self._store(r.json())

    def _store(self, data: dict):
        existing_rt = self._token.get("refresh_token")
        data["expires_at"] = time.time() + data.get("expires_in", 1200)
        self._token = data
        if not data.get("refresh_token") and existing_rt:
            self._token["refresh_token"] = existing_rt
        self._save_token()

    # ── public entry point ────────────────────────────────────

    def get_access_token(self) -> str:
        if self._is_valid():
            return self._token["access_token"]
        rt = self._token.get("refresh_token") or self.cfg.refresh_token
        if rt:
            if not self._token.get("refresh_token"):
                self._token["refresh_token"] = rt
            try:
                self._refresh()
                LOG.info("Token refreshed successfully")
                return self._token["access_token"]
            except Exception as exc:
                LOG.warning("Token refresh failed (%s); re-authorizing …", exc)
        self._exchange_code(self._authorize())
        return self._token["access_token"]


# ══════════════════════════════════════════════════════════════
# RSI Calculator (self-contained – Wilder's smoothing)
# ══════════════════════════════════════════════════════════════

class RSICalculator:
    """
    Stateful RSI using Wilder's smoothed moving average.
    Feed successive close prices via `push(close)`.
    Returns the RSI value once the warm-up period is met, else None.
    """

    def __init__(self, period: int = 9):
        self.period = period
        self._closes: List[float] = []
        self._avg_gain: Optional[float] = None
        self._avg_loss: Optional[float] = None
        self._ready = False
        self._last_close: Optional[float] = None

    def push(self, close: float) -> Optional[float]:
        """Feed one bar close. Returns RSI or None during warm-up."""
        if self._last_close is None:
            self._last_close = close
            self._closes.append(close)
            return None

        change = close - self._last_close
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        self._last_close = close

        if not self._ready:
            self._closes.append(close)
            if len(self._closes) < self.period + 1:
                return None
            deltas = [self._closes[i] - self._closes[i - 1]
                      for i in range(1, len(self._closes))]
            gains = [max(d, 0) for d in deltas[-self.period:]]
            losses = [max(-d, 0) for d in deltas[-self.period:]]
            self._avg_gain = sum(gains) / self.period
            self._avg_loss = sum(losses) / self.period
            self._ready = True
        else:
            self._avg_gain = (self._avg_gain * (self.period - 1) + gain) / self.period
            self._avg_loss = (self._avg_loss * (self.period - 1) + loss) / self.period

        if self._avg_loss == 0:
            return 100.0
        return round(100.0 - 100.0 / (1.0 + self._avg_gain / self._avg_loss), 4)


# ══════════════════════════════════════════════════════════════
# TradeStation Historical Bars Fetcher
# ══════════════════════════════════════════════════════════════

class BarFetcher:
    """
    Fetches historical 5-minute bars from the TradeStation v3 REST API.

    Endpoint:
        GET /v3/marketdata/barcharts/{symbol}
        Params: interval, unit, firstdate, lastdate, sessiontemplate
    """

    def __init__(self, token_mgr: BacktestTokenManager, base_url: str):
        self.token_mgr = token_mgr
        self.base_url = base_url.rstrip("/")

    def fetch_bars(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: int = 5,
        unit: str = "Minute",
        session_template: str = "Default",
    ) -> List[dict]:
        """
        Fetch all 5-min bars between *start_date* and *end_date*.
        Automatically paginates in ~6-month windows to stay within
        the TradeStation per-request bar limit.
        """
        all_bars: List[dict] = []
        window_start = start_date

        while window_start < end_date:
            # Use 6-month windows to stay well within limits
            window_end = min(window_start + timedelta(days=180), end_date)

            bars = self._fetch_window(
                symbol, window_start, window_end, interval, unit, session_template,
            )
            all_bars.extend(bars)
            LOG.info(
                "Fetched %d bars for %s → %s  (total so far: %d)",
                len(bars),
                window_start.strftime("%Y-%m-%d"),
                window_end.strftime("%Y-%m-%d"),
                len(all_bars),
            )
            window_start = window_end

        # Sort by timestamp to guarantee chronological order
        all_bars.sort(key=lambda b: b.get("TimeStamp", ""))
        return all_bars

    def _fetch_window(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        interval: int,
        unit: str,
        session_template: str,
    ) -> List[dict]:
        encoded_sym = requests.utils.quote(symbol, safe="")
        url = f"{self.base_url}/marketdata/barcharts/{encoded_sym}"
        params = {
            "interval": str(interval),
            "unit": unit,
            "firstdate": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lastdate": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sessiontemplate": session_template,
        }

        token = self.token_mgr.get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        LOG.debug("GET %s  params=%s", url, params)
        resp = requests.get(url, params=params, headers=headers, timeout=60)

        if resp.status_code == 401:
            # Token may have expired mid-run; retry once after refresh
            LOG.warning("401 received – refreshing token and retrying …")
            token = self.token_mgr.get_access_token()
            headers = {"Authorization": f"Bearer {token}"}
            resp = requests.get(url, params=params, headers=headers, timeout=60)

        resp.raise_for_status()
        data = resp.json()
        return data.get("Bars", [])


# ══════════════════════════════════════════════════════════════
# Back-Tester
# ══════════════════════════════════════════════════════════════

class SPXBacktester:
    """
    Standalone SPX back-tester.

    1. Loads historical 5-min bars via TradeStation REST API.
    2. Computes RSI at each candle close.
    3. Writes results to a CSV file.
    """

    def __init__(
        self,
        years: int = 1,
        rsi_period: int = 9,
        symbol: str = "$SPX.X",
        output_csv: str = "",
        config_path: str = CONFIG_PATH,
    ):
        self.years = years
        self.rsi_period = rsi_period
        self.symbol = symbol
        self.config_path = config_path

        # Default output filename includes params for traceability
        if output_csv:
            self.output_csv = output_csv
        else:
            tag = datetime.now(tz=EST).strftime("%Y%m%d_%H%M%S")
            self.output_csv = f"backtest_{symbol.replace('$', '').replace('.', '')}_{years}y_RSI{rsi_period}_{tag}.csv"

        self.cfg = BacktestConfig(config_path)
        self.token_mgr = BacktestTokenManager(self.cfg)
        self.bar_fetcher = BarFetcher(self.token_mgr, self.cfg.base_url)
        self.rsi = RSICalculator(period=rsi_period)

    # ── main entry point ──────────────────────────────────────

    def run(self) -> str:
        """Execute the full back-test. Returns the path to the output CSV."""
        LOG.info("═" * 60)
        LOG.info("SPX Back-Tester")
        LOG.info("  Symbol      : %s", self.symbol)
        LOG.info("  Period      : %d year(s)", self.years)
        LOG.info("  RSI Period  : %d", self.rsi_period)
        LOG.info("  Output CSV  : %s", self.output_csv)
        LOG.info("═" * 60)

        # ── 1. Determine date range ──────────────────────────
        end_date = datetime.now(tz=timezone.utc)
        start_date = end_date - timedelta(days=self.years * 365)
        LOG.info(
            "Fetching bars from %s to %s",
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )

        # ── 2. Fetch bars ────────────────────────────────────
        bars = self.bar_fetcher.fetch_bars(
            symbol=self.symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if not bars:
            LOG.error("No bars returned from TradeStation – aborting.")
            return ""
        LOG.info("Total bars fetched: %d", len(bars))

        # ── 3. Calculate RSI & write CSV ─────────────────────
        rows_written = self._process_bars_to_csv(bars)
        LOG.info("Wrote %d data rows to %s", rows_written, self.output_csv)
        LOG.info("═" * 60)
        LOG.info("Back-test complete.")
        return self.output_csv

    # ── internal helpers ──────────────────────────────────────

    @staticmethod
    def _parse_ts_timestamp(ts_str: str) -> datetime:
        """
        Parse a TradeStation timestamp string into a timezone-aware
        datetime in US/Eastern (EST).

        TradeStation returns timestamps in ISO-8601 format, typically
        UTC (e.g. '2025-02-24T14:30:00Z') or with offset.
        """
        ts_str = ts_str.strip()
        # Handle 'Z' suffix → UTC
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(ts_str)
        except ValueError:
            # Fallback for formats like '2025-02-24T14:30:00+0000'
            dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S%z")

        # Convert to EST
        return dt.astimezone(EST)

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        """Convert a value to float safely; return None on failure."""
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _process_bars_to_csv(self, bars: List[dict]) -> int:
        """
        Two-pass CSV generation:
          Pass 1 – compute RSI for every bar, group rows by trading day,
                   and determine each day's closing price.
          Pass 2 – walk each day's bars detecting RSI crossing episodes,
                   mark Bull / Bear entries from the 2nd episode onward,
                   compute P&L (day close − entry candle close), and write CSV.
        """
        # ── Pass 1: build per-day data ────────────────────────
        # Each element: (time_str, open, close, rsi, date_key, dt_est)
        ProcessedBar = Dict  # just a dict for convenience
        days: Dict[str, List[dict]] = {}       # date_key → list of bar dicts
        day_close: Dict[str, float] = {}       # date_key → last bar close

        for bar in bars:
            open_price = self._safe_float(bar.get("Open"))
            close_price = self._safe_float(bar.get("Close"))
            ts_str = bar.get("TimeStamp", "")

            if close_price is None or open_price is None or not ts_str:
                continue

            rsi_val = self.rsi.push(close_price)

            try:
                dt_est = self._parse_ts_timestamp(ts_str)
                time_str = dt_est.strftime("%Y-%m-%d %H:%M:%S EST")
                date_key = dt_est.strftime("%Y-%m-%d")
            except Exception:
                time_str = ts_str
                date_key = ts_str[:10]  # fallback

            row = {
                "time_str": time_str,
                "open": open_price,
                "close": close_price,
                "rsi": rsi_val,
                "date_key": date_key,
            }
            days.setdefault(date_key, []).append(row)
            # Continuously overwrite → last bar in the day wins
            day_close[date_key] = close_price

        # ── Pass 2: detect entry signals & write CSV ──────────
        BULL_RSI_THRESHOLD = 32.0
        BEAR_RSI_THRESHOLD = 68.0

        rows_written = 0
        with open(self.output_csv, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                "Date & Time (EST)",
                "Candle Open",
                "Candle Close",
                "Close RSI",
                "Bull Entry",
                "Bull P&L",
                "Bear Entry",
                "Bear P&L",
            ])

            for date_key in sorted(days.keys()):
                day_bars = days[date_key]
                eod_close = day_close[date_key]

                # Per-day crossing state
                bull_episode_count = 0   # how many distinct dips below threshold
                in_bull_zone = False     # currently below threshold?
                bear_episode_count = 0   # how many distinct spikes above threshold
                in_bear_zone = False     # currently above threshold?

                for row in day_bars:
                    rsi_val = row["rsi"]
                    close_price = row["close"]

                    bull_entry = ""
                    bull_pnl = ""
                    bear_entry = ""
                    bear_pnl = ""

                    if rsi_val is not None:
                        # ── Bull logic: RSI < threshold ───────
                        if rsi_val < BULL_RSI_THRESHOLD:
                            if not in_bull_zone:
                                # New crossing into oversold territory
                                bull_episode_count += 1
                                in_bull_zone = True
                            # Mark from 2nd episode onward
                            if bull_episode_count >= 2:
                                bull_entry = "Bull Entry"
                                bull_pnl = f"{eod_close - close_price:.2f}"
                        else:
                            in_bull_zone = False

                        # ── Bear logic: RSI > threshold ───────
                        if rsi_val > BEAR_RSI_THRESHOLD:
                            if not in_bear_zone:
                                # New crossing into overbought territory
                                bear_episode_count += 1
                                in_bear_zone = True
                            # Mark from 2nd episode onward
                            if bear_episode_count >= 2:
                                bear_entry = "Bear Entry"
                                bear_pnl = f"{eod_close - close_price:.2f}"
                        else:
                            in_bear_zone = False

                    writer.writerow([
                        row["time_str"],
                        f"{row['open']:.2f}",
                        f"{close_price:.2f}",
                        f"{rsi_val:.4f}" if rsi_val is not None else "",
                        bull_entry,
                        bull_pnl,
                        bear_entry,
                        bear_pnl,
                    ])
                    rows_written += 1

        return rows_written


# ══════════════════════════════════════════════════════════════
# CLI Entry Point
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="SPX Back-Tester — fetch historical 5-min bars, compute RSI, export CSV.",
    )
    parser.add_argument(
        "--years", type=int, required=True,
        help="Number of years of historical data to back-test.",
    )
    parser.add_argument(
        "--period", type=int, default=9,
        help="RSI period (default: 9).",
    )
    parser.add_argument(
        "--symbol", type=str, default="$SPX.X",
        help='TradeStation symbol (default: "$SPX.X").',
    )
    parser.add_argument(
        "--output", type=str, default="",
        help="Output CSV filename (auto-generated if omitted).",
    )
    parser.add_argument(
        "--config", type=str, default=CONFIG_PATH,
        help="Path to config.yaml (default: ./config.yaml).",
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )

    args = parser.parse_args()
    _setup_logging(args.log_level)

    bt = SPXBacktester(
        years=args.years,
        rsi_period=args.period,
        symbol=args.symbol,
        output_csv=args.output,
        config_path=args.config,
    )
    csv_path = bt.run()
    if csv_path:
        print(f"\n✅  Results written to: {csv_path}")
    else:
        print("\n❌  Back-test produced no output.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
