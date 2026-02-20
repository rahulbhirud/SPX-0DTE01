"""
spx_stream.py
─────────────
Streams 5-minute SPX candles from the TradeStation API.
Calculates RSI(9) in real-time and monitors for:
  • RSI Plateau      — RSI value barely moves across N consecutive bars
  • RSI Divergence   — price makes new high/low while RSI does NOT (Bullish / Bearish)

Usage:
    python spx_stream.py                  # uses config.yaml in same folder
    python spx_stream.py --config /path/to/config.yaml

OAuth flow (first run):
    The script opens a browser for login, captures the authorization code via
    a local callback server, and saves the token to disk for reuse.

Requirements:
    pip install requests PyYAML
"""

import argparse
import json
import logging
import logging.handlers
import sys
import threading
import time
import webbrowser
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

import requests
import yaml


# ══════════════════════════════════════════════════════════════
# Config Loader
# ══════════════════════════════════════════════════════════════

class Config:
    """Loads and validates config.yaml, exposing typed helpers."""

    def __init__(self, path: str = "config.yaml"):
        with open(path, "r") as f:
            self._raw = yaml.safe_load(f)

    # ── Account / Auth ────────────────────────────────────────

    @property
    def account_mode(self) -> str:
        return self._raw["account_mode"].lower()

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
    def account_id(self) -> str:
        return self._raw["accounts"][self.account_mode]

    @property
    def auth_url(self) -> str:
        return self._raw["endpoints"]["auth_url"]

    @property
    def token_url(self) -> str:
        return self._raw["endpoints"]["token_url"]

    @property
    def base_url(self) -> str:
        return self._raw["endpoints"]["base_url"]

    # ── Streaming ─────────────────────────────────────────────

    @property
    def symbol(self) -> str:
        return self._raw["streaming"]["symbol"]

    @property
    def bar_unit(self) -> str:
        return self._raw["streaming"]["bar_unit"]

    @property
    def bar_interval(self) -> int:
        return int(self._raw["streaming"]["bar_interval"])

    @property
    def bars_back(self) -> int:
        return int(self._raw["streaming"]["bars_back"])

    @property
    def session_template(self) -> str:
        return self._raw["streaming"]["session_template"]

    # ── Token / Logging / Reconnection ───────────────────────

    @property
    def token_path(self) -> str:
        return self._raw["token"]["storage_path"]

    @property
    def reconnection(self) -> dict:
        return self._raw["reconnection"]

    @property
    def logging_cfg(self) -> dict:
        return self._raw["logging"]

    # ── RSI Analysis (all values pulled from config.yaml) ─────

    @property
    def rsi_period(self) -> int:
        return int(self._raw.get("rsi", {}).get("period", 9))

    @property
    def rsi_plateau_window(self) -> int:
        """Consecutive *closed* bars to examine for plateau."""
        return int(self._raw.get("rsi", {}).get("plateau_window", 4))

    @property
    def rsi_plateau_threshold(self) -> float:
        """Max (RSI_max - RSI_min) across the window to call a plateau."""
        return float(self._raw.get("rsi", {}).get("plateau_threshold", 1.5))

    @property
    def rsi_divergence_lookback(self) -> int:
        """How many bars back to compare for divergence."""
        return int(self._raw.get("rsi", {}).get("divergence_lookback", 10))

    @property
    def rsi_divergence_min_price_move_pct(self) -> float:
        """Min % price move (high or low) to qualify a new extreme."""
        return float(self._raw.get("rsi", {}).get("divergence_min_price_move_pct", 0.1))

    @property
    def rsi_divergence_min_rsi_delta(self) -> float:
        """RSI must diverge by at least this many points."""
        return float(self._raw.get("rsi", {}).get("divergence_min_rsi_delta", 2.0))

    @property
    def rsi_overbought(self) -> float:
        return float(self._raw.get("rsi", {}).get("overbought", 70.0))

    @property
    def rsi_oversold(self) -> float:
        return float(self._raw.get("rsi", {}).get("oversold", 30.0))

    # ── Spread Trading ────────────────────────────────────────

    @property
    def spread_underlying(self) -> str:
        return self._raw.get("spreads", {}).get("underlying", "SPXW")

    @property
    def spread_max_delta(self) -> float:
        return float(self._raw.get("spreads", {}).get("max_delta", 10))

    @property
    def spread_max_premium(self) -> float:
        return float(self._raw.get("spreads", {}).get("max_premium", 0.60))

    @property
    def spread_widths(self) -> List[int]:
        return list(self._raw.get("spreads", {}).get("widths", [5, 10, 20]))

    @property
    def spread_quantity(self) -> int:
        return int(self._raw.get("spreads", {}).get("quantity", 1))

    @property
    def spread_order_type(self) -> str:
        return self._raw.get("spreads", {}).get("order_type", "Limit")

    @property
    def spread_time_in_force(self) -> str:
        return self._raw.get("spreads", {}).get("time_in_force", "Day")

    @property
    def spread_dry_run(self) -> bool:
        return bool(self._raw.get("spreads", {}).get("dry_run", True))


# ══════════════════════════════════════════════════════════════
# Logging Setup
# ══════════════════════════════════════════════════════════════

def setup_logging(cfg: Config) -> logging.Logger:
    lc  = cfg.logging_cfg
    log = logging.getLogger("spx_stream")
    log.setLevel(getattr(logging, lc["level"].upper(), logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    if lc.get("log_to_file"):
        fh = logging.handlers.RotatingFileHandler(
            lc["log_file"], maxBytes=lc["max_bytes"], backupCount=lc["backup_count"]
        )
        fh.setFormatter(fmt)
        log.addHandler(fh)

    return log


# ══════════════════════════════════════════════════════════════
# OAuth 2.0 – Auth-Code Flow
# ══════════════════════════════════════════════════════════════

class OAuthCallbackHandler(BaseHTTPRequestHandler):
    auth_code: Optional[str] = None

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        OAuthCallbackHandler.auth_code = params.get("code", [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"<h2>Authorization successful! You can close this tab.</h2>")

    def log_message(self, *args):
        pass


class TokenManager:
    def __init__(self, cfg: Config, logger: logging.Logger):
        self.cfg = cfg
        self.log = logger
        self._token: dict = {}
        self._load_token()

    def _load_token(self):
        p = Path(self.cfg.token_path)
        if p.exists():
            with open(p) as f:
                self._token = json.load(f)
            self.log.info("Loaded saved token from %s", p)
            self.log.debug("Token keys: %s", list(self._token.keys()))
            if "refresh_token" in self._token:
                self.log.debug("Refresh token found")
            else:
                self.log.warning("No refresh_token in saved token file!")
        else:
            self.log.info("No token file found at %s", p)

    def _save_token(self):
        with open(self.cfg.token_path, "w") as f:
            json.dump(self._token, f, indent=2)

    def _is_valid(self) -> bool:
        has_token = bool(self._token.get("access_token"))
        expires_at = self._token.get("expires_at", 0)
        current_time = time.time()
        expires_in_seconds = expires_at - current_time
        is_not_expired = current_time < expires_at - 60  # 60 second buffer
        
        self.log.debug("Token validation: has_token=%s, expires_in=%.0fs, is_valid=%s", 
                      has_token, expires_in_seconds, has_token and is_not_expired)
        
        return has_token and is_not_expired

    def _authorize(self) -> str:
        params = {
            "response_type": "code",
            "client_id":     self.cfg.client_id,
            "redirect_uri":  self.cfg.redirect_uri,
            "audience":      "https://api.tradestation.com",
            "scope":         "openid profile email MarketData ReadAccount Trade Matrix OptionSpreads offline_access",
        }
        self.log.info("Starting OAuth authorization with scopes: %s", params["scope"])
        webbrowser.open(f"{self.cfg.auth_url}?{urlencode(params)}")
        port   = int(urlparse(self.cfg.redirect_uri).port or 3000)
        server = HTTPServer(("localhost", port), OAuthCallbackHandler)
        self.log.info("Waiting for OAuth callback on port %d…", port)
        while OAuthCallbackHandler.auth_code is None:
            server.handle_request()
        code = OAuthCallbackHandler.auth_code
        OAuthCallbackHandler.auth_code = None
        return code

    def _exchange_code(self, code: str):
        self.log.info("Exchanging authorization code for tokens...")
        r = requests.post(self.cfg.token_url, data={
            "grant_type": "authorization_code", "code": code,
            "redirect_uri": self.cfg.redirect_uri,
            "client_id": self.cfg.client_id, "client_secret": self.cfg.client_secret,
        }, timeout=15)
        r.raise_for_status()
        
        response_data = r.json()
        self.log.debug("OAuth token exchange response keys: %s", list(response_data.keys()))
        
        self._store(response_data)

    def _refresh(self):
        self.log.info("Refreshing access token using refresh_token...")
        refresh_token = self._token.get("refresh_token")
        if not refresh_token:
            raise Exception("No refresh_token available")
            
        self.log.debug("Sending refresh request with refresh_token")
        r = requests.post(self.cfg.token_url, data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.cfg.client_id, "client_secret": self.cfg.client_secret,
        }, timeout=15)
        r.raise_for_status()
        
        response_data = r.json()
        self.log.debug("Token refresh response keys: %s", list(response_data.keys()))
        
        self._store(response_data)

    def _store(self, data: dict):
        # Preserve existing refresh token if not provided in new response
        existing_refresh_token = self._token.get("refresh_token")
        
        data["expires_at"] = time.time() + data.get("expires_in", 1200)
        
        # Update token data
        self._token = data
        
        # Restore refresh token if it wasn't in the response but we had one before
        if not data.get("refresh_token") and existing_refresh_token:
            self.log.debug("Preserving existing refresh_token")
            self._token["refresh_token"] = existing_refresh_token
        
        self._save_token()
        
        # Log what we received and stored
        token_info = {
            "access_token": "present" if self._token.get("access_token") else "missing",
            "refresh_token": "present" if self._token.get("refresh_token") else "missing", 
            "expires_in": data.get("expires_in", "unknown"),
            "scopes": data.get("scope", "unknown")
        }
        self.log.info("Token stored: %s", token_info)
        
        if not self._token.get("refresh_token"):
            self.log.warning("⚠️  No refresh_token available! Future token renewals will require re-authorization.")

    def get_access_token(self) -> str:
        self.log.debug("Getting access token...")
        if self._is_valid():
            self.log.debug("Using existing valid token")
            return self._token["access_token"]
        
        if self._token.get("refresh_token"):
            self.log.info("Access token expired, attempting refresh...")
            try:
                self._refresh()
                self.log.info("Token refreshed successfully")
                return self._token["access_token"]
            except Exception as e:
                self.log.warning("Token refresh failed (%s); re-authorizing…", e)
        else:
            self.log.warning("No refresh token available, starting new OAuth flow...")
            
        self.log.info("Starting new OAuth authorization flow")
        self._exchange_code(self._authorize())
        return self._token["access_token"]


# ══════════════════════════════════════════════════════════════
# Data Structures
# ══════════════════════════════════════════════════════════════

@dataclass
class BarSnapshot:
    """Immutable snapshot of one *closed* bar, used for RSI analysis."""
    timestamp: str
    close: float
    high: float
    low: float
    rsi: float


@dataclass
class RSISignal:
    kind: str        # "PLATEAU" | "BEARISH_DIVERGENCE" | "BULLISH_DIVERGENCE"
    timestamp: str
    rsi_value: float
    price: float
    detail: str


# ══════════════════════════════════════════════════════════════
# RSI Engine  (Wilder's smoothing — identical to TradingView RSI)
# ══════════════════════════════════════════════════════════════

class RSIAnalyzer:
    """
    Tracks RSI(period) bar-by-bar and emits RSISignal events.

    ┌─────────────────────────────────────────────────────────┐
    │  PLATEAU                                                │
    │  RSI stays within a tight band (≤ plateau_threshold)   │
    │  for `plateau_window` consecutive closed bars.          │
    │  Most meaningful near overbought / oversold zones.      │
    ├─────────────────────────────────────────────────────────┤
    │  BEARISH DIVERGENCE                                     │
    │  Price makes a higher high vs. N bars ago,              │
    │  but RSI makes a lower high → momentum is waning.      │
    ├─────────────────────────────────────────────────────────┤
    │  BULLISH DIVERGENCE                                     │
    │  Price makes a lower low vs. N bars ago,                │
    │  but RSI makes a higher low → selling pressure fading.  │
    └─────────────────────────────────────────────────────────┘
    """

    def __init__(self, cfg: Config, logger: logging.Logger):
        self.cfg = cfg
        self.log = logger

        self._period          = cfg.rsi_period
        self._plateau_window  = cfg.rsi_plateau_window
        self._plateau_thr     = cfg.rsi_plateau_threshold
        self._div_lookback    = cfg.rsi_divergence_lookback
        self._min_price_move  = cfg.rsi_divergence_min_price_move_pct / 100.0
        self._min_rsi_delta   = cfg.rsi_divergence_min_rsi_delta
        self._overbought      = cfg.rsi_overbought
        self._oversold        = cfg.rsi_oversold

        # Wilder's state
        self._closes:   List[float]      = []   # bootstrap buffer
        self._avg_gain: Optional[float]  = None
        self._avg_loss: Optional[float]  = None
        self._rsi_ready                  = False
        self._last_close: Optional[float] = None

        # Closed-bar history for plateau / divergence checks
        max_hist = max(self._plateau_window, self._div_lookback) + 5
        self._history: Deque[BarSnapshot] = deque(maxlen=max_hist)

        # Current (live / open) bar tracking — for real-time RSI display
        self._live_close: Optional[float] = None

        # Dedup: avoid firing the same signal on consecutive bars
        self._last_plateau_bar: Optional[str] = None
        self._last_div_bar:     Optional[str] = None

    # ──────────────────────────────────────────────────────────
    # Public feed API
    # ──────────────────────────────────────────────────────────

    def feed(self, candle: dict) -> List[RSISignal]:
        """
        Feed a raw candle packet from the stream.
        Returns any RSISignal events generated (may be empty).
        """
        ts     = candle.get("TimeStamp", "")
        close  = _safe_float(candle.get("Close"))
        high   = _safe_float(candle.get("High"))
        low    = _safe_float(candle.get("Low"))
        status = candle.get("Status", "").lower()   # "historical"|"open"|"closed"

        if close is None or high is None or low is None:
            return []

        signals: List[RSISignal] = []

        if status in ("closed", "historical", ""):
            # Finalise RSI for this bar
            # Empty status covers historical backfill bars from TradeStation
            rsi_val = self._step_rsi(close)
            self._live_close = None

            if rsi_val is not None:
                snap = BarSnapshot(ts, close, high, low, rsi_val)
                self._history.append(snap)
                signals += self._check_plateau(snap)
                signals += self._check_divergence(snap)
        else:
            # Open / historical: keep live close for interim RSI display
            self._live_close = close

        return signals

    # ──────────────────────────────────────────────────────────
    # RSI helpers
    # ──────────────────────────────────────────────────────────

    def _step_rsi(self, close: float) -> Optional[float]:
        """Advance Wilder's smoothing by one bar. Returns RSI or None."""
        if self._last_close is None:
            self._last_close = close
            self._closes.append(close)
            return None

        change = close - self._last_close
        gain   = max(change, 0.0)
        loss   = max(-change, 0.0)
        self._last_close = close

        if not self._rsi_ready:
            self._closes.append(close)
            # Need period+1 prices → period changes
            if len(self._closes) < self._period + 1:
                return None
            # Seed with simple average of first `period` changes
            deltas = [self._closes[i] - self._closes[i-1] for i in range(1, len(self._closes))]
            gains  = [max(d, 0) for d in deltas[-self._period:]]
            losses = [max(-d, 0) for d in deltas[-self._period:]]
            self._avg_gain = sum(gains) / self._period
            self._avg_loss = sum(losses) / self._period
            self._rsi_ready = True
        else:
            self._avg_gain = (self._avg_gain * (self._period - 1) + gain) / self._period
            self._avg_loss = (self._avg_loss * (self._period - 1) + loss) / self._period

        return self._rsi_value()

    def _rsi_value(self) -> float:
        if self._avg_loss == 0:
            return 100.0
        return round(100 - 100 / (1 + self._avg_gain / self._avg_loss), 4)

    def current_rsi(self) -> Optional[float]:
        """
        Provisional RSI based on the live (open) bar's current close.
        Purely for display — does NOT advance internal state.
        """
        if not self._rsi_ready or self._live_close is None or self._last_close is None:
            if not self._rsi_ready:
                return None
            # Return last confirmed RSI when bar hasn't moved yet
            return self._history[-1].rsi if self._history else None

        change = self._live_close - self._last_close
        g = max(change, 0.0)
        l = max(-change, 0.0)
        ag = (self._avg_gain * (self._period - 1) + g) / self._period
        al = (self._avg_loss * (self._period - 1) + l) / self._period
        if al == 0:
            return 100.0
        return round(100 - 100 / (1 + ag / al), 4)

    def warmup_bars_remaining(self) -> int:
        """How many more closed bars before RSI is ready."""
        if self._rsi_ready:
            return 0
        return max(0, self._period + 1 - len(self._closes))

    # ──────────────────────────────────────────────────────────
    # Plateau Detection
    # ──────────────────────────────────────────────────────────

    def _check_plateau(self, latest: BarSnapshot) -> List[RSISignal]:
        """
        Plateau = RSI high - RSI low across the last `plateau_window` bars
        is within `plateau_threshold`.
        """
        if len(self._history) < self._plateau_window:
            return []

        window     = list(self._history)[-self._plateau_window:]
        rsi_values = [b.rsi for b in window]
        rsi_range  = max(rsi_values) - min(rsi_values)

        if rsi_range > self._plateau_thr:
            return []
        if self._last_plateau_bar == latest.timestamp:
            return []
        self._last_plateau_bar = latest.timestamp

        avg_rsi = sum(rsi_values) / len(rsi_values)
        zone    = self._zone_label(avg_rsi)

        return [RSISignal(
            kind="PLATEAU",
            timestamp=latest.timestamp,
            rsi_value=round(avg_rsi, 2),
            price=latest.close,
            detail=(
                f"RSI range={rsi_range:.2f} pts over {self._plateau_window} bars "
                f"(avg RSI={avg_rsi:.2f}) {zone}"
            ),
        )]

    # ──────────────────────────────────────────────────────────
    # Divergence Detection
    # ──────────────────────────────────────────────────────────

    def _check_divergence(self, latest: BarSnapshot) -> List[RSISignal]:
        """
        Compare latest bar vs. the reference bar `divergence_lookback` bars ago.

        Bearish: price High > ref High  AND  RSI < ref RSI  (by min_rsi_delta)
        Bullish: price Low  < ref Low   AND  RSI > ref RSI  (by min_rsi_delta)
        """
        if len(self._history) < self._div_lookback:
            return []
        if self._last_div_bar == latest.timestamp:
            return []

        hist = list(self._history)
        ref  = hist[-(self._div_lookback)]   # reference pivot bar

        # ── Bearish ───────────────────────────────────────────
        price_hh = latest.high > ref.high * (1 + self._min_price_move)
        rsi_lh   = (ref.rsi - latest.rsi) >= self._min_rsi_delta

        if price_hh and rsi_lh:
            self._last_div_bar = latest.timestamp
            return [RSISignal(
                kind="BEARISH_DIVERGENCE",
                timestamp=latest.timestamp,
                rsi_value=latest.rsi,
                price=latest.close,
                detail=(
                    f"High: {ref.high:.2f} → {latest.high:.2f} "
                    f"(+{(latest.high/ref.high - 1)*100:.2f}%)  |  "
                    f"RSI: {ref.rsi:.2f} → {latest.rsi:.2f} "
                    f"(−{ref.rsi - latest.rsi:.2f} pts)  "
                    f"over {self._div_lookback} bars"
                ),
            )]

        # ── Bullish ───────────────────────────────────────────
        price_ll = latest.low < ref.low * (1 - self._min_price_move)
        rsi_hl   = (latest.rsi - ref.rsi) >= self._min_rsi_delta

        if price_ll and rsi_hl:
            self._last_div_bar = latest.timestamp
            return [RSISignal(
                kind="BULLISH_DIVERGENCE",
                timestamp=latest.timestamp,
                rsi_value=latest.rsi,
                price=latest.close,
                detail=(
                    f"Low: {ref.low:.2f} → {latest.low:.2f} "
                    f"(−{(1 - latest.low/ref.low)*100:.2f}%)  |  "
                    f"RSI: {ref.rsi:.2f} → {latest.rsi:.2f} "
                    f"(+{latest.rsi - ref.rsi:.2f} pts)  "
                    f"over {self._div_lookback} bars"
                ),
            )]

        return []

    # ──────────────────────────────────────────────────────────
    # Utility
    # ──────────────────────────────────────────────────────────

    def _zone_label(self, rsi: float) -> str:
        if rsi >= self._overbought:
            return "⚠ OVERBOUGHT"
        if rsi <= self._oversold:
            return "⚠ OVERSOLD"
        return ""

    def rsi_status_str(self) -> str:
        """One-liner for bar log — shows live RSI and zone tag."""
        rsi = self.current_rsi()
        if rsi is None:
            rem = self.warmup_bars_remaining()
            return f"RSI(9)=warming ({rem} bars left)"
        return f"RSI(9)={rsi:6.2f}  {self._zone_label(rsi)}"


# ══════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════

def _safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ══════════════════════════════════════════════════════════════
# 0DTE Credit Spread Trader
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


# ══════════════════════════════════════════════════════════════
# SPX Candle Streamer
# ══════════════════════════════════════════════════════════════

class SPXStreamer:
    """
    Streams 5-min SPX candles from TradeStation and pipes each bar
    through the RSIAnalyzer for real-time RSI, plateau, and divergence logging.
    """

    def __init__(self, cfg: Config, token_mgr: TokenManager, logger: logging.Logger):
        self.cfg       = cfg
        self.token_mgr = token_mgr
        self.log       = logger
        self._running  = False
        self._response = None          # active streaming response (for clean shutdown)
        self._rsi      = RSIAnalyzer(cfg, logger)
        self.spreads   = OptionSpreadTrader(cfg, token_mgr, logger)
        self._shutdown_event = threading.Event()  # For interruptible sleeps

    # ──────────────────────────────────────────────────────────
    # Candle handler
    # ──────────────────────────────────────────────────────────

    def on_candle(self, candle: dict):
        ts     = candle.get("TimeStamp", "")
        o      = candle.get("Open", "—")
        h      = candle.get("High", "—")
        l      = candle.get("Low", "—")
        c      = candle.get("Close", "—")
        vol    = candle.get("TotalVolume", "—")
        status = candle.get("Status", "").lower()

        signals = self._rsi.feed(candle)

        self.log.info(
            "[%-24s] O=%-9s H=%-9s L=%-9s C=%-9s Vol=%-8s [%-10s] | %s",
            ts, o, h, l, c, vol, status.upper(), self._rsi.rsi_status_str(),
        )

        for sig in signals:
            self._emit_signal(sig)

    # ──────────────────────────────────────────────────────────
    # Signal logger
    # ──────────────────────────────────────────────────────────

    _BORDER = "═" * 74

    def _emit_signal(self, sig: RSISignal):
        B = self._BORDER

        if sig.kind == "PLATEAU":
            self.log.warning(B)
            self.log.warning(
                "  📊  RSI PLATEAU  |  %s  |  RSI(9)=%.2f  |  Price=%.2f",
                sig.timestamp, sig.rsi_value, sig.price,
            )
            self.log.warning("  %s", sig.detail)
            self.log.warning(
                "  ⤷ RSI momentum has stalled — watch for breakout or reversal."
            )
            self.log.warning(B)

        elif sig.kind == "BEARISH_DIVERGENCE":
            self.log.warning(B)
            self.log.warning(
                "  🔻  BEARISH DIVERGENCE  |  %s  |  RSI(9)=%.2f  |  Price=%.2f",
                sig.timestamp, sig.rsi_value, sig.price,
            )
            self.log.warning("  %s", sig.detail)
            self.log.warning(
                "  ⤷ Price printing higher highs but RSI weakening → "
                "bullish momentum is fading; potential reversal ahead."
            )
            self.log.warning(B)

        elif sig.kind == "BULLISH_DIVERGENCE":
            self.log.warning(B)
            self.log.warning(
                "  🔺  BULLISH DIVERGENCE  |  %s  |  RSI(9)=%.2f  |  Price=%.2f",
                sig.timestamp, sig.rsi_value, sig.price,
            )
            self.log.warning("  %s", sig.detail)
            self.log.warning(
                "  ⤷ Price printing lower lows but RSI recovering → "
                "selling pressure is fading; potential bounce ahead."
            )
            self.log.warning(B)

    def on_heartbeat(self):
        self.log.debug("♥ heartbeat")

    def on_error(self, msg: str):
        self.log.error("Stream error: %s", msg)

    # ──────────────────────────────────────────────────────────
    # Streaming plumbing
    # ──────────────────────────────────────────────────────────

    def _build_stream_url(self) -> str:
        sym = requests.utils.quote(self.cfg.symbol, safe="")
        return (
            f"{self.cfg.base_url}/marketdata/stream/barcharts/{sym}"
            f"?unit={self.cfg.bar_unit}"
            f"&interval={self.cfg.bar_interval}"
            f"&barsBack={self.cfg.bars_back}"
            f"&sessionTemplate={self.cfg.session_template}"
        )

    def _stream_once(self):
        url   = self._build_stream_url()
        token = self.token_mgr.get_access_token()
        hdrs  = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.tradestation.streams.v2+json",
        }

        self.log.info(
            "Connecting | mode=%-4s | account=%s | symbol=%s | %d-min | RSI(%d)",
            self.cfg.account_mode.upper(), self.cfg.account_id,
            self.cfg.symbol, self.cfg.bar_interval, self.cfg.rsi_period,
        )

        with requests.get(url, headers=hdrs, stream=True, timeout=(15, 30)) as resp:
            self._response = resp
            resp.raise_for_status()
            self.log.info("Stream connected. Receiving candles…")
            self.log.info("─" * 100)
            self.log.info(
                "[%-24s] %-9s %-9s %-9s %-9s %-8s %-12s | %s",
                "Timestamp", "Open", "High", "Low", "Close", "Volume", "Status", "RSI(9)",
            )
            self.log.info("─" * 100)

            for raw in resp.iter_lines():
                if not self._running:
                    break
                if raw is None or not raw:
                    continue
                line = raw.decode("utf-8").strip()
                if line.startswith("data:"):
                    line = line[5:].strip()
                if not line or line == "{}":
                    self.on_heartbeat()
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    self.log.warning("Non-JSON: %s", line)
                    continue

                if "Heartbeat" in data:
                    self.on_heartbeat()
                elif "Error" in data:
                    self.on_error(data.get("Message", str(data)))
                else:
                    self.on_candle(data)

    def stream(self):
        self._running = True
        self._shutdown_event.clear()
        rc = self.cfg.reconnection
        attempt, delay = 0, rc["retry_delay_seconds"]

        while self._running:
            try:
                self._stream_once()
                if not self._running:
                    self.log.info("Shutdown requested during stream - exiting.")
                    break
                if self._running:
                    self.log.warning("Stream ended unexpectedly. Reconnecting…")
            except requests.exceptions.HTTPError as e:
                self.log.error("HTTP %s", e)
                if e.response is not None and e.response.status_code == 401:
                    self.log.info("Token expired; will re-authorize.")
            except requests.exceptions.RequestException as e:
                self.log.error("Connection error: %s", e)
            except (AttributeError, OSError) as e:
                # Expected when stop() closes the response mid-read
                if not self._running:
                    self.log.info("Stream closed during shutdown.")
                    break  # Exit the retry loop on clean shutdown
                else:
                    self.log.exception("Unexpected: %s", e)
            except Exception as e:
                self.log.exception("Unexpected: %s", e)

            # Critical: Check shutdown before any delays
            if not self._running:
                self.log.info("Stream shutdown requested - exiting retry loop.")
                break
            if not rc["enabled"]:
                self.log.info("Reconnection disabled - exiting.")
                break
            attempt += 1
            if attempt > rc["max_retries"]:
                self.log.error("Max retries reached. Exiting.")
                break
                
            # Interruptible sleep - can be woken up by shutdown
            self.log.info("Reconnect %d/%d in %ds…", attempt, rc["max_retries"], delay)
            if self._shutdown_event.wait(timeout=delay):  # Returns True if set before timeout
                self.log.info("Shutdown signal received during reconnect delay.")
                break
            
            # Double-check after sleep
            if not self._running:
                self.log.info("Shutdown detected after reconnect delay.")
                break
                
            delay = min(delay * 2, rc["max_delay_seconds"])
        
        self.log.info("Stream loop exited.")

    def stop(self):
        self.log.info("Stopping stream…")
        self._running = False
        self._shutdown_event.set()  # Wake up any sleeping reconnect delays
        
        # Close the active response to unblock iter_lines()
        if self._response is not None:
            try:
                self._response.close()
                self.log.debug("Stream response closed.")
            except Exception as e:
                self.log.debug(f"Exception closing response: {e}")
        
        self.log.info("Stream stopped successfully.")


# ══════════════════════════════════════════════════════════════
# Entry Point
# ══════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description="Stream SPX 5-min candles with RSI(9) analysis")
    p.add_argument("--config", default="config.yaml")
    return p.parse_args()


def main():
    args = parse_args()
    if not Path(args.config).exists():
        print(f"[ERROR] Config not found: {args.config}")
        sys.exit(1)

    cfg = Config(args.config)
    log = setup_logging(cfg)

    log.info("═" * 70)
    log.info("  SPX Candle Streamer + RSI(9) Monitor")
    log.info("  Mode    : %s", cfg.account_mode.upper())
    log.info("  Symbol  : %s   Interval: %d-min", cfg.symbol, cfg.bar_interval)
    log.info("  RSI     : period=%d  |  OB=%.0f  OS=%.0f",
             cfg.rsi_period, cfg.rsi_overbought, cfg.rsi_oversold)
    log.info("  Plateau : window=%d bars  threshold=±%.2f RSI pts",
             cfg.rsi_plateau_window, cfg.rsi_plateau_threshold)
    log.info("  Divergence: lookback=%d bars  min_price=%.2f%%  min_rsi_delta=%.1f pts",
             cfg.rsi_divergence_lookback,
             cfg.rsi_divergence_min_price_move_pct,
             cfg.rsi_divergence_min_rsi_delta)
    log.info("═" * 70)

    token_mgr = TokenManager(cfg, log)
    streamer  = SPXStreamer(cfg, token_mgr, log)

    import signal
    import threading
    
    shutdown_event = threading.Event()
    
    def _sigint(sig, frame):
        log.info("\n🛑 Shutdown signal received (SIGINT)...")
        streamer.stop()
        shutdown_event.set()
        
    def _sigterm(sig, frame):
        log.info("\n🛑 Shutdown signal received (SIGTERM)...")
        streamer.stop()
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigterm)
    
    try:
        log.info("Press Ctrl+C to stop streaming...")
        streamer.stream()
    except KeyboardInterrupt:
        log.info("\n🛑 KeyboardInterrupt caught - shutting down...")
        streamer.stop()
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        streamer.stop()
    finally:
        log.info("🏁 Done. Goodbye.")


if __name__ == "__main__":
    main()
