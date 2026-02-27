"""
spx_stream.py
─────────────
Streams 5-minute SPX candles from the TradeStation API.
Calculates RSI(9) in real-time.

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
import os
import signal
import sys
import threading
import time
import webbrowser
from collections import deque
from dataclasses import dataclass

from exhaustion_detector import ExhaustionDetector
from options_trader import OptionsTrader
from position_tracker import PositionTracker
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Deque, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse

import requests
import yaml


# ══════════════════════════════════════════════════════════════
# Config Loader
# ══════════════════════════════════════════════════════════════

class Config:
    """Loads and validates config.yaml, exposing typed helpers."""

    def __init__(self, path: str = "yaml/config.yaml"):
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
    def refresh_token(self) -> Optional[str]:
        return self._raw["credentials"].get("refresh_token")

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
    def rsi_overbought(self) -> float:
        return float(self._raw.get("rsi", {}).get("overbought", 70.0))

    @property
    def rsi_oversold(self) -> float:
        return float(self._raw.get("rsi", {}).get("oversold", 30.0))

    # ── Options Chain Scheduler ───────────────────────────────

    @property
    def options_scheduler_enabled(self) -> bool:
        return bool(self._raw.get("options_scheduler", {}).get("enabled", False))

    @property
    def options_underlying(self) -> str:
        return self._raw.get("options_scheduler", {}).get("underlying", "$SPXW.X")

    @property
    def options_fetch_interval(self) -> int:
        return int(self._raw.get("options_scheduler", {}).get("fetch_interval", 5))

    @property
    def options_strike_proximity(self) -> int:
        return int(self._raw.get("options_scheduler", {}).get("strike_proximity", 200))

    @property
    def options_spread_widths(self) -> list:
        return list(self._raw.get("options_scheduler", {}).get("spread_widths", [5, 10, 15, 20]))

    @property
    def options_max_delta(self) -> float:
        return float(self._raw.get("options_scheduler", {}).get("max_delta", 0.10))

    @property
    def options_min_premium(self) -> float:
        return float(self._raw.get("options_scheduler", {}).get("min_premium", 0.40))

    @property
    def options_max_premium(self) -> float:
        return float(self._raw.get("options_scheduler", {}).get("max_premium", 0.60))

    @property
    def options_default_quantity(self) -> int:
        return int(self._raw.get("options_scheduler", {}).get("default_quantity", 1))

    @property
    def options_profit_target_pct(self) -> float:
        return float(self._raw.get("options_scheduler", {}).get("profit_target_pct", 40))


# ══════════════════════════════════════════════════════════════
# Logging Setup
# ══════════════════════════════════════════════════════════════

def setup_logging(cfg: Config) -> logging.Logger:
    lc  = cfg.logging_cfg
    log = logging.getLogger("spx_stream")
    log.setLevel(getattr(logging, lc["level"].upper(), logging.INFO))

    import datetime, time as _time
    class _ESTFormatter(logging.Formatter):
        """Formatter that always logs in US/Eastern (EST/EDT)."""
        _tz = datetime.timezone(datetime.timedelta(hours=-5), "EST")
        def formatTime(self, record, datefmt=None):
            dt = datetime.datetime.fromtimestamp(record.created, tz=self._tz)
            if datefmt:
                return dt.strftime(datefmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")

    fmt = _ESTFormatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    if lc.get("log_to_file"):
        # Ensure log directory exists when using a nested path (e.g. ./log/spx_stream.log)
        log_path = lc["log_file"]
        log_dir = os.path.dirname(os.path.abspath(log_path))
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        fh = logging.handlers.RotatingFileHandler(
            log_path, maxBytes=lc["max_bytes"], backupCount=lc["backup_count"]
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
                self.log.debug("Refresh token found in saved file")
            else:
                self.log.warning("No refresh_token in saved token file!")
        else:
            self.log.info("No token file found at %s", p)
        
        # Use refresh token from config as fallback if not in saved token
        if not self._token.get("refresh_token") and self.cfg.refresh_token:
            self.log.info("Using refresh_token from config.yaml")
            self._token["refresh_token"] = self.cfg.refresh_token

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
            "scope":         "openid profile email MarketData ReadAccount Trade Matrix offline_access",
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
        
        # Check for refresh token in loaded token or config
        refresh_token = self._token.get("refresh_token") or self.cfg.refresh_token
        if refresh_token:
            self.log.info("Access token expired, attempting refresh...")
            # Ensure refresh token is in our token dict for the refresh method
            if not self._token.get("refresh_token"):
                self._token["refresh_token"] = refresh_token
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

class RSIAnalyzer:
    """Tracks RSI(period) bar-by-bar using Wilder's smoothing."""

    def __init__(self, cfg: Config, logger: logging.Logger):
        self.cfg = cfg
        self.log = logger

        self._period          = cfg.rsi_period
        self._overbought      = cfg.rsi_overbought
        self._oversold        = cfg.rsi_oversold

        # Wilder's state
        self._closes:   List[float]      = []   # bootstrap buffer
        self._avg_gain: Optional[float]  = None
        self._avg_loss: Optional[float]  = None
        self._rsi_ready                  = False
        self._last_close: Optional[float] = None

        # Closed-bar history for RSI display
        max_hist = self._period + 5
        self._history: Deque[BarSnapshot] = deque(maxlen=max_hist)

        # Current (live / open) bar tracking — for real-time RSI display
        self._live_close: Optional[float] = None

        # Bar-close detection: buffer the open bar until timestamp changes
        # TradeStation streaming does NOT send a Status field; all ticks
        # arrive with an empty status.  We detect bar closes by watching
        # for a new TimeStamp, which means the previous bar has closed.
        self._current_bar_ts: Optional[str] = None
        self._pending_bar: Optional[dict] = None  # last tick of the open bar

    # ──────────────────────────────────────────────────────────
    # Public feed API
    # ──────────────────────────────────────────────────────────

    def feed(self, candle: dict) -> None:
        """
        Feed a raw candle packet from the stream.

        Bar-close detection
        ───────────────────
        TradeStation streaming sends ticks with the *same* TimeStamp while
        a bar is open.  A *new* TimeStamp means the previous bar has closed.
        We buffer the open bar and only advance RSI state when a close is
        confirmed (by timestamp change or explicit "closed"/"historical"
        Status field).
        """
        ts     = candle.get("TimeStamp", "")
        close  = _safe_float(candle.get("Close"))
        high   = _safe_float(candle.get("High"))
        low    = _safe_float(candle.get("Low"))
        status = candle.get("Status", "").lower()

        if close is None or high is None or low is None:
            return

        is_closed = status in ("closed", "historical")

        # ── Detect implicit close: timestamp changed → previous bar closed ──
        if (not is_closed
                and self._current_bar_ts is not None
                and ts != self._current_bar_ts):
            self._close_pending_bar()

        # ── Explicitly closed / historical bar ───────────────────────────
        if is_closed:
            # Flush any buffered bar from a different timestamp first
            if self._pending_bar is not None and self._current_bar_ts != ts:
                self._close_pending_bar()

            rsi_val = self._step_rsi(close)
            self._live_close = None
            self._current_bar_ts = None
            self._pending_bar = None

            if rsi_val is not None:
                snap = BarSnapshot(ts, close, high, low, rsi_val)
                self._history.append(snap)
        else:
            # ── Open bar tick: buffer it, update live close for display ──
            self._current_bar_ts = ts
            self._pending_bar = {"ts": ts, "close": close, "high": high, "low": low}
            self._live_close = close

        return

    def _close_pending_bar(self) -> None:
        """Finalise the buffered open bar: advance RSI."""
        if self._pending_bar is None:
            return

        bar = self._pending_bar
        rsi_val = self._step_rsi(bar["close"])
        self._live_close = None
        self._pending_bar = None

        if rsi_val is not None:
            snap = BarSnapshot(bar["ts"], bar["close"], bar["high"], bar["low"], rsi_val)
            self._history.append(snap)

        return

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


    def rsi_status_str(self) -> str:
        """One-liner for bar log — shows provisional RSI during open bar, confirmed on close."""
        if not self._rsi_ready or not self._history:
            rem = self.warmup_bars_remaining()
            return f"RSI({self._period})=warming ({rem} bars left)"

        # Provisional RSI while bar is open; confirmed once it closes
        rsi = self.current_rsi()
        if rsi is None:
            rsi = self._history[-1].rsi
        return f"RSI({self._period})={rsi:6.2f}"


# ══════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════

def _safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ══════════════════════════════════════════════════════════════
# SPX Candle Streamer
# ══════════════════════════════════════════════════════════════

class SPXStreamer:
    """
    Streams 5-min SPX candles from TradeStation and pipes each bar
    through the RSIAnalyzer for real-time RSI logging.
    """

    _STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json", "dashboard_state.json")
    _STREAM_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json", "stream_data")

    def __init__(self, cfg: Config, token_mgr: TokenManager, logger: logging.Logger):
        self.cfg       = cfg
        self.token_mgr = token_mgr
        self.log       = logger
        self._running  = False
        self._response = None          # active streaming response (for clean shutdown)
        self._rsi      = RSIAnalyzer(cfg, logger)
        self._exhaustion = ExhaustionDetector()
        self._trader = OptionsTrader(cfg, token_mgr, logger)
        self._position_tracker = PositionTracker(cfg, token_mgr, logger)
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

        self._rsi.feed(candle)

        self.log.info(
            "[%-24s] O=%-9s H=%-9s L=%-9s C=%-9s Vol=%-8s [%-10s] | %s",
            ts, o, h, l, c, vol, status.upper(), self._rsi.rsi_status_str(),
        )

        # ── Persist candle to daily JSON file ────────────────
        daily_path = self._save_candle_to_daily_json(candle)

        # ── Run exhaustion detection on latest candle ────────
        exh = None
        if daily_path:
            try:
                exh = self._exhaustion.process_latest_candle(daily_path)
                if exh:
                    self.log.info(
                        "🔔 %s  strength=%d  reasons=%s",
                        exh["type"], exh["strength"], "; ".join(exh["reasons"]),
                    )
                    self._open_spread_on_exhaustion(exh)
            except Exception as exc:
                self.log.warning("Exhaustion detection error: %s", exc)

        # ── Monitor positions & auto-close at profit target ─
        self._position_tracker.monitor_and_close_profitable(
            self._trader,
            profit_target_pct=self.cfg.options_profit_target_pct,
        )

        # ── Update dashboard state file ──────────────────────
        self._write_dashboard_state(candle, exh)

    # ──────────────────────────────────────────────────────────
    # Auto-open spread on exhaustion
    # ──────────────────────────────────────────────────────────

    def _open_spread_on_exhaustion(self, exhaustion: dict) -> None:
        """Open a credit spread when an exhaustion signal is detected.

        * BULL_EXHAUSTION → open a **call** credit spread (expect reversal down)
        * BEAR_EXHAUSTION → open a **put** credit spread (expect reversal up)
        """
        sig_type = exhaustion.get("type", "")
        try:
            if sig_type == "BULL_EXHAUSTION":
                self.log.info("Bull exhaustion detected — opening call credit spread")
                self._trader.open_call_credit_spread()
            elif sig_type == "BEAR_EXHAUSTION":
                self.log.info("Bear exhaustion detected — opening put credit spread")
                self._trader.open_put_credit_spread()
            else:
                self.log.warning("Unknown exhaustion type: %s — skipping trade", sig_type)
        except Exception as exc:
            self.log.error("Failed to open spread on %s: %s", sig_type, exc)

    # ──────────────────────────────────────────────────────────
    # Dashboard State Writer
    # ──────────────────────────────────────────────────────────

    def _write_dashboard_state(self, candle: dict, exhaustion: Optional[dict] = None):
        """Write current state to a JSON file for the dashboard UI."""
        import datetime as _dt
        est = _dt.timezone(_dt.timedelta(hours=-5), "EST")
        now_est = _dt.datetime.now(est).strftime("%Y-%m-%d %H:%M:%S %Z")

        rsi_val = self._rsi.current_rsi()
        rsi_display = round(rsi_val, 2) if rsi_val is not None else None

        state = {
            "price": str(candle.get("Close", "—")),
            "rsi": rsi_display,
            "rsi_period": self._rsi._period,
            "timestamp": candle.get("TimeStamp", ""),
            "updated_at": now_est,
            "open": str(candle.get("Open", "—")),
            "high": str(candle.get("High", "—")),
            "low": str(candle.get("Low", "—")),
            "close": str(candle.get("Close", "—")),
            "volume": str(candle.get("TotalVolume", "—")),
            "status": candle.get("Status", ""),
            "exhaustion": exhaustion,
        }
        try:
            tmp = self._STATE_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(state, f)
            os.replace(tmp, self._STATE_FILE)
        except Exception:
            pass  # Non-critical; don't disrupt streaming

    # ──────────────────────────────────────────────────────────
    # Daily candle persistence
    # ──────────────────────────────────────────────────────────

    def _save_candle_to_daily_json(self, candle: dict) -> Optional[str]:
        """Append a candle record to the day's JSON file under stream_data/.
        Returns the filepath on success, None on failure."""
        import datetime as _dt

        ts_str = candle.get("TimeStamp", "")
        est_tz = _dt.timezone(_dt.timedelta(hours=-5), "EST")

        try:
            # Parse the ISO-8601 timestamp and convert to EST
            dt_utc = _dt.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            dt_est = dt_utc.astimezone(est_tz)
        except (ValueError, AttributeError):
            dt_est = _dt.datetime.now(est_tz)

        date_key = dt_est.strftime("%Y-%m-%d")
        ts_est_str = dt_est.strftime("%Y-%m-%d %I:%M:%S %p")

        os.makedirs(self._STREAM_DATA_DIR, exist_ok=True)
        filepath = os.path.join(self._STREAM_DATA_DIR, f"{date_key}.json")

        # Build the record to persist
        rsi_val = self._rsi.current_rsi()
        record = {
            "TimeStamp":   ts_est_str,
            "Open":        candle.get("Open", ""),
            "High":        candle.get("High", ""),
            "Low":         candle.get("Low", ""),
            "Close":       candle.get("Close", ""),
            "TotalVolume": candle.get("TotalVolume", ""),
            "Status":      candle.get("Status", ""),
            "RSI":         round(rsi_val, 4) if rsi_val is not None else None,
        }

        try:
            # Read existing array, upsert by TimeStamp, rewrite
            if os.path.exists(filepath):
                with open(filepath, "r") as f:
                    try:
                        data = json.load(f)
                    except json.JSONDecodeError:
                        data = []
            else:
                data = []

            # Ensure we always have at most one record per candle timestamp
            replaced = False
            for idx, existing in enumerate(data):
                if existing.get("TimeStamp") == ts_est_str:
                    data[idx] = record
                    replaced = True
                    break

            if not replaced:
                data.append(record)

            tmp = filepath + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, filepath)
            return filepath
        except Exception as e:
            self.log.warning("Failed to save candle to %s: %s", filepath, e)
            return None

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
                "Timestamp", "Open", "High", "Low", "Close", "Volume", "Status", f"RSI({self.cfg.rsi_period})",
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
    p = argparse.ArgumentParser(description="Stream SPX 5-min candles with RSI analysis")
    p.add_argument("--config", default="yaml/config.yaml")
    return p.parse_args()


def main():
    args = parse_args()
    if not Path(args.config).exists():
        print(f"[ERROR] Config not found: {args.config}")
        sys.exit(1)

    cfg = Config(args.config)
    log = setup_logging(cfg)

    log.info("═" * 70)
    log.info("  SPX Candle Streamer + RSI(%d) Monitor", cfg.rsi_period)
    log.info("  Mode    : %s", cfg.account_mode.upper())
    log.info("  Symbol  : %s   Interval: %d-min", cfg.symbol, cfg.bar_interval)
    log.info("  RSI     : period=%d  |  OB=%.0f  OS=%.0f",
             cfg.rsi_period, cfg.rsi_overbought, cfg.rsi_oversold)
    log.info("═" * 70)

    token_mgr = TokenManager(cfg, log)
    streamer  = SPXStreamer(cfg, token_mgr, log)

    # ── Start embedded dashboard server in a background thread ──
    from dashboard import app as dashboard_app
    import logging as _logging
    # Suppress Flask/Werkzeug request logs to keep streamer output clean
    _logging.getLogger("werkzeug").setLevel(_logging.WARNING)
    dashboard_port = 5050
    dashboard_thread = threading.Thread(
        target=lambda: dashboard_app.run(
            host="0.0.0.0", port=dashboard_port, debug=False, use_reloader=False
        ),
        daemon=True,
    )
    dashboard_thread.start()
    log.info("📊 Dashboard started at http://localhost:%d", dashboard_port)
    threading.Timer(2.0, lambda: webbrowser.open(f"http://localhost:{dashboard_port}")).start()

    # ── Start options-chain scheduler in a background thread ──
    options_scheduler = None
    if cfg.options_scheduler_enabled:
        from options_chain_scheduler import OptionsChainScheduler
        options_scheduler = OptionsChainScheduler(cfg, token_mgr, log)
        options_scheduler.start()
        log.info("📋 Options chain scheduler started (every %ds)", cfg.options_fetch_interval)
    else:
        log.info("Options chain scheduler is disabled in config.")

    shutdown_event = threading.Event()
    
    def _sigint(sig, frame):
        log.info("\n🛑 Shutdown signal received (SIGINT)...")
        if options_scheduler:
            options_scheduler.stop()
        streamer.stop()
        shutdown_event.set()
        
    def _sigterm(sig, frame):
        log.info("\n🛑 Shutdown signal received (SIGTERM)...")
        if options_scheduler:
            options_scheduler.stop()
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
