import argparse
import csv
import json
import logging
import sys
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse
from zoneinfo import ZoneInfo

import requests
import yaml


# ============================================================
# CONSTANTS
# ============================================================
RSI_PERIOD        = 14
RSI_OVERBOUGHT    = 70
RSI_OVERSOLD      = 30
VOLUME_AVG_PERIOD = 20
ATR_PERIOD        = 14
LOOKBACK          = 10    # candles to scan for swing high/low
MIN_THRUST_BARS   = 3     # consecutive directional bars
DIVERGENCE_BARS   = 5     # bars to check RSI divergence

EST = timezone(timedelta(hours=-5))
MAX_BARS_PER_REQUEST = 57_600
CONFIG_PATH = "config.yaml"
TOKEN_PATH  = "./json/ts_token.json"

LOG = logging.getLogger("back_test_ex")


# ============================================================
# Logging
# ============================================================

def _setup_logging(level: str = "INFO") -> None:
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


# ============================================================
# Config Loader (self-contained)
# ============================================================

class _Config:
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


# ============================================================
# OAuth 2.0 Token Manager (self-contained)
# ============================================================

class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    auth_code: Optional[str] = None

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        _OAuthCallbackHandler.auth_code = params.get("code", [None])[0]
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"<h2>Authorization successful! You can close this tab.</h2>")

    def log_message(self, *args):
        pass


class _TokenManager:
    def __init__(self, cfg: _Config):
        self.cfg = cfg
        self._token: dict = {}
        self._load_token()

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

    def _is_valid(self) -> bool:
        has_token = bool(self._token.get("access_token"))
        expires_at = self._token.get("expires_at", 0)
        return has_token and time.time() < expires_at - 60

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


# ============================================================
# TradeStation Bar Fetcher (self-contained)
# ============================================================

class _BarFetcher:
    def __init__(self, token_mgr: _TokenManager, base_url: str):
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
        all_bars: List[dict] = []
        window_start = start_date

        while window_start < end_date:
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
            LOG.warning("401 received – refreshing token and retrying …")
            token = self.token_mgr.get_access_token()
            headers = {"Authorization": f"Bearer {token}"}
            resp = requests.get(url, params=params, headers=headers, timeout=60)

        resp.raise_for_status()
        data = resp.json()
        return data.get("Bars", [])


# ============================================================
# BackTestEx
# ============================================================

class BackTestEx:
    def __init__(self, years, symbol="$SPX.X", config_path=CONFIG_PATH):
        self.years = years
        self.symbol = symbol
        self.bars = []
        self.results = []

        # Initialise TradeStation connectivity
        self.cfg = _Config(config_path)
        self.token_mgr = _TokenManager(self.cfg)
        self.bar_fetcher = _BarFetcher(self.token_mgr, self.cfg.base_url)

    # ============================================================
    # DATA FETCHING
    # ============================================================

    @staticmethod
    def _parse_ts_timestamp(ts_str: str) -> datetime:
        """Parse TradeStation timestamp → timezone-aware datetime (EST)."""
        ts_str = ts_str.strip()
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(ts_str)
        except ValueError:
            dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S%z")
        return dt.astimezone(EST)

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def fetch_5min_bars(self):
        """Fetch 5-min SPX bars from TradeStation REST API for self.years."""
        end_date = datetime.now(tz=timezone.utc)
        start_date = end_date - timedelta(days=self.years * 365)
        LOG.info(
            "Fetching %s 5-min bars from %s to %s",
            self.symbol,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )

        raw_bars = self.bar_fetcher.fetch_bars(
            symbol=self.symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if not raw_bars:
            LOG.error("No bars returned from TradeStation – aborting.")
            sys.exit(1)

        LOG.info("Total bars fetched: %d", len(raw_bars))

        for bar in raw_bars:
            o = self._safe_float(bar.get("Open"))
            h = self._safe_float(bar.get("High"))
            l = self._safe_float(bar.get("Low"))
            c = self._safe_float(bar.get("Close"))
            v = self._safe_float(bar.get("TotalVolume", 0))
            ts = bar.get("TimeStamp", "")

            if o is None or h is None or l is None or c is None or not ts:
                continue

            dt_est = self._parse_ts_timestamp(ts)

            self.bars.append({
                'datetime': dt_est,
                'open': o,
                'high': h,
                'low': l,
                'close': c,
                'volume': v or 0,
                'rsi': None,
                'atr': None,
                'avgVolume': None,
            })

    # ============================================================
    # INDICATOR CALCULATIONS
    # ============================================================

    def calculate_rsi(self, period=RSI_PERIOD):
        closes = [bar['close'] for bar in self.bars]
        rsi_values = [None] * len(closes)
        for i in range(period, len(closes)):
            gains = 0
            losses = 0
            for j in range(i - period + 1, i + 1):
                change = closes[j] - closes[j - 1]
                if change > 0:
                    gains += change
                else:
                    losses -= change
            avg_gain = gains / period
            avg_loss = losses / period
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))
            rsi_values[i] = rsi
        return rsi_values

    def calculate_atr(self, period=ATR_PERIOD):
        atr_values = [None] * len(self.bars)
        for i in range(1, len(self.bars)):
            prev_close = self.bars[i - 1]['close']
            high = self.bars[i]['high']
            low  = self.bars[i]['low']
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            if i >= period:
                trs = []
                for j in range(i - period + 1, i + 1):
                    pc = self.bars[j - 1]['close']
                    h = self.bars[j]['high']
                    l = self.bars[j]['low']
                    trs.append(max(h - l, abs(h - pc), abs(l - pc)))
                atr_values[i] = sum(trs) / period
        return atr_values

    def calculate_avg_volume(self, period=VOLUME_AVG_PERIOD):
        avg_vols = [None] * len(self.bars)
        for i in range(period - 1, len(self.bars)):
            vols = [self.bars[j]['volume'] for j in range(i - period + 1, i + 1)]
            avg_vols[i] = sum(vols) / period
        return avg_vols

    def attach_indicators(self):
        """Pre-compute RSI, ATR, avgVolume and attach to each bar."""
        rsi_vals = self.calculate_rsi()
        atr_vals = self.calculate_atr()
        avg_vol_vals = self.calculate_avg_volume()
        for i, bar in enumerate(self.bars):
            bar['rsi'] = rsi_vals[i]
            bar['atr'] = atr_vals[i]
            bar['avgVolume'] = avg_vol_vals[i]

    # ============================================================
    # HELPER FUNCTIONS
    # ============================================================

    @staticmethod
    def is_bullish(candle):
        return candle['close'] > candle['open']

    @staticmethod
    def is_bearish(candle):
        return candle['close'] < candle['open']

    @staticmethod
    def body_size(candle):
        return abs(candle['close'] - candle['open'])

    @staticmethod
    def upper_wick(candle):
        return candle['high'] - max(candle['open'], candle['close'])

    @staticmethod
    def lower_wick(candle):
        return min(candle['open'], candle['close']) - candle['low']

    @staticmethod
    def candle_range(candle):
        return candle['high'] - candle['low']

    @staticmethod
    def is_volume_climax(candle):
        if candle['avgVolume'] is None or candle['avgVolume'] == 0:
            return False
        return candle['volume'] > (candle['avgVolume'] * 2.0)

    @staticmethod
    def recent_highest(candles, n):
        return max(c['high'] for c in candles[-n:])

    @staticmethod
    def recent_lowest(candles, n):
        return min(c['low'] for c in candles[-n:])

    # ============================================================
    # PATTERN DETECTORS
    # ============================================================

    def is_shooting_star(self, candle):
        """Shooting Star / Pin Bar → Bull Exhaustion signal."""
        rng = self.candle_range(candle)
        if rng == 0:
            return False
        upper_wick_ratio = self.upper_wick(candle) / rng
        body_ratio = self.body_size(candle) / rng
        return upper_wick_ratio >= 0.60 and body_ratio <= 0.25

    def is_hammer(self, candle):
        """Hammer / Pin Bar → Bear Exhaustion signal."""
        rng = self.candle_range(candle)
        if rng == 0:
            return False
        lower_wick_ratio = self.lower_wick(candle) / rng
        body_ratio = self.body_size(candle) / rng
        return lower_wick_ratio >= 0.60 and body_ratio <= 0.25

    def is_bearish_engulfing(self, prev, curr):
        return (self.is_bullish(prev) and self.is_bearish(curr)
                and curr['open'] >= prev['close']
                and curr['close'] <= prev['open']
                and self.body_size(curr) > self.body_size(prev))

    def is_bullish_engulfing(self, prev, curr):
        return (self.is_bearish(prev) and self.is_bullish(curr)
                and curr['open'] <= prev['close']
                and curr['close'] >= prev['open']
                and self.body_size(curr) > self.body_size(prev))

    # ────── RSI Divergence ──────

    @staticmethod
    def has_bearish_divergence(candles, n=DIVERGENCE_BARS):
        """Price making higher highs but RSI making lower highs."""
        half = n // 2
        if len(candles) < n or half == 0:
            return False
        recent = candles[-half:]
        earlier = candles[-n:-half]
        if not recent or not earlier:
            return False
        price_high_now  = max(c['high'] for c in recent)
        price_high_prev = max(c['high'] for c in earlier)
        rsi_now  = [c['rsi'] for c in recent  if c['rsi'] is not None]
        rsi_prev = [c['rsi'] for c in earlier if c['rsi'] is not None]
        if not rsi_now or not rsi_prev:
            return False
        return price_high_now > price_high_prev and max(rsi_now) < max(rsi_prev)

    @staticmethod
    def has_bullish_divergence(candles, n=DIVERGENCE_BARS):
        """Price making lower lows but RSI making higher lows."""
        half = n // 2
        if len(candles) < n or half == 0:
            return False
        recent = candles[-half:]
        earlier = candles[-n:-half]
        if not recent or not earlier:
            return False
        price_low_now  = min(c['low'] for c in recent)
        price_low_prev = min(c['low'] for c in earlier)
        rsi_now  = [c['rsi'] for c in recent  if c['rsi'] is not None]
        rsi_prev = [c['rsi'] for c in earlier if c['rsi'] is not None]
        if not rsi_now or not rsi_prev:
            return False
        return price_low_now < price_low_prev and min(rsi_now) > min(rsi_prev)

    # ────── Consecutive Thrust Bars ──────

    def count_consecutive_bull_bars(self, candles, n):
        count = 0
        for c in reversed(candles[-n:]):
            if self.is_bullish(c):
                count += 1
            else:
                break
        return count

    def count_consecutive_bear_bars(self, candles, n):
        count = 0
        for c in reversed(candles[-n:]):
            if self.is_bearish(c):
                count += 1
            else:
                break
        return count

    # ────── Plateau / Stall Detection ──────

    def is_price_plateau(self, candles, n=4):
        if len(candles) < n:
            return False
        bodies = [self.body_size(c) for c in candles[-n:]]
        return bodies[-1] < bodies[-2] < bodies[-3]

    # ────── Swing High / Low ──────

    def at_swing_high(self, candles, curr):
        if len(candles) < LOOKBACK:
            return False
        return curr['high'] >= self.recent_highest(candles, LOOKBACK) * 0.999

    def at_swing_low(self, candles, curr):
        if len(candles) < LOOKBACK:
            return False
        return curr['low'] <= self.recent_lowest(candles, LOOKBACK) * 1.001

    # ============================================================
    # MAIN EXHAUSTION DETECTION ENGINE
    # ============================================================

    def detect_exhaustion(self, candles):
        """Scan the candle window and return an exhaustion signal dict or None."""
        if len(candles) < max(LOOKBACK, DIVERGENCE_BARS, 4) + 1:
            return None

        curr = candles[-1]
        prev = candles[-2]

        # ── BULL EXHAUSTION CHECK ──
        score = 0
        reasons = []

        if self.at_swing_high(candles, curr):
            if self.is_shooting_star(curr):
                score += 1
                reasons.append("Shooting Star at swing high")

            if self.is_bearish_engulfing(prev, curr):
                score += 2
                reasons.append("Bearish Engulfing")

            if curr['rsi'] is not None and curr['rsi'] >= RSI_OVERBOUGHT:
                score += 1
                reasons.append(f"RSI Overbought ({curr['rsi']:.1f})")

            if self.has_bearish_divergence(candles):
                score += 2
                reasons.append("Bearish RSI Divergence")

            if self.is_volume_climax(curr):
                score += 1
                reasons.append("Volume Climax / Blow-off")

            if self.is_price_plateau(candles):
                score += 1
                reasons.append("Price Plateau / Body Shrink")

            bull_count = self.count_consecutive_bull_bars(candles, 10)
            if bull_count >= MIN_THRUST_BARS:
                score += 1
                reasons.append(f"Overextended bull thrust ({bull_count} bars)")

            if score >= 2:
                return {
                    'type': 'BULL_EXHAUSTION',
                    'strength': min(score, 5),
                    'timestamp': curr['datetime'],
                    'triggerPrice': curr['close'],
                    'reasons': reasons,
                }

        # ── BEAR EXHAUSTION CHECK ──
        score = 0
        reasons = []

        if self.at_swing_low(candles, curr):
            if self.is_hammer(curr):
                score += 1
                reasons.append("Hammer at swing low")

            if self.is_bullish_engulfing(prev, curr):
                score += 2
                reasons.append("Bullish Engulfing")

            if curr['rsi'] is not None and curr['rsi'] <= RSI_OVERSOLD:
                score += 1
                reasons.append(f"RSI Oversold ({curr['rsi']:.1f})")

            if self.has_bullish_divergence(candles):
                score += 2
                reasons.append("Bullish RSI Divergence")

            if self.is_volume_climax(curr):
                score += 1
                reasons.append("Volume Climax / Capitulation")

            if self.is_price_plateau(candles):
                score += 1
                reasons.append("Price Plateau / Body Shrink")

            bear_count = self.count_consecutive_bear_bars(candles, 10)
            if bear_count >= MIN_THRUST_BARS:
                score += 1
                reasons.append(f"Overextended bear thrust ({bear_count} bars)")

            if score >= 2:
                return {
                    'type': 'BEAR_EXHAUSTION',
                    'strength': min(score, 5),
                    'timestamp': curr['datetime'],
                    'triggerPrice': curr['close'],
                    'reasons': reasons,
                }

        return None

    # ============================================================
    # RUN & CSV OUTPUT
    # ============================================================

    def run(self):
        self.fetch_5min_bars()
        self.attach_indicators()

        # Rolling window size — enough for all look-backs
        window_size = 100

        for i in range(len(self.bars)):
            bar = self.bars[i]
            # Skip bars where indicators are not yet available
            if bar['rsi'] is None:
                continue

            # Build rolling window up to current bar
            start = max(0, i - window_size + 1)
            window = self.bars[start:i + 1]

            signal = self.detect_exhaustion(window)

            est_tz = ZoneInfo('America/New_York')
            est_time = bar['datetime'].astimezone(est_tz)
            row = [
                est_time.strftime('%Y-%m-%d %I:%M:%S %p'),
                bar['open'],
                bar['close'],
                round(bar['rsi'], 2),
            ]
            if signal and signal['strength'] > 3 and (
                (signal['type'] == 'BEAR_EXHAUSTION' and bar['rsi'] is not None and bar['rsi'] < 32) or
                (signal['type'] == 'BULL_EXHAUSTION' and bar['rsi'] is not None and bar['rsi'] > 68)
            ):
                row.append(signal['type'])
                row.append(signal['strength'])
                row.append('; '.join(signal['reasons']))
            else:
                row.extend(['', '', ''])

            self.results.append(row)

        self.write_csv()
        print(f"Backtest complete — {len(self.results)} rows written to backtest_ex_results.csv")

    def write_csv(self):
        with open('backtest_ex_results.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Date & Time (EST)',
                'Candle Open',
                'Candle Close',
                'Close RSI',
                'Exhaustion Type',
                'Exhaustion Strength',
                'Exhaustion Reasons',
            ])
            writer.writerows(self.results)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Back Testing Strategy — Exhaustion Detector')
    parser.add_argument('--years', type=int, required=True, help='Number of years to backtest')
    parser.add_argument('--symbol', type=str, default='$SPX.X', help='TradeStation symbol (default: $SPX.X)')
    parser.add_argument('--config', type=str, default=CONFIG_PATH, help='Path to config.yaml')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='Logging level')
    args = parser.parse_args()
    _setup_logging(args.log_level)
    bt = BackTestEx(args.years, symbol=args.symbol, config_path=args.config)
    bt.run()
