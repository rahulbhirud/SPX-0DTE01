"""
ExhaustionDetector — Applies bull / bear exhaustion analysis from BackTestEx
to the live 5-min stream_data JSON candles.

Usage (standalone):
    detector = ExhaustionDetector()
    detector.process_daily_json("json/stream_data/2026-02-27.json")

Usage (from SPXStreamer):
    Called automatically after each candle is persisted.
"""

import json
import logging
import os
from typing import Dict, List, Optional

# ============================================================
# CONSTANTS  (mirrored from back_test_ex)
# ============================================================
RSI_PERIOD        = 14
RSI_OVERBOUGHT    = 70
RSI_OVERSOLD      = 30
VOLUME_AVG_PERIOD = 20
ATR_PERIOD        = 14
LOOKBACK          = 10
MIN_THRUST_BARS   = 3
DIVERGENCE_BARS   = 5

# Thresholds for tagging a candle (user requirement)
MIN_STRENGTH      = 3       # strength must be > 3  (i.e. >= 4)
BEAR_RSI_CEILING  = 32      # RSI < 32 for bear exhaustion
BULL_RSI_FLOOR    = 68      # RSI > 68 for bull exhaustion

LOG = logging.getLogger("exhaustion_detector")


class ExhaustionDetector:
    """
    Reads a daily stream-data JSON file, normalises the candles,
    computes ATR / avgVolume indicators, runs the exhaustion engine
    on every bar, and writes the result back into each candle record
    as new JSON fields.

    Added fields (only when criteria are met):
        "Exhaustion": {
            "type":     "BULL_EXHAUSTION" | "BEAR_EXHAUSTION",
            "strength": <int>,
            "reasons":  [<str>, ...]
        }

    When the criteria are NOT met the "Exhaustion" key is set to null
    so downstream consumers can distinguish "analysed, no signal" from
    "not yet analysed".
    """

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────

    def process_daily_json(self, filepath: str) -> int:
        """
        Analyse *all* candles in a daily JSON file and tag each one.

        Returns the number of candles that were marked with an exhaustion
        signal.
        """
        if not os.path.exists(filepath):
            LOG.warning("File not found: %s", filepath)
            return 0

        raw_candles = self._load_json(filepath)
        if not raw_candles:
            return 0

        bars = self._normalise_candles(raw_candles)
        self._attach_indicators(bars)

        tagged = 0
        window_size = 100

        for i, bar in enumerate(bars):
            if bar["rsi"] is None:
                raw_candles[i]["Exhaustion"] = None
                continue

            start = max(0, i - window_size + 1)
            window = bars[start : i + 1]

            signal = self._detect_exhaustion(window)
            exhaustion = self._apply_filter(signal, bar)

            raw_candles[i]["Exhaustion"] = exhaustion
            if exhaustion is not None:
                tagged += 1

        self._save_json(filepath, raw_candles)
        LOG.info("Processed %s — %d/%d candles tagged", filepath, tagged, len(bars))
        return tagged

    def process_latest_candle(self, filepath: str) -> Optional[Dict]:
        """
        Fast path — only evaluate the last candle (called after each
        candle append in the streamer).  Returns the exhaustion dict
        if tagged, else None.
        """
        if not os.path.exists(filepath):
            return None

        raw_candles = self._load_json(filepath)
        if not raw_candles:
            return None

        bars = self._normalise_candles(raw_candles)
        self._attach_indicators(bars)

        last_idx = len(bars) - 1
        bar = bars[last_idx]
        if bar["rsi"] is None:
            raw_candles[last_idx]["Exhaustion"] = None
            self._save_json(filepath, raw_candles)
            return None

        start = max(0, last_idx - 100 + 1)
        window = bars[start : last_idx + 1]

        signal = self._detect_exhaustion(window)
        exhaustion = self._apply_filter(signal, bar)

        raw_candles[last_idx]["Exhaustion"] = exhaustion
        self._save_json(filepath, raw_candles)
        return exhaustion

    # ──────────────────────────────────────────────────────────
    # Filter — strength > 3  AND  RSI guard
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _apply_filter(signal: Optional[Dict], bar: Dict) -> Optional[Dict]:
        """Return a trimmed exhaustion dict when the signal passes the
        strength + RSI gate, else None."""
        if signal is None:
            return None

        if signal["strength"] <= MIN_STRENGTH:
            return None

        rsi = bar["rsi"]
        if rsi is None:
            return None

        sig_type = signal["type"]
        if sig_type == "BEAR_EXHAUSTION" and rsi >= BEAR_RSI_CEILING:
            return None
        if sig_type == "BULL_EXHAUSTION" and rsi <= BULL_RSI_FLOOR:
            return None

        return {
            "type":     sig_type,
            "strength": signal["strength"],
            "reasons":  signal["reasons"],
        }

    # ──────────────────────────────────────────────────────────
    # JSON I/O
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _load_json(filepath: str) -> List[Dict]:
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except (json.JSONDecodeError, OSError) as exc:
            LOG.warning("Could not load %s: %s", filepath, exc)
            return []

    @staticmethod
    def _save_json(filepath: str, data: List[Dict]) -> None:
        tmp = filepath + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, filepath)
        except OSError as exc:
            LOG.warning("Could not write %s: %s", filepath, exc)

    # ──────────────────────────────────────────────────────────
    # Normalise stream-data records → internal bar dicts
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _normalise_candles(self, raw: List[Dict]) -> List[Dict]:
        """Convert stream-data JSON records to the dict shape that the
        exhaustion engine expects."""
        bars: List[Dict] = []
        for rec in raw:
            bars.append({
                "open":      self._safe_float(rec.get("Open")),
                "high":      self._safe_float(rec.get("High")),
                "low":       self._safe_float(rec.get("Low")),
                "close":     self._safe_float(rec.get("Close")),
                "volume":    self._safe_float(rec.get("TotalVolume")) or 0,
                "rsi":       self._safe_float(rec.get("RSI")),
                "atr":       None,
                "avgVolume": None,
                "timestamp": rec.get("TimeStamp", ""),
            })
        return bars

    # ──────────────────────────────────────────────────────────
    # Indicator attachment  (ATR + avgVolume)
    # ──────────────────────────────────────────────────────────

    def _attach_indicators(self, bars: List[Dict]) -> None:
        atr_vals = self._calculate_atr(bars)
        avg_vol_vals = self._calculate_avg_volume(bars)
        for i, bar in enumerate(bars):
            bar["atr"] = atr_vals[i]
            bar["avgVolume"] = avg_vol_vals[i]

    @staticmethod
    def _calculate_atr(bars: List[Dict], period: int = ATR_PERIOD) -> List[Optional[float]]:
        atr_values: List[Optional[float]] = [None] * len(bars)
        for i in range(1, len(bars)):
            if i >= period:
                trs = []
                for j in range(i - period + 1, i + 1):
                    pc = bars[j - 1]["close"]
                    h  = bars[j]["high"]
                    lo = bars[j]["low"]
                    if None in (pc, h, lo):
                        continue
                    trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
                if trs:
                    atr_values[i] = sum(trs) / len(trs)
        return atr_values

    @staticmethod
    def _calculate_avg_volume(bars: List[Dict], period: int = VOLUME_AVG_PERIOD) -> List[Optional[float]]:
        avg_vols: List[Optional[float]] = [None] * len(bars)
        for i in range(period - 1, len(bars)):
            vols = [bars[j]["volume"] for j in range(i - period + 1, i + 1)]
            avg_vols[i] = sum(vols) / period
        return avg_vols

    # ──────────────────────────────────────────────────────────
    # Candle helpers  (identical to BackTestEx)
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _is_bullish(candle: Dict) -> bool:
        return candle["close"] > candle["open"]

    @staticmethod
    def _is_bearish(candle: Dict) -> bool:
        return candle["close"] < candle["open"]

    @staticmethod
    def _body_size(candle: Dict) -> float:
        return abs(candle["close"] - candle["open"])

    @staticmethod
    def _upper_wick(candle: Dict) -> float:
        return candle["high"] - max(candle["open"], candle["close"])

    @staticmethod
    def _lower_wick(candle: Dict) -> float:
        return min(candle["open"], candle["close"]) - candle["low"]

    @staticmethod
    def _candle_range(candle: Dict) -> float:
        return candle["high"] - candle["low"]

    @staticmethod
    def _is_volume_climax(candle: Dict) -> bool:
        if candle["avgVolume"] is None or candle["avgVolume"] == 0:
            return False
        return candle["volume"] > (candle["avgVolume"] * 2.0)

    @staticmethod
    def _recent_highest(candles: List[Dict], n: int) -> float:
        return max(c["high"] for c in candles[-n:])

    @staticmethod
    def _recent_lowest(candles: List[Dict], n: int) -> float:
        return min(c["low"] for c in candles[-n:])

    # ──────────────────────────────────────────────────────────
    # Pattern detectors
    # ──────────────────────────────────────────────────────────

    def _is_shooting_star(self, candle: Dict) -> bool:
        rng = self._candle_range(candle)
        if rng == 0:
            return False
        return (self._upper_wick(candle) / rng) >= 0.60 and (self._body_size(candle) / rng) <= 0.25

    def _is_hammer(self, candle: Dict) -> bool:
        rng = self._candle_range(candle)
        if rng == 0:
            return False
        return (self._lower_wick(candle) / rng) >= 0.60 and (self._body_size(candle) / rng) <= 0.25

    def _is_bearish_engulfing(self, prev: Dict, curr: Dict) -> bool:
        return (self._is_bullish(prev) and self._is_bearish(curr)
                and curr["open"] >= prev["close"]
                and curr["close"] <= prev["open"]
                and self._body_size(curr) > self._body_size(prev))

    def _is_bullish_engulfing(self, prev: Dict, curr: Dict) -> bool:
        return (self._is_bearish(prev) and self._is_bullish(curr)
                and curr["open"] <= prev["close"]
                and curr["close"] >= prev["open"]
                and self._body_size(curr) > self._body_size(prev))

    # ────── RSI Divergence ──────

    @staticmethod
    def _has_bearish_divergence(candles: List[Dict], n: int = DIVERGENCE_BARS) -> bool:
        half = n // 2
        if len(candles) < n or half == 0:
            return False
        recent  = candles[-half:]
        earlier = candles[-n:-half]
        if not recent or not earlier:
            return False
        price_high_now  = max(c["high"] for c in recent)
        price_high_prev = max(c["high"] for c in earlier)
        rsi_now  = [c["rsi"] for c in recent  if c["rsi"] is not None]
        rsi_prev = [c["rsi"] for c in earlier if c["rsi"] is not None]
        if not rsi_now or not rsi_prev:
            return False
        return price_high_now > price_high_prev and max(rsi_now) < max(rsi_prev)

    @staticmethod
    def _has_bullish_divergence(candles: List[Dict], n: int = DIVERGENCE_BARS) -> bool:
        half = n // 2
        if len(candles) < n or half == 0:
            return False
        recent  = candles[-half:]
        earlier = candles[-n:-half]
        if not recent or not earlier:
            return False
        price_low_now  = min(c["low"] for c in recent)
        price_low_prev = min(c["low"] for c in earlier)
        rsi_now  = [c["rsi"] for c in recent  if c["rsi"] is not None]
        rsi_prev = [c["rsi"] for c in earlier if c["rsi"] is not None]
        if not rsi_now or not rsi_prev:
            return False
        return price_low_now < price_low_prev and min(rsi_now) > min(rsi_prev)

    # ────── Consecutive Thrust Bars ──────

    def _count_consecutive_bull_bars(self, candles: List[Dict], n: int) -> int:
        count = 0
        for c in reversed(candles[-n:]):
            if self._is_bullish(c):
                count += 1
            else:
                break
        return count

    def _count_consecutive_bear_bars(self, candles: List[Dict], n: int) -> int:
        count = 0
        for c in reversed(candles[-n:]):
            if self._is_bearish(c):
                count += 1
            else:
                break
        return count

    # ────── Plateau / Stall ──────

    def _is_price_plateau(self, candles: List[Dict], n: int = 4) -> bool:
        if len(candles) < n:
            return False
        bodies = [self._body_size(c) for c in candles[-n:]]
        return bodies[-1] < bodies[-2] < bodies[-3]

    # ────── Swing High / Low ──────

    def _at_swing_high(self, candles: List[Dict], curr: Dict) -> bool:
        if len(candles) < LOOKBACK:
            return False
        return curr["high"] >= self._recent_highest(candles, LOOKBACK) * 0.999

    def _at_swing_low(self, candles: List[Dict], curr: Dict) -> bool:
        if len(candles) < LOOKBACK:
            return False
        return curr["low"] <= self._recent_lowest(candles, LOOKBACK) * 1.001

    # ──────────────────────────────────────────────────────────
    # Main exhaustion engine  (identical logic to BackTestEx)
    # ──────────────────────────────────────────────────────────

    def _detect_exhaustion(self, candles: List[Dict]) -> Optional[Dict]:
        if len(candles) < max(LOOKBACK, DIVERGENCE_BARS, 4) + 1:
            return None

        curr = candles[-1]
        prev = candles[-2]

        # ── BULL EXHAUSTION ──
        score = 0
        reasons: List[str] = []

        if self._at_swing_high(candles, curr):
            if self._is_shooting_star(curr):
                score += 1
                reasons.append("Shooting Star at swing high")

            if self._is_bearish_engulfing(prev, curr):
                score += 2
                reasons.append("Bearish Engulfing")

            if curr["rsi"] is not None and curr["rsi"] >= RSI_OVERBOUGHT:
                score += 1
                reasons.append(f"RSI Overbought ({curr['rsi']:.1f})")

            if self._has_bearish_divergence(candles):
                score += 2
                reasons.append("Bearish RSI Divergence")

            if self._is_volume_climax(curr):
                score += 1
                reasons.append("Volume Climax / Blow-off")

            if self._is_price_plateau(candles):
                score += 1
                reasons.append("Price Plateau / Body Shrink")

            bull_count = self._count_consecutive_bull_bars(candles, 10)
            if bull_count >= MIN_THRUST_BARS:
                score += 1
                reasons.append(f"Overextended bull thrust ({bull_count} bars)")

            if score >= 2:
                return {
                    "type":         "BULL_EXHAUSTION",
                    "strength":     min(score, 5),
                    "timestamp":    curr["timestamp"],
                    "triggerPrice": curr["close"],
                    "reasons":      reasons,
                }

        # ── BEAR EXHAUSTION ──
        score = 0
        reasons = []

        if self._at_swing_low(candles, curr):
            if self._is_hammer(curr):
                score += 1
                reasons.append("Hammer at swing low")

            if self._is_bullish_engulfing(prev, curr):
                score += 2
                reasons.append("Bullish Engulfing")

            if curr["rsi"] is not None and curr["rsi"] <= RSI_OVERSOLD:
                score += 1
                reasons.append(f"RSI Oversold ({curr['rsi']:.1f})")

            if self._has_bullish_divergence(candles):
                score += 2
                reasons.append("Bullish RSI Divergence")

            if self._is_volume_climax(curr):
                score += 1
                reasons.append("Volume Climax / Capitulation")

            if self._is_price_plateau(candles):
                score += 1
                reasons.append("Price Plateau / Body Shrink")

            bear_count = self._count_consecutive_bear_bars(candles, 10)
            if bear_count >= MIN_THRUST_BARS:
                score += 1
                reasons.append(f"Overextended bear thrust ({bear_count} bars)")

            if score >= 2:
                return {
                    "type":         "BEAR_EXHAUSTION",
                    "strength":     min(score, 5),
                    "timestamp":    curr["timestamp"],
                    "triggerPrice": curr["close"],
                    "reasons":      reasons,
                }

        return None
