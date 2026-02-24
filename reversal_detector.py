"""
reversal_detector.py
════════════════════
Multi-Bar Lookback Reversal Detector
=====================================
Detects bullish (bottom) and bearish (top) reversal patterns using
only 5-minute OHLC candle data — no indicators required.

**Detection philosophy — early signal (5-10 min):**
  1. Accumulate *exhaustion* evidence across a 12-20 bar lookback window
     (bodies shrinking, rejection wicks, base / ceiling forming, floor /
     ceiling tested multiple times, higher-low / lower-high forming).
  2. As soon as 3+ exhaustion criteria are met **without** a trigger candle,
     publish a **BUILDING** phase (early warning).
  3. The moment a qualifying *trigger candle* appears (soft or hard), upgrade
     to **TRIGGERED** phase with entry / stop / target — within 1-2 bars
     (5-10 min) of the actual reversal starting.

Target: 30% retracement of the prior move.

Reads candle data from the ``stream_data/`` folder (one JSON file per day,
written by SPXStreamer).

Usage (standalone):
    python reversal_detector.py                     # today's file
    python reversal_detector.py --date 2026-02-23   # specific day

Usage (imported):
    from reversal_detector import ReversalDetector, detect_reversal

    detector = ReversalDetector()
    result   = detector.run()          # latest candles from today
    result   = detector.run("2026-02-23")

    if result['signal']:
        print(result['phase'], result['direction'], result['entry'])
"""

import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# ══════════════════════════════════════════════════════════════
# Candle dataclass
# ══════════════════════════════════════════════════════════════

@dataclass
class Candle:
    """Standard OHLC candle representation."""
    open: float
    high: float
    low: float
    close: float
    timestamp: Optional[str] = None

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def full_range(self) -> float:
        return self.high - self.low

    @property
    def is_bullish(self) -> bool:
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open

    @property
    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low

    @property
    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    @property
    def lower_wick_ratio(self) -> float:
        if self.full_range == 0:
            return 0
        return self.lower_wick / self.full_range

    @property
    def upper_wick_ratio(self) -> float:
        if self.full_range == 0:
            return 0
        return self.upper_wick / self.full_range

    @property
    def close_strength_bull(self) -> float:
        """How close the close is to the high (1.0 = closed at high)."""
        if self.full_range == 0:
            return 0
        return (self.close - self.low) / self.full_range

    @property
    def close_strength_bear(self) -> float:
        """How close the close is to the low (1.0 = closed at low)."""
        if self.full_range == 0:
            return 0
        return (self.high - self.close) / self.full_range


# ══════════════════════════════════════════════════════════════
# Swing-point helpers
# ══════════════════════════════════════════════════════════════

def _find_swing_lows(bars: List[Candle], depth: int = 1) -> List[Tuple[int, float]]:
    """Find swing low points in a series of candles."""
    swings = []
    for i in range(depth, len(bars) - depth):
        if all(bars[i].low <= bars[i - j].low for j in range(1, depth + 1)) and \
           all(bars[i].low <= bars[i + j].low for j in range(1, depth + 1)):
            swings.append((i, bars[i].low))
    return swings


def _find_swing_highs(bars: List[Candle], depth: int = 1) -> List[Tuple[int, float]]:
    """Find swing high points in a series of candles."""
    swings = []
    for i in range(depth, len(bars) - depth):
        if all(bars[i].high >= bars[i - j].high for j in range(1, depth + 1)) and \
           all(bars[i].high >= bars[i + j].high for j in range(1, depth + 1)):
            swings.append((i, bars[i].high))
    return swings


# ══════════════════════════════════════════════════════════════
# Internal reversal logic
# ══════════════════════════════════════════════════════════════

def _detect_bullish_reversal(
    candles: List[Candle],
    first_half: List[Candle],
    last_8: List[Candle],
    last_6: List[Candle],
    bodies_shrinking: bool,
    highest: float,
    abs_lowest: float,
    min_move: float,
    retracement: float,
) -> Dict[str, Any]:
    """
    Detect bearish-to-bullish (bottom) reversal with two-tier trigger.

    Returns a dict with ``phase`` in ("none", "building", "triggered").
    """
    last = candles[-1]
    prev = candles[-2]

    # -- Measure the selloff ------------------------------------------------
    drop = highest - abs_lowest
    if drop < min_move:
        return {"signal": False, "direction": "bull", "phase": "none",
                "score": 0, "reason": "insufficient selloff"}

    # -- Exhaustion criteria (softened for early detection) -------------------
    # 1. Buying pressure: long lower wicks (0.3 → 0.25)
    avg_wick = sum(c.lower_wick_ratio for c in last_8) / len(last_8)
    wick_pressure = avg_wick > 0.25

    # 2. Base formation at lows (0.15 → 0.20)
    lows = [c.low for c in last_6]
    floor = min(lows)
    low_range = max(lows) - floor
    base_forming = low_range < drop * 0.20
    floor_touches = sum(1 for l in lows if (l - floor) < drop * 0.06)
    floor_tested = floor_touches >= 2

    # 3. Higher low (swing analysis)
    swing_lows = _find_swing_lows(candles[-12:])
    higher_low = len(swing_lows) >= 2 and swing_lows[-1][1] > swing_lows[-2][1]

    base_criteria = [bodies_shrinking, wick_pressure, base_forming,
                     floor_tested, higher_low]
    base_score = sum(base_criteria)

    # -- Trigger candle (two tiers) ------------------------------------------
    # Soft: bullish candle, moderate strength, close above prev close
    soft_trigger = (last.is_bullish
                    and last.close_strength_bull > 0.55
                    and last.close > prev.close)
    # Hard: bullish candle, high strength, close above prev high
    hard_trigger = (last.is_bullish
                    and last.close_strength_bull > 0.70
                    and last.close > prev.high)

    # -- Phase determination -------------------------------------------------
    if (hard_trigger or soft_trigger) and base_score >= 3:
        phase = "triggered"
        total_score = base_score + 1   # +1 for trigger
    elif base_score >= 3:
        phase = "building"
        total_score = base_score
    else:
        phase = "none"
        total_score = base_score

    signal = phase in ("triggered", "building")

    # -- Compute trade levels when triggered ---------------------------------
    entry  = round(last.close, 2)                      if phase == "triggered" else None
    stop   = round(floor - 0.5, 2)                     if phase == "triggered" else None
    target = round(abs_lowest + drop * retracement, 2) if phase == "triggered" else None
    risk   = round(entry - stop, 2)                    if entry and stop else None
    reward = round(target - entry, 2)                  if entry and target else None
    rr     = round(reward / max(risk, 0.01), 2)        if risk and reward else None

    return {
        "signal": signal,
        "phase": phase,
        "direction": "bull",
        "score": total_score,
        "max_score": 6,
        "entry": entry,
        "stop": stop,
        "target": target,
        "risk": risk,
        "reward": reward,
        "rr_ratio": rr,
        "details": {
            "pattern": "multi_bar_bottom",
            "prior_trend": "downtrend",
            "move_size": round(drop, 2),
            "bodies_shrinking": bodies_shrinking,
            "buying_pressure": wick_pressure,
            "avg_lower_wick_ratio": round(avg_wick, 3),
            "base_forming": base_forming,
            "floor_level": round(floor, 2),
            "floor_tested": floor_tested,
            "floor_touches": floor_touches,
            "higher_low": higher_low,
            "soft_trigger": soft_trigger,
            "hard_trigger": hard_trigger,
        },
    }


def _detect_bearish_reversal(
    candles: List[Candle],
    first_half: List[Candle],
    last_8: List[Candle],
    last_6: List[Candle],
    bodies_shrinking: bool,
    lowest: float,
    abs_highest: float,
    min_move: float,
    retracement: float,
) -> Dict[str, Any]:
    """
    Detect bullish-to-bearish (top) reversal with two-tier trigger.

    Returns a dict with ``phase`` in ("none", "building", "triggered").
    """
    last = candles[-1]
    prev = candles[-2]

    # -- Measure the rally ---------------------------------------------------
    rally = abs_highest - lowest
    if rally < min_move:
        return {"signal": False, "direction": "bear", "phase": "none",
                "score": 0, "reason": "insufficient rally"}

    # -- Exhaustion criteria (softened for early detection) -------------------
    avg_wick = sum(c.upper_wick_ratio for c in last_8) / len(last_8)
    wick_pressure = avg_wick > 0.25

    highs = [c.high for c in last_6]
    ceiling = max(highs)
    high_range = ceiling - min(highs)
    ceiling_forming = high_range < rally * 0.20
    ceiling_touches = sum(1 for h in highs if (ceiling - h) < rally * 0.06)
    ceiling_tested = ceiling_touches >= 2

    swing_highs = _find_swing_highs(candles[-12:])
    lower_high = len(swing_highs) >= 2 and swing_highs[-1][1] < swing_highs[-2][1]

    base_criteria = [bodies_shrinking, wick_pressure, ceiling_forming,
                     ceiling_tested, lower_high]
    base_score = sum(base_criteria)

    # -- Trigger candle (two tiers) ------------------------------------------
    soft_trigger = (last.is_bearish
                    and last.close_strength_bear > 0.55
                    and last.close < prev.close)
    hard_trigger = (last.is_bearish
                    and last.close_strength_bear > 0.70
                    and last.close < prev.low)

    # -- Phase determination -------------------------------------------------
    if (hard_trigger or soft_trigger) and base_score >= 3:
        phase = "triggered"
        total_score = base_score + 1
    elif base_score >= 3:
        phase = "building"
        total_score = base_score
    else:
        phase = "none"
        total_score = base_score

    signal = phase in ("triggered", "building")

    entry  = round(last.close, 2)                          if phase == "triggered" else None
    stop   = round(ceiling + 0.5, 2)                       if phase == "triggered" else None
    target = round(abs_highest - rally * retracement, 2)   if phase == "triggered" else None
    risk   = round(stop - entry, 2)                        if entry and stop else None
    reward = round(entry - target, 2)                      if entry and target else None
    rr     = round(reward / max(risk, 0.01), 2)            if risk and reward else None

    return {
        "signal": signal,
        "phase": phase,
        "direction": "bear",
        "score": total_score,
        "max_score": 6,
        "entry": entry,
        "stop": stop,
        "target": target,
        "risk": risk,
        "reward": reward,
        "rr_ratio": rr,
        "details": {
            "pattern": "multi_bar_top",
            "prior_trend": "uptrend",
            "move_size": round(rally, 2),
            "bodies_shrinking": bodies_shrinking,
            "selling_pressure": wick_pressure,
            "avg_upper_wick_ratio": round(avg_wick, 3),
            "ceiling_forming": ceiling_forming,
            "ceiling_level": round(ceiling, 2),
            "ceiling_tested": ceiling_tested,
            "ceiling_touches": ceiling_touches,
            "lower_high": lower_high,
            "soft_trigger": soft_trigger,
            "hard_trigger": hard_trigger,
        },
    }


# ══════════════════════════════════════════════════════════════
# Public detection function — multi-bar lookback
# ══════════════════════════════════════════════════════════════

def detect_reversal(
    candles: List[Candle],
    min_move: float = 2.0,
    retracement: float = 0.30,
) -> Dict[str, Any]:
    """
    Multi-bar lookback reversal detection with early trigger.

    Needs >= 8 bars for meaningful analysis.  Uses a sliding window of up
    to the last 20 bars to detect exhaustion + trigger.

    Returns a dict that always contains:
      - signal    (bool)  – True if building or triggered
      - phase     (str)   – "none" | "building" | "triggered"
      - direction (str)   – "bull" | "bear" | None

    When phase == "triggered" the dict also has entry / stop / target / rr.
    When phase == "building"  exhaustion details are provided (no trade levels).

    Args:
        candles:     List of Candle objects (minimum 8).
        min_move:    Min point range to qualify (default 2 for SPX 5-min).
        retracement: Target as fraction of prior move (default 0.30).
    """
    n = len(candles)

    if n < 3:
        return {"signal": False, "phase": "none", "direction": None,
                "score": 0, "reason": f"need >= 3 candles, got {n}"}

    if n < 8:
        return {"signal": False, "phase": "none", "direction": None,
                "score": 0, "reason": f"need >= 8 candles for multi-bar, got {n}"}

    # ── Use last 20 bars (or all if fewer) ───────────────────
    window = candles[-20:] if n > 20 else candles
    wn = len(window)

    # ── Compute slices shared by both directions ─────────────
    half       = wn // 2
    first_half = window[:half]
    last_8     = window[-8:]
    last_6     = window[-6:]

    # Bodies shrinking = momentum fading
    first_bodies  = [c.body for c in first_half]
    recent_bodies = [c.body for c in last_6]
    avg_first     = sum(first_bodies)  / len(first_bodies)  if first_bodies  else 1.0
    avg_recent    = sum(recent_bodies) / len(recent_bodies) if recent_bodies else 1.0
    bodies_shrinking = avg_recent < avg_first * 0.65

    highest = max(c.high for c in window)
    lowest  = min(c.low  for c in window)

    # ── Run both directions ──────────────────────────────────
    bull = _detect_bullish_reversal(
        window, first_half, last_8, last_6,
        bodies_shrinking, highest, lowest, min_move, retracement,
    )
    bear = _detect_bearish_reversal(
        window, first_half, last_8, last_6,
        bodies_shrinking, lowest, highest, min_move, retracement,
    )

    # ── Pick the best result ─────────────────────────────────
    _phase_rank = {"triggered": 2, "building": 1, "none": 0}

    bull_rank = _phase_rank.get(bull.get("phase", "none"), 0)
    bear_rank = _phase_rank.get(bear.get("phase", "none"), 0)

    if bull_rank > bear_rank:
        winner = bull
    elif bear_rank > bull_rank:
        winner = bear
    elif bull.get("score", 0) >= bear.get("score", 0):
        winner = bull
    else:
        winner = bear

    # If neither has a signal, return a clean "no signal" dict
    if not winner.get("signal"):
        return {
            "signal": False,
            "phase": "none",
            "direction": None,
            "score": 0,
            "bull_score": bull.get("score", 0),
            "bear_score": bear.get("score", 0),
            "max_score": 6,
            "reason": "exhaustion criteria not met",
        }

    return winner


# ══════════════════════════════════════════════════════════════
# ReversalDetector – reads stream_data/ JSON files
# ══════════════════════════════════════════════════════════════

_STREAM_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stream_data")


class ReversalDetector:
    """
    Loads 5-min candles from the daily JSON files saved by SPXStreamer
    and runs the reversal pattern detector on them.

    Usage:
        detector = ReversalDetector()
        result   = detector.run()              # today
        result   = detector.run("2026-02-23")  # specific date
    """

    def __init__(
        self,
        stream_data_dir: str = _STREAM_DATA_DIR,
        min_move: float = 2.0,
        retracement: float = 0.30,
        logger: Optional[logging.Logger] = None,
    ):
        self.stream_data_dir = stream_data_dir
        self.min_move = min_move
        self.retracement = retracement
        self.log = logger or logging.getLogger("reversal_detector")

    # ── Data loading ─────────────────────────────────────────

    def _daily_filepath(self, date_str: str) -> str:
        return os.path.join(self.stream_data_dir, f"{date_str}.json")

    def load_candles(self, date_str: Optional[str] = None) -> List[Candle]:
        """
        Load candles for a given date (YYYY-MM-DD).
        Defaults to today (US-Eastern).
        Deduplicates by timestamp — keeps the *last* record per timestamp
        (which represents the final/closed state of that bar).
        """
        if date_str is None:
            est = timezone(timedelta(hours=-5), "EST")
            date_str = datetime.now(est).strftime("%Y-%m-%d")

        filepath = self._daily_filepath(date_str)
        if not os.path.exists(filepath):
            self.log.warning("No candle file found: %s", filepath)
            return []

        with open(filepath, "r") as f:
            raw = json.load(f)

        candles: List[Candle] = []
        seen_ts: Optional[str] = None
        for rec in raw:
            try:
                ts = rec.get("TimeStamp", "")
                c = Candle(
                    open=float(rec["Open"]),
                    high=float(rec["High"]),
                    low=float(rec["Low"]),
                    close=float(rec["Close"]),
                    timestamp=ts,
                )
                # Keep only the last record per timestamp (=closed bar)
                if ts == seen_ts and candles:
                    candles[-1] = c
                else:
                    candles.append(c)
                    seen_ts = ts
            except (KeyError, ValueError, TypeError) as e:
                self.log.debug("Skipping bad record: %s — %s", rec, e)
        return candles

    # ── Run detection ────────────────────────────────────────

    def run(self, date_str: Optional[str] = None) -> Dict[str, Any]:
        """
        Load candles for *date_str* and return the reversal detection result
        using the full multi-bar lookback window.
        """
        candles = self.load_candles(date_str)
        if len(candles) < 8:
            return {
                "signal": False,
                "phase": "none",
                "direction": None,
                "reason": f"only {len(candles)} candle(s) — need at least 8",
            }

        result = detect_reversal(candles, self.min_move, self.retracement)
        # Attach timestamp context
        result["candle_count"] = len(candles)
        result["last_timestamp"] = candles[-1].timestamp
        return result

    def scan_all(self, date_str: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Slide a 20-bar window over every bar in the day and return
        **all** signals found (building + triggered).
        """
        candles = self.load_candles(date_str)
        signals: List[Dict[str, Any]] = []
        window_size = 20
        for i in range(max(8, window_size), len(candles) + 1):
            window = candles[max(0, i - window_size) : i]
            result = detect_reversal(window, self.min_move, self.retracement)
            if result.get("signal"):
                result["window_end_index"] = i - 1
                result["timestamp"] = candles[i - 1].timestamp
                signals.append(result)
        return signals


# ══════════════════════════════════════════════════════════════
# Pretty-print helper
# ══════════════════════════════════════════════════════════════

def _print_result(result: Dict[str, Any]) -> None:
    print(f"Signal:    {result.get('signal', False)}")
    phase = result.get("phase", "none")
    print(f"Phase:     {phase.upper()}")
    print(f"Direction: {result.get('direction', 'N/A')}")

    if result.get("signal"):
        print(f"Score:     {result.get('score', '?')} / {result.get('max_score', 6)}")
        pattern = result.get("details", {}).get("pattern", "?")
        print(f"Pattern:   {pattern}")
        if phase == "triggered":
            print(f"Entry:     {result['entry']}")
            print(f"Stop:      {result['stop']}")
            print(f"Target:    {result['target']}  (30% retracement)")
            print(f"Risk:      {result.get('risk', '?')} pts")
            print(f"Reward:    {result.get('reward', '?')} pts")
            print(f"R:R Ratio: {result.get('rr_ratio', '?')}:1")
        else:
            print("           (exhaustion building — no trade levels yet)")
    else:
        print(f"Reason:    {result.get('reason', 'N/A')}")
        if "bull_score" in result:
            print(f"Bull score: {result['bull_score']}")
            print(f"Bear score: {result['bear_score']}")

    for k, v in result.get("details", {}).items():
        print(f"  {k:.<35} {v}")


# ══════════════════════════════════════════════════════════════
# CLI entry point
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Detect price-action reversals from saved 5-min SPX candles"
    )
    parser.add_argument(
        "--date", default=None,
        help="Date to analyse (YYYY-MM-DD). Defaults to today (US-Eastern).",
    )
    parser.add_argument(
        "--scan", action="store_true",
        help="Slide a 20-bar window across the entire day and print all signals.",
    )
    parser.add_argument(
        "--min-move", type=float, default=2.0,
        help="Minimum move range in points (default: 2).",
    )
    parser.add_argument(
        "--retracement", type=float, default=0.30,
        help="Target retracement fraction (default: 0.30).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    detector = ReversalDetector(
        min_move=args.min_move,
        retracement=args.retracement,
    )

    if args.scan:
        signals = detector.scan_all(args.date)
        if not signals:
            print("No reversal signals found.")
        for i, sig in enumerate(signals, 1):
            print(f"\n{'─' * 55}")
            print(f"Signal #{i}  @  {sig.get('timestamp', '?')}  [{sig.get('phase', '?').upper()}]")
            print(f"{'─' * 55}")
            _print_result(sig)
    else:
        result = detector.run(args.date)
        print("=" * 50)
        print("REVERSAL DETECTION RESULT")
        print("=" * 50)
        if result.get("last_timestamp"):
            print(f"Candles:   {result['candle_count']}  (last: {result['last_timestamp']})")
        _print_result(result)


if __name__ == "__main__":
    main()
