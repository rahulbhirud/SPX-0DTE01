"""
Tests for ExhaustionDetector.
"""

import json
import os
import tempfile

import pytest

from exhaustion_detector import (
    ExhaustionDetector,
    BEAR_RSI_CEILING,
    BULL_RSI_FLOOR,
    MIN_STRENGTH,
)


# ── Helpers ──────────────────────────────────────────────────

def _make_candle(
    ts="2026-02-27 10:00:00 AM",
    o=5800, h=5810, lo=5790, c=5805,
    vol=50000, rsi=50.0, status="",
):
    """Build a single stream-data JSON record."""
    return {
        "TimeStamp":   ts,
        "Open":        str(o),
        "High":        str(h),
        "Low":         str(lo),
        "Close":       str(c),
        "TotalVolume": str(vol),
        "Status":      status,
        "RSI":         rsi,
    }


def _write_candles(candles, filepath):
    with open(filepath, "w") as f:
        json.dump(candles, f)


def _read_candles(filepath):
    with open(filepath, "r") as f:
        return json.load(f)


# ── Fixtures ─────────────────────────────────────────────────

@pytest.fixture
def detector():
    return ExhaustionDetector()


@pytest.fixture
def tmp_json(tmp_path):
    """Return a temp .json filepath (doesn't exist yet)."""
    return str(tmp_path / "test_day.json")


# ── Tests ────────────────────────────────────────────────────

class TestNormalisation:

    def test_normalise_converts_strings_to_floats(self, detector):
        raw = [_make_candle(o=100, h=110, lo=90, c=105, vol=9999, rsi=45.0)]
        bars = detector._normalise_candles(raw)
        assert bars[0]["open"] == 100.0
        assert bars[0]["high"] == 110.0
        assert bars[0]["low"] == 90.0
        assert bars[0]["close"] == 105.0
        assert bars[0]["volume"] == 9999.0
        assert bars[0]["rsi"] == 45.0

    def test_normalise_handles_none_rsi(self, detector):
        raw = [_make_candle(rsi=None)]
        bars = detector._normalise_candles(raw)
        assert bars[0]["rsi"] is None


class TestApplyFilter:

    def test_returns_none_when_signal_is_none(self):
        assert ExhaustionDetector._apply_filter(None, {"rsi": 25}) is None

    def test_returns_none_when_strength_too_low(self):
        signal = {"type": "BEAR_EXHAUSTION", "strength": 3, "reasons": ["x"]}
        assert ExhaustionDetector._apply_filter(signal, {"rsi": 20}) is None

    def test_bear_exhaustion_passes_when_rsi_below_ceiling(self):
        signal = {"type": "BEAR_EXHAUSTION", "strength": 4, "reasons": ["a", "b"]}
        result = ExhaustionDetector._apply_filter(signal, {"rsi": 25.0})
        assert result is not None
        assert result["type"] == "BEAR_EXHAUSTION"

    def test_bear_exhaustion_rejected_when_rsi_above_ceiling(self):
        signal = {"type": "BEAR_EXHAUSTION", "strength": 4, "reasons": ["a"]}
        assert ExhaustionDetector._apply_filter(signal, {"rsi": 50.0}) is None

    def test_bull_exhaustion_passes_when_rsi_above_floor(self):
        signal = {"type": "BULL_EXHAUSTION", "strength": 5, "reasons": ["a"]}
        result = ExhaustionDetector._apply_filter(signal, {"rsi": 75.0})
        assert result is not None
        assert result["type"] == "BULL_EXHAUSTION"

    def test_bull_exhaustion_rejected_when_rsi_below_floor(self):
        signal = {"type": "BULL_EXHAUSTION", "strength": 5, "reasons": ["a"]}
        assert ExhaustionDetector._apply_filter(signal, {"rsi": 50.0}) is None


class TestProcessDailyJson:

    def test_file_not_found_returns_zero(self, detector, tmp_json):
        assert detector.process_daily_json(tmp_json) == 0

    def test_empty_file_returns_zero(self, detector, tmp_json):
        _write_candles([], tmp_json)
        assert detector.process_daily_json(tmp_json) == 0

    def test_adds_exhaustion_key_to_every_candle(self, detector, tmp_json):
        """Even candles with no signal should get Exhaustion=null."""
        candles = [
            _make_candle(ts=f"2026-02-27 {h}:00:00 AM", rsi=50)
            for h in ["09:35", "09:40", "09:45", "09:50", "09:55",
                       "10:00", "10:05", "10:10", "10:15", "10:20",
                       "10:25", "10:30", "10:35"]
        ]
        _write_candles(candles, tmp_json)
        detector.process_daily_json(tmp_json)
        result = _read_candles(tmp_json)
        for rec in result:
            assert "Exhaustion" in rec

    def test_preserves_existing_fields(self, detector, tmp_json):
        candles = [_make_candle()]
        _write_candles(candles, tmp_json)
        detector.process_daily_json(tmp_json)
        result = _read_candles(tmp_json)
        assert result[0]["Open"] == candles[0]["Open"]
        assert result[0]["Close"] == candles[0]["Close"]
        assert result[0]["RSI"] == candles[0]["RSI"]


class TestProcessLatestCandle:

    def test_returns_none_for_missing_file(self, detector, tmp_json):
        assert detector.process_latest_candle(tmp_json) is None

    def test_returns_none_when_no_signal(self, detector, tmp_json):
        candles = [
            _make_candle(ts=f"2026-02-27 {h}:00:00 AM", rsi=50)
            for h in ["09:35", "09:40", "09:45", "09:50", "09:55",
                       "10:00", "10:05", "10:10", "10:15", "10:20",
                       "10:25", "10:30", "10:35"]
        ]
        _write_candles(candles, tmp_json)
        assert detector.process_latest_candle(tmp_json) is None

    def test_last_candle_gets_exhaustion_key(self, detector, tmp_json):
        candles = [
            _make_candle(ts=f"2026-02-27 {h}:00:00 AM", rsi=50)
            for h in ["09:35", "09:40", "09:45", "09:50", "09:55",
                       "10:00", "10:05", "10:10", "10:15", "10:20"]
        ]
        _write_candles(candles, tmp_json)
        detector.process_latest_candle(tmp_json)
        result = _read_candles(tmp_json)
        assert "Exhaustion" in result[-1]


class TestCandelHelpers:

    def test_is_bullish(self, detector):
        bar = {"open": 100, "close": 110, "high": 115, "low": 95}
        assert detector._is_bullish(bar) is True
        assert detector._is_bearish(bar) is False

    def test_is_bearish(self, detector):
        bar = {"open": 110, "close": 100, "high": 115, "low": 95}
        assert detector._is_bearish(bar) is True

    def test_body_size(self, detector):
        bar = {"open": 100, "close": 110, "high": 115, "low": 95}
        assert detector._body_size(bar) == 10.0

    def test_shooting_star(self, detector):
        # Upper wick 80% of range, tiny body
        candle = {"open": 100, "close": 101, "high": 110, "low": 99}
        assert detector._is_shooting_star(candle) is True

    def test_hammer(self, detector):
        # Lower wick 80% of range, tiny body
        candle = {"open": 109, "close": 110, "high": 111, "low": 100}
        assert detector._is_hammer(candle) is True


class TestOnRealStreamData:
    """Run the detector on the actual stream data file if it exists."""

    REAL_FILE = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "json", "stream_data", "2026-02-27.json",
    )

    @pytest.mark.skipif(
        not os.path.exists(REAL_FILE),
        reason="Live stream data file not present"
    )
    def test_process_real_file(self, detector):
        """Smoke-test: process the real file without error and
        verify every record now has the Exhaustion key."""
        # Work on a copy so we don't mutate the live file
        import shutil, tempfile
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            shutil.copy2(self.REAL_FILE, tmp.name)
            tmp_path = tmp.name

        try:
            tagged = detector.process_daily_json(tmp_path)
            data = _read_candles(tmp_path)
            for rec in data:
                assert "Exhaustion" in rec
            # tagged count should be non-negative
            assert tagged >= 0
        finally:
            os.unlink(tmp_path)
