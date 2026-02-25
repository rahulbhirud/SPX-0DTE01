"""Tests for position_tracker.py — spread matching, P&L, and parsing."""

import logging
from unittest.mock import MagicMock, patch

import pytest
import requests as _req

from position_tracker import (
    CreditSpread,
    PositionLeg,
    PositionTracker,
    _parse_option_type_from_symbol,
    _parse_strike_from_symbol,
    _safe_float,
    _truncate_time,
)


# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def logger():
    return logging.getLogger("test_position_tracker")


@pytest.fixture
def cfg():
    c = MagicMock()
    c.account_id = "SIM123"
    c.base_url = "https://sim-api.tradestation.com/v3"
    return c


@pytest.fixture
def token_mgr():
    t = MagicMock()
    t.get_access_token.return_value = "fake-token"
    return t


# ── Helper tests ──────────────────────────────────────────────

class TestHelpers:
    def test_truncate_time(self):
        assert _truncate_time("2026-02-22T14:36:50Z") == "2026-02-22T14:36"
        assert _truncate_time("2026-02-22T14:36:50.123Z") == "2026-02-22T14:36"
        assert _truncate_time("short") == "short"

    def test_parse_option_type(self):
        assert _parse_option_type_from_symbol("SPXW 260223P6780") == "PUT"
        assert _parse_option_type_from_symbol("SPXW 260223C7000") == "CALL"
        assert _parse_option_type_from_symbol("UNKNOWN") == ""

    def test_parse_strike(self):
        assert _parse_strike_from_symbol("SPXW 260223P6780") == 6780.0
        assert _parse_strike_from_symbol("SPXW 260223C7000.5") == 7000.5
        assert _parse_strike_from_symbol("UNKNOWN") == 0.0

    def test_safe_float(self):
        assert _safe_float("3.5") == 3.5
        assert _safe_float(None) == 0.0
        assert _safe_float("bad", -1.0) == -1.0


# ── Parse position ───────────────────────────────────────────

class TestParsePosition:
    def test_parses_short_leg(self):
        pos = {
            "Symbol": "SPXW 260223P6780",
            "Quantity": "-1",
            "LongShort": "SHORT",
            "StrikePrice": "6780",
            "OptionType": "PUT",
            "AveragePrice": "3.50",
            "Last": "2.00",
            "MarketValue": "-200.0",
            "OpenedDateTime": "2026-02-22T14:36:50Z",
            "Delta": "-0.08",
        }
        leg = PositionTracker._parse_position(pos)
        assert leg is not None
        assert leg.strike == 6780.0
        assert leg.quantity == -1
        assert leg.trade_action == "SELL"
        assert leg.option_type == "PUT"
        assert leg.delta == -0.08

    def test_parses_long_leg(self):
        pos = {
            "Symbol": "SPXW 260223P6750",
            "Quantity": "1",
            "LongShort": "LONG",
            "StrikePrice": "6750",
            "OptionType": "PUT",
            "AveragePrice": "2.80",
            "Last": "1.20",
            "MarketValue": "120.0",
            "OpenedDateTime": "2026-02-22T14:36:50Z",
            "Delta": "0.04",
        }
        leg = PositionTracker._parse_position(pos)
        assert leg is not None
        assert leg.strike == 6750.0
        assert leg.quantity == 1
        assert leg.trade_action == "BUY"

    def test_empty_symbol_returns_none(self):
        assert PositionTracker._parse_position({"Symbol": ""}) is None


# ── Spread matching ──────────────────────────────────────────

class TestMatchSpreads:
    def _make_leg(self, **kwargs) -> PositionLeg:
        defaults = {
            "symbol": "SPXW 260223P6780",
            "strike": 6780.0,
            "option_type": "PUT",
            "quantity": -1,
            "trade_action": "SELL",
            "avg_price": 3.50,
            "last_price": 2.00,
            "bid": 1.90,
            "ask": 2.10,
            "market_value": -200.0,
            "opened_dt": "2026-02-22T14:36",
            "delta": -0.08,
            "raw": {},
        }
        defaults.update(kwargs)
        return PositionLeg(**defaults)

    def test_put_credit_spread_matched(self, cfg, token_mgr, logger):
        tracker = PositionTracker(cfg, token_mgr, logger)
        short = self._make_leg(
            symbol="SPXW 260223P6780", strike=6780, quantity=-1,
            trade_action="SELL", avg_price=3.50, last_price=2.00,
            bid=1.90, ask=2.10,
            option_type="PUT", opened_dt="2026-02-22T14:36", delta=-0.08
        )
        long = self._make_leg(
            symbol="SPXW 260223P6750", strike=6750, quantity=1,
            trade_action="BUY", avg_price=2.80, last_price=1.20,
            bid=1.10, ask=1.30,
            option_type="PUT", opened_dt="2026-02-22T14:36", delta=0.04
        )
        spreads = tracker._match_spreads([short, long])
        assert len(spreads) == 1
        s = spreads[0]
        assert s.spread_type == "Put Credit"
        assert s.short_strike == 6780
        assert s.long_strike == 6750
        assert s.contracts == 1
        assert s.short_delta == -0.08
        # credit = 3.50 - 2.80 = 0.70
        assert abs(s.credit_received - 0.70) < 0.01
        # cost to close = short_ask - long_bid = 2.10 - 1.10 = 1.00
        assert abs(s.current_cost - 1.00) < 0.01
        # P&L = (0.70 - 1.00) * 1 * 100 = -30.0
        assert abs(s.open_pnl - (-30.0)) < 0.01

    def test_call_credit_spread_matched(self, cfg, token_mgr, logger):
        tracker = PositionTracker(cfg, token_mgr, logger)
        short = self._make_leg(
            symbol="SPXW 260223C7000", strike=7000, quantity=-1,
            trade_action="SELL", avg_price=2.00, last_price=1.00,
            bid=0.90, ask=1.10,
            option_type="CALL", opened_dt="2026-02-22T14:36", delta=-0.05
        )
        long = self._make_leg(
            symbol="SPXW 260223C7020", strike=7020, quantity=1,
            trade_action="BUY", avg_price=1.30, last_price=0.50,
            bid=0.40, ask=0.60,
            option_type="CALL", opened_dt="2026-02-22T14:36", delta=0.03
        )
        spreads = tracker._match_spreads([short, long])
        assert len(spreads) == 1
        assert spreads[0].spread_type == "Call Credit"

    def test_different_times_are_separate_spreads(self, cfg, token_mgr, logger):
        tracker = PositionTracker(cfg, token_mgr, logger)
        legs = [
            self._make_leg(quantity=-1, opened_dt="2026-02-22T14:30"),
            self._make_leg(quantity=1, opened_dt="2026-02-22T14:30"),
            self._make_leg(quantity=-1, opened_dt="2026-02-22T15:00"),
            self._make_leg(quantity=1, opened_dt="2026-02-22T15:00"),
        ]
        spreads = tracker._match_spreads(legs)
        assert len(spreads) == 2

    def test_unmatched_legs_not_paired(self, cfg, token_mgr, logger):
        tracker = PositionTracker(cfg, token_mgr, logger)
        legs = [
            self._make_leg(quantity=-1, opened_dt="2026-02-22T14:30"),
            # No long leg at same time
        ]
        spreads = tracker._match_spreads(legs)
        assert len(spreads) == 0


# ── API integration ──────────────────────────────────────────

class TestGetOpenSpreads:
    @patch("position_tracker.requests.get")
    def test_returns_matched_spreads(self, mock_get, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {
            "Positions": [
                {
                    "Symbol": "SPXW 260223P6780",
                    "Quantity": "-1",
                    "LongShort": "SHORT",
                    "StrikePrice": "6780",
                    "OptionType": "PUT",
                    "AveragePrice": "3.50",
                    "Last": "2.00",
                    "MarketValue": "-200.0",
                    "OpenedDateTime": "2026-02-22T14:36:50Z",
                    "Delta": "-0.08",
                },
                {
                    "Symbol": "SPXW 260223P6750",
                    "Quantity": "1",
                    "LongShort": "LONG",
                    "StrikePrice": "6750",
                    "OptionType": "PUT",
                    "AveragePrice": "2.80",
                    "Last": "1.20",
                    "MarketValue": "120.0",
                    "OpenedDateTime": "2026-02-22T14:36:50Z",
                    "Delta": "0.04",
                },
            ]
        }
        mock_get.return_value = resp

        tracker = PositionTracker(cfg, token_mgr, logger)
        spreads = tracker.get_open_spreads()

        assert len(spreads) == 1
        assert spreads[0].spread_type == "Put Credit"
        assert spreads[0].short_strike == 6780.0
        assert spreads[0].long_strike == 6750.0
        mock_get.assert_called_once()

    @patch("position_tracker.requests.get")
    def test_empty_positions_returns_empty(self, mock_get, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {"Positions": []}
        mock_get.return_value = resp

        tracker = PositionTracker(cfg, token_mgr, logger)
        assert tracker.get_open_spreads() == []

    @patch("position_tracker.requests.get")
    def test_http_error_raises(self, mock_get, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        resp.text = "server error"
        resp.raise_for_status.side_effect = _req.exceptions.HTTPError("500", response=resp)
        mock_get.return_value = resp

        tracker = PositionTracker(cfg, token_mgr, logger)
        with pytest.raises(_req.exceptions.HTTPError):
            tracker.get_open_spreads()
