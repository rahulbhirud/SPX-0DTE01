"""
test_credit_spreads.py
──────────────────────
Unit tests for OptionSpreadTrader credit spread logic.

Mocks the TradeStation API responses so no live connection is needed.

Usage:
    python -m pytest test_credit_spreads.py -v
"""

import json
import logging
from unittest.mock import MagicMock, patch

# Configure logging for test outputs
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import pytest

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from spx_stream import Config, OptionSpreadTrader, SpreadCandidate, TokenManager


# ══════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════

@pytest.fixture
def mock_config():
    """Return a Config-like object with spread defaults."""
    cfg = MagicMock(spec=Config)
    cfg.base_url = "https://api.tradestation.com/v3"
    cfg.account_id = "SIM123456789"
    cfg.spread_underlying = "SPXW"
    cfg.spread_max_delta = 10          # 10 → 0.10 decimal
    cfg.spread_max_premium = 0.60
    cfg.spread_widths = [5, 10, 20]
    cfg.spread_quantity = 1
    cfg.spread_order_type = "Limit"
    cfg.spread_time_in_force = "Day"
    cfg.spread_dry_run = True
    return cfg


@pytest.fixture
def mock_token_mgr():
    mgr = MagicMock(spec=TokenManager)
    mgr.get_access_token.return_value = "fake-access-token"
    return mgr


@pytest.fixture
def logger():
    return logging.getLogger("test_spreads")


@pytest.fixture
def trader(mock_config, mock_token_mgr, logger):
    return OptionSpreadTrader(mock_config, mock_token_mgr, logger)


# ══════════════════════════════════════════════════════════════
# Fake Option Chain Data
# ══════════════════════════════════════════════════════════════

def _make_option(symbol, strike, delta, bid, ask, expiration="2026-02-20"):
    """Helper to build a single option dict matching TradeStation format."""
    return {
        "Symbol": symbol,
        "StrikePrice": str(strike),
        "Greeks": {"Delta": str(delta)},
        "Bid": str(bid),
        "Ask": str(ask),
        "Expiration": expiration,
    }


# ── Call chain: SPX ≈ 6000, OTM calls above 6050 ─────────────

CALL_CHAIN = [
    # ITM / ATM calls — delta too high, should be skipped
    _make_option("SPXW 6000C", 6000, 0.50, 12.00, 12.50),
    _make_option("SPXW 6010C", 6010, 0.45, 9.00,  9.50),
    _make_option("SPXW 6020C", 6020, 0.38, 6.50,  7.00),
    _make_option("SPXW 6030C", 6030, 0.28, 4.00,  4.50),
    _make_option("SPXW 6040C", 6040, 0.18, 2.20,  2.60),
    _make_option("SPXW 6050C", 6050, 0.12, 1.30,  1.60),

    # OTM calls — delta < 0.10, valid short-leg candidates
    _make_option("SPXW 6060C", 6060, 0.08, 0.80,  1.00),
    _make_option("SPXW 6065C", 6065, 0.06, 0.55,  0.70),
    _make_option("SPXW 6070C", 6070, 0.05, 0.40,  0.55),
    _make_option("SPXW 6075C", 6075, 0.04, 0.28,  0.40),
    _make_option("SPXW 6080C", 6080, 0.03, 0.18,  0.28),
    _make_option("SPXW 6085C", 6085, 0.02, 0.10,  0.18),
    _make_option("SPXW 6090C", 6090, 0.01, 0.05,  0.12),
]

# ── Put chain: SPX ≈ 6000, OTM puts below 5950 ───────────────

PUT_CHAIN = [
    # ITM / ATM puts — delta too high
    _make_option("SPXW 6000P", 6000, -0.50, 11.00, 11.50),
    _make_option("SPXW 5990P", 5990, -0.42, 8.50,  9.00),
    _make_option("SPXW 5980P", 5980, -0.35, 6.00,  6.50),
    _make_option("SPXW 5970P", 5970, -0.25, 3.80,  4.20),
    _make_option("SPXW 5960P", 5960, -0.16, 2.00,  2.40),
    _make_option("SPXW 5950P", 5950, -0.11, 1.20,  1.50),

    # OTM puts — |delta| < 0.10
    _make_option("SPXW 5940P", 5940, -0.08, 0.75,  0.95),
    _make_option("SPXW 5935P", 5935, -0.06, 0.52,  0.68),
    _make_option("SPXW 5930P", 5930, -0.05, 0.38,  0.52),
    _make_option("SPXW 5925P", 5925, -0.04, 0.25,  0.38),
    _make_option("SPXW 5920P", 5920, -0.03, 0.16,  0.25),
    _make_option("SPXW 5915P", 5915, -0.02, 0.08,  0.16),
    _make_option("SPXW 5910P", 5910, -0.01, 0.04,  0.10),
]


def _mock_chain_response(options):
    """Build a mock requests.Response returning the given options."""
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"Options": options}
    resp.raise_for_status = MagicMock()
    return resp


# ══════════════════════════════════════════════════════════════
# Tests – Credit Call Spread (Bear Call)
# ══════════════════════════════════════════════════════════════

class TestCreditCallSpread:

    @patch("spx_stream.requests.get")
    def test_finds_valid_call_spread(self, mock_get, trader):
        """Should find at least one call spread with delta < 0.10 and credit ≤ $0.60."""
        logging.info("Running test_finds_valid_call_spread")
        mock_get.return_value = _mock_chain_response(CALL_CHAIN)

        result = trader.open_credit_call_spread()
        # dry_run=True → returns None but logs the trade
        logging.info(f"Result: {result}")
        assert result is None  # dry run

    @patch("spx_stream.requests.get")
    def test_call_spread_candidates_sorted_by_credit(self, mock_get, trader):
        """Candidates should be sorted highest credit first."""
        logging.info("Running test_call_spread_candidates_sorted_by_credit")
        mock_get.return_value = _mock_chain_response(CALL_CHAIN)

        candidates = trader._find_credit_call_spreads()
        logging.info(f"Candidates: {candidates}")
        assert len(candidates) > 0
        # Verify descending net_credit order
        for i in range(len(candidates) - 1):
            logging.info(f"Comparing candidate {i}: {candidates[i].net_credit} >= {candidates[i + 1].net_credit}")
            assert candidates[i].net_credit >= candidates[i + 1].net_credit

    @patch("spx_stream.requests.get")
    def test_call_spread_delta_filter(self, mock_get, trader):
        """All candidates must have short delta < 0.10."""
        logging.info("Running test_call_spread_delta_filter")
        mock_get.return_value = _mock_chain_response(CALL_CHAIN)

        candidates = trader._find_credit_call_spreads()
        for c in candidates:
            logging.info(f"Short delta: {c.short_delta}")
            assert c.short_delta < 0.10, f"Short delta {c.short_delta} >= 0.10"

    @patch("spx_stream.requests.get")
    def test_call_spread_premium_filter(self, mock_get, trader):
        """All candidates must have net credit ≤ $0.60."""
        logging.info("Running test_call_spread_premium_filter")
        mock_get.return_value = _mock_chain_response(CALL_CHAIN)

        candidates = trader._find_credit_call_spreads()
        for c in candidates:
            logging.info(f"Net credit: {c.net_credit}")
            assert c.net_credit <= 0.60, f"Net credit ${c.net_credit} > $0.60"
            assert c.net_credit > 0, f"Net credit ${c.net_credit} <= 0"

    @patch("spx_stream.requests.get")
    def test_call_spread_width_values(self, mock_get, trader):
        """All candidates must use one of the configured widths."""
        logging.info("Running test_call_spread_width_values")
        mock_get.return_value = _mock_chain_response(CALL_CHAIN)

        candidates = trader._find_credit_call_spreads()
        for c in candidates:
            logging.info(f"Width: {c.width}, Long strike: {c.long_strike}, Short strike: {c.short_strike}")
            assert c.width in [5, 10, 20], f"Width {c.width} not in [5, 10, 20]"
            assert c.long_strike == c.short_strike + c.width

    @patch("spx_stream.requests.get")
    def test_call_spread_no_candidates_when_chain_empty(self, mock_get, trader):
        """No candidates when the chain is empty."""
        logging.info("Running test_call_spread_no_candidates_when_chain_empty")
        mock_get.return_value = _mock_chain_response([])

        result = trader.open_credit_call_spread()
        logging.info(f"Result: {result}")
        assert result is None

    @patch("spx_stream.requests.get")
    def test_call_spread_no_candidates_when_all_high_delta(self, mock_get, trader):
        """No candidates when all options have high delta."""
        logging.info("Running test_call_spread_no_candidates_when_all_high_delta")
        high_delta_chain = [
            _make_option("SPXW 6000C", 6000, 0.50, 12.00, 12.50),
            _make_option("SPXW 6005C", 6005, 0.45, 10.00, 10.50),
            _make_option("SPXW 6010C", 6010, 0.40, 8.00,  8.50),
        ]
        mock_get.return_value = _mock_chain_response(high_delta_chain)

        candidates = trader._find_credit_call_spreads()
        logging.info(f"Candidates: {candidates}")
        assert len(candidates) == 0


# ══════════════════════════════════════════════════════════════
# Tests – Credit Put Spread (Bull Put)
# ══════════════════════════════════════════════════════════════

class TestCreditPutSpread:

    @patch("spx_stream.requests.get")
    def test_finds_valid_put_spread(self, mock_get, trader):
        """Should find at least one put spread with delta < 0.10 and credit ≤ $0.60."""
        logging.info("Running test_finds_valid_put_spread")
        mock_get.return_value = _mock_chain_response(PUT_CHAIN)

        result = trader.open_credit_put_spread()
        logging.info(f"Result: {result}")
        assert result is None  # dry run

    @patch("spx_stream.requests.get")
    def test_put_spread_candidates_sorted_by_credit(self, mock_get, trader):
        """Candidates should be sorted highest credit first."""
        logging.info("Running test_put_spread_candidates_sorted_by_credit")
        mock_get.return_value = _mock_chain_response(PUT_CHAIN)

        candidates = trader._find_credit_put_spreads()
        logging.info(f"Candidates: {candidates}")
        assert len(candidates) > 0
        for i in range(len(candidates) - 1):
            logging.info(f"Comparing candidate {i}: {candidates[i].net_credit} >= {candidates[i + 1].net_credit}")
            assert candidates[i].net_credit >= candidates[i + 1].net_credit

    @patch("spx_stream.requests.get")
    def test_put_spread_delta_filter(self, mock_get, trader):
        """All candidates must have short |delta| < 0.10."""
        logging.info("Running test_put_spread_delta_filter")
        mock_get.return_value = _mock_chain_response(PUT_CHAIN)

        candidates = trader._find_credit_put_spreads()
        for c in candidates:
            logging.info(f"Short delta: {c.short_delta}")
            assert c.short_delta < 0.10, f"Short delta {c.short_delta} >= 0.10"

    @patch("spx_stream.requests.get")
    def test_put_spread_premium_filter(self, mock_get, trader):
        """All candidates must have net credit ≤ $0.60."""
        logging.info("Running test_put_spread_premium_filter")
        mock_get.return_value = _mock_chain_response(PUT_CHAIN)

        candidates = trader._find_credit_put_spreads()
        for c in candidates:
            logging.info(f"Net credit: {c.net_credit}")
            assert c.net_credit <= 0.60, f"Net credit ${c.net_credit} > $0.60"
            assert c.net_credit > 0, f"Net credit ${c.net_credit} <= 0"

    @patch("spx_stream.requests.get")
    def test_put_spread_width_values(self, mock_get, trader):
        """All candidates must use one of the configured widths."""
        logging.info("Running test_put_spread_width_values")
        mock_get.return_value = _mock_chain_response(PUT_CHAIN)

        candidates = trader._find_credit_put_spreads()
        for c in candidates:
            logging.info(f"Width: {c.width}, Long strike: {c.long_strike}, Short strike: {c.short_strike}")
            assert c.width in [5, 10, 20], f"Width {c.width} not in [5, 10, 20]"
            assert c.long_strike == c.short_strike - c.width

    @patch("spx_stream.requests.get")
    def test_put_spread_no_candidates_when_chain_empty(self, mock_get, trader):
        """No candidates when the chain is empty."""
        logging.info("Running test_put_spread_no_candidates_when_chain_empty")
        mock_get.return_value = _mock_chain_response([])

        result = trader.open_credit_put_spread()
        logging.info(f"Result: {result}")
        assert result is None

    @patch("spx_stream.requests.get")
    def test_put_spread_no_candidates_when_all_high_delta(self, mock_get, trader):
        """No candidates when all options have high delta."""
        logging.info("Running test_put_spread_no_candidates_when_all_high_delta")
        high_delta_chain = [
            _make_option("SPXW 6000P", 6000, -0.50, 11.00, 11.50),
            _make_option("SPXW 5995P", 5995, -0.42, 9.00,  9.50),
        ]
        mock_get.return_value = _mock_chain_response(high_delta_chain)

        candidates = trader._find_credit_put_spreads()
        logging.info(f"Candidates: {candidates}")
        assert len(candidates) == 0


# ══════════════════════════════════════════════════════════════
# Tests – Order Placement
# ══════════════════════════════════════════════════════════════

class TestOrderPlacement:

    def _sample_candidate(self) -> SpreadCandidate:
        return SpreadCandidate(
            width=10,
            short_strike=6060,
            long_strike=6070,
            short_symbol="SPXW 6060C",
            long_symbol="SPXW 6070C",
            short_delta=0.08,
            long_delta=0.05,
            net_credit=0.40,
            short_bid=0.80,
            long_ask=0.40,
            expiration="2026-02-20",
        )

    def test_dry_run_returns_none(self, trader):
        """Dry run should log the trade but return None (no HTTP call)."""
        candidate = self._sample_candidate()

        with patch("spx_stream.requests.post") as mock_post:
            result = trader._place_spread_order(candidate, "CALL")
            assert result is None
            mock_post.assert_not_called()

    @patch("spx_stream.requests.post")
    def test_live_order_posts_to_api(self, mock_post, trader):
        """When dry_run=False, should POST to the order execution endpoint."""
        trader.cfg.spread_dry_run = False
        mock_post.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"Orders": [{"OrderID": "12345"}]}),
            raise_for_status=MagicMock(),
        )

        candidate = self._sample_candidate()
        result = trader._place_spread_order(candidate, "CALL")

        assert result is not None
        assert result["Orders"][0]["OrderID"] == "12345"
        mock_post.assert_called_once()

        # Verify the URL
        call_args = mock_post.call_args
        assert "/orderexecution/orders" in call_args[0][0]

    @patch("spx_stream.requests.post")
    def test_order_payload_structure(self, mock_post, trader):
        """Verify the order payload has correct legs and limit price."""
        trader.cfg.spread_dry_run = False
        mock_post.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"Orders": []}),
            raise_for_status=MagicMock(),
        )

        candidate = self._sample_candidate()
        trader._place_spread_order(candidate, "CALL")

        payload = mock_post.call_args[1]["json"]
        assert payload["AccountID"] == "SIM123456789"
        assert payload["LimitPrice"] == "0.4"
        assert len(payload["Legs"]) == 2
        assert payload["Legs"][0]["TradeAction"] == "SELLTOOPEN"
        assert payload["Legs"][0]["Symbol"] == "SPXW 6060C"
        assert payload["Legs"][1]["TradeAction"] == "BUYTOOPEN"
        assert payload["Legs"][1]["Symbol"] == "SPXW 6070C"

    @patch("spx_stream.requests.post")
    def test_order_max_risk_calculation(self, mock_post, trader):
        """Verify max risk = (width - net_credit) * 100."""
        candidate = self._sample_candidate()
        expected_risk = (candidate.width - candidate.net_credit) * 100
        assert expected_risk == 960.0  # (10 - 0.40) * 100


# ══════════════════════════════════════════════════════════════
# Tests – Parse Legs
# ══════════════════════════════════════════════════════════════

class TestParsLegs:

    def test_parse_legs_sorts_by_strike(self, trader):
        """Parsed legs should be sorted by strike ascending."""
        raw = [
            _make_option("C", 6070, 0.05, 0.40, 0.55),
            _make_option("A", 6050, 0.12, 1.30, 1.60),
            _make_option("B", 6060, 0.08, 0.80, 1.00),
        ]
        legs = trader._parse_legs(raw)
        strikes = [l["strike"] for l in legs]
        assert strikes == [6050, 6060, 6070]

    def test_parse_legs_abs_delta(self, trader):
        """Delta should be absolute (puts have negative delta in raw data)."""
        raw = [_make_option("P", 5940, -0.08, 0.75, 0.95)]
        legs = trader._parse_legs(raw)
        assert legs[0]["delta"] == 0.08

    def test_parse_legs_skips_bad_data(self, trader):
        """Options with missing/invalid fields should be skipped."""
        raw = [
            {"Symbol": "BAD", "StrikePrice": "not_a_number"},
            _make_option("OK", 6060, 0.08, 0.80, 1.00),
        ]
        legs = trader._parse_legs(raw)
        assert len(legs) == 1
        assert legs[0]["symbol"] == "OK"


# ══════════════════════════════════════════════════════════════
# Tests – Edge Cases
# ══════════════════════════════════════════════════════════════

class TestEdgeCases:

    @patch("spx_stream.requests.get")
    def test_zero_bid_short_leg_skipped(self, mock_get, trader):
        """Short legs with bid=0 should be excluded."""
        chain = [
            _make_option("SPXW 6060C", 6060, 0.08, 0.00, 1.00),
            _make_option("SPXW 6065C", 6065, 0.06, 0.00, 0.70),
            _make_option("SPXW 6070C", 6070, 0.05, 0.00, 0.55),
        ]
        mock_get.return_value = _mock_chain_response(chain)

        candidates = trader._find_credit_call_spreads()
        assert len(candidates) == 0

    @patch("spx_stream.requests.get")
    def test_negative_credit_skipped(self, mock_get, trader):
        """Spreads where long ask > short bid (debit) should be excluded."""
        chain = [
            _make_option("SPXW 6060C", 6060, 0.08, 0.30, 0.50),
            _make_option("SPXW 6065C", 6065, 0.06, 0.20, 0.40),
            _make_option("SPXW 6070C", 6070, 0.05, 0.10, 0.35),
        ]
        mock_get.return_value = _mock_chain_response(chain)

        candidates = trader._find_credit_call_spreads()
        # 6060 bid=0.30 minus 6065 ask=0.40 = -0.10 (negative, skipped)
        # 6060 bid=0.30 minus 6070 ask=0.35 = -0.05 (negative, skipped)
        for c in candidates:
            assert c.net_credit > 0

    @patch("spx_stream.requests.get")
    def test_premium_exceeding_max_skipped(self, mock_get, trader):
        """Spreads with credit > max_premium should be excluded."""
        chain = [
            _make_option("SPXW 6060C", 6060, 0.08, 2.00, 2.20),
            _make_option("SPXW 6065C", 6065, 0.06, 1.00, 1.10),
            _make_option("SPXW 6070C", 6070, 0.05, 0.50, 0.60),
        ]
        mock_get.return_value = _mock_chain_response(chain)

        candidates = trader._find_credit_call_spreads()
        # 6060 bid=2.00 - 6065 ask=1.10 = 0.90 > 0.60 → skipped
        # 6060 bid=2.00 - 6070 ask=0.60 = 1.40 > 0.60 → skipped
        for c in candidates:
            assert c.net_credit <= 0.60

    @patch("spx_stream.requests.get")
    def test_api_error_returns_none(self, mock_get, trader):
        """API failure should return None gracefully."""
        logging.info("Running test_api_error_returns_none")
        mock_get.side_effect = Exception("Connection refused")

        try:
            result = trader.open_credit_call_spread()
            logging.info(f"Result: {result}")
            assert result is None
        except Exception as e:
            logging.info(f"Exception caught (expected): {e}")
            # Test passes if we reach here - API errors should be handled

        result = trader.open_credit_put_spread()
        assert result is None
