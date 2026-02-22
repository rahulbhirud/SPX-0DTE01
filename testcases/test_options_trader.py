"""Tests for options_trader.py – OCC symbol building and spread selection."""

import json
import logging
import os
import tempfile
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from options_trader import OptionsTrader, _build_occ_symbol, _safe_float


# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def logger():
    return logging.getLogger("test_options_trader")


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


def _write_data(tmp_dir, data: dict) -> str:
    path = os.path.join(tmp_dir, "options_data_config.json")
    with open(path, "w") as f:
        json.dump(data, f)
    return path


# ── _build_occ_symbol ────────────────────────────────────────

class TestBuildOccSymbol:
    def test_put_symbol(self):
        sym = _build_occ_symbol("$SPXW.X", "2026-02-23", "Put", 6775.0)
        assert sym == "SPXW 260223P6775"

    def test_call_symbol(self):
        sym = _build_occ_symbol("$SPXW.X", "2026-02-23", "Call", 6980.0)
        assert sym == "SPXW 260223C6980"

    def test_fractional_strike(self):
        sym = _build_occ_symbol("$SPXW.X", "2026-02-23", "Put", 6775.5)
        assert sym == "SPXW 260223P6775.5"

    def test_strips_dollar_and_suffix(self):
        sym = _build_occ_symbol("$SPX.X", "2026-01-15", "Call", 5000.0)
        assert sym.startswith("SPX 260115")

    def test_plain_root(self):
        sym = _build_occ_symbol("SPXW", "2026-03-01", "Put", 7000.0)
        assert sym == "SPXW 260301P7000"

    def test_short_root_padded(self):
        sym = _build_occ_symbol("SPY", "2026-06-15", "Call", 450.0)
        assert sym == "SPY 260615C450"


# ── _safe_float ───────────────────────────────────────────────

class TestSafeFloat:
    def test_valid(self):
        assert _safe_float("3.14") == 3.14

    def test_none(self):
        assert _safe_float(None) == 0.0

    def test_garbage(self):
        assert _safe_float("abc", -1.0) == -1.0


# ── _pick_max_premium ────────────────────────────────────────

class TestPickMaxPremium:
    def test_returns_highest_premium(self, cfg, token_mgr, logger):
        trader = OptionsTrader(cfg, token_mgr, logger)
        spreads = [
            {"short_symbol": "A", "long_symbol": "B", "net_credit": 0.5},
            {"short_symbol": "C", "long_symbol": "D", "net_credit": 0.7},
            {"short_symbol": "E", "long_symbol": "F", "net_credit": 0.6},
        ]
        best = trader._pick_max_premium(spreads)
        assert best["net_credit"] == 0.7

    def test_skips_empty_symbols(self, cfg, token_mgr, logger):
        trader = OptionsTrader(cfg, token_mgr, logger)
        spreads = [
            {"short_symbol": "", "long_symbol": "", "net_credit": 0.9},
            {"short_symbol": "A", "long_symbol": "B", "net_credit": 0.4},
        ]
        best = trader._pick_max_premium(spreads)
        assert best["net_credit"] == 0.4

    def test_returns_none_when_all_invalid(self, cfg, token_mgr, logger):
        trader = OptionsTrader(cfg, token_mgr, logger)
        spreads = [
            {"short_symbol": "", "long_symbol": "", "net_credit": 0.5},
        ]
        assert trader._pick_max_premium(spreads) is None


# ── _load_spreads with symbol backfill ────────────────────────

class TestLoadSpreadsBackfill:
    """Verify _load_spreads fills in missing symbols from metadata."""

    SAMPLE_DATA = {
        "symbol": "$SPXW.X",
        "current_price": 6910.22,
        "expiration": "2026-02-23",
        "credit_put_spreads": [
            {
                "spread_type": "bull_put",
                "short_strike": 6775.0,
                "long_strike": 6745.0,
                "width": 30,
                "short_symbol": "",
                "long_symbol": "",
                "net_credit": 0.6,
                "short_bid": 3.9,
                "long_ask": 3.3,
                "max_risk": 2940.0,
            }
        ],
        "credit_call_spreads": [
            {
                "spread_type": "bear_call",
                "short_strike": 6980.0,
                "long_strike": 7000.0,
                "width": 20,
                "short_symbol": "",
                "long_symbol": "",
                "net_credit": 0.7,
                "short_bid": 1.1,
                "long_ask": 0.4,
                "max_risk": 1930.0,
            }
        ],
    }

    def test_put_symbols_built(self, cfg, token_mgr, logger):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_data(tmp, self.SAMPLE_DATA)
            trader = OptionsTrader(cfg, token_mgr, logger)
            trader.DATA_FILE = path

            spreads = trader._load_spreads("credit_put_spreads")
            assert len(spreads) == 1
            s = spreads[0]
            assert s["short_symbol"] == "SPXW 260223P6775"
            assert s["long_symbol"] == "SPXW 260223P6745"

    def test_call_symbols_built(self, cfg, token_mgr, logger):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_data(tmp, self.SAMPLE_DATA)
            trader = OptionsTrader(cfg, token_mgr, logger)
            trader.DATA_FILE = path

            spreads = trader._load_spreads("credit_call_spreads")
            assert len(spreads) == 1
            s = spreads[0]
            assert s["short_symbol"] == "SPXW 260223C6980"
            assert s["long_symbol"] == "SPXW 260223C7000"

    def test_existing_symbols_not_overwritten(self, cfg, token_mgr, logger):
        data = {
            "symbol": "$SPXW.X",
            "expiration": "2026-02-23",
            "credit_put_spreads": [
                {
                    "spread_type": "bull_put",
                    "short_strike": 6775.0,
                    "long_strike": 6745.0,
                    "short_symbol": "EXISTING_SHORT",
                    "long_symbol": "EXISTING_LONG",
                    "net_credit": 0.6,
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_data(tmp, data)
            trader = OptionsTrader(cfg, token_mgr, logger)
            trader.DATA_FILE = path

            spreads = trader._load_spreads("credit_put_spreads")
            assert spreads[0]["short_symbol"] == "EXISTING_SHORT"
            assert spreads[0]["long_symbol"] == "EXISTING_LONG"

    def test_missing_file_returns_empty(self, cfg, token_mgr, logger):
        trader = OptionsTrader(cfg, token_mgr, logger)
        trader.DATA_FILE = "/nonexistent/path.json"
        assert trader._load_spreads("credit_put_spreads") == []


# ── End-to-end: open_put_credit_spread with backfill ──────────

class TestOpenPutCreditSpreadBackfill:
    """Ensure the put-spread workflow succeeds after symbol backfill."""

    DATA = {
        "symbol": "$SPXW.X",
        "current_price": 6910.22,
        "expiration": "2026-02-23",
        "credit_put_spreads": [
            {
                "spread_type": "bull_put",
                "short_strike": 6775.0,
                "long_strike": 6745.0,
                "width": 30,
                "short_symbol": "",
                "long_symbol": "",
                "short_delta": 0.0885,
                "long_delta": 0.0666,
                "net_credit": 0.6,
                "short_bid": 3.9,
                "long_ask": 3.3,
                "max_risk": 2940.0,
            }
        ],
    }

    @patch("options_trader.requests.post")
    def test_order_submitted_with_built_symbols(self, mock_post, cfg, token_mgr, logger):
        # First call = orderconfirm, Second call = orders
        confirm_resp = MagicMock()
        confirm_resp.ok = True
        confirm_resp.json.return_value = {"ConfirmationMessage": "ok"}

        order_resp = MagicMock()
        order_resp.ok = True
        order_resp.json.return_value = {
            "Orders": [{"OrderID": "12345", "Status": "Ok"}]
        }
        mock_post.side_effect = [confirm_resp, order_resp]

        with tempfile.TemporaryDirectory() as tmp:
            path = _write_data(tmp, self.DATA)
            trader = OptionsTrader(cfg, token_mgr, logger)
            trader.DATA_FILE = path

            result = trader.open_put_credit_spread(quantity=1)

        assert result is not None
        assert result["Orders"][0]["OrderID"] == "12345"

        # Two POST calls: confirm + order
        assert mock_post.call_count == 2

        # Verify confirmation call uses same bare payload
        confirm_call = mock_post.call_args_list[0]
        confirm_url = confirm_call.args[0] if confirm_call.args else confirm_call.kwargs.get("url", "")
        assert "orderconfirm" in confirm_url
        confirm_json = confirm_call.kwargs.get("json") or confirm_call[1].get("json")
        assert confirm_json["OrderType"] == "Limit"
        assert confirm_json["TimeInForce"] == {"Duration": "GTC"}

        # Verify order call payload
        order_call = mock_post.call_args_list[1]
        payload = order_call.kwargs.get("json") or order_call[1].get("json")
        assert payload["OrderType"] == "Limit"
        assert "Route" not in payload
        legs = payload["Legs"]
        assert legs[0]["Symbol"] == "SPXW 260223P6775"
        assert legs[1]["Symbol"] == "SPXW 260223P6745"


# ── Payload structure ─────────────────────────────────────────

class TestBuildOrderPayload:
    """Verify the order payload matches TradeStation V3 requirements."""

    def test_order_type_is_limit(self, cfg, token_mgr, logger):
        trader = OptionsTrader(cfg, token_mgr, logger)
        spread = {
            "short_symbol": "SPXW 260223P6775",
            "long_symbol": "SPXW 260223P6745",
            "net_credit": 0.6,
        }
        payload = trader._build_order_payload(spread, quantity=1)
        assert payload["OrderType"] == "Limit"

    def test_limit_price_is_negative(self, cfg, token_mgr, logger):
        trader = OptionsTrader(cfg, token_mgr, logger)
        spread = {
            "short_symbol": "SPXW 260223P6775",
            "long_symbol": "SPXW 260223P6745",
            "net_credit": 0.6,
        }
        payload = trader._build_order_payload(spread, quantity=1)
        assert payload["LimitPrice"] == "-0.60"

    def test_no_route_field(self, cfg, token_mgr, logger):
        trader = OptionsTrader(cfg, token_mgr, logger)
        spread = {
            "short_symbol": "A",
            "long_symbol": "B",
            "net_credit": 0.5,
        }
        payload = trader._build_order_payload(spread, quantity=1)
        assert "Route" not in payload

    def test_time_in_force_gtc(self, cfg, token_mgr, logger):
        trader = OptionsTrader(cfg, token_mgr, logger)
        spread = {
            "short_symbol": "A",
            "long_symbol": "B",
            "net_credit": 0.5,
        }
        payload = trader._build_order_payload(spread, quantity=1)
        assert payload["TimeInForce"] == {"Duration": "GTC"}

    def test_legs_structure(self, cfg, token_mgr, logger):
        trader = OptionsTrader(cfg, token_mgr, logger)
        spread = {
            "short_symbol": "SHORT",
            "long_symbol": "LONG",
            "net_credit": 0.5,
        }
        payload = trader._build_order_payload(spread, quantity=2)
        legs = payload["Legs"]
        assert len(legs) == 2
        assert legs[0]["TradeAction"] == "SELLTOOPEN"
        assert legs[0]["Quantity"] == "2"
        assert legs[1]["TradeAction"] == "BUYTOOPEN"
        assert legs[1]["Quantity"] == "2"


# ── Error handling ────────────────────────────────────────────

class TestExtractRespBody:
    def test_none_response(self, cfg, token_mgr, logger):
        assert OptionsTrader._extract_resp_body(None) == "<no response>"

    def test_text_response(self, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.text = '{"error": "bad request"}'
        assert OptionsTrader._extract_resp_body(resp) == '{"error": "bad request"}'

    def test_empty_text_falls_back_to_content(self, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.text = ""
        resp.content = b"raw error"
        assert OptionsTrader._extract_resp_body(resp) == "raw error"


class TestConfirmOrderRejectsOnFailure:

    @patch("options_trader.requests.post")
    def test_confirm_failure_prevents_order(self, mock_post, cfg, token_mgr, logger):
        """If orderconfirm returns 400, the order should NOT be placed."""
        import requests as _req

        confirm_resp = MagicMock()
        confirm_resp.ok = False
        confirm_resp.status_code = 400
        confirm_resp.text = '{"Message": "Invalid symbol"}'
        confirm_resp.raise_for_status.side_effect = _req.exceptions.HTTPError(
            response=confirm_resp
        )
        mock_post.return_value = confirm_resp

        with tempfile.TemporaryDirectory() as tmp:
            data = {
                "symbol": "$SPXW.X",
                "expiration": "2026-02-23",
                "credit_put_spreads": [
                    {
                        "spread_type": "bull_put",
                        "short_strike": 6775.0,
                        "long_strike": 6745.0,
                        "short_symbol": "SPXW 260223P6775",
                        "long_symbol": "SPXW 260223P6745",
                        "net_credit": 0.6,
                        "width": 30,
                    }
                ],
            }
            path = _write_data(tmp, data)
            trader = OptionsTrader(cfg, token_mgr, logger)
            trader.DATA_FILE = path

            result = trader.open_put_credit_spread(quantity=1)

        assert result is None
        # Only the confirm call was made, not the order placement
        assert mock_post.call_count == 1
