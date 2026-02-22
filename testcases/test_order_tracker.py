"""Tests for order_tracker.py."""

import logging
from unittest.mock import MagicMock, patch

import pytest
import requests as _req

from order_tracker import OrderTracker


@pytest.fixture
def logger():
    return logging.getLogger("test_order_tracker")


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


class TestOrderTracker:
    @patch("order_tracker.requests.get")
    def test_get_open_orders_filters_non_open(self, mock_get, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {
            "Orders": [
                {"OrderID": "1", "Status": "Open"},
                {"OrderID": "2", "Status": "Working"},
                {"OrderID": "3", "Status": "Filled"},
                {"OrderID": "4", "Status": "Canceled"},
            ]
        }
        mock_get.return_value = resp

        tracker = OrderTracker(cfg, token_mgr, logger)
        result = tracker.get_open_orders()

        assert [o["OrderID"] for o in result] == ["1", "2"]
        mock_get.assert_called_once()

    @patch("order_tracker.requests.get")
    def test_get_open_orders_accepts_order_status_key(self, mock_get, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {
            "Orders": [
                {"OrderID": "10", "OrderStatus": "pending"},
                {"OrderID": "11", "OrderStatus": "Rejected"},
            ]
        }
        mock_get.return_value = resp

        tracker = OrderTracker(cfg, token_mgr, logger)
        result = tracker.get_open_orders()

        assert [o["OrderID"] for o in result] == ["10"]

    @patch("order_tracker.requests.get")
    def test_get_open_orders_http_error_raises(self, mock_get, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        resp.text = "server error"
        resp.raise_for_status.side_effect = _req.exceptions.HTTPError("500 Error", response=resp)
        mock_get.return_value = resp

        tracker = OrderTracker(cfg, token_mgr, logger)

        with pytest.raises(_req.exceptions.HTTPError):
            tracker.get_open_orders()

    @patch("order_tracker.requests.delete")
    def test_cancel_order_success(self, mock_delete, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {"Orders": [{"OrderID": "77", "Status": "PendingCancel"}]}
        mock_delete.return_value = resp

        tracker = OrderTracker(cfg, token_mgr, logger)
        result = tracker.cancel_order("77")

        assert result["Orders"][0]["OrderID"] == "77"
        mock_delete.assert_called_once()

    @patch("order_tracker.requests.get")
    def test_get_open_orders_accepts_lowercase_orders_key(self, mock_get, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {
            "orders": [
                {"OrderID": "21", "Status": "OPEN"},
                {"OrderID": "22", "Status": "FILLED"},
            ]
        }
        mock_get.return_value = resp

        tracker = OrderTracker(cfg, token_mgr, logger)
        result = tracker.get_open_orders()

        assert [o["OrderID"] for o in result] == ["21"]

    @patch("order_tracker.requests.get")
    def test_get_open_orders_uses_status_description(self, mock_get, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {
            "Orders": [
                {"OrderID": "31", "StatusDescription": "Working"},
                {"OrderID": "32", "StatusDescription": "Rejected"},
            ]
        }
        mock_get.return_value = resp

        tracker = OrderTracker(cfg, token_mgr, logger)
        result = tracker.get_open_orders()

        assert [o["OrderID"] for o in result] == ["31"]

    @patch("order_tracker.requests.get")
    def test_get_open_orders_accepts_requested_open_status_values(self, mock_get, cfg, token_mgr, logger):
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {
            "Orders": [
                {"OrderID": "41", "Status": "ACK"},
                {"OrderID": "42", "Status": "Received"},
                {"OrderID": "43", "Status": "DON"},
                {"OrderID": "44", "Status": "Queued"},
                {"OrderID": "45", "Status": "FPR"},
                {"OrderID": "46", "Status": "Partial Fill (Alive)"},
                {"OrderID": "47", "Status": "OPN"},
                {"OrderID": "48", "Status": "Sent"},
                {"OrderID": "49", "Status": "OSO"},
                {"OrderID": "50", "Status": "OSO Order"},
                {"OrderID": "51", "Status": "PLA"},
                {"OrderID": "52", "Status": "Sending"},
                {"OrderID": "53", "Status": "Filled"},
                {"OrderID": "54", "Status": "OUT"},
            ]
        }
        mock_get.return_value = resp

        tracker = OrderTracker(cfg, token_mgr, logger)
        result = tracker.get_open_orders()

        assert [o["OrderID"] for o in result] == [
            "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52"
        ]
