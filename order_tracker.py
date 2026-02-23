"""
order_tracker.py
────────────────
Provides a lightweight API wrapper to retrieve currently open orders
for the configured TradeStation account.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import requests


class OrderTracker:
    """Fetches and filters account orders to return open orders."""

    _OPEN_STATUSES = {
        "ACK",
        "DON",
        "FPR",
        "OPN",
        "OSO",
        "PLA",
    }

    _CLOSED_STATUSES = {
        "CANCELLED",
        "CANCELED",
        "EXPIRED",
        "FAILED",
        "FILLED",
        "REJECTED",
        "DONEFOR_DAY",
        "DONE_FOR_DAY",
        "DONE",
        "OUT",
        "REJ",
        "FLL"
    }

    def __init__(self, cfg, token_mgr, logger: logging.Logger):
        self.cfg = cfg
        self.token_mgr = token_mgr
        self.log = logger

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """Return all currently open orders for ``cfg.account_id``.

        This method fetches account orders from the brokerage endpoint,
        then filters the response to orders in open/working states.
        """
        url = f"{self.cfg.base_url}/brokerage/accounts/{self.cfg.account_id}/orders"
        token = self.token_mgr.get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        self.log.debug("Fetching account orders from %s", url)
        resp = requests.get(url, headers=headers, timeout=20)
        if not resp.ok:
            self.log.error(
                "Open-orders request failed (HTTP %d): %s",
                resp.status_code,
                self._extract_resp_body(resp),
            )
            resp.raise_for_status()

        body = resp.json()
        orders = self._extract_orders(body)
        if not isinstance(orders, list):
            self.log.warning("Unexpected orders response shape; expected a list of order objects.")
            return []

        open_orders = [order for order in orders if self._is_open_order(order)]
        self.log.info("Found %d open orders (out of %d total).", len(open_orders), len(orders))
        return open_orders

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an order by order id and return API response JSON."""
        oid = str(order_id).strip()
        if not oid:
            raise ValueError("order_id is required")

        url = f"{self.cfg.base_url}/orderexecution/orders/{oid}"
        token = self.token_mgr.get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        self.log.info("Canceling order %s", oid)
        resp = requests.delete(url, headers=headers, timeout=20)
        if not resp.ok:
            self.log.error(
                "Cancel-order request failed for %s (HTTP %d): %s",
                oid,
                resp.status_code,
                self._extract_resp_body(resp),
            )
            resp.raise_for_status()

        try:
            return resp.json()
        except ValueError:
            return {"success": True, "message": "Cancel request accepted.", "order_id": oid}

    @classmethod
    def _is_open_order(cls, order: Dict[str, Any]) -> bool:
        """Return True if an order appears to be open/working.

        If status is missing or unknown, default to True so valid orders
        are not accidentally hidden in the UI.
        """
        status = (
            order.get("Status")
            or order.get("status")
            or order.get("OrderStatus")
            or order.get("orderStatus")
            or order.get("StatusDescription")
            or order.get("statusDescription")
            or ""
        )
        normalized = (
            str(status)
            .strip()
            .upper()
            .replace(" ", "_")
            .replace("-", "_")
        )
        if not normalized:
            return True
        if normalized in cls._OPEN_STATUSES:
            return True
        if normalized in cls._CLOSED_STATUSES:
            return False
        if "FILL" in normalized or "REJECT" in normalized or "CANCEL" in normalized:
            return False
        return True

    @staticmethod
    def _extract_orders(body: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract orders list from common response envelope variants."""
        for key in ("Orders", "orders", "Items", "items", "Data", "data"):
            value = body.get(key)
            if isinstance(value, list):
                return value
        return []

    @staticmethod
    def _extract_resp_body(resp) -> str:
        """Best-effort extraction of an HTTP response body."""
        if resp is None:
            return "<no response>"
        try:
            return resp.text or resp.content.decode("utf-8", errors="replace")
        except Exception:
            try:
                return resp.content.decode("utf-8", errors="replace")
            except Exception:
                return "<unreadable>"
