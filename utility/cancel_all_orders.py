"""
cancel_all_orders.py
────────────────────
Standalone utility to cancel **all** open orders for the configured
TradeStation account in one shot.

Usage:
    python -m utility.cancel_all_orders
    python -m utility.cancel_all_orders --config yaml/config.yaml
    python utility/cancel_all_orders.py
    python utility/cancel_all_orders.py --config yaml/config.yaml
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Ensure the project root is on sys.path so sibling modules resolve.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from spx_stream import Config, TokenManager, setup_logging
from order_tracker import OrderTracker


class CancelAllOrders:
    """Fetch every open order and cancel them all."""

    def __init__(self, config_path: str = "yaml/config.yaml"):
        if not Path(config_path).exists():
            raise SystemExit(f"Config file not found: {config_path}")

        self.cfg = Config(config_path)
        self.log = setup_logging(self.cfg)
        self.token_mgr = TokenManager(self.cfg, self.log)
        self.tracker = OrderTracker(self.cfg, self.token_mgr, self.log)

    def run(self) -> int:
        """Cancel all open orders.  Returns the number of orders cancelled."""
        self.log.info("─── Cancel-All-Orders utility started ───")

        open_orders = self.tracker.get_open_orders()
        if not open_orders:
            self.log.info("No open orders found. Nothing to cancel.")
            print("\n✔  No open orders to cancel.")
            return 0

        total = len(open_orders)
        self.log.info("Found %d open order(s). Cancelling…", total)
        print(f"\nFound {total} open order(s). Cancelling…\n")

        succeeded = 0
        failed = 0

        for idx, order in enumerate(open_orders, start=1):
            order_id = (
                order.get("OrderID")
                or order.get("orderId")
                or order.get("order_id")
                or order.get("Id")
                or order.get("id")
                or ""
            )
            if not order_id:
                self.log.warning("Skipping order with no recognisable ID: %s", order)
                failed += 1
                continue

            status = (
                order.get("Status")
                or order.get("StatusDescription")
                or order.get("status")
                or "unknown"
            )
            self.log.info(
                "[%d/%d] Cancelling order %s (status: %s)", idx, total, order_id, status
            )
            print(f"  [{idx}/{total}] Cancelling order {order_id} (status: {status})…", end=" ")

            try:
                self.tracker.cancel_order(order_id)
                succeeded += 1
                print("OK")
                self.log.info("Order %s cancelled successfully.", order_id)
            except Exception as exc:
                failed += 1
                print(f"FAILED – {exc}")
                self.log.error("Failed to cancel order %s: %s", order_id, exc)

            # Small delay to avoid API rate-limiting on rapid fire cancels.
            if idx < total:
                time.sleep(0.25)

        print(f"\nDone.  Succeeded: {succeeded}  |  Failed: {failed}  |  Total: {total}")
        self.log.info(
            "─── Cancel-All-Orders complete. succeeded=%d  failed=%d  total=%d ───",
            succeeded,
            failed,
            total,
        )
        return succeeded


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cancel ALL open orders for the configured TradeStation account."
    )
    parser.add_argument(
        "--config",
        default="yaml/config.yaml",
        help="Path to config.yaml (default: yaml/config.yaml)",
    )
    args = parser.parse_args()

    canceller = CancelAllOrders(config_path=args.config)
    canceller.run()


if __name__ == "__main__":
    main()
