#!/usr/bin/env python3
"""Quick verification that open-bar ticks no longer corrupt RSI state."""

from spx_stream import RSIAnalyzer, Config
from unittest.mock import MagicMock
import logging

cfg = MagicMock(spec=Config)
cfg.rsi_period = 9
cfg.rsi_plateau_window = 4
cfg.rsi_plateau_threshold = 1.5
cfg.rsi_divergence_lookback = 10
cfg.rsi_divergence_min_price_move_pct = 0.1
cfg.rsi_divergence_min_rsi_delta = 2.0
cfg.rsi_overbought = 70.0
cfg.rsi_oversold = 30.0

log = logging.getLogger("test")
rsi = RSIAnalyzer(cfg, log)

# Simulate 15 bars via timestamp changes (each bar = unique ts)
closes = [100, 101, 102, 101.5, 103, 102, 104, 103.5, 105, 104, 103, 105, 106, 104.5, 103]

for i, c in enumerate(closes):
    ts = f"2026-02-20T10:{i:02d}:00Z"
    candle = {"TimeStamp": ts, "Open": str(c), "High": str(c+0.5),
              "Low": str(c-0.5), "Close": str(c)}
    rsi.feed(candle)

# Last bar is still pending. Force close by feeding new ts.
candle_new = {"TimeStamp": "2026-02-20T10:15:00Z", "Open": "103",
              "High": "103.5", "Low": "102.5", "Close": "103"}
rsi.feed(candle_new)

print(f"RSI ready     : {rsi._rsi_ready}")
print(f"History len   : {len(rsi._history)}")
print(f"Status line   : {rsi.rsi_status_str()}")

# Snapshot avg_gain / avg_loss BEFORE open-bar ticks
ag_before = rsi._avg_gain
al_before = rsi._avg_loss

# Simulate many open-bar ticks at the SAME timestamp (should NOT mutate state)
for tick_close in [103.2, 104, 102, 105, 101, 99, 107, 100, 106]:
    candle_tick = {"TimeStamp": "2026-02-20T10:15:00Z", "Open": "103",
                   "High": "107", "Low": "99", "Close": str(tick_close)}
    rsi.feed(candle_tick)
    prov = rsi.current_rsi()
    print(f"  tick close={tick_close:6.1f}  provisional RSI={prov:6.2f}  | {rsi.rsi_status_str()}")

# Verify state was NOT mutated
assert rsi._avg_gain == ag_before, f"avg_gain CHANGED: {ag_before} -> {rsi._avg_gain}"
assert rsi._avg_loss == al_before, f"avg_loss CHANGED: {al_before} -> {rsi._avg_loss}"

print("\n✅ PASS — open-bar ticks did NOT corrupt RSI(9) state.")
print(f"   avg_gain = {rsi._avg_gain:.6f}  (unchanged)")
print(f"   avg_loss = {rsi._avg_loss:.6f}  (unchanged)")
