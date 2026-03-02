#!/usr/bin/env python3
"""Quick test to verify RSI distance config is loaded correctly."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from spx_stream import Config

# Load config
cfg = Config("yaml/config.yaml")

print("RSI Configuration:")
print(f"  Period: {cfg.rsi_period}")
print(f"  MA Period: {cfg.rsi_ma_period}")
print(f"  Overbought: {cfg.rsi_overbought}")
print(f"  Oversold: {cfg.rsi_oversold}")
print(f"  Min Crossover Distance: {cfg.rsi_min_crossover_distance}")

assert cfg.rsi_period == 14, "RSI period should be 14"
assert cfg.rsi_ma_period == 9, "RSI MA period should be 9"
assert cfg.rsi_min_crossover_distance == 8.0, "Min crossover distance should be 8.0"

print("\n✅ All config values loaded correctly!")
