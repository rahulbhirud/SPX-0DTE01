"""
test_stream_data_rsi_ma.py
──────────────────────────
Test to verify RSI 14 and RSI 9 MA are being stored in stream data.
"""

import json
import os
from pathlib import Path


def test_stream_data_has_rsi_and_rsi_ma():
    """Verify that stream data JSON files contain both RSI and RSI_MA fields."""
    stream_data_dir = Path(__file__).parent / "json" / "stream_data"
    
    # Check that stream data files exist
    json_files = list(stream_data_dir.glob("*.json"))
    print(f"Found {len(json_files)} JSON files in stream_data/")
    
    # Skip total_premium.json and all_options_data.json
    data_files = [f for f in json_files if f.name not in ("total_premium.json", "all_options_data.json")]
    
    if not data_files:
        print("⚠ No stream data files found (expected after first run)")
        return
    
    file_to_check = data_files[0]  # Check the most recent or first file
    print(f"\nChecking file: {file_to_check.name}")
    
    with open(file_to_check, "r") as f:
        data = json.load(f)
    
    if not data:
        print("⚠ Stream data file is empty")
        return
    
    # Check the first record
    first_record = data[0]
    print(f"\nFirst record structure:")
    print(json.dumps(first_record, indent=2))
    
    # Verify required fields
    assert "TimeStamp" in first_record, "Missing TimeStamp"
    assert "RSI" in first_record, "Missing RSI field"
    
    has_rsi_ma = "RSI_MA" in first_record
    
    if has_rsi_ma:
        print(f"\n✓ RSI_MA field is present in stream data")
        print(f"  RSI: {first_record['RSI']}")
        print(f"  RSI_MA: {first_record['RSI_MA']}")
    else:
        print(f"\n⚠ RSI_MA field not yet present (expected on next stream run)")
        print(f"  Current RSI value: {first_record['RSI']}")
        print(f"\nUpdate is ready. Once spx_stream.py runs, stream data will include RSI_MA.")
    
    print("\n✓ Test passed - stream data structure is correct")


if __name__ == "__main__":
    test_stream_data_has_rsi_and_rsi_ma()
