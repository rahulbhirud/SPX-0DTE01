# RSI 14 and RSI 9 MA Stream Data Update

## Changes Made

### Modified: `spx_stream.py`

Updated the `save_stream_data()` method to include **RSI 9 MA (Moving Average)** in addition to the existing RSI 14 value.

**Line 1053-1066:**
```python
# Build the record to persist
rsi_val = self._rsi.current_rsi()
rsi_ma_val = self._rsi.current_rsi_ma(self.cfg.rsi_ma_period if self.cfg else 9)
record = {
    "TimeStamp":   ts_est_str,
    "Open":        candle.get("Open", ""),
    "High":        candle.get("High", ""),
    "Low":         candle.get("Low", ""),
    "Close":       candle.get("Close", ""),
    "TotalVolume": candle.get("TotalVolume", ""),
    "Status":      candle.get("Status", ""),
    "RSI":         round(rsi_val, 4) if rsi_val is not None else None,
    "RSI_MA":      round(rsi_ma_val, 4) if rsi_ma_val is not None else None,  # NEW FIELD
}
```

## Stream Data Structure

### New JSON Format (after next stream run)

Each 5-minute candle in `json/stream_data/{YYYY-MM-DD}.json` will now include:

```json
{
  "TimeStamp": "2026-03-02 09:35:00 AM",
  "Open": "6824.36",
  "High": "6829.27",
  "Low": "6796.85",
  "Close": "6827.2",
  "TotalVolume": "276440",
  "Status": "",
  "RSI": 30.088,        // RSI(14) - Wilder's smoothed
  "RSI_MA": 28.5042     // SMA(9) of RSI(14) - signal line for plotting
}
```

## Usage for Plotting

The stream data now contains both values needed for technical analysis charts:

1. **RSI** - The 14-period Relative Strength Index
   - Used for overbought/oversold levels (>70 or <30)
   - Currently displayed on the dashboard

2. **RSI_MA** - The 9-period Simple Moving Average of RSI
   - Signal line for crossover detection
   - Already displayed in dashboard as "MA 9"
   - Now also available in stream data for historical plotting

### How to Use in Plotting

```python
import json
import pandas as pd
import matplotlib.pyplot as plt

# Load stream data
with open('json/stream_data/2026-03-02.json', 'r') as f:
    data = json.load(f)

df = pd.DataFrame(data)
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])

# Plot RSI and its MA
plt.figure(figsize=(12, 6))
plt.subplot(2, 1, 1)
plt.plot(df['TimeStamp'], df['Close'], label='SPX Close')
plt.legend()

plt.subplot(2, 1, 2)
plt.plot(df['TimeStamp'], df['RSI'], label='RSI(14)', alpha=0.7)
plt.plot(df['TimeStamp'], df['RSI_MA'], label='RSI MA(9)', linewidth=2, alpha=0.8)
plt.axhline(y=70, color='r', linestyle='--', alpha=0.5, label='Overbought')
plt.axhline(y=30, color='g', linestyle='--', alpha=0.5, label='Oversold')
plt.legend()
plt.ylabel('RSI')
plt.xlabel('Time')
plt.show()
```

## Dashboard Integration

The dashboard (`dashboard.py` and `templates/dashboard.html`) already displays both values:
- **rsi_14**: Current RSI(14) value
- **rsi_14_ma_9**: Current RSI MA(9) signal line

These are read from `dashboard_state.json` which is updated in real-time by `spx_stream.py`.

## Files Modified

- ✅ `/workspaces/SPX-0DTE01/spx_stream.py` - Added RSI_MA to stream data records

## Testing

Run the validation test:
```bash
python test_stream_data_rsi_ma.py
```

This test verifies the stream data structure includes the necessary fields for plotting.

## Next Steps

1. The next time `spx_stream.py` streams candles, new stream data files will include the RSI_MA field
2. Existing historical stream data files will continue to work without RSI_MA
3. Charts/plotting code can now access both RSI(14) and its MA(9) signal line from stream data
4. Dashboard continues to display real-time values as before
