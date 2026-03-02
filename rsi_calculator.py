import pandas as pd
import numpy as np


def calculate_rsi(closes: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate RSI using Wilder's smoothing method (exponential moving average).

    This matches the standard RSI calculation used by TradingView, thinkorswim,
    and most professional charting platforms.

    Args:
        closes: Series of closing prices (5-min candle closes).
        period: RSI lookback period (default 14).

    Returns:
        Series of RSI values.
    """
    delta = closes.diff()

    gains = delta.clip(lower=0.0)
    losses = (-delta).clip(lower=0.0)

    # Wilder's smoothing: EMA with alpha = 1/period
    # First value is SMA over the initial `period` bars,
    # then each subsequent value uses the recursive formula:
    #   avg = prev_avg * (period - 1) / period + current / period
    alpha = 1.0 / period
    avg_gain = gains.ewm(alpha=alpha, min_periods=period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=alpha, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))

    return rsi


def calculate_rsi_with_ma(
    df: pd.DataFrame,
    rsi_period: int = 14,
    ma_period: int = 9,
    close_col: str = "close",
) -> pd.DataFrame:
    """
    Calculate RSI and its moving average, appending columns to the dataframe.

    Args:
        df: DataFrame with OHLCV 5-min candle data.
        rsi_period: RSI lookback period (default 14).
        ma_period: SMA period applied to RSI (default 9).
        close_col: Name of the close price column.

    Returns:
        DataFrame with added 'rsi_14' and 'rsi_14_ma_9' columns.
    """
    df = df.copy()

    rsi_col = f"rsi_{rsi_period}"
    ma_col = f"rsi_{rsi_period}_ma_{ma_period}"

    df[rsi_col] = calculate_rsi(df[close_col], period=rsi_period)
    df[ma_col] = df[rsi_col].rolling(window=ma_period, min_periods=ma_period).mean()

    return df


# ---------------------------------------------------------------------------
# Example usage with synthetic 5-min candle data
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    np.random.seed(42)
    n_bars = 200

    # Simulate 5-min closes starting from SPX ~5800
    returns = np.random.normal(loc=0.0, scale=0.0005, size=n_bars)
    prices = 5800.0 * np.cumprod(1 + returns)

    timestamps = pd.date_range(
        start="2026-03-01 09:30", periods=n_bars, freq="5min"
    )

    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": prices * (1 + np.random.normal(0, 0.0001, n_bars)),
            "high": prices * (1 + abs(np.random.normal(0, 0.0003, n_bars))),
            "low": prices * (1 - abs(np.random.normal(0, 0.0003, n_bars))),
            "close": prices,
            "volume": np.random.randint(100, 5000, n_bars),
        }
    )

    df = calculate_rsi_with_ma(df, rsi_period=14, ma_period=9)

    # Show a sample of rows where both RSI and MA are populated
    print(df[["timestamp", "close", "rsi_14", "rsi_14_ma_9"]].dropna().to_string(index=False))
