import numpy as np
import pandas as pd


def compute_atr(df, period=14):
    high, low, close = df["High"], df["Low"], df["Close"]
    tr = np.maximum(high - low,
                    np.abs(high - close.shift()),
                    np.abs(low - close.shift()))
    return pd.Series(tr).rolling(period).mean()


def compute_adx(df, period=14):
    high, low, close = df["High"], df["Low"], df["Close"]
    plus_dm = high.diff()
    minus_dm = low.diff()

    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)

    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr)
    minus_di = 100 * (minus_dm.abs().rolling(period).mean() / atr)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    adx = dx.rolling(period).mean()
    return adx


def compute_rsi(series, period=2):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def compute_avwap(hist, anchor_date):
    mask = hist.index >= anchor_date
    if not mask.any():
        return np.nan
    sub = hist[mask]
    price = sub["Close"]
    vol = sub["Volume"]
    if vol.sum() == 0:
        return np.nan
    vwap = (price * vol).sum() / vol.sum()
    return vwap
