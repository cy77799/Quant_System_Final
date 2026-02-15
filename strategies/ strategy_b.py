import pandas as pd
import numpy as np
from .base import SwingBase
from .utils import compute_atr, compute_rsi, compute_avwap
from universal_backtester import Order


class StrategyB_AVWAPPullback(SwingBase):
    def __init__(self,
                 anchor_type="maxvolume",
                 avwap_touch_pct=0.25,
                 rsi_period=2,
                 rsi_oversold=10,
                 confirm_reversal=True,
                 trend_ma_short=50,
                 trend_ma_long=200,
                 stop_loss_atr=2.0,
                 target_r=2.0,
                 max_hold_days=20,
                 **kwargs):
        super().__init__("StrategyB_AVWAP", max_hold_days=max_hold_days, **kwargs)
        self.anchor_type = anchor_type
        self.avwap_touch_pct = avwap_touch_pct
        self.rsi_period = rsi_period
        self.rsi_oversold = rsi_oversold
        self.confirm_reversal = confirm_reversal
        self.trend_ma_short = trend_ma_short
        self.trend_ma_long = trend_ma_long
        self.stop_loss_atr = stop_loss_atr
        self.target_r = target_r
        self.anchor_dates = {}

    def _get_anchor_date(self, ticker, hist, date):
        if self.anchor_type == "fractal":
            recent_lows = hist["Low"].tail(60)
            return recent_lows.idxmin()
        elif self.anchor_type == "maxvolume":
            vol_slice = hist["Volume"].tail(60)
            return vol_slice.idxmax()
        else:
            return pd.Timestamp(f"{date.year}-01-01")

    def generate_signals(self, date, universe_prices, current_portfolio_value):
        orders = []
        for ticker, df in universe_prices.items():
            if date not in df.index:
                continue
            hist = df.loc[:date]
            if len(hist) < 200:
                continue
            if not self.passes_universe_filters(ticker, hist):
                continue

            close = hist["Close"]
            sma_short = close.rolling(self.trend_ma_short).mean().iloc[-1]
            sma_long = close.rolling(self.trend_ma_long).mean().iloc[-1]
            if pd.isna(sma_short) or pd.isna(sma_long):
                continue
            if not (close.iloc[-1] > sma_long and sma_short > sma_long):
                continue

            anchor = self._get_anchor_date(ticker, hist, date)
            avwap = compute_avwap(hist, anchor)
            if np.isnan(avwap):
                continue

            atr14 = compute_atr(hist, 14).iloc[-1]
            if pd.isna(atr14):
                continue

            low_today = hist["Low"].iloc[-1]
            if abs(low_today - avwap) > self.avwap_touch_pct * atr14:
                continue

            rsi = compute_rsi(close, self.rsi_period).iloc[-1]
            if pd.isna(rsi) or rsi > self.rsi_oversold:
                continue

            if self.confirm_reversal and len(hist) >= 2:
                if not (hist["Close"].iloc[-1] > hist["High"].iloc[-2]):
                    continue

            entry_price = hist["Close"].iloc[-1]
            swing_low = hist.loc[anchor:date, "Low"].min()
            stop_loss = min(swing_low - 0.1 * atr14,
                            entry_price - self.stop_loss_atr * atr14)

            shares = self.compute_position_size(entry_price, stop_loss, current_portfolio_value)
            if shares <= 0:
                continue

            self.positions[ticker] = {
                "entry_date": date,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "highest": entry_price,
                "bars": 0,
                "partial": False,
                "shares": shares,
                "direction": 1,
                "avwap": avwap,
                "anchor": anchor
            }
            orders.append(Order(ticker, "MARKET", quantity=shares))

        return orders

    def check_specific_exits(self, ticker, pos, current_price, date, universe_prices):
        orders = []
        entry = pos["entry_price"]
        stop = pos["stop_loss"]

        if (entry - stop) != 0:
            r_multiple = (current_price - entry) / (entry - stop)
            if r_multiple >= self.target_r:
                orders.append(Order(ticker, "MARKET", quantity=-pos["shares"]))
                del self.positions[ticker]
                return orders

        avwap = pos.get("avwap")
        if avwap is not None and ticker in universe_prices:
            df = universe_prices[ticker].loc[:date]
            if len(df) >= 2:
                last_two = df["Close"].iloc[-2:]
                if (last_two < avwap).all():
                    orders.append(Order(ticker, "MARKET", quantity=-pos["shares"]))
                    del self.positions[ticker]

        return orders
