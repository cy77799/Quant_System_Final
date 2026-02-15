import pandas as pd
from .base import SwingBase
from .utils import compute_atr
from universal_backtester import Order


class StrategyA_VCPBreakout(SwingBase):
    def __init__(self,
                 atr_short=5,
                 atr_long=20,
                 contraction_ratio=0.7,
                 lookback_high=20,
                 breakout_buffer=0.1,
                 rvol_threshold=1.5,
                 stop_loss_atr=1.5,
                 target_r=2.0,
                 trail_atr=3.0,
                 max_hold_days=30,
                 **kwargs):
        super().__init__("StrategyA_VCP", max_hold_days=max_hold_days, **kwargs)
        self.atr_short = atr_short
        self.atr_long = atr_long
        self.contraction_ratio = contraction_ratio
        self.lookback_high = lookback_high
        self.breakout_buffer = breakout_buffer
        self.rvol_threshold = rvol_threshold
        self.stop_loss_atr = stop_loss_atr
        self.target_r = target_r
        self.trail_atr = trail_atr

    def generate_signals(self, date, universe_prices, current_portfolio_value):
        orders = []
        for ticker, df in universe_prices.items():
            if date not in df.index:
                continue
            hist = df.loc[:date]
            if len(hist) < max(self.atr_long, 120, self.lookback_high) + 5:
                continue

            if not self.passes_universe_filters(ticker, hist):
                continue

            atr_short = compute_atr(hist, self.atr_short).iloc[-1]
            atr_long = compute_atr(hist, self.atr_long).iloc[-1]
            if pd.isna(atr_short) or pd.isna(atr_long) or atr_long == 0:
                continue

            contraction_cond = (atr_short / atr_long) <= self.contraction_ratio
            atr14 = compute_atr(hist, 14).iloc[-120:]
            if len(atr14) >= 120:
                pct_rank = (atr14 <= atr_short).sum() / 120.0
                contraction_cond = contraction_cond or (pct_rank <= 0.2)

            if not contraction_cond:
                continue

            highest_last = hist["High"].rolling(self.lookback_high).max().iloc[-2]
            close_today = hist["Close"].iloc[-1]
            if pd.isna(highest_last) or close_today <= highest_last + self.breakout_buffer * atr_short:
                continue

            vol_avg = hist["Volume"].tail(20).mean()
            if vol_avg == 0 or (hist["Volume"].iloc[-1] / vol_avg) < self.rvol_threshold:
                continue

            entry_price = hist["Close"].iloc[-1]
            stop_loss = min(hist["Low"].tail(self.lookback_high).min(),
                            entry_price - self.stop_loss_atr * atr_short)

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
                "direction": 1
            }
            orders.append(Order(ticker, "MARKET", quantity=shares))

        return orders

    def check_specific_exits(self, ticker, pos, current_price, date, universe_prices):
        orders = []
        entry = pos["entry_price"]
        stop = pos["stop_loss"]

        if (entry - stop) == 0:
            return orders

        r_multiple = (current_price - entry) / (entry - stop)

        if r_multiple >= self.target_r and not pos.get("partial", False):
            half_shares = pos["shares"] / 2
            if half_shares > 0:
                orders.append(Order(ticker, "MARKET", quantity=-half_shares))
                pos["shares"] -= half_shares
                pos["partial"] = True

        # Chandelier trail
        atr = compute_atr(universe_prices[ticker].loc[:date], 14).iloc[-1]
        highest = pos.get("highest", entry)
        if not pd.isna(atr):
            trail_stop = highest - self.trail_atr * atr
            if current_price < trail_stop:
                orders.append(Order(ticker, "MARKET", quantity=-pos["shares"]))
                del self.positions[ticker]

        return orders
