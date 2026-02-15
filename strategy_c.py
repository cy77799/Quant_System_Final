import pandas as pd
from .base import SwingBase
from .utils import compute_adx, compute_atr
from universal_backtester import Order


class StrategyC_BollingerReversion(SwingBase):
    def __init__(self,
                 bb_period=20,
                 bb_std=2,
                 adx_period=14,
                 adx_threshold=25,
                 adx_disable=30,
                 stop_loss_atr=1.5,
                 max_hold_days=5,
                 **kwargs):
        super().__init__("StrategyC_BBReversion", max_hold_days=max_hold_days, **kwargs)
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.adx_period = adx_period
        self.adx_threshold = adx_threshold
        self.adx_disable = adx_disable
        self.stop_loss_atr = stop_loss_atr

    def generate_signals(self, date, universe_prices, current_portfolio_value):
        orders = []
        for ticker, df in universe_prices.items():
            if date not in df.index:
                continue
            hist = df.loc[:date]
            if len(hist) < max(self.bb_period, self.adx_period) + 5:
                continue
            if not self.passes_universe_filters(ticker, hist):
                continue

            adx = compute_adx(hist, self.adx_period).iloc[-1]
            if pd.isna(adx) or adx >= self.adx_disable:
                continue
            if adx >= self.adx_threshold:
                continue

            close = hist["Close"]
            ma = close.rolling(self.bb_period).mean().iloc[-1]
            std = close.rolling(self.bb_period).std().iloc[-1]
            if pd.isna(ma) or pd.isna(std) or std == 0:
                continue

            lower_band = ma - self.bb_std * std
            pct_b = (close.iloc[-1] - lower_band) / (2 * self.bb_std * std)

            if pct_b < 0:
                atr14 = compute_atr(hist, 14).iloc[-1]
                if pd.isna(atr14):
                    continue

                entry_price = hist["Close"].iloc[-1]
                stop_loss = entry_price - self.stop_loss_atr * atr14

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
                    "bb_ma": ma
                }
                orders.append(Order(ticker, "MARKET", quantity=shares))

        return orders

    def check_specific_exits(self, ticker, pos, current_price, date, universe_prices):
        orders = []
        bb_ma = pos.get("bb_ma")
        if bb_ma is not None and current_price >= bb_ma:
            orders.append(Order(ticker, "MARKET", quantity=-pos["shares"]))
            del self.positions[ticker]
        return orders
