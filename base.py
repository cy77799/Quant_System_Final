import pandas as pd
import numpy as np
from abc import abstractmethod
from universal_backtester import BaseStrategy, Order


class SwingBase(BaseStrategy):
    """
    Swing 策略共用基礎類別
    處理：宇宙過濾、風險計算、持倉管理、通用離場
    """
    def __init__(self,
                 name,
                 risk_per_trade=0.005,
                 min_price=5.0,
                 min_avg_dollar_vol=20e6,
                 max_hold_days=30):
        super().__init__(name)
        self.risk_per_trade = risk_per_trade
        self.min_price = min_price
        self.min_avg_dollar_vol = min_avg_dollar_vol
        self.max_hold_days = max_hold_days

        # 持倉記錄: {ticker: { 'entry_date', 'entry_price', 'stop_loss', 'highest', 'bars', 'partial', 'shares' }}
        self.positions = {}

    # ------------------------------------------------------------
    # 宇宙過濾
    # ------------------------------------------------------------
    def passes_universe_filters(self, ticker, hist_data):
        if hist_data is None or len(hist_data) < 20:
            return False

        price_col = self._get_price_col(hist_data)
        latest = hist_data.iloc[-1]
        if price_col not in latest or latest[price_col] < self.min_price:
            return False

        # 20日平均成交額
        avg_dollar_vol = (hist_data[price_col] * hist_data["Volume"]).tail(20).mean()
        if avg_dollar_vol < self.min_avg_dollar_vol:
            return False

        return True

    def _get_price_col(self, df):
        if "Adj Close" in df.columns:
            return "Adj Close"
        return "Close"

    # ------------------------------------------------------------
    # 風險管理：倉位計算
    # ------------------------------------------------------------
    def compute_position_size(self, entry_price, stop_price, current_portfolio_value):
        risk_amount = current_portfolio_value * self.risk_per_trade
        stop_loss_points = abs(entry_price - stop_price)
        if stop_loss_points <= 0:
            return 0

        shares = risk_amount / stop_loss_points

        # 最大倉位限制 (例如 NAV 10%)
        max_notional = current_portfolio_value * 0.10
        if shares * entry_price > max_notional:
            shares = max_notional / entry_price

        return shares

    # ------------------------------------------------------------
    # 持倉更新
    # ------------------------------------------------------------
    def update_positions(self, date, prices_dict):
        for ticker, pos in list(self.positions.items()):
            if ticker not in prices_dict:
                continue
            df = prices_dict[ticker]
            if df is None or df.empty:
                continue

            price_col = self._get_price_col(df)
            current_price = df.loc[date, price_col] if date in df.index else None
            if current_price is None or pd.isna(current_price):
                continue

            # 更新最高價
            if current_price > pos.get("highest", 0):
                pos["highest"] = current_price

            # 更新持倉日數
            pos["bars"] = pos.get("bars", 0) + 1

    # ------------------------------------------------------------
    # 通用離場條件
    # ------------------------------------------------------------
    def check_common_exits(self, ticker, pos, current_price):
        orders = []

        # Time stop: 未達 +1R 而到期
        if pos.get("bars", 0) >= self.max_hold_days:
            entry = pos["entry_price"]
            stop = pos["stop_loss"]
            r_multiple = (current_price - entry) / (entry - stop) if (entry - stop) != 0 else 0
            if r_multiple < 1.0:
                orders.append(Order(ticker, "MARKET", quantity=-pos.get("shares", 0)))
                del self.positions[ticker]
                return orders

        return orders

    # ------------------------------------------------------------
    # 子類必須實現
    # ------------------------------------------------------------
    @abstractmethod
    def generate_signals(self, date, universe_prices, current_portfolio_value):
        pass

    @abstractmethod
    def check_specific_exits(self, ticker, pos, current_price, date, universe_prices):
        return []

    # ------------------------------------------------------------
    # 主 on_bar
    # ------------------------------------------------------------
    def on_bar(self, date, universe_prices, current_portfolio_value):
        orders = []

        # 1) 更新持倉狀態
        self.update_positions(date, universe_prices)

        # 2) 為所有持倉下 stop order (用 backtester 負責觸發)
        for ticker, pos in list(self.positions.items()):
            if pos.get("stop_loss") is not None:
                orders.append(Order(ticker, "STOP_LIMIT", quantity=0, stop_loss=pos["stop_loss"]))

        # 3) 檢查離場條件
        for ticker, pos in list(self.positions.items()):
            if ticker not in universe_prices:
                continue
            df = universe_prices[ticker]
            price_col = self._get_price_col(df)
            current_price = df.loc[date, price_col] if date in df.index else None
            if current_price is None or pd.isna(current_price):
                continue

            orders.extend(self.check_common_exits(ticker, pos, current_price))
            orders.extend(self.check_specific_exits(ticker, pos, current_price, date, universe_prices))

        # 4) 產生新信號
        orders.extend(self.generate_signals(date, universe_prices, current_portfolio_value))
        return orders
