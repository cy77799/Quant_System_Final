import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class Order:
    ticker: str
    order_type: str
    quantity: float = 0.0
    target_weight: Optional[float] = None
    stop_loss: Optional[float] = None


class BaseStrategy(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def on_bar(self, date, universe_prices, current_portfolio_value) -> List[Order]:
        pass


class TransactionCostModel:
    def __init__(self, commission_rate=0.001, slippage=0.001, min_commission=1.0):
        self.commission_rate = commission_rate
        self.slippage = slippage
        self.min_commission = min_commission

    def calc_commission(self, trade_value):
        return max(trade_value * self.commission_rate, self.min_commission)

    def apply_slippage(self, price, qty):
        return price * (1 + self.slippage) if qty > 0 else price * (1 - self.slippage)


class UniversalBacktester:
    def __init__(self, initial_capital=100000, calendar_ticker="SPY", allow_fractional=True, cost_model=None):
        self.initial_capital = initial_capital
        self.calendar_ticker = calendar_ticker
        self.allow_fractional = allow_fractional
        self.cost_model = cost_model or TransactionCostModel()

        self.cash = initial_capital
        self.positions: Dict[str, float] = {}
        self.equity_curve = []
        self.trade_log = []
        self.turnover_log = []

    def _build_trading_days(self, prices_dict, start_date, end_date):
        if self.calendar_ticker in prices_dict:
            idx = prices_dict[self.calendar_ticker].loc[start_date:end_date].index
            return pd.DatetimeIndex(idx)
        all_indices = [df.loc[start_date:end_date].index for df in prices_dict.values()]
        union_idx = sorted(set().union(*all_indices))
        return pd.DatetimeIndex(union_idx)

    def _align_prices(self, prices_dict, trading_days):
        return {t: df.reindex(trading_days).ffill() for t, df in prices_dict.items()}

    def _get_bar(self, ticker, date, prices_dict):
        if ticker in prices_dict and date in prices_dict[ticker].index:
            return prices_dict[ticker].loc[date]
        return None

    def _calc_portfolio_value(self, date, prices_dict):
        value = self.cash
        for ticker, qty in self.positions.items():
            bar = self._get_bar(ticker, date, prices_dict)
            if bar is not None:
                value += qty * bar["Close"]
        return value

    def _process_trade(self, date, ticker, qty, price):
        if qty == 0:
            return

        exec_price = self.cost_model.apply_slippage(price, qty)
        trade_value = abs(qty) * exec_price
        commission = self.cost_model.calc_commission(trade_value)

        if qty > 0 and (trade_value + commission) > self.cash:
            if not self.allow_fractional:
                return
            max_affordable = self.cash / (exec_price * (1 + self.cost_model.commission_rate))
            qty = max_affordable
            trade_value = abs(qty) * exec_price
            commission = self.cost_model.calc_commission(trade_value)

        self.cash -= (qty * exec_price + commission)
        self.positions[ticker] = self.positions.get(ticker, 0) + qty

        if abs(self.positions[ticker]) < 1e-8:
            del self.positions[ticker]

        self.trade_log.append({
            "Date": date,
            "Ticker": ticker,
            "Action": "BUY" if qty > 0 else "SELL",
            "Qty": qty,
            "Price": exec_price,
            "Commission": commission
        })

    def _execute_orders(self, orders, date, prices_dict, portfolio_value):
        for order in [o for o in orders if o.order_type == "STOP_LIMIT"]:
            bar = self._get_bar(order.ticker, date, prices_dict)
            if bar is None or order.stop_loss is None:
                continue
            if bar["Low"] <= order.stop_loss:
                qty = order.quantity if order.quantity != 0 else -self.positions.get(order.ticker, 0)
                self._process_trade(date, order.ticker, qty, order.stop_loss)

        target_orders = [o for o in orders if o.order_type == "TARGET_WEIGHT"]
        if target_orders:
            target_map = {o.ticker: o.target_weight for o in target_orders}
            for ticker in list(self.positions.keys()):
                if ticker not in target_map:
                    target_map[ticker] = 0.0

            turnover = 0.0
            for ticker, target_weight in target_map.items():
                bar = self._get_bar(ticker, date, prices_dict)
                if bar is None:
                    continue
                price = bar["Close"]
                current_qty = self.positions.get(ticker, 0)
                current_value = current_qty * price
                target_value = portfolio_value * target_weight
                diff_value = target_value - current_value
                qty_to_trade = diff_value / price
                turnover += abs(diff_value)
                self._process_trade(date, ticker, qty_to_trade, price)

            self.turnover_log.append({"Date": date, "Turnover": turnover / portfolio_value})

        for order in [o for o in orders if o.order_type == "MARKET"]:
            bar = self._get_bar(order.ticker, date, prices_dict)
            if bar is None:
                continue
            self._process_trade(date, order.ticker, order.quantity, bar["Close"])

    def run(self, strategy: BaseStrategy, prices_dict, start_date, end_date):
        print(f"🚀 啟動回測引擎: {strategy.name}")
        trading_days = self._build_trading_days(prices_dict, start_date, end_date)
        prices_dict = self._align_prices(prices_dict, trading_days)

        for date in trading_days:
            portfolio_value = self._calc_portfolio_value(date, prices_dict)
            self.equity_curve.append({"Date": date, "Equity": portfolio_value})
            orders = strategy.on_bar(date, prices_dict, portfolio_value)
            if orders:
                self._execute_orders(orders, date, prices_dict, portfolio_value)

        return pd.DataFrame(self.equity_curve)


class PerformanceAnalyzer:
    def __init__(self, risk_free_rate=0.02):
        self.risk_free_rate = risk_free_rate

    def analyze(self, equity_curve: pd.DataFrame, benchmark_prices: Optional[pd.Series] = None):
        equity_curve = equity_curve.set_index("Date")
        returns = equity_curve["Equity"].pct_change().dropna()

        metrics = {}
        years = (equity_curve.index[-1] - equity_curve.index[0]).days / 365.25
        total_return = equity_curve["Equity"].iloc[-1] / equity_curve["Equity"].iloc[0] - 1
        metrics["CAGR"] = (1 + total_return) ** (1 / years) - 1
        metrics["Total Return"] = total_return
        metrics["Volatility"] = returns.std() * np.sqrt(252)

        excess = returns - self.risk_free_rate / 252
        metrics["Sharpe"] = excess.mean() / returns.std() * np.sqrt(252)

        downside = returns[returns < 0]
        metrics["Sortino"] = (metrics["CAGR"] - self.risk_free_rate) / (downside.std() * np.sqrt(252))

        cumulative = (1 + returns).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        metrics["Max Drawdown"] = drawdown.min()
        metrics["Calmar"] = metrics["CAGR"] / abs(metrics["Max Drawdown"])

        if benchmark_prices is not None:
            benchmark_returns = benchmark_prices.pct_change().dropna()
            strat_ret, bench_ret = returns.align(benchmark_returns, join="inner")
            cov = np.cov(strat_ret, bench_ret)[0][1]
            beta = cov / np.var(bench_ret)
            alpha = (strat_ret.mean() * 252) - (self.risk_free_rate + beta * (bench_ret.mean() * 252 - self.risk_free_rate))
            info_ratio = (strat_ret.mean() - bench_ret.mean()) / (strat_ret - bench_ret).std() * np.sqrt(252)

            metrics["Alpha"] = alpha
            metrics["Beta"] = beta
            metrics["Information Ratio"] = info_ratio

        rolling = self._rolling_metrics(equity_curve)
        return metrics, rolling

    def _rolling_max_drawdown(self, equity, window):
        def calc_dd(x):
            s = pd.Series(x)
            running_max = s.cummax()
            dd = (s - running_max) / running_max
            return dd.min()
        return equity.rolling(window).apply(calc_dd, raw=False)

    def _rolling_metrics(self, equity_curve, windows_years=(3, 5)):
        rolling_results = []
        equity = equity_curve["Equity"]
        daily_returns = equity.pct_change().dropna()

        for years in windows_years:
            window = years * 252
            rolling_cagr = (equity / equity.shift(window)) ** (252 / window) - 1
            rolling_vol = daily_returns.rolling(window).std() * np.sqrt(252)
            rolling_sharpe = (rolling_cagr - self.risk_free_rate) / rolling_vol
            rolling_dd = self._rolling_max_drawdown(equity, window)

            df = pd.DataFrame({
                "Rolling_CAGR": rolling_cagr,
                "Rolling_Volatility": rolling_vol,
                "Rolling_Sharpe": rolling_sharpe,
                "Rolling_MaxDD": rolling_dd
            })
            df["Window_Years"] = years
            rolling_results.append(df)

        return pd.concat(rolling_results)
