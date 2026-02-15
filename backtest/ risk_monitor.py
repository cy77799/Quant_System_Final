import json
import os
import pandas as pd


def load_portfolio_state(path):
    if not os.path.exists(path):
        return {"cash_usd": 0.0, "positions": {}}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_portfolio_state(path, state):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _get_hist_slice(df, asof):
    asof = pd.to_datetime(asof)
    if asof in df.index:
        return df.loc[:asof]
    loc_idx = df.index.get_indexer([asof], method="pad")[0]
    if loc_idx == -1:
        return None
    return df.iloc[:loc_idx + 1]


def _latest_close(df, asof):
    hist = _get_hist_slice(df, asof)
    if hist is None or hist.empty:
        return None
    return float(hist["Close"].iloc[-1])


def check_market_filter(price_data, asof, symbol="SPY", ma_window=200):
    if symbol not in price_data:
        return {
            "ok": True,
            "message": f"⚠️ 找不到 {symbol}，無法判斷 MA{ma_window}，視為通過。"
        }

    df = price_data[symbol]
    hist = _get_hist_slice(df, asof)
    if hist is None or len(hist) < ma_window:
        return {
            "ok": True,
            "message": f"⚠️ {symbol} 歷史不足 {ma_window} 日，視為通過。"
        }

    ma = hist["Close"].tail(ma_window).mean()
    latest = hist["Close"].iloc[-1]
    ok = latest >= ma
    msg = f"{symbol} 最新價 {latest:.2f} | MA{ma_window} {ma:.2f}"
    return {"ok": ok, "message": msg}


def evaluate_positions(state, price_data, asof, low_window=50, max_drawdown=-0.30):
    alerts = []
    positions = state.get("positions", {})

    for ticker, info in positions.items():
        if ticker not in price_data:
            alerts.append(f"⚠️ {ticker} 無價格資料，跳過風險檢查。")
            continue

        df = price_data[ticker]
        hist = _get_hist_slice(df, asof)
        if hist is None or len(hist) < low_window:
            alerts.append(f"⚠️ {ticker} 歷史不足 {low_window} 日，跳過風險檢查。")
            continue

        latest = hist["Close"].iloc[-1]
        low_50 = hist["Close"].tail(low_window).min()

        entry_price = info.get("avg_cost", None)
        if entry_price is None or entry_price <= 0:
            entry_price = float(hist["Close"].iloc[-1])

        peak = hist["Close"].max()
        drawdown = (latest - peak) / peak if peak > 0 else 0.0

        # 觸發條件：50日低 / 最大回撤
        if latest < low_50:
            alerts.append(f"🚨 {ticker} 跌破 {low_window}日低位 ({latest:.2f} < {low_50:.2f})")

        if drawdown <= max_drawdown:
            alerts.append(f"🚨 {ticker} 最大回撤 {drawdown*100:.1f}% (<= {max_drawdown*100:.0f}%)")

    return alerts
