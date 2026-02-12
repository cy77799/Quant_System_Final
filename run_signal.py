import pandas as pd
import os
import datetime
import requests

from data_layer import Config, UniverseProvider, PriceDownloader
from strategy_long_term import LongTermStrategy
from risk_monitor import load_portfolio_state, check_market_filter, evaluate_positions

# ==========================================
# 📲 Telegram 配置 (請填入你的資料)
# ==========================================
TG_TOKEN = ""
TG_CHAT_ID = ""

PORTFOLIO_PATH = "data/portfolio_state.json"

def send_telegram_message(message):
    if "YOUR_" in TG_TOKEN:
        print("⚠️ Telegram Token 未設定，只輸出到 console：")
        print(message)
        return

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("✅ Telegram 通知已發送")
        else:
            print(f"❌ Telegram 發送失敗: {response.text}")
    except Exception as e:
        print(f"❌ 連接 Telegram 失敗: {e}")

def main():
    print("=" * 60)
    print("📡 QUANT SIGNAL: 生成今日交易信號")
    print("=" * 60)

    # Step 1: Universe
    print("\n[Step 1] 檢查 Universe...")
    u_provider = UniverseProvider()
    universe_df = u_provider.build_universe()
    tickers = universe_df["Ticker"].tolist()

    # Step 2: 更新數據
    print("[Step 2] 檢查/更新數據...")
    downloader = PriceDownloader()
    existing_files = [f for f in os.listdir(Config.PRICES_DIR) if f.endswith(".parquet")]
    if len(existing_files) < len(tickers) * 0.5:
        print("⚠️ 數據不足，開始下載...")
        downloader.download_all(tickers)

    # Step 3: 載入數據
    print("[Step 3] 載入數據...")
    price_data = downloader.load_prices(tickers)
    print(f"✅ 已載入 {len(price_data)} 隻股票")

    # Step 4: 讀取持倉
    print("[Step 4] 讀取持倉...")
    state = load_portfolio_state(PORTFOLIO_PATH)
    current_positions = state.get("positions", {})
    cash_usd = state.get("cash_usd", 0.0)

    # Step 5: 市場風險開關 (SPY < MA200 就暫停加倉)
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    market_check = check_market_filter(price_data, today_str, symbol="SPY", ma_window=200)

    # Step 6: 個股風險檢查（50日低 + 最大回撤30%）
    alerts = evaluate_positions(state, price_data, today_str, low_window=50, max_drawdown=-0.30)

    # Step 7: 計算信號
    print("[Step 7] 計算策略信號...")
    strategy = LongTermStrategy(
        top_n=15,
        max_sector_count=4,
        fundamentals_df=universe_df
    )
    target_weights = strategy.generate_signals(today_str, price_data)

    # 若大市跌穿 MA200 -> 暫停加倉
    if not market_check["ok"]:
        target_weights = {}

    # 組合 Telegram 訊息
        # 組合 Telegram 訊息（清晰分段）
    msg = f"📅 {today_str} 長線策略\n"
    msg += f"【市場】{market_check['message']} "
    msg += "✅\n" if market_check["ok"] else "❌\n"

    msg += f"【現金】USD {cash_usd:.2f}\n"
    msg += f"【持倉】{len(current_positions)} 隻\n"

    if not market_check["ok"]:
        msg += "🚫 市場風險：SPY < MA200，暫停加倉，只留現金\n"

    if alerts:
        msg += "\n【風險警告】\n"
        for a in alerts:
            msg += f"- {a}\n"

    if not target_weights:
        msg += "\n【今日信號】無新增買入\n"
    else:
        msg += "\n【今日信號】\n"
        df_res = pd.DataFrame(list(target_weights.items()), columns=["Ticker", "Weight"])
        df_res = df_res.sort_values(by="Weight", ascending=False)
        for _, row in df_res.iterrows():
            msg += f"- {row['Ticker']}: {row['Weight']*100:.1f}%\n"


    send_telegram_message(msg)

if __name__ == "__main__":
    main()
