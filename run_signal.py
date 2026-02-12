import pandas as pd
import os
import datetime
import requests

from data_layer import Config, UniverseProvider, PriceDownloader
from strategy_long_term import LongTermStrategy

# ==========================================
# 📲 Telegram 配置 (請填入你的資料)
# ==========================================
TG_TOKEN = ""
TG_CHAT_ID = ""

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

    # Step 4: 計算信號
    print("[Step 4] 計算策略信號...")
    strategy = LongTermStrategy(
        top_n=15,
        max_sector_count=4,
        fundamentals_df=universe_df
    )

    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    target_weights = strategy.generate_signals(today_str, price_data)

    if not target_weights:
        msg = f"📅 *{today_str} 信號報告*\n\n⚠️ 今日無買入建議。"
    else:
        msg = f"📅 *{today_str} 長線策略信號*\n"
        msg += f"🎯 目標持倉: {len(target_weights)} 隻\n"
        msg += "-" * 25 + "\n"

        df_res = pd.DataFrame(list(target_weights.items()), columns=["Ticker", "Weight"])
        df_res = df_res.sort_values(by="Weight", ascending=False)

        for _, row in df_res.iterrows():
            ticker = row["Ticker"]
            weight = row["Weight"]
            sector = "N/A"
            if "Sector" in universe_df.columns:
                match = universe_df.loc[universe_df["Ticker"] == ticker, "Sector"]
                if not match.empty:
                    sector = match.values[0]
            msg += f"*{ticker}* ({sector[:10]}): `{weight:.1%}`\n"

        msg += "-" * 25 + "\n"
        msg += "💡 *建議操作:* 請根據上述權重調整倉位。"

    print("\n" + msg)
    send_telegram_message(msg)

if __name__ == "__main__":
    main()
