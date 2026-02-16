import pandas as pd
import os
from datetime import datetime

from engine.pipeline import QuantPipeline
from universal_backtester import UniversalBacktester, TransactionCostModel, PerformanceAnalyzer


def main():
    print("=" * 60)
    print("🚀 QUANT SYSTEM: 自動化回測流程啟動")
    print("=" * 60)

    pipeline = QuantPipeline()

    # ==========================================
    # Step 1: 數據準備
    # ==========================================
    print("\n[Step 1/4] 正在準備數據...")

    universe_df, tickers = pipeline.build_universe()
    print(f"✅ Universe Ready: {len(tickers)} 隻股票")

    pipeline.ensure_prices(tickers)

    print("📥 正在將 Parquet 載入內存...")
    price_data = pipeline.load_prices(tickers)
    print(f"✅ 成功載入 {len(price_data)} 隻股票數據")

    if len(price_data) == 0:
        print("❌ 錯誤：沒有可用的價格數據，請檢查 internet 或數據目錄。")
        return

    # ==========================================
    # Step 2: 初始化策略
    # ==========================================
    print("\n[Step 2/4] 初始化長線策略...")

    strategy = pipeline.init_long_term_strategy(
        universe_df,
        top_n=15,
        max_sector_count=4,
        rebalance_freq="Q"
    )
    print(f"🧠 策略: {strategy.name} (Top {strategy.top_n}, Freq: {strategy.rebalance_freq})")

    # ==========================================
    # Step 3: 執行回測
    # ==========================================
    print("\n[Step 3/4] 開始執行回測 (2015 - today)...")

    cost_model = TransactionCostModel(
        commission_rate=0.001,
        slippage=0.001,
        min_commission=1.0
    )

    backtester = UniversalBacktester(
        initial_capital=100_000,
        cost_model=cost_model
    )

    start_date = "2015-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    results_df = backtester.run(
        strategy=strategy,
        prices_dict=price_data,
        start_date=start_date,
        end_date=end_date
    )

    # ==========================================
    # Step 4: 結果分析
    # ==========================================
    print("\n[Step 4/4] 回測完成！生成報告...")

    if results_df is None or results_df.empty:
        print("⚠️ 無回測結果 (可能無交易發生)")
        return

    analyzer = PerformanceAnalyzer()

    benchmark = None
    if "SPY" in price_data:
        benchmark = price_data["SPY"]["Close"]

    metrics, rolling = analyzer.analyze(results_df, benchmark_prices=benchmark)

    print("\n" + "=" * 40)
    print("📊 PERFORMANCE SUMMARY")
    print("=" * 40)
    print(f"🗓️  區間: {start_date} 至 {end_date}")
    print(f"💰 初始資金: ${results_df['Equity'].iloc[0]:,.0f}")
    print(f"💰 最終資金: ${results_df['Equity'].iloc[-1]:,.0f}")
    print(f"📈 總回報:   {metrics['Total Return']:.2%}")
    print(f"🚀 年化回報 (CAGR): {metrics['CAGR']:.2%}")
    print(f"📉 最大回撤 (MaxDD): {metrics['Max Drawdown']:.2%}")
    if "Alpha" in metrics:
        print(f"🧠 Alpha: {metrics['Alpha']:.2%} | Beta: {metrics['Beta']:.2f} | IR: {metrics['Information Ratio']:.2f}")
    print("=" * 40)

    results_df.to_csv("backtest_results.csv", index=False)
    rolling.to_csv("rolling_metrics.csv")
    print("\n💾 已輸出: backtest_results.csv / rolling_metrics.csv")

    if backtester.trade_log:
        pd.DataFrame(backtester.trade_log).to_csv("trade_log.csv", index=False)
        print("💾 交易記錄已儲存至: trade_log.csv")


if __name__ == "__main__":
    main()