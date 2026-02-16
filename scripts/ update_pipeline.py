from layers.data_hub import DataHub
from layers.data_layer import UniverseProvider
from utils.validation import validate_missing, validate_spikes
from utils.reporting import append_report
from pathlib import Path

def run_update():
    hub = DataHub()
    processed_dir = Path(hub.price.config['paths']['processed_data'])
    processed_dir.mkdir(parents=True, exist_ok=True)

    # 0) Universe（S&P500 + EXTRA_ETFS）
    u_provider = UniverseProvider()
    universe_df = u_provider.build_universe()
    tickers = universe_df["Ticker"].dropna().unique().tolist()
    print(f"✅ Universe Ready: {len(tickers)} tickers")

    # 1) 更新價格
    hub.price.download(symbols=tickers, force=True)
    close_df = hub.price.load()

    # 2) 技術指標
    tech = hub.build_technical()
    tech.add_rsi(14)
    tech.add_atr(14)
    tech.add_adx(14)
    tech.add_vwap()

    tech.save_indicators(processed_dir / "indicators")
    tech.save_unified(processed_dir / "indicators_all.parquet")

    # 3) 基本面（用 S&P500 tickers 嘗試過濾）
    hub.fundamentals.download_quarterly(tickers)

    # 4) 宏觀
    hub.macro.download_all()

    # 5) 驗證
    missing = validate_missing(close_df)
    spikes = validate_spikes(close_df)

    if not missing.empty:
        print("Missing warning:", missing)
    if spikes > 0:
        print("Spike warning count:", spikes)

    # ✅ 寫 report
    report_path = processed_dir / "update_report.csv"
    append_report(report_path, {
        "rows_price": len(close_df),
        "missing_cols": len(missing),
        "spike_count": spikes
    })

if __name__ == "__main__":
    run_update()
