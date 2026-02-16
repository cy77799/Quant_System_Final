import pandas as pd
import simfin as sf
from simfin.names import SIMFIN_ID, TICKER
from config import load_config
from layers.data_layer import UniverseProvider

def _extract_ticker_column(df):
    # 1) Ticker 在欄位
    if TICKER in df.columns:
        return df, TICKER
    if "Ticker" in df.columns:
        return df, "Ticker"

    # 2) Ticker 在 index
    if df.index.name in [TICKER, "Ticker"]:
        df = df.reset_index()
        return df, df.columns[0]

    # 3) index 係 multi-index
    if isinstance(df.index, pd.MultiIndex):
        if TICKER in df.index.names or "Ticker" in df.index.names:
            df = df.reset_index()
            ticker_col = TICKER if TICKER in df.columns else "Ticker"
            return df, ticker_col

    raise KeyError(f"找不到 Ticker 欄位／index，現有欄位: {list(df.columns)} | index: {df.index.names}")

def main():
    config = load_config()

    # 1) 取得 S&P500 tickers
    u = UniverseProvider()
    universe_df = u.build_universe(include_extra_etf=False)
    tickers = set(universe_df["Ticker"].dropna().unique().tolist())

    # 2) 讀 SimFin companies
    sf.set_api_key(config["data"]["fundamentals"].get("api_key"))
    sf.set_data_dir(config["paths"]["raw_data"])
    companies = sf.load_companies(market="us")

    companies, ticker_col = _extract_ticker_column(companies)

    # 3) 找 SimFinId 欄位
    if SIMFIN_ID in companies.columns:
        id_col = SIMFIN_ID
    elif "SimFinId" in companies.columns:
        id_col = "SimFinId"
    else:
        raise KeyError(f"companies 冇 SimFinId 欄位: {list(companies.columns)}")

    companies["TickerNorm"] = companies[ticker_col].astype(str).str.strip().str.upper()

    mapping = companies[companies["TickerNorm"].isin(tickers)][[ticker_col, id_col]].drop_duplicates()
    mapping = mapping.rename(columns={ticker_col: "Ticker", id_col: "SimFinId"})

    # 4) 輸出 mapping
    out_path = "data/simfin_mapping.csv"
    mapping.to_csv(out_path, index=False)
    print(f"✅ 已產生 mapping：{out_path}（共 {len(mapping)} 筆）")

if __name__ == "__main__":
    main()
