import simfin as sf
from simfin.names import *
import pandas as pd
from pathlib import Path
from config import load_config

class FundamentalsLoader:
    def __init__(self, api_key=None, config=None):
        self.config = config or load_config()
        sf.set_api_key(api_key or self.config['data']['fundamentals'].get('api_key'))
        sf.set_data_dir(self.config['paths']['raw_data'])

    def _load_mapping(self):
        mapping_path = Path(
            self.config.get("data", {})
                .get("fundamentals", {})
                .get("mapping_file", "data/simfin_mapping.csv")
        )
        if not mapping_path.exists():
            return None
        df = pd.read_csv(mapping_path)
        df.columns = [c.strip() for c in df.columns]
        if "Ticker" not in df.columns or "SimFinId" not in df.columns:
            raise KeyError(f"mapping 檔案欄位錯誤，需要 Ticker, SimFinId: {list(df.columns)}")
        df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
        return df

    def _get_simfin_ids(self, symbols):
        # 1) 先用 mapping 檔（如果有）
        mapping = self._load_mapping()
        if mapping is not None:
            return mapping[mapping["Ticker"].isin(symbols)]["SimFinId"].dropna().unique()

        # 2) fallback：嘗試從 companies 取得 ticker
        companies = sf.load_companies(market='us')
        if TICKER in companies.columns:
            ticker_col = TICKER
        elif "Ticker" in companies.columns:
            ticker_col = "Ticker"
        else:
            print(f"⚠️ companies 冇 Ticker 欄位，無法用 symbols 過濾 fundamentals")
            return []

        if SIMFIN_ID in companies.columns:
            id_col = SIMFIN_ID
        elif "SimFinId" in companies.columns:
            id_col = "SimFinId"
        else:
            print(f"⚠️ companies 冇 SimFinId 欄位，無法用 symbols 過濾 fundamentals")
            return []

        return companies[companies[ticker_col].isin(symbols)][id_col].unique()

    def download_quarterly(self, symbols):
        df = sf.load_income(variant='quarterly', market='us')

        # 轉用 SimFinId 過濾
        simfin_ids = self._get_simfin_ids(symbols)
        if SIMFIN_ID in df.columns:
            id_col = SIMFIN_ID
        elif "SimFinId" in df.columns:
            id_col = "SimFinId"
        else:
            raise KeyError(f"找不到 SimFinId 欄位於 income, 現有欄位: {list(df.columns)}")

        if simfin_ids is not None and len(simfin_ids) > 0:
            df = df[df[id_col].isin(simfin_ids)]
        else:
            print("⚠️ 冇可用 mapping，fundamentals 先全量保存（之後可加 mapping 再過濾）")

        keep_cols = [id_col, REPORT_DATE, CURRENCY, REVENUE, GROSS_PROFIT,
                     OPERATING_INCOME, NET_INCOME, EPS]
        keep_cols = [c for c in keep_cols if c in df.columns]
        df = df[keep_cols]

        out_path = Path(self.config['paths']['raw_data']) / "fundamentals_quarterly.parquet"
        df.to_parquet(out_path)
        return df

    def load_latest(self, as_of_date):
        df = pd.read_parquet(Path(self.config['paths']['raw_data']) / "fundamentals_quarterly.parquet")

        if SIMFIN_ID in df.columns:
            id_col = SIMFIN_ID
        elif "SimFinId" in df.columns:
            id_col = "SimFinId"
        else:
            raise KeyError(f"找不到 SimFinId 欄位於 fundamentals, 現有欄位: {list(df.columns)}")

        df = df[df[REPORT_DATE] <= pd.to_datetime(as_of_date)]
        latest = df.sort_values(REPORT_DATE).groupby(id_col).last().reset_index()
        return latest
