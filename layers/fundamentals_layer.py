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

    def _get_simfin_ids(self, symbols):
        companies = sf.load_companies(market='us')
        # companies 內有 Ticker + SimFinId
        if TICKER in companies.columns:
            ticker_col = TICKER
        elif "Ticker" in companies.columns:
            ticker_col = "Ticker"
        else:
            raise KeyError(f"找不到 Ticker 欄位於 companies, 現有欄位: {list(companies.columns)}")

        if SIMFIN_ID in companies.columns:
            id_col = SIMFIN_ID
        elif "SimFinId" in companies.columns:
            id_col = "SimFinId"
        else:
            raise KeyError(f"找不到 SimFinId 欄位於 companies, 現有欄位: {list(companies.columns)}")

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

        df = df[df[id_col].isin(simfin_ids)]

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
