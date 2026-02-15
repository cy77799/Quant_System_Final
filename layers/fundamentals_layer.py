import simfin as sf
from simfin.names import *
import pandas as pd
from pathlib import Path
from utils.config import load_config

class FundamentalsLoader:
    def __init__(self, api_key=None, config=None):
        self.config = config or load_config()
        sf.set_api_key(api_key or self.config['data']['fundamentals'].get('api_key'))
        sf.set_data_dir(self.config['paths']['raw_data'])

    def download_quarterly(self, symbols):
        df = sf.load_income(variant='quarterly', market='us')
        df = df[df[TICKER].isin(symbols)]
        keep_cols = [TICKER, REPORT_DATE, CURRENCY, REVENUE, GROSS_PROFIT,
                     OPERATING_INCOME, NET_INCOME, EPS]
        df = df[keep_cols]

        out_path = Path(self.config['paths']['raw_data']) / "fundamentals_quarterly.parquet"
        df.to_parquet(out_path)
        return df

    def load_latest(self, as_of_date):
        df = pd.read_parquet(Path(self.config['paths']['raw_data']) / "fundamentals_quarterly.parquet")
        df = df[df[REPORT_DATE] <= pd.to_datetime(as_of_date)]
        latest = df.sort_values(REPORT_DATE).groupby(TICKER).last().reset_index()
        return latest
