from fredapi import Fred
import pandas as pd
from pathlib import Path
from utils.config import load_config

class MacroLoader:
    def __init__(self, api_key=None, config=None):
        self.config = config or load_config()
        self.fred = Fred(api_key=api_key or self.config['data']['macro'].get('fred_api_key'))
        self.raw_dir = Path(self.config['paths']['raw_data'])
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def download_series(self, series_id, name):
        data = self.fred.get_series(series_id)
        df = pd.DataFrame(data, columns=[name])
        df.index.name = 'date'
        return df

    def download_all(self):
        vix = self.download_series('VIXCLS', 'VIX')
        fedfunds = self.download_series('FEDFUNDS', 'FEDFUNDS')
        cpi = self.download_series('CPIAUCSL', 'CPI')

        df = vix.join(fedfunds, how='outer').join(cpi, how='outer')
        out_path = self.raw_dir / "macro.parquet"
        df.to_parquet(out_path)
        return df

    def load(self, start=None, end=None):
        df = pd.read_parquet(self.raw_dir / "macro.parquet")
        df = df.sort_index()
        if start:
            df = df.loc[start:]
        if end:
            df = df.loc[:end]
        return df
