import yfinance as yf
import pandas as pd
from pathlib import Path
from utils.config import load_config

class PriceLoader:
    def __init__(self, config=None):
        self.config = config or load_config()
        self.raw_dir = Path(self.config['paths']['raw_data'])
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def download(self, symbols=None, start=None, end=None, force=False):
        if symbols is None:
            symbols = self.config['universe']['symbols']
        start = start or self.config['data']['price']['start_date']
        end = end or self.config['data']['price']['end_date']

        out_path = self.raw_dir / f"prices_{start}_{end}.parquet"
        if out_path.exists() and not force:
            return pd.read_parquet(out_path)

        data = yf.download(symbols, start=start, end=end, auto_adjust=True, group_by='ticker')
        df_list = []
        for symbol in symbols:
            if symbol in data.columns.levels[0]:
                sym = data[symbol].copy()
                sym['Symbol'] = symbol
                sym = sym.reset_index().rename(columns={'Date': 'date'})
                df_list.append(sym)

        if not df_list:
            return pd.DataFrame()

        df = pd.concat(df_list, ignore_index=True)
        df.to_parquet(out_path, index=False)
        return df

    def load(self, start=None, end=None):
        files = sorted(self.raw_dir.glob("prices_*.parquet"))
        if not files:
            df = self.download()
        else:
            df = pd.read_parquet(files[-1])

        if start:
            df = df[df['date'] >= pd.to_datetime(start)]
        if end:
            df = df[df['date'] <= pd.to_datetime(end)]

        pivot = df.pivot(index='date', columns='Symbol', values='Close')
        return pivot

    def load_ohlcv(self, start=None, end=None):
        files = sorted(self.raw_dir.glob("prices_*.parquet"))
        if not files:
            df = self.download()
        else:
            df = pd.read_parquet(files[-1])

        if start:
            df = df[df['date'] >= pd.to_datetime(start)]
        if end:
            df = df[df['date'] <= pd.to_datetime(end)]

        df = df.set_index(['date', 'Symbol']).sort_index()
        return df
