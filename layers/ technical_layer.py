import pandas as pd
import pandas_ta as ta
from pathlib import Path

class TechnicalIndicator:
    def __init__(self, price_close_df, ohlcv_df=None):
        self.price_close_df = price_close_df
        self.ohlcv_df = ohlcv_df
        self.indicators = {}

    def add_rsi(self, length=14):
        rsi = ta.rsi(self.price_close_df, length=length)
        self.indicators[f'RSI_{length}'] = rsi
        return rsi

    def add_bbands(self, length=20, std=2):
        bb = ta.bbands(self.price_close_df, length=length, std=std)
        self.indicators[f'BBL_{length}'] = bb.iloc[:, 0]
        self.indicators[f'BBM_{length}'] = bb.iloc[:, 1]
        self.indicators[f'BBU_{length}'] = bb.iloc[:, 2]
        return bb

    def add_atr(self, length=14):
        if self.ohlcv_df is None:
            raise ValueError("ATR 需要 OHLCV")

        atr_all = []
        for symbol, sym_df in self.ohlcv_df.groupby(level=1):
            df = sym_df.droplevel(1)
            atr = ta.atr(df['High'], df['Low'], df['Close'], length=length)
            atr.name = symbol
            atr_all.append(atr)

        atr_df = pd.concat(atr_all, axis=1)
        self.indicators[f'ATR_{length}'] = atr_df
        return atr_df

    def add_adx(self, length=14):
        if self.ohlcv_df is None:
            raise ValueError("ADX 需要 OHLCV")

        adx_all = []
        for symbol, sym_df in self.ohlcv_df.groupby(level=1):
            df = sym_df.droplevel(1)
            adx = ta.adx(df['High'], df['Low'], df['Close'], length=length)
            adx = adx[f'ADX_{length}']
            adx.name = symbol
            adx_all.append(adx)

        adx_df = pd.concat(adx_all, axis=1)
        self.indicators[f'ADX_{length}'] = adx_df
        return adx_df

    def add_vwap(self):
        if self.ohlcv_df is None:
            raise ValueError("VWAP 需要 OHLCV")

        vwap_all = []
        for symbol, sym_df in self.ohlcv_df.groupby(level=1):
            df = sym_df.droplevel(1)
            vwap = ta.vwap(df['High'], df['Low'], df['Close'], df['Volume'])
            vwap.name = symbol
            vwap_all.append(vwap)

        vwap_df = pd.concat(vwap_all, axis=1)
        self.indicators['VWAP'] = vwap_df
        return vwap_df

    # ✅ 1) cache：每個指標獨立存檔
    def save_indicators(self, out_dir):
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        for name, df in self.indicators.items():
            df.to_parquet(out_dir / f"{name}.parquet")

    # ✅ 2) unified output：合併成一個 parquet
    def to_unified_frame(self):
        frames = []
        for name, df in self.indicators.items():
            frames.append(pd.concat({name: df}, axis=1))
        if not frames:
            return pd.DataFrame()
        unified = pd.concat(frames, axis=1)
        # columns 變成 MultiIndex: (indicator, symbol)
        return unified

    def save_unified(self, out_path):
        unified = self.to_unified_frame()
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        unified.to_parquet(out_path)
        return unified
