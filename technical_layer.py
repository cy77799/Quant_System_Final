import pandas as pd
import pandas_ta as ta

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

    def unified_output(self):
        """
        統一輸出：dict of {indicator_name: DataFrame}
        index=日期，columns=股票
        """
        return self.indicators
