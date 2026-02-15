from layers.price_layer import PriceLoader
from layers.technical_layer import TechnicalIndicator
from layers.fundamentals_layer import FundamentalsLoader
from layers.macro_layer import MacroLoader

class DataHub:
    def __init__(self, config=None):
        self.price = PriceLoader(config=config)
        self.fundamentals = FundamentalsLoader(config=config)
        self.macro = MacroLoader(config=config)

    def load_price(self, start=None, end=None):
        return self.price.load(start=start, end=end)

    def load_ohlcv(self, start=None, end=None):
        return self.price.load_ohlcv(start=start, end=end)

    def build_technical(self, start=None, end=None):
        close_df = self.price.load(start=start, end=end)
        ohlcv_df = self.price.load_ohlcv(start=start, end=end)
        return TechnicalIndicator(close_df, ohlcv_df)
