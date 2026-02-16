import os
from typing import Tuple, List, Dict

from data_layer import Config, UniverseProvider, PriceDownloader
from strategy_long_term import LongTermStrategy


class QuantPipeline:
    def __init__(self, strategy_cls=LongTermStrategy):
        self.strategy_cls = strategy_cls
        self.universe_provider = UniverseProvider()
        self.price_downloader = PriceDownloader()

    def build_universe(self) -> Tuple[object, List[str]]:
        universe_df = self.universe_provider.build_universe()
        tickers = universe_df["Ticker"].tolist()
        return universe_df, tickers

    def ensure_prices(self, tickers: List[str], min_ratio: float = 0.5) -> List[str]:
        existing_files = [
            f for f in os.listdir(Config.PRICES_DIR)
            if f.endswith(".parquet")
        ]
        if len(existing_files) < len(tickers) * min_ratio:
            print("⚠️ 數據不足，開始下載...")
            self.price_downloader.download_all(tickers)
        return existing_files

    def load_prices(self, tickers: List[str]) -> Dict[str, object]:
        return self.price_downloader.load_prices(tickers)

    def init_long_term_strategy(self, universe_df, **kwargs):
        return self.strategy_cls(fundamentals_df=universe_df, **kwargs)