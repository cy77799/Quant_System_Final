import pandas as pd
import yfinance as yf
import requests
import os
import datetime
import time
from io import StringIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# ==========================================
# ⚙️ 1. 配置層 (Configuration)
# ==========================================
class Config:
    DATA_DIR = "data"
    PRICES_DIR = os.path.join(DATA_DIR, "prices_parquet")
    UNIVERSE_FILE = os.path.join(DATA_DIR, "universe.csv")
    FAILED_FILE = os.path.join(DATA_DIR, "failed_tickers.csv")

    CACHE_DAYS = 7

    EXTRA_ETFS = ["SPY", "QQQ", "IWM", "VTI", "TLT", "GLD"]

    START_DATE = "2015-01-01"
    END_DATE = None

    # ✅ 下載線程數調低
    MAX_WORKERS = 3

    REQUEST_TIMEOUT = 10
    RETRY = 5
    SLEEP_BETWEEN_RETRIES = 1.0


# ==========================================
# 🌍 2. Universe Provider
# ==========================================
class UniverseProvider:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def _normalize_ticker(self, ticker):
        if pd.isna(ticker):
            return None
        clean = str(ticker).strip().upper().replace(".", "-").replace(" ", "")
        return clean if clean else None

    def _is_cache_valid(self):
        if not os.path.exists(Config.UNIVERSE_FILE):
            return False
        last_modified = os.path.getmtime(Config.UNIVERSE_FILE)
        days_old = (datetime.datetime.now().timestamp() - last_modified) / 86400
        return days_old < Config.CACHE_DAYS

    def _load_cache(self):
        if os.path.exists(Config.UNIVERSE_FILE):
            return pd.read_csv(Config.UNIVERSE_FILE)
        return None

    def _save_cache(self, df):
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        df.to_csv(Config.UNIVERSE_FILE, index=False)

    def fetch_sp500(self):
        print("📥 抓取 S&P 500 成分股...")
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

        for i in range(Config.RETRY):
            try:
                resp = requests.get(url, headers=self.headers, timeout=Config.REQUEST_TIMEOUT)
                resp.raise_for_status()
                tables = pd.read_html(StringIO(resp.text))
                df = tables[0]

                sym_col = "Symbol" if "Symbol" in df.columns else df.columns[0]
                df = df.rename(columns={
                    sym_col: "Ticker",
                    "GICS Sector": "Sector",
                    "GICS Sub-Industry": "Industry"
                })

                df["Ticker"] = df["Ticker"].apply(self._normalize_ticker)
                df["Type"] = "Stock"
                df = df.dropna(subset=["Ticker"]).drop_duplicates(subset=["Ticker"])

                cols = ["Ticker", "Sector", "Industry", "Type"]
                return df[cols].copy()
            except Exception as e:
                print(f"⚠️ 抓取失敗 (第 {i+1}/{Config.RETRY})：{e}")
                time.sleep(Config.SLEEP_BETWEEN_RETRIES)

        cached = self._load_cache()
        if cached is not None:
            print("✅ Wikipedia 抓取失敗，改用本地 cache")
            return cached

        raise RuntimeError("❌ 無法取得 S&P500 成分股，也找不到 cache")

    def build_universe(self, include_extra_etf=True):
        if self._is_cache_valid():
            print("✅ 使用快取 Universe")
            return self._load_cache()

        df = self.fetch_sp500()

        if include_extra_etf:
            extra = pd.DataFrame({
                "Ticker": Config.EXTRA_ETFS,
                "Sector": "ETF",
                "Industry": "ETF",
                "Type": "ETF"
            })
            df = pd.concat([df, extra], ignore_index=True)

        df["Sector"] = df["Sector"].fillna("Unknown")
        df["Industry"] = df["Industry"].fillna("Unknown")

        self._save_cache(df)
        return df


# ==========================================
# 📦 3. Price Downloader
# ==========================================
class PriceDownloader:
    def __init__(self):
        os.makedirs(Config.PRICES_DIR, exist_ok=True)

    def _download_one(self, ticker):
        for i in range(Config.RETRY):
            try:
                df = yf.download(
                    ticker,
                    start=Config.START_DATE,
                    end=Config.END_DATE,
                    auto_adjust=True,
                    progress=False,
                    threads=False  # ✅ 禁用 yfinance 自己的 threads
                )

                time.sleep(0.3)  # ✅ 降速，避免被封

                if df.empty:
                    continue

                required = {"Close", "High", "Low", "Volume"}
                if not required.issubset(set(df.columns)):
                    continue

                return ticker, df
            except Exception:
                time.sleep(Config.SLEEP_BETWEEN_RETRIES)

        return ticker, None

    def download_all(self, tickers):
        results = {}
        failed = []

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            for ticker, df in tqdm(executor.map(self._download_one, tickers), total=len(tickers)):
                if df is None:
                    failed.append(ticker)
                else:
                    results[ticker] = df
                    df.to_parquet(os.path.join(Config.PRICES_DIR, f"{ticker}.parquet"))

        if failed:
            pd.DataFrame({"Ticker": failed}).to_csv(Config.FAILED_FILE, index=False)
        return results

    def load_prices(self, tickers):
        data = {}
        for t in tickers:
            path = os.path.join(Config.PRICES_DIR, f"{t}.parquet")
            if os.path.exists(path):
                data[t] = pd.read_parquet(path)
        return data


if __name__ == "__main__":
    provider = UniverseProvider()
    universe = provider.build_universe()
    tickers = universe["Ticker"].tolist()
    downloader = PriceDownloader()
    prices = downloader.download_all(tickers)
    print(f"✅ 下載完成，共 {len(prices)} 隻股票")
