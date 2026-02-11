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
    DATA_DIR = 'data'
    PRICES_DIR = os.path.join(DATA_DIR, 'prices_parquet')
    UNIVERSE_FILE = os.path.join(DATA_DIR, 'universe.csv')
    FAILED_FILE = os.path.join(DATA_DIR, 'failed_tickers.csv')

    # Universe cache
    CACHE_DAYS = 7

    # 額外 ETF（不跑因子，只做核心或對沖）
    EXTRA_ETFS = ['QQQ', 'IWM', 'VTI', 'TLT', 'GLD']

    # 回測數據長度
    START_DATE = '2015-01-01'
    END_DATE = None  # None = 今日，避免 look-ahead 可手動鎖定

    # 下載線程數
    MAX_WORKERS = 10

    # Request 設定
    REQUEST_TIMEOUT = 10
    RETRY = 3
    SLEEP_BETWEEN_RETRIES = 1.0


# ==========================================
# 🌍 2. Universe Provider (股票池生成)
# ==========================================
class UniverseProvider:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def _normalize_ticker(self, ticker):
        if pd.isna(ticker):
            return None
        clean = str(ticker).strip().upper().replace('.', '-')
        clean = clean.replace(' ', '')
        return clean if clean else None

    def _is_cache_valid(self):
        if not os.path.exists(Config.UNIVERSE_FILE):
            return False
        last_modified = os.path.getmtime(Config.UNIVERSE_FILE)
        days_old = (datetime.datetime.now().timestamp() - last_modified) / 86400
        return days_old < Config.CACHE_DAYS

    def fetch_sp500(self):
        print("📥 抓取 S&P 500 成分股...")
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        for i in range(Config.RETRY):
            try:
                resp = requests.get(url, headers=self.headers, timeout=Config.REQUEST_TIMEOUT)
                resp.raise_for_status()
                tables = pd.read_html(StringIO(resp.text))
                df = tables[0]

                sym_col = 'Symbol' if 'Symbol' in df.columns else df.columns[0]
                df = df.rename(columns={
                    sym_col: 'Ticker',
                    'GICS Sector': 'Sector',
                    'GICS Sub-Industry': 'Industry'
                })

                df['Ticker'] = df['Ticker'].apply(self._normalize_ticker)
                df['Type'] = 'Stock'
                df = df.dropna(subset=['Ticker'])
                df = df.drop_duplicates(subset=['Ticker'])

                cols = ['Ticker', 'Sector', 'Industry', 'Type']
                return df[cols].copy()
            except Exception as e:
                print(f"⚠️ S&P500 ���取失敗 ({i+1}/{Config.RETRY}): {e}")
                time.sleep(Config.SLEEP_BETWEEN_RETRIES)
        return pd.DataFrame()

    def fetch_etfs(self):
        print(f"📥 加入核心 ETF: {Config.EXTRA_ETFS}")
        data = []
        for etf in Config.EXTRA_ETFS:
            data.append({
                'Ticker': etf,
                'Sector': 'ETF',
                'Industry': 'ETF',
                'Type': 'ETF'
            })
        return pd.DataFrame(data)

    def build_and_save(self, force_refresh=False):
        if not os.path.exists(Config.DATA_DIR):
            os.makedirs(Config.DATA_DIR)

        if self._is_cache_valid() and not force_refresh:
            print("✅ Universe 使用緩存")
            return pd.read_csv(Config.UNIVERSE_FILE)

        df_sp500 = self.fetch_sp500()
        df_etfs = self.fetch_etfs()
        full_df = pd.concat([df_sp500, df_etfs], ignore_index=True)
        full_df = full_df.drop_duplicates(subset=['Ticker'])
        full_df['Last_Updated'] = datetime.datetime.now().strftime('%Y-%m-%d')
        full_df.to_csv(Config.UNIVERSE_FILE, index=False)
        print(f"✅ Universe 建立完成: {len(full_df)} 隻標的 -> {Config.UNIVERSE_FILE}")
        return full_df


# ==========================================
# 🏭 3. Data Engine (Loader + Cleaner + Output)
# ==========================================
class DataEngine:
    def __init__(self, universe_df):
        self.universe_df = universe_df
        if not os.path.exists(Config.PRICES_DIR):
            os.makedirs(Config.PRICES_DIR)

    def _clean_data(self, df):
        if df is None or df.empty:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        expected_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in expected_cols):
            return None

        df = df[expected_cols].copy()
        df.dropna(inplace=True)
        df = df[df['Volume'] > 0]

        return df

    def _download_one(self, ticker):
        save_path = os.path.join(Config.PRICES_DIR, f"{ticker}.parquet")
        end_date = Config.END_DATE or datetime.datetime.today().strftime('%Y-%m-%d')

        for i in range(Config.RETRY):
            try:
                raw_df = yf.download(
                    ticker,
                    start=Config.START_DATE,
                    end=end_date,
                    progress=False,
                    auto_adjust=True
                )
                clean_df = self._clean_data(raw_df)
                if clean_df is None or len(clean_df) < 50:
                    return False
                clean_df.to_parquet(save_path, compression='snappy')
                return True
            except Exception:
                time.sleep(Config.SLEEP_BETWEEN_RETRIES)
        return False

    def run_pipeline(self):
        tickers = self.universe_df['Ticker'].tolist()
        print(f"\n🚀 開始下載 {len(tickers)} 隻標的歷史數據 (Parquet)...")
        print(f"📂 存儲路徑: {Config.PRICES_DIR}")

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            results = list(tqdm(executor.map(self._download_one, tickers), total=len(tickers), unit="stock"))

        failed = [t for t, ok in zip(tickers, results) if not ok]
        pd.DataFrame({'Ticker': failed}).to_csv(Config.FAILED_FILE, index=False)

        print(f"\n✅ Pipeline 執行完畢")
        print(f"📊 成功入庫: {len(tickers) - len(failed)} / {len(tickers)}")
        if failed:
            print(f"⚠️ 失敗清單已保存: {Config.FAILED_FILE}")


# ==========================================
# 🎬 Main Execution
# ==========================================
if __name__ == "__main__":
    provider = UniverseProvider()
    universe_df = provider.build_and_save()

    engine = DataEngine(universe_df)
    engine.run_pipeline()

    print("\n🔍 [Quality Check] 隨機讀取一隻股��驗證...")
    sample_ticker = 'AAPL'
    sample_path = os.path.join(Config.PRICES_DIR, f"{sample_ticker}.parquet")
    if os.path.exists(sample_path):
        df = pd.read_parquet(sample_path)
        print(f"Ticker: {sample_ticker}")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Rows: {len(df)}")
        print(df.tail(3))
    else:
        print(f"⚠️ 搵唔到 {sample_ticker} 數據")