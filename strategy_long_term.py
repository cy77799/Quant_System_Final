import pandas as pd
import numpy as np
from scipy.stats import zscore

class LongTermStrategy:
    """
    Phase 1: 長線動量 + 低波幅策略 (Momentum + Low Volatility)
    整合了原本 Part 2 (因子) 和 Part 3 (組隊) 的邏輯。
    """
    
    def __init__(self, top_n=15, max_sector_count=4):
        self.name = "LongTerm_Mom_Vol"
        self.top_n = top_n  # 持倉數量
        self.max_sector_count = max_sector_count # 行業限制
        self.min_price = 5.0 # 仙股過濾
        
    def generate_signals(self, current_date, universe_prices, fundamentals_df=None):
        """
        核心函數：輸入歷史數據，輸出當日建議倉位
        
        Parameters:
            current_date: 回測當前的日期 (e.g., '2020-03-31')
            universe_prices: 字典 {ticker: dataframe}，包含所有歷史價格
            fundamentals_df: (可選) 基本面數據
            
        Returns:
            target_weights: 字典 {'AAPL': 0.06, 'MSFT': 0.05...}
        """
        
        # ==========================================
        # 1. 數據準備 (Data Prep & Filters)
        # ==========================================
        candidates = []
        
        # 確保 current_date 係 Timestamp 格式
        current_dt = pd.to_datetime(current_date)
        
        for ticker, df in universe_prices.items():
            # 關鍵防守：只切片拿取 "current_date" 之前的數據
            # 絕對防止用到未來數據
            if current_dt not in df.index:
                # 嘗試找最近的一個交易日 (Prev business day)
                loc_idx = df.index.get_indexer([current_dt], method='pad')[0]
                if loc_idx == -1: continue # 該股票當時還未上市
                hist_data = df.iloc[:loc_idx+1]
            else:
                hist_data = df.loc[:current_dt]
            
            # 數據長度檢查 (至少要有 1 年數據計 Momentum)
            if len(hist_data) < 252: 
                continue
                
            latest = hist_data.iloc[-1]
            
            # 過濾 1: 價格過低
            if latest['Close'] < self.min_price: 
                continue
            
            # 過濾 2: 流動性 (簡單檢查 Volume)
            if latest['Volume'] == 0:
                continue

            # ==========================================
            # 2. 因子計算 (Factor Calculation)
            # ==========================================
            try:
                # A. Momentum (12M - 1M)
                # 邏輯：(T-21) / (T-252) - 1
                p_lag = hist_data['Close'].iloc[-21]      # 一個月前
                p_base = hist_data['Close'].iloc[-252]    # 一年前
                
                if p_base > 0:
                    mom_score = (p_lag / p_base) - 1
                else:
                    mom_score = np.nan
                
                # B. Volatility (用於 Risk Parity)
                # 過去 60 日年化波動率
                daily_ret = hist_data['Close'].pct_change().tail(60)
                vol_score = daily_ret.std() * np.sqrt(252)
                
                if pd.isna(mom_score) or pd.isna(vol_score) or vol_score == 0:
                    continue

                candidates.append({
                    'Ticker': ticker,
                    'Momentum': mom_score,
                    'Volatility': vol_score,
                    'Close': latest['Close'],
                    # 如果以後有 Sector 數據，在這裡加入
                    'Sector': 'Unknown' 
                })
                
            except Exception as e:
                continue

        # 轉成 DataFrame
        df = pd.DataFrame(candidates)
        if df.empty: return {}

        # ==========================================
        # 3. 評分與篩選 (Scoring & Selection)
        # ==========================================
        # Z-Score 標準化 (讓分數可比較)
        df['Momentum_Z'] = zscore(df['Momentum'])
        
        # 綜合評分 (目前只看 Momentum，未來可加 Value/Quality)
        df['Composite_Score'] = df['Momentum_Z']
        
        # 排序：分數高者優先
        df = df.sort_values(by='Composite_Score', ascending=False)
        
        # 選股邏輯 (Top N)
        # (這裡預留了 Sector Filter 的位置，目前先選 Top 15)
        selected_df = df.head(self.top_n).copy()
        
        if selected_df.empty: return {}

        # ==========================================
        # 4. 權重分配 (Portfolio Construction)
        # ==========================================
        # 邏輯：波動率加權 (Risk Parity 簡易版)
        # 權重與波動率成反比 (1/Vol)
        selected_df['Inv_Vol'] = 1 / selected_df['Volatility']
        
        # 再乘上評分因子 (分數越高，權重越大)
        # 這裡用 Rank 權重會穩陣啲，避免 Z-Score 極端值
        # 簡單起見，我們只用 1/Vol 
        selected_df['Raw_Weight'] = selected_df['Inv_Vol']
        
        # 歸一化 (Normalization)
        total_raw = selected_df['Raw_Weight'].sum()
        selected_df['Final_Weight'] = selected_df['Raw_Weight'] / total_raw
        
        # 輸出結果字典
        portfolio_weights = dict(zip(selected_df['Ticker'], selected_df['Final_Weight']))
        
        return portfolio_weights

# 測試用 (當你直接 Run 呢個 File 時執行)
if __name__ == "__main__":
    print("🧪 測試 Strategy 模組...")
    # 這裡你需要確保 data/prices_parquet 有數據
    try:
        # 載入少少數據試下
        import os
        prices_dir = 'data/prices_parquet'
        sample_prices = {}
        files = [f for f in os.listdir(prices_dir) if f.endswith('.parquet')][:50]
        
        print(f"📂 載入 {len(files)} 隻股票做測試...")
        for f in files:
            ticker = f.replace('.parquet', '')
            sample_prices[ticker] = pd.read_parquet(os.path.join(prices_dir, f))
            
        # 測試生成信號
        strategy = LongTermStrategy(top_n=5)
        test_date = '2023-06-30'
        
        print(f"📅 模擬日期: {test_date}")
        signals = strategy.generate_signals(test_date, sample_prices)
        
        print("\n✅ 輸出信號:")
        for ticker, weight in signals.items():
            print(f"  - {ticker}: {weight:.2%}")
            
    except Exception as e:
        print(f"⚠️ 測試失敗 (可能未有數據): {e}")
