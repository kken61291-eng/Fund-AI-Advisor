import akshare as ak
import tushare as ts
import pandas as pd
import os
from utils import retry, logger

class DataFetcher:
    def __init__(self):
        self.ts_token = os.getenv("TUSHARE_TOKEN")
        if self.ts_token:
            try:
                ts.set_token(self.ts_token)
                self.pro = ts.pro_api()
            except: pass

    @retry(retries=3)
    def get_fund_history(self, code):
        """
        全量获取：尝试场内ETF -> 尝试场外基金 -> 生成日线+真周线
        """
        try:
            # 1. 优先尝试场内 ETF 接口 (数据更全)
            try:
                df = ak.fund_etf_hist_em(symbol=code, period="daily", adjust="qfq")
                df = df.rename(columns={"日期": "date", "收盘": "close", "最高": "high", "最低": "low", "开盘": "open"})
            except:
                # 2. 回退到场外基金接口
                df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
                df = df.rename(columns={"净值日期": "date", "单位净值": "close"})
                df['high'] = df['close']
                df['low'] = df['close']

            if df is None or df.empty: return None

            df['date'] = pd.to_datetime(df['date'])
            df['close'] = pd.to_numeric(df['close'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df = df.sort_values('date').set_index('date')
            
            # 3. 生成真周线 (周五聚合)
            weekly_df = df.resample('W-FRI').agg({
                'close': 'last',
                'high': 'max',
                'low': 'min'
            }).dropna()

            return {
                "daily": df,
                "weekly": weekly_df
            }
        except Exception as e:
            logger.error(f"数据获取失败 {code}: {str(e)[:50]}")
            return None
