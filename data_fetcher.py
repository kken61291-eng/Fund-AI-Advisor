import akshare as ak
import pandas as pd
from utils import retry, logger

class DataFetcher:
    def __init__(self):
        # V10.2: 修复成交量缺失问题
        pass

    @retry(retries=1, backoff_factor=1)
    def _fetch_em(self, code):
        """
        来源1: 东方财富 (EastMoney)
        """
        try:
            # 接口：场内ETF历史行情
            df = ak.fund_etf_hist_em(symbol=code, period="daily", adjust="qfq")
            # [关键修复] 增加 "成交量": "volume"
            df = df.rename(columns={
                "日期": "date", 
                "收盘": "close", 
                "最高": "high", 
                "最低": "low", 
                "开盘": "open",
                "成交量": "volume" 
            })
            return df
        except: 
            return None

    @retry(retries=2, backoff_factor=2)
    def _fetch_sina(self, code):
        """
        来源2: 新浪财经 (Sina)
        """
        try:
            if code.startswith("5"):
                symbol = f"sh{code}"
            elif code.startswith("1") or code.startswith("0") or code.startswith("3"):
                symbol = f"sz{code}"
            else:
                symbol = f"sh{code}"

            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            # 新浪接口 akshare 返回的列名通常已经是 volume，但为了保险清洗一下
            # 假设返回的是英文列名，直接使用
            return df
        except: 
            return None

    def get_fund_history(self, code):
        """
        V10.2 主逻辑
        """
        df = None
        source_name = ""

        # 1. 尝试东方财富
        df = self._fetch_em(code)
        if df is not None and not df.empty:
            source_name = "EM"
        
        # 2. 尝试新浪
        else:
            df = self._fetch_sina(code)
            if df is not None and not df.empty:
                source_name = "Sina"

        if df is None or df.empty:
            logger.error(f"❌ 所有数据源均获取失败: {code}")
            return None

        try:
            # [关键修复] 确保 volume 列存在，如果没有则补0，防止报错
            if 'volume' not in df.columns:
                df['volume'] = 0

            df['date'] = pd.to_datetime(df['date'])
            df['close'] = pd.to_numeric(df['close'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df['volume'] = pd.to_numeric(df['volume'])
            
            df = df.sort_values('date').set_index('date')
            
            # 生成真周线
            weekly_df = df.resample('W-FRI').agg({
                'close': 'last',
                'high': 'max',
                'low': 'min',
                'volume': 'sum' # 周线成交量求和
            }).dropna()
            
            if len(weekly_df) < 10: return None

            return {
                "daily": df,
                "weekly": weekly_df
            }

        except Exception as e:
            logger.error(f"数据清洗失败 {code} [{source_name}]: {e}")
            return None
