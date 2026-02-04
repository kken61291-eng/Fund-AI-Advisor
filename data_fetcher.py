import akshare as ak
import tushare as ts
import yfinance as yf
import pandas as pd
import os
import socket
from datetime import datetime, timedelta
from utils import retry, logger

class DataFetcher:
    def __init__(self):
        self.ts_token = os.getenv("TUSHARE_TOKEN")
        self.pro = None
        if self.ts_token:
            try:
                ts.set_token(self.ts_token)
                self.pro = ts.pro_api()
            except Exception as e:
                logger.warning(f"Tushare 初始化警告: {e}")

    @retry(retries=1, backoff_factor=1)
    def _fetch_em(self, code):
        try:
            socket.setdefaulttimeout(15)
            df = ak.fund_etf_hist_em(symbol=code, period="daily", adjust="qfq")
            df = df.rename(columns={"日期": "date", "收盘": "close", "最高": "high", "最低": "low", "开盘": "open", "成交量": "volume"})
            return df
        except: return None
        finally: socket.setdefaulttimeout(None)

    @retry(retries=1, backoff_factor=1)
    def _fetch_sina(self, code):
        try:
            socket.setdefaulttimeout(15)
            symbol = f"sh{code}" if code.startswith("5") else f"sz{code}"
            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            return df
        except: return None
        finally: socket.setdefaulttimeout(None)

    @retry(retries=2, backoff_factor=2)
    def _fetch_tushare(self, code):
        if not self.pro: return None
        try:
            ts_code = f"{code}.SH" if code.startswith("5") else f"{code}.SZ"
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            df = self.pro.fund_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df is None or df.empty: return None
            df = df.rename(columns={"trade_date": "date", "vol": "volume"})
            df = df.sort_values('date')
            return df
        except: return None

    @retry(retries=3, backoff_factor=1)
    def _fetch_yahoo(self, code):
        try:
            socket.setdefaulttimeout(20)
            yahoo_symbol = f"{code}.SS" if (code.startswith('5') or code.startswith('6')) else f"{code}.SZ"
            ticker = yf.Ticker(yahoo_symbol)
            df = ticker.history(period="1y", auto_adjust=True)
            if df is None or df.empty: return None
            df = df.reset_index().rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
            return df
        except Exception as e:
            logger.error(f"Yahoo 获取失败 {code}: {e}")
            return None
        finally: socket.setdefaulttimeout(None)

    def get_fund_history(self, code):
        df = self._fetch_em(code)
        if df is None or df.empty: df = self._fetch_sina(code)
        if df is None or df.empty: df = self._fetch_tushare(code)
        if df is None or df.empty: df = self._fetch_yahoo(code)

        if df is None or df.empty:
            logger.error(f"❌ 四重数据源全线失败: {code}")
            return None

        try:
            if 'volume' not in df.columns: df['volume'] = 0
            df['date'] = pd.to_datetime(df['date'])
            df['close'] = pd.to_numeric(df['close'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df['volume'] = pd.to_numeric(df['volume'])
            df = df.sort_values('date').set_index('date')
            weekly_df = df.resample('W-FRI').agg({'close': 'last', 'high': 'max', 'low': 'min', 'volume': 'sum'}).dropna()
            if len(weekly_df) < 5: return None
            return {"daily": df, "weekly": weekly_df}
        except Exception as e:
            logger.error(f"数据清洗失败 {code}: {e}")
            return None
