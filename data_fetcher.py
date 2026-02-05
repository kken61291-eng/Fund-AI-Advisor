import akshare as ak
import pandas as pd
import time
from datetime import datetime, timedelta
from utils import logger, retry

try:
    import yfinance as yf
except ImportError:
    yf = None

class DataFetcher:
    def __init__(self):
        pass

    @retry(retries=2, delay=2)
    def get_fund_history(self, code, period='3y'):
        try:
            symbol = code
            if not code.startswith('sh') and not code.startswith('sz'):
                symbol = f"sh{code}" if code.startswith('5') or code.startswith('6') else f"sz{code}"
            
            df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date="20200101", end_date="20500101")
            if not df.empty:
                df = df.rename(columns={"日期": "date", "收盘": "close", "最高": "high", "最低": "low", "开盘": "open", "成交量": "volume"})
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                if df.index.tz is not None: df.index = df.index.tz_localize(None)
                return df
        except Exception as e:
            logger.warning(f"东财源获取失败 {code}: {str(e)[:50]}")

        try:
            time.sleep(1)
            symbol = f"sh{code}" if code.startswith('5') or code.startswith('6') else f"sz{code}"
            df = ak.stock_zh_index_daily(symbol=symbol)
            if not df.empty:
                df = df.rename(columns={"date": "date", "close": "close", "high": "high", "low": "low", "open": "open", "volume": "volume"})
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                if df.index.tz is not None: df.index = df.index.tz_localize(None)
                return df
        except Exception:
            pass

        if yf:
            try:
                time.sleep(1)
                suffix = ".SS" if code.startswith('5') or code.startswith('6') else ".SZ"
                symbol = code + suffix
                ticker = yf.Ticker(symbol)
                df = ticker.history(period="2y")
                if not df.empty:
                    df = df.rename(columns={"Close": "close", "High": "high", "Low": "low", "Open": "open", "Volume": "volume"})
                    if df.index.tz is not None:
                        df.index = df.index.tz_localize(None)
                    return df
            except Exception as e:
                logger.error(f"Yahoo 获取失败 {code}: {e}")

        logger.error(f"❌ 四重数据源全线失败: {code}")
        return None
