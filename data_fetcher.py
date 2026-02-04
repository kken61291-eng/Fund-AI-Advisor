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
        # 初始化 Tushare (作为中间防线)
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
        """
        [源1] AkShare - 东方财富
        优点: 数据最全，含精准成交量
        缺点: GitHub Actions 环境容易被封 IP
        """
        try:
            socket.setdefaulttimeout(15)
            df = ak.fund_etf_hist_em(symbol=code, period="daily", adjust="qfq")
            df = df.rename(columns={
                "日期": "date", "收盘": "close", "最高": "high", 
                "最低": "low", "开盘": "open", "成交量": "volume"
            })
            return df
        except: return None
        finally: socket.setdefaulttimeout(None)

    @retry(retries=1, backoff_factor=1)
    def _fetch_sina(self, code):
        """
        [源2] AkShare - 新浪财经
        优点: 备用爬虫，有时候 EM 挂了它还能用
        """
        try:
            socket.setdefaulttimeout(15)
            symbol = f"sh{code}" if code.startswith("5") else f"sz{code}"
            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            # 新浪接口通常自带 volume
            return df
        except: return None
        finally: socket.setdefaulttimeout(None)

    @retry(retries=2, backoff_factor=2)
    def _fetch_tushare(self, code):
        """
        [源3] Tushare Pro (回归)
        优点: API 方式，不封 IP，数据规范
        缺点: 需要积分 (2000分)
        """
        if not self.pro: return None
        try:
            ts_code = f"{code}.SH" if code.startswith("5") else f"{code}.SZ"
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            
            # 注意: 如果积分不足，这里会抛出异常，代码会捕获并跳到 Yahoo
            df = self.pro.fund_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            if df is None or df.empty: return None
            
            df = df.rename(columns={"trade_date": "date", "vol": "volume"})
            df = df.sort_values('date') # Tushare 默认倒序，需转正序
            return df
        except Exception as e:
            # 记录错误但不中断，方便后续排查权限问题
            logger.warning(f"Tushare 尝试失败 (可能是权限不足): {e}")
            return None

    @retry(retries=3, backoff_factor=1)
    def _fetch_yahoo(self, code):
        """
        [源4] Yahoo Finance
        优点: 免费，全球节点，永不封号 (兜底神器)
        缺点: 偶尔连接慢
        """
        try:
            socket.setdefaulttimeout(20) # 强制超时保护
            
            if code.startswith('5') or code.startswith('6'):
                yahoo_symbol = f"{code}.SS"
            else:
                yahoo_symbol = f"{code}.SZ"
            
            ticker = yf.Ticker(yahoo_symbol)
            df = ticker.history(period="1y", auto_adjust=True)
            
            if df is None or df.empty: return None
            
            df = df.reset_index()
            df = df.rename(columns={
                "Date": "date", "Open": "open", "High": "high", 
                "Low": "low", "Close": "close", "Volume": "volume"
            })
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
            return df
        except Exception as e:
            logger.error(f"Yahoo 获取失败 {code}: {e}")
            return None
        finally: socket.setdefaulttimeout(None)

    def get_fund_history(self, code):
        """
        V11.1 四级火箭获取逻辑
        EM -> Sina -> Tushare -> Yahoo
        """
        df = None
        source_name = ""

        # 1. 尝试 EM
        df = self._fetch_em(code)
        if df is not None and not df.empty: source_name = "EM"
        
        # 2. 尝试 Sina
        if df is None or df.empty:
            df = self._fetch_sina(code)
            if df is not None and not df.empty: source_name = "Sina"

        # 3. 尝试 Tushare (已恢复)
        if df is None or df.empty:
            df = self._fetch_tushare(code)
            if df is not None and not df.empty: source_name = "Tushare"

        # 4. 尝试 Yahoo
        if df is None or df.empty:
            df = self._fetch_yahoo(code)
            if df is not None and not df.empty: source_name = "Yahoo"

        # 全挂了
        if df is None or df.empty:
            logger.error(f"❌ 四重数据源全线失败: {code}")
            return None

        try:
            # 标准化清洗
            if 'volume' not in df.columns: df['volume'] = 0
            
            df['date'] = pd.to_datetime(df['date'])
            df['close'] = pd.to_numeric(df['close'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df['volume'] = pd.to_numeric(df['volume'])
            
            df = df.sort_values('date').set_index('date')
            
            # 生成周线 (这是多因子分析的基础)
            weekly_df = df.resample('W-FRI').agg({
                'close': 'last', 'high': 'max', 'low': 'min', 'volume': 'sum'
            }).dropna()
            
            if len(weekly_df) < 5: return None

            return {"daily": df, "weekly": weekly_df}

        except Exception as e:
            logger.error(f"数据清洗失败 {code} [{source_name}]: {e}")
            return None
