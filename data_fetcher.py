import akshare as ak
import pandas as pd
import time
import random
from datetime import datetime, time as dt_time
from utils import logger, retry, get_beijing_time

class DataFetcher:
    def __init__(self):
        # [V15.12] 扩充 User-Agent 池
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
        ]

    def _get_random_header(self):
        return {"User-Agent": random.choice(self.user_agents)}

    def _verify_data_freshness(self, df, fund_code, source_name):
        """
        [新增] 数据新鲜度审计
        验证拿到的数据是否是"热乎"的
        """
        if df is None or df.empty: return
        
        last_date = pd.to_datetime(df.index[-1]).date()
        now_bj = get_beijing_time()
        today_date = now_bj.date()
        
        # 判断当前是否为交易时间 (简单判断: 9:30 - 15:00)
        is_trading_time = (dt_time(9, 30) <= now_bj.time() <= dt_time(15, 0))
        
        # 日志前缀
        log_prefix = f"📅 [{source_name}] {fund_code} 最新日期: {last_date}"
        
        if last_date == today_date:
            logger.info(f"{log_prefix} | ✅ 数据已更新至今日")
        elif last_date < today_date:
            days_gap = (today_date - last_date).days
            if is_trading_time and days_gap >= 1:
                # 如果在交易时间，拿到的却是旧数据，发出警告
                logger.warning(f"{log_prefix} | ⚠️ 滞后 {days_gap} 天 (可能今日尚未开盘或数据源延迟)")
            else:
                # 非交易时间或周末，数据滞后是正常的
                logger.info(f"{log_prefix} | ⏸️ 闭市/非交易日")
        else:
            logger.warning(f"{log_prefix} | ❓ 未来数据? 请检查系统时间")

    @retry(retries=2, delay=2) 
    def get_fund_history(self, fund_code, days=250):
        """
        获取K线数据。优先级：东财 -> 新浪 -> 腾讯
        """
        # --- 1. 尝试东财 (EastMoney) ---
        for attempt in range(3):
            try:
                # 递增延迟防止封禁
                sleep_time = 2 + attempt * 1.5 + random.uniform(0, 1)
                time.sleep(sleep_time)
                
                df = ak.fund_etf_hist_em(
                    symbol=fund_code, 
                    period="daily", 
                    start_date="20240101", 
                    end_date="20500101", 
                    adjust="qfq"
                )
                
                rename_map = {'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}
                df.rename(columns=rename_map, inplace=True)
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                
                if not df.empty:
                    self._verify_data_freshness(df, fund_code, "东财主源")
                    return df
            
            except Exception as e:
                if attempt == 2:
                    logger.warning(f"⚠️ 东财受阻 {fund_code}: {str(e)[:50]}... 切换备用。")

        # --- 2. 尝试新浪 (Sina) ---
        sina_df = self._fetch_sina_fallback(fund_code)
        if sina_df is not None:
            self._verify_data_freshness(sina_df, fund_code, "新浪备用")
            return sina_df

        # --- 3. 尝试腾讯 (Tencent) ---
        tx_df = self._fetch_tx_fallback(fund_code)
        if tx_df is not None:
            self._verify_data_freshness(tx_df, fund_code, "腾讯保底")
            return tx_df
            
        return None

    def _fetch_sina_fallback(self, fund_code):
        try:
            time.sleep(1.5) 
            df = ak.fund_etf_hist_sina(symbol=fund_code)
            
            if df is None or df.empty: return None

            if df.index.name in ['date', '日期'] or isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()

            # 暴力清洗
            if len(df.columns) >= 6:
                df.columns = ['date', 'open', 'high', 'low', 'close', 'volume'] + list(df.columns[6:])
            
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                # 类型清洗
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                return df
            return None
        except Exception:
            return None

    def _fetch_tx_fallback(self, fund_code):
        try:
            time.sleep(1)
            prefix = 'sh' if fund_code.startswith('5') else ('sz' if fund_code.startswith('1') else '')
            if not prefix: return None
            symbol = f"{prefix}{fund_code}"
            
            df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date="20240101", adjust="qfq")
            
            rename_map = {'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}
            df.rename(columns=rename_map, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)

            if not df.empty:
                return df
            return None
        except Exception:
            return None
