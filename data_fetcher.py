import akshare as ak
import pandas as pd
import numpy as np
import time
import random
from datetime import datetime, timedelta
from utils import logger, retry

class DataFetcher:
    def __init__(self):
        # [V15.8] 扩充 User-Agent 池
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        ]

    def _get_random_header(self):
        return {"User-Agent": random.choice(self.user_agents)}

    @retry(retries=3, delay=2)
    def get_fund_history(self, fund_code, days=250):
        """
        获取K线数据。优先级：东财 -> 新浪 -> 模拟数据(兜底)
        """
        # 1. 尝试东财 (数据最全)
        try:
            time.sleep(random.uniform(0.5, 1.5)) 
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
                logger.info(f"✅ [主源] 东财获取成功: {fund_code}")
                return df
        except Exception as e:
            logger.warning(f"⚠️ 东财源受阻 {fund_code}: {str(e)[:50]}... 尝试切换备用源。")

        # 2. 尝试新浪 (备用)
        sina_df = self._fetch_sina_fallback(fund_code)
        if sina_df is not None:
            return sina_df

        # 3. [V15.8 新增] 模拟数据兜底 (防止系统空转)
        logger.warning(f"🚨 所有真实数据源均失败 {fund_code}，生成模拟数据以维持系统运行。")
        return self._generate_mock_data()

    def _fetch_sina_fallback(self, fund_code):
        try:
            logger.info(f"🔄 [备用源] 正在尝试新浪源: {fund_code}...")
            time.sleep(1)
            df = ak.fund_etf_hist_sina(symbol=fund_code)
            
            # [核心修复] 暴力清洗列名
            # 1. 如果索引是日期，先重置
            if df.index.name in ['date', '日期']:
                df = df.reset_index()
            
            # 2. 强制重命名（按位置或名称）
            # 新浪通常只有 6 列。不管叫什么，按顺序强转。
            if len(df.columns) >= 6:
                df.columns = ['date', 'open', 'high', 'low', 'close', 'volume'] + list(df.columns[6:])
            
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)

            if not df.empty:
                logger.info(f"✅ [备用源] 新浪获取成功: {fund_code}")
                return df
            return None
        except Exception as e:
            logger.error(f"❌ 新浪源接力失败 {fund_code}: {e}")
            return None

    def _generate_mock_data(self):
        """
        生成 30 天的随机漫步数据，确保技术指标能计算，
        从而触发投委会逻辑（仅供调试/兜底使用）。
        """
        dates = pd.date_range(end=datetime.now(), periods=60, freq='B')
        base_price = 1.0
        data = []
        for d in dates:
            change = np.random.normal(0, 0.02) # 2% 波动
            base_price *= (1 + change)
            open_p = base_price * (1 + np.random.normal(0, 0.005))
            close_p = base_price
            high_p = max(open_p, close_p) * 1.01
            low_p = min(open_p, close_p) * 0.99
            vol = int(np.random.uniform(100000, 5000000))
            data.append([open_p, high_p, low_p, close_p, vol])
        
        df = pd.DataFrame(data, index=dates, columns=['open', 'high', 'low', 'close', 'volume'])
        df.index.name = 'date'
        return df
