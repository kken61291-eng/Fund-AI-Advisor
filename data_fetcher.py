import akshare as ak
import pandas as pd
import time
import random
from datetime import datetime
from utils import logger, retry

class DataFetcher:
    def __init__(self):
        # [V15.9] 扩充 User-Agent 池，模拟不同浏览器
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ]

    def _get_random_header(self):
        return {"User-Agent": random.choice(self.user_agents)}

    @retry(retries=3, delay=5) # 失败重试间隔增加到5秒
    def get_fund_history(self, fund_code, days=250):
        """
        获取K线数据。
        优先级：东财 -> 新浪 -> 放弃 (绝不使用模拟数据)
        """
        # 1. 尝试东财 (数据最全)
        try:
            # [关键修改] 增加较长的随机延时，模拟人类操作，防止被封
            sleep_time = random.uniform(3.0, 6.0)
            # logger.info(f"⏳ [东财] 等待 {sleep_time:.1f}s 以绕过封锁: {fund_code}")
            time.sleep(sleep_time)
            
            df = ak.fund_etf_hist_em(
                symbol=fund_code, 
                period="daily", 
                start_date="20240101", 
                end_date="20500101", 
                adjust="qfq"
            )
            
            # 东财列名标准化
            rename_map = {'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}
            df.rename(columns=rename_map, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            if not df.empty:
                logger.info(f"✅ [主源] 东财获取成功: {fund_code}")
                return df
            
        except Exception as e:
            logger.warning(f"⚠️ 东财源受阻 {fund_code}: {str(e)[:50]}... 切换备用源。")

        # 2. 尝试新浪 (备用)
        return self._fetch_sina_fallback(fund_code)

    def _fetch_sina_fallback(self, fund_code):
        """
        备用源：新浪财经
        [修复] 强力处理列名不一致问题
        """
        try:
            logger.info(f"🔄 [备用源] 正在尝试新浪源: {fund_code}...")
            time.sleep(2) # 备用源也稍微延时
            df = ak.fund_etf_hist_sina(symbol=fund_code)
            
            # 1. 如果索引本身就是日期，先reset出来变成列
            if df.index.name in ['date', '日期']:
                df = df.reset_index()
            
            # 2. 暴力清洗列名：不管新浪返回中文还是英文，前6列肯定是 OHLCV
            # 新浪返回通常是: date, open, high, low, close, volume (顺序可能变，但前几列固定)
            # 这里做一个全兼容映射
            rename_map = {
                '日期': 'date', 'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume',
                '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'
            }
            df.rename(columns=rename_map, inplace=True)

            # 3. 兜底：如果 rename 没生效（列名完全变了），按位置强制重命名
            # 假设前6列顺序为: date, open, high, low, close, volume
            if 'date' not in df.columns and len(df.columns) >= 6:
                df.columns = ['date', 'open', 'high', 'low', 'close', 'volume'] + list(df.columns[6:])
            
            # 4. 最终检查
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                
                if not df.empty:
                    logger.info(f"✅ [备用源] 新浪获取成功: {fund_code}")
                    return df
            
            logger.error(f"❌ 新浪源数据解析失败: {fund_code}")
            return None

        except Exception as e:
            logger.error(f"❌ 所有真实数据源均失败 {fund_code}: {e}")
            return None
