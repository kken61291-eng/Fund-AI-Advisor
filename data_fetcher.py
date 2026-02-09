import akshare as ak
import pandas as pd
import time
import random
from datetime import datetime
from utils import logger, retry

class DataFetcher:
    def __init__(self):
        # [V15.11] 针对东财封锁，扩充更多真实浏览器 UA
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]

    def _get_random_header(self):
        return {"User-Agent": random.choice(self.user_agents)}

    @retry(retries=2, delay=2) 
    def get_fund_history(self, fund_code, days=250):
        """
        获取K线数据。
        策略：死磕东财(3次递增重试) -> 强洗新浪 -> 腾讯保底
        """
        # --- 1. 攻坚东财 (EastMoney) ---
        # 东财数据质量最好，值得多试几次
        for attempt in range(3):
            try:
                # 指数级退避：第一次3s，第二次6s，第三次9s
                wait_time = (attempt + 1) * 3 + random.uniform(0, 1)
                # logger.info(f"⏳ [东财] 第{attempt+1}次尝试，等待 {wait_time:.1f}s...")
                time.sleep(wait_time)
                
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
                # 如果是最后一次尝试，打印警告并继续下面的备用源
                if attempt == 2:
                    logger.warning(f"⚠️ 东财彻底受阻 {fund_code}: {str(e)[:50]}... 切换新浪。")
                else:
                    pass # 静默重试

        # --- 2. 强洗新浪 (Sina) ---
        sina_df = self._fetch_sina_fallback(fund_code)
        if sina_df is not None:
            return sina_df

        # --- 3. 腾讯保底 (Tencent) ---
        return self._fetch_tx_fallback(fund_code)

    def _fetch_sina_fallback(self, fund_code):
        """
        备用源：新浪财经
        [修复逻辑] 无论新浪返回什么乱七八糟的格式，强制清洗为标准格式
        """
        try:
            logger.info(f"🔄 [备用源] 正在尝试新浪源: {fund_code}...")
            time.sleep(2) 
            df = ak.fund_etf_hist_sina(symbol=fund_code)
            
            if df is None or df.empty:
                return None

            # [关键] 检查索引是否就是日期
            if df.index.name in ['date', '日期'] or isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()

            # [关键] 暴力重命名：不管列名是中文还是英文，还是乱码
            # 只要列数足够，就按 OHLCV 的顺序强制赋值
            # 新浪通常结构：Date, Open, High, Low, Close, Volume
            if len(df.columns) >= 6:
                # 强制覆盖列名
                new_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
                # 保留多余的列（如果有）
                if len(df.columns) > 6:
                    new_columns.extend(df.columns[6:])
                df.columns = new_columns
            
            # 转换日期格式
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                
                # 数据类型清洗，防止字符串混入
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                logger.info(f"✅ [备用源] 新浪清洗成功: {fund_code}")
                return df
            
            logger.error(f"❌ 新浪源结构异常 {fund_code}: {list(df.columns)}")
            return None

        except Exception as e:
            logger.error(f"❌ 新浪源处理失败 {fund_code}: {e}")
            return None

    def _fetch_tx_fallback(self, fund_code):
        """
        [新增] 腾讯财经源
        """
        try:
            logger.info(f"🔄 [三号源] 正在尝试腾讯源: {fund_code}...")
            time.sleep(1)
            
            # 腾讯需要 sh/sz 前缀
            prefix = 'sh' if fund_code.startswith('5') else ('sz' if fund_code.startswith('1') else '')
            if not prefix: return None
            symbol = f"{prefix}{fund_code}"
            
            df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date="20240101", adjust="qfq")
            
            rename_map = {'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}
            df.rename(columns=rename_map, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)

            if not df.empty:
                logger.info(f"✅ [三号源] 腾讯获取成功: {fund_code}")
                return df
            return None
        except Exception:
            return None
