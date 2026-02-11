import akshare as ak
import pandas as pd
import time
import random
import os
import yaml
from datetime import datetime, time as dt_time
import logging

# ===================== 临时补充 utils 模块缺失的部分（如果需要） =====================
def get_beijing_time():
    """获取北京时间（东八区）"""
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=8)))

# 简易日志配置
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def retry(retries=3, delay=5):
    """简易重试装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == retries - 1:
                        raise e
                    time.sleep(delay)
            return None
        return wrapper
    return decorator
# ====================================================================================

class DataFetcher:
    # [V15.17] 统一字段规范（所有数据源返回的字段结构）
    UNIFIED_COLUMNS = [
        'date', 'open', 'high', 'low', 'close', 'volume',
        'amount', 'amplitude', 'pct_change', 'change', 'turnover_rate',
        'fetch_time'  # 数据抓取时间
    ]
    
    def __init__(self):
        self.DATA_DIR = "data_cache"
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)
            
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
        ]

    def _verify_data_freshness(self, df, fund_code, source_name):
        """数据新鲜度审计 (通用)"""
        if df is None or df.empty: 
            return
        
        try:
            last_date = pd.to_datetime(df.index[-1]).date()
            now_bj = get_beijing_time()
            today_date = now_bj.date()
            is_trading_time = (dt_time(9, 30) <= now_bj.time() <= dt_time(15, 0))
            
            log_prefix = f"📅 [{source_name}] {fund_code} 最新日期: {last_date}"
            
            if last_date == today_date:
                logger.info(f"{log_prefix} | ✅ 数据已更新至今日")
            elif last_date < today_date:
                days_gap = (today_date - last_date).days
                if is_trading_time and days_gap >= 1:
                    logger.warning(f"{log_prefix} | ⚠️ 数据滞后 {days_gap} 天 (请运行爬虫更新)")
                else:
                    logger.info(f"{log_prefix} | ⏸️ 历史数据就绪")
        except Exception as e:
            logger.warning(f"审计数据新鲜度失败: {e}")

    def _standardize_dataframe(self, df, source_name):
        """
        [V15.17] 标准化 DataFrame：确保所有数据源返回统一的字段结构
        缺失字段填充为 NaN
        """
        if df is None or df.empty:
            return df
            
        # 确保所有统一字段都存在，缺失的填充为 NaN
        for col in self.UNIFIED_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        
        # 按统一顺序排列列
        df = df[self.UNIFIED_COLUMNS]
        
        # 数据类型转换
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 
                       'amplitude', 'pct_change', 'change', 'turnover_rate']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

    @retry(retries=3, delay=5)
    def _fetch_from_network(self, fund_code):
        """
        [私有方法] 纯联网获取数据 (东财 -> 新浪 -> 腾讯)
        所有数据源统一返回标准字段结构
        """
        fetch_time = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        
        # 1. 东财 (EastMoney) - 优先数据源，字段最全
        try:
            time.sleep(random.uniform(1.0, 2.0)) 
            df = ak.fund_etf_hist_em(
                symbol=fund_code, 
                period="daily", 
                start_date="20200101", 
                end_date="20500101", 
                adjust="qfq"
            )
            
            # 东财字段映射（最全）
            rename_map = {
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '换手率': 'turnover_rate'
            }
            df.rename(columns=rename_map, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df['fetch_time'] = fetch_time
            df['source'] = 'eastmoney'
            
            df = self._standardize_dataframe(df, "东财")
            if not df.empty: 
                return df, "东财"
        except Exception as e:
            logger.error(f"东财数据源异常: {e}")
            pass

        # 2. 新浪 (Sina) - 字段有限，缺失字段填充 NaN
        try:
            time.sleep(1)
            df = ak.fund_etf_hist_sina(symbol=fund_code)
            
            if df.index.name in ['date', '日期']: 
                df = df.reset_index()
            
            # 新浪返回字段：日期、开盘、收盘、最高、最低、成交量（字段名可能为英文或中文）
            # 需要智能识别列名
            col_mapping = {}
            for col in df.columns:
                col_str = str(col).lower()
                if col_str in ['date', '日期']:
                    col_mapping[col] = 'date'
                elif col_str in ['open', '开盘']:
                    col_mapping[col] = 'open'
                elif col_str in ['close', '收盘', 'latest']:
                    col_mapping[col] = 'close'
                elif col_str in ['high', '最高']:
                    col_mapping[col] = 'high'
                elif col_str in ['low', '最低']:
                    col_mapping[col] = 'low'
                elif col_str in ['volume', '成交量', 'vol']:
                    col_mapping[col] = 'volume'
            
            df.rename(columns=col_mapping, inplace=True)
            
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                
                # 新浪缺失字段填充为 NaN
                df['amount'] = pd.NA
                df['amplitude'] = pd.NA
                df['pct_change'] = pd.NA
                df['change'] = pd.NA
                df['turnover_rate'] = pd.NA
                df['fetch_time'] = fetch_time
                df['source'] = 'sina'
                
                # 基础类型清洗
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns: 
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df = self._standardize_dataframe(df, "新浪")
                return df, "新浪"
        except Exception as e:
            logger.error(f"新浪数据源异常: {e}")
            pass

        # 3. 腾讯 (Tencent) - 字段较全，与东财类似
        try:
            time.sleep(1)
            prefix = 'sh' if fund_code.startswith('5') else ('sz' if fund_code.startswith('1') else '')
            if prefix:
                df = ak.stock_zh_a_hist_tx(
                    symbol=f"{prefix}{fund_code}", 
                    start_date="20200101", 
                    adjust="qfq"
                )
                
                # 腾讯字段映射（与东财类似）
                rename_map = {
                    '日期': 'date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume',
                    '成交额': 'amount',
                    '振幅': 'amplitude',
                    '涨跌幅': 'pct_change',
                    '涨跌额': 'change',
                    '换手率': 'turnover_rate'
                }
                df.rename(columns=rename_map, inplace=True)
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                df['fetch_time'] = fetch_time
                df['source'] = 'tencent'
                
                df = self._standardize_dataframe(df, "腾讯")
                if not df.empty: 
                    return df, "腾讯"
        except Exception as e:
            logger.error(f"腾讯数据源异常: {e}")
            pass
        
        return None, None

    def update_cache(self, fund_code):
        """
        [爬虫专用] 联网下载数据并保存到本地 CSV
        """
        df, source = self._fetch_from_network(fund_code)
        if df is not None and not df.empty:
            file_path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
            df.to_csv(file_path)
            logger.info(f"💾 [{source}] {fund_code} 数据已保存至 {file_path} (统一字段结构)")
            
            # [优化] 如果是东财数据，强制等待 40 秒，防止接口封禁
            if source == "东财":
                logger.info("⏳ [东财] 触发频率保护机制，等待 40 秒...")
                time.sleep(40)
                
            return True
        else:
            logger.error(f"❌ {fund_code} 所有数据源(东财/新浪/腾讯)均获取失败")
            return False

    def get_fund_history(self, fund_code, days=250):
        """
        [主程序专用] 只读模式：直接从本地 CSV 读取数据
        """
        file_path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
        
        if not os.path.exists(file_path):
            logger.warning(f"⚠️ 本地缓存缺失: {fund_code}，请等待 GitHub Action 爬虫运行")
            return None
            
        try:
            df = pd.read_csv(file_path, index_col='date', parse_dates=['date'])
            
            # 解析抓取时间字段
            if 'fetch_time' in df.columns:
                df['fetch_time'] = pd.to_datetime(df['fetch_time'])
            
            self._verify_data_freshness(df, fund_code, "本地缓存")
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取本地缓存失败 {fund_code}: {e}")
            return None

# ==========================================
# [新增] 独立运行入口 (让此脚本变身爬虫)
# ==========================================
if __name__ == "__main__":
    print("🚀 [DataFetcher] 启动多源行情抓取 (V15.17 Unified Fields)...")
    
    def load_config_local():
        try:
            with open('config.yaml', 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except:
            return {}

    cfg = load_config_local()
    funds = cfg.get('funds', [])
    
    if not funds:
        print("⚠️ 未找到基金列表，请检查 config.yaml")
        exit()

    fetcher = DataFetcher()
    success_count = 0
    
    for fund in funds:
        code = fund.get('code')
        name = fund.get('name')
        print(f"🔄 更新: {name} ({code})...")
        
        try:
            if fetcher.update_cache(code):
                success_count += 1
            time.sleep(random.uniform(1.0, 2.0))
        except Exception as e:
            print(f"❌ 更新异常 {name}: {e}")
            
    print(f"🏁 行情更新完成: {success_count}/{len(funds)} (统一字段结构)")
