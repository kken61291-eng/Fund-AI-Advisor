import akshare as ak
import pandas as pd
import time
import random
import os
import yaml
import logging
import requests
import gc
from datetime import datetime, time as dt_time

# ===================== 工具函数 =====================
def get_beijing_time():
    """获取北京时间（东八区）"""
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=8)))

# 简易日志配置
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def retry(retries=3, delay=10):
    """简易重试装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"⚠️ [Retry {i+1}/{retries}] 操作失败: {e}, 等待 {delay}s 后重试...")
                    if i == retries - 1:
                        logger.error(f"❌ 重试耗尽，最终失败: {e}")
                        return None, None 
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def force_close_connections():
    """[V17.0] 强制关闭所有网络连接"""
    try:
        if hasattr(ak, '_session') and ak._session:
            try:
                ak._session.close()
                ak._session = None
            except:
                pass
        gc.collect()
        time.sleep(0.5)
    except Exception as e:
        logger.debug(f"关闭连接时出错: {e}")

# [新增 V17.0] 使用 curl_cffi 创建模拟浏览器会话
def create_browser_session():
    """创建模拟 Chrome 浏览器的 curl_cffi 会话，绕过 TLS 指纹检测"""
    try:
        from curl_cffi import requests as curl_requests
        
        # 模拟 Chrome 120 的 TLS 指纹
        session = curl_requests.Session(
            impersonate="chrome120",  # 关键：模拟真实浏览器指纹
            timeout=30
        )
        return session
    except ImportError:
        logger.warning("curl_cffi 未安装，回退到普通 requests")
        return None
# ====================================================================

class DataFetcher:
    UNIFIED_COLUMNS = [
        'date', 'open', 'high', 'low', 'close', 'volume',
        'amount', 'amplitude', 'pct_change', 'change', 'turnover_rate',
        'fetch_time'
    ]
    
    def __init__(self):
        self.DATA_DIR = "data_cache"
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)
            
        # [V17.0] 扩充 User-Agent 池，增加移动端
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        ]

    def _get_random_headers(self):
        """生成随机请求头"""
        ua = random.choice(self.user_agents)
        return {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'close',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        }

    def _verify_data_freshness(self, df, fund_code, source_name):
        """数据新鲜度审计"""
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
                    logger.warning(f"{log_prefix} | ⚠️ 数据滞后 {days_gap} 天")
                else:
                    logger.info(f"{log_prefix} | ⏸️ 历史数据就绪")
        except Exception as e:
            logger.warning(f"审计数据新鲜度失败: {e}")

    def _standardize_dataframe(self, df, source_name):
        """标准化 DataFrame"""
        if df is None or df.empty:
            return df
        
        df = df.copy()
            
        for col in self.UNIFIED_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        
        df = df[self.UNIFIED_COLUMNS]
        
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 
                       'amplitude', 'pct_change', 'change', 'turnover_rate']
        for col in numeric_cols:
            if col in df.columns:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

    @retry(retries=2, delay=25)
    def _fetch_eastmoney(self, fund_code, fetch_time):
        """[V17.0] 使用 curl_cffi 模拟浏览器获取东财数据"""
        logger.info(f"🌐 [东财] 模拟浏览器获取 {fund_code}...")
        
        try:
            # [关键 V17.0] 使用 curl_cffi 的浏览器模拟功能
            # 这会自动处理 TLS 指纹、HTTP/2 等
            browser_session = create_browser_session()
            
            if browser_session:
                # 使用 curl_cffi 时，通过 akshare 的底层机制注入
                # 注意：akshare 1.18+ 内部使用了 curl_cffi，我们尝试设置其 session
                try:
                    # 尝试替换 akshare 内部 session
                    original_session = getattr(ak, '_session', None)
                    ak._session = browser_session
                except:
                    browser_session = None
            
            # 调用接口
            df = ak.fund_etf_hist_em(
                symbol=fund_code, 
                period="daily", 
                start_date="20250101", 
                end_date="20500101", 
                adjust="qfq"
            )
            
            # 恢复原始 session
            try:
                if browser_session and original_session:
                    ak._session = original_session
            except:
                pass
            
            if df is not None and not df.empty:
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
                return df, "东财"
                
        finally:
            force_close_connections()
            logger.info(f"🔌 [东财] 会话已清理")

    @retry(retries=2, delay=15)
    def _fetch_sina(self, fund_code, fetch_time):
        """[V17.0] 获取新浪数据"""
        logger.info(f"🌐 [新浪] 获取 {fund_code}...")
        
        try:
            df = ak.fund_etf_hist_sina(symbol=fund_code)
            
            if df is not None and not df.empty:
                if df.index.name in ['date', '日期']: 
                    df = df.reset_index()
                
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
                    
                    df['amount'] = pd.NA
                    df['amplitude'] = pd.NA
                    df['pct_change'] = pd.NA
                    df['change'] = pd.NA
                    df['turnover_rate'] = pd.NA
                    df['fetch_time'] = fetch_time
                    df['source'] = 'sina'
                    
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        if col in df.columns: 
                            df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')
                    
                    df = self._standardize_dataframe(df, "新浪")
                    return df, "新浪"
        finally:
            force_close_connections()
            logger.info(f"🔌 [新浪] 连接已关闭")

    @retry(retries=2, delay=15)
    def _fetch_tencent(self, fund_code, fetch_time):
        """[V17.0] 获取腾讯数据"""
        logger.info(f"🌐 [腾讯] 获取 {fund_code}...")
        
        try:
            prefix = 'sh' if fund_code.startswith('5') else ('sz' if fund_code.startswith('1') else '')
            if prefix:
                df = ak.stock_zh_a_hist_tx(
                    symbol=f"{prefix}{fund_code}", 
                    start_date="20200101", 
                    adjust="qfq"
                )
                
                if df is not None and not df.empty:
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
                    return df, "腾讯"
        finally:
            force_close_connections()
            logger.info(f"🔌 [腾讯] 连接已关闭")

    def _fetch_from_network(self, fund_code):
        """[V17.0] 主获取逻辑：东财 -> 新浪 -> 腾讯"""
        fetch_time = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        
        # 1. 东财 - 使用浏览器模拟
        try:
            wait = random.uniform(8.0, 15.0)  # [V17.0] 增加初始等待
            logger.info(f"⏳ 预等待 {wait:.1f}s...")
            time.sleep(wait)
            
            df, source = self._fetch_eastmoney(fund_code, fetch_time)
            if df is not None and not df.empty:
                return df, source
        except Exception as e:
            logger.error(f"❌ 东财失败: {e}")
            force_close_connections()

        # 2. 新浪
        try:
            time.sleep(random.uniform(5.0, 10.0))
            df, source = self._fetch_sina(fund_code, fetch_time)
            if df is not None and not df.empty:
                return df, source
        except Exception as e:
            logger.error(f"⚠️ 新浪失败: {e}")

        # 3. 腾讯
        try:
            time.sleep(random.uniform(5.0, 10.0))
            df, source = self._fetch_tencent(fund_code, fetch_time)
            if df is not None and not df.empty:
                return df, source
        except Exception as e:
            logger.error(f"⚠️ 腾讯失败: {e}")
        
        return None, None

    def update_cache(self, fund_code):
        """[V17.0] 更新单个基金数据"""
        df, source = self._fetch_from_network(fund_code)
        
        if df is None:
            logger.error(f"❌ {fund_code} 所有数据源均获取失败")
            return False

        if not df.empty:
            file_path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
            df.to_csv(file_path)
            logger.info(f"💾 [{source}] {fund_code} 数据已保存至 {file_path}")
            
            # [V17.0] 东财成功后等待 50-70 秒（更保守）
            if source == "东财":
                wait_time = random.uniform(50, 70)
                logger.info(f"⏳ [东财] 强制冷却 {wait_time:.1f}s...")
                time.sleep(wait_time)
            
            return True
        else:
            logger.error(f"❌ {fund_code} 数据为空")
            return False

    def get_fund_history(self, fund_code, days=250):
        """读取本地缓存"""
        file_path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
        
        if not os.path.exists(file_path):
            logger.warning(f"⚠️ 本地缓存缺失: {fund_code}")
            return None
            
        try:
            df = pd.read_csv(file_path, index_col='date', parse_dates=['date'])
            
            if 'fetch_time' in df.columns:
                df['fetch_time'] = pd.to_datetime(df['fetch_time'])
            
            self._verify_data_freshness(df, fund_code, "本地缓存")
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取本地缓存失败 {fund_code}: {e}")
            return None

# ==========================================
# [V17.0] 主程序入口 - 随机顺序 + 浏览器模拟
# ==========================================
if __name__ == "__main__":
    print("🚀 [DataFetcher] 启动 (V17.0 Browser-Impersonate Mode)...")
    
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

    # 随机打乱获取顺序
    random.shuffle(funds)
    logger.info(f"🎲 随机获取顺序: {[f.get('code') for f in funds]}")

    fetcher = DataFetcher()
    success_count = 0
    
    for idx, fund in enumerate(funds):
        code = fund.get('code')
        name = fund.get('name')
        print(f"🔄 [{idx+1}/{len(funds)}] 更新: {name} ({code})...")
        
        try:
            if fetcher.update_cache(code):
                success_count += 1
            
            # 基金间基础间隔
            if idx < len(funds) - 1:
                base_wait = random.uniform(5.0, 10.0)
                logger.info(f"⏳ 基础间隔等待 {base_wait:.1f}s...")
                time.sleep(base_wait)
                
        except Exception as e:
            print(f"❌ 更新异常 {name}: {e}")
            force_close_connections()
            time.sleep(random.uniform(15, 20))
            
    print(f"🏁 完成: {success_count}/{len(funds)} (浏览器模拟模式)")
