import akshare as ak
import pandas as pd
import time
import random
import os
import yaml
import logging
import requests
import gc
from datetime import datetime, time as dt_time, date

# ===================== 工具函数 (保持不变) =====================
def get_beijing_time():
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=8)))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def retry(retries=3, delay=10):
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

# ===================== DataFetcher 类 (核心修改) =====================

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
        
        # [修改点1] 增加 Spot 数据缓存变量
        self.spot_data_cache = None
        self.spot_data_date = None

    def _standardize_dataframe(self, df, source_name):
        """标准化 DataFrame格式"""
        if df is None or df.empty:
            return df
        df = df.copy()
        for col in self.UNIFIED_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[self.UNIFIED_COLUMNS]
        # 强制转为数字类型，防止出现字符串
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 
                       'amplitude', 'pct_change', 'change', 'turnover_rate']
        for col in numeric_cols:
            if col in df.columns:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def _init_spot_data(self):
        """[新增] 仅在启动时运行一次：拉取全市场 ETF 实时行情"""
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        # 如果缓存里已经有今天的数据，直接跳过
        if self.spot_data_cache is not None and self.spot_data_date == today_str:
            return True

        logger.info("🚀 [东财] 正在拉取全市场 ETF 实时快照 (Spot)...")
        try:
            # 这里的接口非常关键，获取所有 ETF 的当前价格
            df = ak.fund_etf_spot_em()
            
            if df is not None and not df.empty:
                # 建立代码索引，方便后续 O(1) 复杂度查找
                # 注意：确保代码列是字符串类型
                df['code'] = df['代码'].astype(str)
                self.spot_data_cache = df.set_index('code')
                self.spot_data_date = today_str
                logger.info(f"✅ 全市场快照获取成功，共 {len(df)} 条数据")
                return True
            else:
                logger.warning("⚠️ 全市场快照返回为空")
        except Exception as e:
            logger.error(f"❌ 全市场快照获取失败: {e}")
            self.spot_data_cache = None
        return False

    def _fetch_eastmoney(self, fund_code, fetch_time):
        """
        [重写] 即使是获取东财数据，也不再请求网络，而是从 spot 缓存读取 + 拼接本地历史
        """
        # 1. 确保有全量缓存
        if self.spot_data_cache is None:
            if not self._init_spot_data():
                return None, None # 初始化失败，后续会触发 failover 去跑新浪/腾讯

        # 2. 在缓存中查找当前基金
        if fund_code not in self.spot_data_cache.index:
            # 这种情况可能是代码填错了，或者该基金今日停牌/未上市
            # logger.debug(f"⚠️ [Spot] 未找到 {fund_code}")
            return None, None

        try:
            # 3. 提取当日数据行
            row = self.spot_data_cache.loc[fund_code]
            
            # 构造当日的 DataFrame (单行)
            # 注意：Spot接口没有具体日期字段，默认归为"今天"
            # 必须使用 .date() 确保索引对齐
            today_date = pd.Timestamp(datetime.now().date())
            
            new_data = {
                'date': today_date,
                'open': row['开盘价'],
                'high': row['最高价'],
                'low': row['最低价'],
                'close': row['最新价'],
                'volume': row['成交量'],
                'amount': row['成交额'],
                'pct_change': row['涨跌幅'],
                'change': row.get('涨跌额', 0),
                'turnover_rate': row.get('换手率', 0),
                'fetch_time': fetch_time,
                'source': 'eastmoney_spot'
            }
            
            df_new = pd.DataFrame([new_data])
            df_new.set_index('date', inplace=True)

            # 4. [核心逻辑] 读取本地 CSV 并拼接
            file_path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
            
            if os.path.exists(file_path):
                try:
                    # 读取旧数据
                    df_old = pd.read_csv(file_path, index_col='date', parse_dates=['date'])
                    
                    # 检查今天的数据是否已存在
                    if today_date in df_old.index:
                        # 如果存在，则更新这一行（覆盖）
                        df_old.update(df_new)
                        df_final = df_old
                    else:
                        # 如果不存在，追加到末尾
                        df_final = pd.concat([df_old, df_new])
                    
                    # 确保按日期排序
                    df_final.sort_index(inplace=True)
                    return self._standardize_dataframe(df_final, "东财"), "东财"
                except Exception as e:
                    logger.error(f"⚠️ 读取本地文件 {fund_code} 失败: {e}，将仅返回当日数据")
                    return self._standardize_dataframe(df_new, "东财"), "东财"
            else:
                # 如果没有本地文件（第一次运行），则只返回这一行数据
                # 注意：这意味着你的 CSV 里只有这一天的数据
                return self._standardize_dataframe(df_new, "东财"), "东财"

        except Exception as e:
            logger.error(f"❌ [东财Spot] 解析数据异常: {e}")
            return None, None

    # --- 新浪和腾讯的逻辑保持原样，作为备用兜底 ---
    @retry(retries=2, delay=15)
    def _fetch_sina(self, fund_code, fetch_time):
        logger.info(f"🌐 [新浪] 获取 {fund_code}...")
        try:
            df = ak.fund_etf_hist_sina(symbol=fund_code)
            if df is not None and not df.empty:
                # 简单处理新浪数据格式
                if 'date' in df.columns: 
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                df['fetch_time'] = fetch_time
                return self._standardize_dataframe(df, "新浪"), "新浪"
        except Exception as e:
            logger.error(f"新浪失败: {e}")
        return None, None

    @retry(retries=2, delay=15)
    def _fetch_tencent(self, fund_code, fetch_time):
        logger.info(f"🌐 [腾讯] 获取 {fund_code}...")
        try:
            prefix = 'sh' if fund_code.startswith('5') else ('sz' if fund_code.startswith('1') else '')
            df = ak.stock_zh_a_hist_tx(symbol=f"{prefix}{fund_code}", start_date="20250101", adjust="qfq")
            if df is not None and not df.empty:
                df.rename(columns={'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}, inplace=True)
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                df['fetch_time'] = fetch_time
                return self._standardize_dataframe(df, "腾讯"), "腾讯"
        except Exception as e:
            logger.error(f"腾讯失败: {e}")
        return None, None

    def _fetch_from_network(self, fund_code):
        """主获取逻辑"""
        fetch_time = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")

        # 1. 优先尝试东财 (现在是极速 Spot 模式)
        # 不需要 sleep 了，因为是读内存
        df, source = self._fetch_eastmoney(fund_code, fetch_time)
        if df is not None:
            return df, source

        # 2. 如果 Spot 里没有（比如停牌），尝试新浪（获取历史）
        time.sleep(random.uniform(2, 5))
        df, source = self._fetch_sina(fund_code, fetch_time)
        if df is not None:
            return df, source

        # 3. 最后尝试腾讯
        time.sleep(random.uniform(2, 5))
        df, source = self._fetch_tencent(fund_code, fetch_time)
        if df is not None:
            return df, source
            
        return None, None

    def update_cache(self, fund_code):
        """更新接口，保持写入逻辑不变"""
        df, source = self._fetch_from_network(fund_code)
        
        if df is not None and not df.empty:
            file_path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
            # 无论来源是哪，都直接覆盖写入（因为 _fetch_eastmoney 已经做好了拼接）
            df.to_csv(file_path)
            logger.info(f"💾 [{source}] {fund_code} 数据已更新")
            return True
        else:
            logger.error(f"❌ {fund_code} 更新失败")
            return False

# ===================== 主程序 =====================
if __name__ == "__main__":
    print("🚀 [DataFetcher] 启动 (Spot 极速模式)...")
    
    # 读取配置
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
            funds = cfg.get('funds', [])
    except:
        funds = [] # 此时请确保你的config.yaml存在
    
    if not funds:
        print("⚠️ 未找到基金列表")
        exit()

    fetcher = DataFetcher()
    
    # [关键步骤] 初始化全市场数据 (只请求1次)
    fetcher._init_spot_data()

    success_count = 0
    for idx, fund in enumerate(funds):
        code = str(fund.get('code')) # 确保是字符串
        name = fund.get('name')
        
        # 这里的 update_cache 速度会非常快
        if fetcher.update_cache(code):
            success_count += 1
            
        # 极速模式下，不需要 sleep 很久，微小的间隔即可
        if idx % 10 == 0: 
            print(f"进度: {idx+1}/{len(funds)}...")
            
    print(f"🏁 完成: {success_count}/{len(funds)}")
