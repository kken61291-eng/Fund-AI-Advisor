import pandas as pd
import time
import random
import os
import yaml
import logging
import gc
import json
from datetime import datetime, time as dt_time
from typing import Optional, Tuple

# [关键依赖] 引入 curl_cffi 模拟浏览器指纹
try:
    from curl_cffi import requests as cffi_requests
    from curl_cffi.requests.exceptions import RequestException
except ImportError:
    raise ImportError("请先安装 curl_cffi: pip install curl_cffi>=0.5.10")

# ===================== 工具函数 =====================
def get_beijing_time():
    """获取北京时间"""
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=8)))

# 配置日志
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def retry(retries: int = 3, delay: float = 5.0):
    """重试装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"⚠️ [Retry {i+1}/{retries}] {func.__name__} 失败: {e}")
                    if i < retries - 1:
                        time.sleep(delay * (i + 1))  # 递增延迟
            logger.error(f"❌ {func.__name__} 重试耗尽")
            return None
        return wrapper
    return decorator

# ===================== DataFetcher 类 =====================
class DataFetcher:
    UNIFIED_COLUMNS = [
        'date', 'open', 'high', 'low', 'close', 'volume',
        'amount', 'amplitude', 'pct_change', 'change', 'turnover_rate',
        'fetch_time'
    ]
    
    def __init__(self):
        self.DATA_DIR = "data_cache"
        os.makedirs(self.DATA_DIR, exist_ok=True)
        
        # 缓存全市场数据
        self.spot_data_cache: Optional[pd.DataFrame] = None
        self.spot_data_date: Optional[str] = None
        
        # 创建 session 复用（curl_cffi 支持）
        self.session = cffi_requests.Session(impersonate="chrome120")

    def __del__(self):
        """清理 session"""
        if hasattr(self, 'session'):
            try:
                self.session.close()
            except:
                pass

    def _standardize_dataframe(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """标准化 DataFrame 格式"""
        if df is None or df.empty:
            return df
        
        df = df.copy()
        
        # 确保所有统一字段都存在
        for col in self.UNIFIED_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        
        # 按统一顺序排列
        df = df[self.UNIFIED_COLUMNS]
        
        # 强制转为数字类型
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 
                       'amplitude', 'pct_change', 'change', 'turnover_rate']
        for col in numeric_cols:
            if col in df.columns:
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

    @retry(retries=3, delay=5)
    def _fetch_eastmoney_raw_spot(self) -> Optional[pd.DataFrame]:
        """
        [核心黑科技] 使用 curl_cffi 直接请求东财原始接口
        绕过 Akshare，直接模拟 Chrome 120 获取全市场 ETF 数据
        """
        # [修复] URL 去除空格
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        
        # 东财 ETF 板块参数
        params = {
            "pn": "1",
            "pz": "5000",  # 一次拉取 5000 只，覆盖所有 ETF
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024",  # 涵盖所有场内基金
            # [修复] 补充完整字段：f7=振幅, f8=换手率
            "fields": "f12,f14,f2,f3,f4,f5,f6,f7,f8,f15,f16,f17,f18",
            "_": str(int(time.time() * 1000))
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            # [修复] Referer 去除空格
            "Referer": "http://quote.eastmoney.com/",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "X-Requested-With": "XMLHttpRequest",
        }

        logger.info("🚀 [黑科技] 正在伪装 Chrome 请求东财全市场数据...")
        
        try:
            # [关键] impersonate="chrome120" 让服务器认为这是真实浏览器
            r = self.session.get(
                url, 
                params=params, 
                headers=headers, 
                timeout=15
            )
            
            if r.status_code != 200:
                logger.error(f"❌ 请求返回状态码: {r.status_code}, 响应: {r.text[:200]}")
                return None

            data_json = r.json()
            
            # 验证数据结构
            if not data_json or data_json.get('rc') != 0:
                logger.error(f"❌ 东财返回错误: {data_json.get('rt', '未知错误')}")
                return None
                
            if 'data' not in data_json or 'diff' not in data_json['data']:
                logger.error("❌ 东财返回数据格式异常")
                return None
                
            raw_list = data_json['data']['diff']
            
            if not raw_list:
                logger.warning("⚠️ 东财返回空列表")
                return None
            
            # 转换为 DataFrame
            df = pd.DataFrame(raw_list)
            
            # [修复] 完整字段映射
            rename_map = {
                'f12': 'code',          # 代码
                'f14': 'name',          # 名称
                'f2': 'close',          # 最新价
                'f3': 'pct_change',     # 涨跌幅(%)
                'f4': 'change',         # 涨跌额
                'f5': 'volume',         # 成交量(手)
                'f6': 'amount',         # 成交额
                'f7': 'amplitude',      # [新增] 振幅(%)
                'f8': 'turnover_rate',  # [修复] 换手率(%)
                'f17': 'open',          # 开盘价
                'f15': 'high',          # 最高价
                'f16': 'low',           # 最低价
                'f18': 'pre_close',     # 昨收
            }
            
            # 只重命名存在的列
            existing_cols = {k: v for k, v in rename_map.items() if k in df.columns}
            df.rename(columns=existing_cols, inplace=True)
            
            # 确保 code 是字符串
            df['code'] = df['code'].astype(str).str.strip()
            
            logger.info(f"✅ 获取到 {len(df)} 条数据，字段: {list(df.columns)}")
            return df.set_index('code')

        except RequestException as e:
            logger.error(f"❌ [curl_cffi] 网络请求失败: {e}")
            raise  # 让重试装饰器处理
        except Exception as e:
            logger.error(f"❌ [curl_cffi] 处理失败: {e}")
            return None

    def _init_spot_data(self) -> bool:
        """初始化全市场数据（带缓存）"""
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        
        # 检查缓存
        if self.spot_data_cache is not None and self.spot_data_date == today_str:
            logger.info("✅ 使用今日已缓存的 Spot 数据")
            return True

        # 获取新数据
        df = self._fetch_eastmoney_raw_spot()
        if df is not None and not df.empty:
            self.spot_data_cache = df
            self.spot_data_date = today_str
            logger.info(f"✅ 全市场快照缓存成功，共 {len(df)} 条")
            return True
        
        logger.error("❌ 无法获取全市场数据")
        return False

    def _safe_float(self, val, default: float = 0.0) -> float:
        """安全转换为浮点数"""
        if val is None or val == '-' or val == '':
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    def update_cache(self, fund_code: str) -> bool:
        """更新单个基金缓存"""
        fetch_time = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        
        # 1. 确保全量数据已加载
        if self.spot_data_cache is None:
            if not self._init_spot_data():
                return False

        # 2. 查找数据（兼容带市场前缀的代码）
        clean_code = str(fund_code).strip()
        if len(clean_code) > 6:
            clean_code = clean_code[-6:]
            
        if clean_code not in self.spot_data_cache.index:
            logger.warning(f"⚠️ 未找到 {fund_code} (clean: {clean_code})")
            return False

        try:
            row = self.spot_data_cache.loc[clean_code]
            
            # 3. 构造当日 DataFrame
            today_date = pd.Timestamp(get_beijing_time().date())
            
            new_data = {
                'date': today_date,
                'open': self._safe_float(row.get('open')),
                'high': self._safe_float(row.get('high')),
                'low': self._safe_float(row.get('low')),
                'close': self._safe_float(row.get('close')),
                'volume': self._safe_float(row.get('volume')),
                'amount': self._safe_float(row.get('amount')),
                'amplitude': self._safe_float(row.get('amplitude')),  # [新增]
                'pct_change': self._safe_float(row.get('pct_change')),
                'change': self._safe_float(row.get('change')),
                'turnover_rate': self._safe_float(row.get('turnover_rate')),
                'fetch_time': fetch_time,
                'source': 'eastmoney_spot'
            }
            
            df_new = pd.DataFrame([new_data])
            df_new.set_index('date', inplace=True)

            # 4. 拼接到本地 CSV
            file_path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
            
            if os.path.exists(file_path):
                try:
                    df_old = pd.read_csv(file_path, index_col='date', parse_dates=['date'])
                    
                    # 更新或追加
                    if today_date in df_old.index:
                        df_old.update(df_new)
                        df_final = df_old
                    else:
                        df_final = pd.concat([df_old, df_new])
                    
                    # 去重排序
                    df_final = df_final[~df_final.index.duplicated(keep='last')]
                    df_final.sort_index(inplace=True)
                    
                except Exception as e:
                    logger.warning(f"⚠️ 读取历史数据失败，使用新数据: {e}")
                    df_final = df_new
            else:
                df_final = df_new

            # 标准化并保存
            final_df = self._standardize_dataframe(df_final, "东财")
            final_df.to_csv(file_path)
            
            logger.info(f"💾 [东财] {fund_code} 更新成功 (收盘价: {new_data['close']:.3f}, 涨跌: {new_data['pct_change']:.2f}%)")
            return True

        except Exception as e:
            logger.error(f"❌ 处理数据异常 {fund_code}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False

# ===================== 主程序 =====================
if __name__ == "__main__":
    print("🚀 [DataFetcher] 启动 (curl_cffi 东财专用版 V18.1)...")
    
    # 加载配置
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
            funds = cfg.get('funds', [])
    except Exception as e:
        logger.error(f"读取配置失败: {e}")
        funds = []
    
    if not funds:
        print("⚠️ 未找到 config.yaml 或基金列表为空")
        exit(1)

    fetcher = DataFetcher()
    
    # 初始化全市场数据（只请求1次）
    if not fetcher._init_spot_data():
        logger.error("❌ 无法获取全市场数据，退出")
        exit(1)

    # 批量更新
    success_count = 0
    total = len(funds)
    
    for idx, fund in enumerate(funds):
        code = str(fund.get('code', '')).strip()
        name = fund.get('name', 'Unknown')
        
        if not code or len(code) < 6:
            logger.warning(f"⚠️ 跳过无效代码: {fund}")
            continue
            
        logger.info(f"🔄 [{idx+1}/{total}] {name} ({code})")
        
        if fetcher.update_cache(code):
            success_count += 1
            
        # 每 10 个输出进度
        if (idx + 1) % 10 == 0 or idx == total - 1:
            logger.info(f"📊 进度: {idx+1}/{total}, 成功: {success_count}")
            
    logger.info(f"🏁 完成: {success_count}/{total}")
    print(f"🏁 行情更新完成: {success_count}/{total}")
    
    # 清理
    del fetcher
    gc.collect()
