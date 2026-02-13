import pandas as pd
import time
import random
import os
import yaml
import logging
import gc
from datetime import datetime, time as dt_time
from typing import Optional, List

try:
    from curl_cffi import requests as cffi_requests
    from curl_cffi.requests.exceptions import RequestException
except ImportError:
    raise ImportError("请先安装 curl_cffi: pip install curl_cffi>=0.5.10")

# ===================== 工具函数 =====================
def get_beijing_time():
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=8)))

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def retry(retries: int = 3, delay: float = 5.0):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"⚠️ [Retry {i+1}/{retries}] {func.__name__} 失败: {e}")
                    if i < retries - 1:
                        time.sleep(delay * (i + 1))
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
        
        self.spot_data_cache: Optional[pd.DataFrame] = None
        self.spot_data_date: Optional[str] = None
        self.session = cffi_requests.Session(impersonate="chrome120")

    def __del__(self):
        if hasattr(self, 'session'):
            try:
                self.session.close()
            except:
                pass

    def _standardize_dataframe(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
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

    def _fetch_single_page(self, pn: int, pz: int = 100) -> Optional[List[dict]]:
        """获取单页数据"""
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        
        params = {
            "pn": str(pn),      # 页码
            "pz": str(pz),      # 每页数量，最大100
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024",
            "fields": "f12,f14,f2,f3,f4,f5,f6,f7,f8,f15,f16,f17,f18",
            "_": str(int(time.time() * 1000))
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "http://quote.eastmoney.com/",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "X-Requested-With": "XMLHttpRequest",
        }

        try:
            r = self.session.get(url, params=params, headers=headers, timeout=15)
            
            if r.status_code != 200:
                logger.error(f"❌ 页 {pn} 请求失败: {r.status_code}")
                return None

            data_json = r.json()
            
            if not data_json or data_json.get('rc') != 0:
                logger.error(f"❌ 页 {pn} API错误: {data_json.get('rt', '未知')}")
                return None
                
            if 'data' not in data_json or 'diff' not in data_json['data']:
                return None
                
            return data_json['data']['diff']
            
        except Exception as e:
            logger.error(f"❌ 页 {pn} 异常: {e}")
            return None

    @retry(retries=3, delay=5)
    def _fetch_eastmoney_raw_spot(self) -> Optional[pd.DataFrame]:
        """
        [修复 V19.0] 分页获取所有 ETF 数据
        东财 API 每页最大 100 条，需要遍历所有页面
        """
        logger.info("🚀 [黑科技] 正在分页获取东财全市场 ETF 数据...")
        
        all_data = []
        page = 1
        max_pages = 100  # 安全上限，防止无限循环
        
        while page <= max_pages:
            logger.info(f"📄 获取第 {page} 页...")
            page_data = self._fetch_single_page(page, pz=100)
            
            if not page_data:
                break
                
            all_data.extend(page_data)
            logger.info(f"   本页 {len(page_data)} 条，累计 {len(all_data)} 条")
            
            # 如果本页不足 100 条，说明是最后一页
            if len(page_data) < 100:
                break
                
            page += 1
            time.sleep(random.uniform(0.5, 1.5))  # 页间随机延迟
        
        if not all_data:
            logger.error("❌ 未获取到任何数据")
            return None
            
        logger.info(f"✅ 共获取 {len(all_data)} 条 ETF 数据")
        
        # 转换为 DataFrame
        df = pd.DataFrame(all_data)
        
        # 字段映射
        rename_map = {
            'f12': 'code',
            'f14': 'name',
            'f2': 'close',
            'f3': 'pct_change',
            'f4': 'change',
            'f5': 'volume',
            'f6': 'amount',
            'f7': 'amplitude',
            'f8': 'turnover_rate',
            'f17': 'open',
            'f15': 'high',
            'f16': 'low',
            'f18': 'pre_close',
        }
        
        existing_cols = {k: v for k, v in rename_map.items() if k in df.columns}
        df.rename(columns=existing_cols, inplace=True)
        
        # 清理代码格式（去除市场前缀）
        df['code'] = df['code'].astype(str).str.strip().str.replace(r'^[shsz]+', '', regex=True)
        
        # 去重（以防万一）
        df = df.drop_duplicates(subset=['code'], keep='first')
        
        logger.info(f"✅ 去重后共 {len(df)} 条，字段: {list(df.columns)}")
        return df.set_index('code')

    def _init_spot_data(self) -> bool:
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        
        if self.spot_data_cache is not None and self.spot_data_date == today_str:
            logger.info("✅ 使用今日已缓存的 Spot 数据")
            return True

        df = self._fetch_eastmoney_raw_spot()
        if df is not None and not df.empty:
            self.spot_data_cache = df
            self.spot_data_date = today_str
            return True
        return False

    def _safe_float(self, val, default: float = 0.0) -> float:
        if val is None or val == '-' or val == '':
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    def update_cache(self, fund_code: str) -> bool:
        fetch_time = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        
        if self.spot_data_cache is None:
            if not self._init_spot_data():
                return False

        clean_code = str(fund_code).strip()
        # 统一处理代码格式
        clean_code = clean_code.lower().replace('sh', '').replace('sz', '')
        
        if clean_code not in self.spot_data_cache.index:
            logger.warning(f"⚠️ 未找到 {fund_code} (clean: {clean_code})")
            return False

        try:
            row = self.spot_data_cache.loc[clean_code]
            today_date = pd.Timestamp(get_beijing_time().date())
            
            new_data = {
                'date': today_date,
                'open': self._safe_float(row.get('open')),
                'high': self._safe_float(row.get('high')),
                'low': self._safe_float(row.get('low')),
                'close': self._safe_float(row.get('close')),
                'volume': self._safe_float(row.get('volume')),
                'amount': self._safe_float(row.get('amount')),
                'amplitude': self._safe_float(row.get('amplitude')),
                'pct_change': self._safe_float(row.get('pct_change')),
                'change': self._safe_float(row.get('change')),
                'turnover_rate': self._safe_float(row.get('turnover_rate')),
                'fetch_time': fetch_time,
                'source': 'eastmoney_spot'
            }
            
            df_new = pd.DataFrame([new_data])
            df_new.set_index('date', inplace=True)

            file_path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
            
            if os.path.exists(file_path):
                try:
                    df_old = pd.read_csv(file_path, index_col='date', parse_dates=['date'])
                    
                    if today_date in df_old.index:
                        df_old.update(df_new)
                        df_final = df_old
                    else:
                        df_final = pd.concat([df_old, df_new])
                    
                    df_final = df_final[~df_final.index.duplicated(keep='last')]
                    df_final.sort_index(inplace=True)
                    
                except Exception as e:
                    logger.warning(f"⚠️ 读取历史失败: {e}")
                    df_final = df_new
            else:
                df_final = df_new

            final_df = self._standardize_dataframe(df_final, "东财")
            final_df.to_csv(file_path)
            
            logger.info(f"💾 [东财] {fund_code} 更新成功 (收盘: {new_data['close']:.3f}, 涨跌: {new_data['pct_change']:.2f}%)")
            return True

        except Exception as e:
            logger.error(f"❌ 处理异常 {fund_code}: {e}")
            return False

# ===================== 主程序 =====================
if __name__ == "__main__":
    print("🚀 [DataFetcher] 启动 (curl_cffi 分页版 V19.0)...")
    
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
            funds = cfg.get('funds', [])
    except Exception as e:
        logger.error(f"配置错误: {e}")
        funds = []
    
    if not funds:
        print("⚠️ 未找到基金列表")
        exit(1)

    fetcher = DataFetcher()
    
    if not fetcher._init_spot_data():
        logger.error("❌ 初始化失败")
        exit(1)

    success_count = 0
    total = len(funds)
    
    for idx, fund in enumerate(funds):
        code = str(fund.get('code', '')).strip()
        name = fund.get('name', 'Unknown')
        
        if not code:
            continue
            
        logger.info(f"🔄 [{idx+1}/{total}] {name} ({code})")
        
        if fetcher.update_cache(code):
            success_count += 1
            
        if (idx + 1) % 10 == 0 or idx == total - 1:
            logger.info(f"📊 进度: {idx+1}/{total}, 成功: {success_count}")
            
    logger.info(f"🏁 完成: {success_count}/{total}")
    print(f"🏁 行情更新完成: {success_count}/{total}")
    
    del fetcher
    gc.collect()
