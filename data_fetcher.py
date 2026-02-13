import pandas as pd
import time
import random
import os
import yaml
import logging
import gc
import sys
from datetime import datetime, time as dt_time
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass

# 检查依赖
try:
    from curl_cffi import requests as cffi_requests
    from curl_cffi.requests.exceptions import RequestException, ProxyError, Timeout
except ImportError:
    print("❌ 请先安装依赖: pip install curl_cffi pandas pyyaml")
    sys.exit(1)

# ===================== 配置加载 (环境变量优先) =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 🟢 核心修改：优先从系统环境变量获取 Key (适配 GitHub Secrets)
SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY", "")

# 如果环境变量没拿到，尝试读取本地 settings.py (方便本地调试)
if not SCRAPERAPI_KEY:
    try:
        import settings
        SCRAPERAPI_KEY = getattr(settings, 'SCRAPERAPI_KEY', "")
    except ImportError:
        pass

# 如果 ScraperAPI 额度耗尽(403错误)或失败，是否允许自动降级为本机直连？
ALLOW_DIRECT_FALLBACK = True 

if not SCRAPERAPI_KEY:
    logger.warning("⚠️ 未检测到 SCRAPERAPI_KEY (Secrets 或 settings)，将仅使用直连模式")
else:
    # 隐藏 Key 的中间部分，只打印首尾，用于日志确认
    masked_key = f"{SCRAPERAPI_KEY[:4]}****{SCRAPERAPI_KEY[-4:]}" if len(SCRAPERAPI_KEY) > 8 else "****"
    logger.info(f"🔑 已加载 ScraperAPI Key: {masked_key}")

def get_beijing_time():
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=8)))

# ===================== DataFetcher =====================
class DataFetcher:
    UNIFIED_COLUMNS = ['date', 'open', 'high', 'low', 'close', 'volume',
                       'amount', 'amplitude', 'pct_change', 'change', 'turnover_rate', 'fetch_time']
    
    def __init__(self):
        self.DATA_DIR = "data_cache"
        os.makedirs(self.DATA_DIR, exist_ok=True)
        
        self.spot_data_cache: Optional[pd.DataFrame] = None
        self.spot_data_date: Optional[str] = None
        self.session: Optional[cffi_requests.Session] = None
        
        self.total_funds = 0
        self.success_count = 0
        
    def _get_scraperapi_proxy(self) -> str:
        return f"http://scraperapi:{SCRAPERAPI_KEY}@proxy-server.scraperapi.com:8001"

    def _create_session(self, use_proxy: bool = True):
        self._close_session()
        try:
            # 只有当 Key 存在且 use_proxy 为 True 时才使用代理
            if use_proxy and SCRAPERAPI_KEY:
                proxy_url = self._get_scraperapi_proxy()
                self.session = cffi_requests.Session(
                    impersonate="chrome120",
                    proxies={"http": proxy_url, "https": proxy_url},
                    timeout=60,
                    verify=False 
                )
            else:
                self.session = cffi_requests.Session(
                    impersonate="chrome120",
                    timeout=30
                )
        except Exception as e:
            logger.error(f"❌ 创建 session 失败: {e}")
            raise

    def _close_session(self):
        if self.session:
            try:
                self.session.close()
            except:
                pass
            self.session = None
            gc.collect()

    def _safe_request(self, url: str, params: dict, headers: dict, max_retries: int = 3) -> Optional[dict]:
        # 如果 Key 存在，默认优先尝试代理
        use_proxy_default = True if SCRAPERAPI_KEY else False
        
        if self.session is None:
            self._create_session(use_proxy=use_proxy_default)

        for attempt in range(max_retries):
            try:
                if not self.session: raise Exception("Session Lost")
                r = self.session.get(url, params=params, headers=headers)
                
                # 403 处理：Key 无效或额度耗尽
                if r.status_code == 403:
                    logger.warning("⚠️ ScraperAPI 返回 403 (额度耗尽/Key错误)")
                    if ALLOW_DIRECT_FALLBACK:
                         self._create_session(use_proxy=False)
                         try:
                             r = self.session.get(url, params=params, headers=headers)
                             r.raise_for_status()
                             return r.json()
                         except Exception as e:
                             return None
                    else:
                        return None
                        
                r.raise_for_status()
                return r.json()
            except (ProxyError, Timeout, RequestException) as e:
                time.sleep(2) 
                # 最后一次尝试且允许降级，则切直连
                if attempt == max_retries - 1 and ALLOW_DIRECT_FALLBACK:
                     self._create_session(use_proxy=False)
                     try:
                         r = self.session.get(url, params=params, headers=headers)
                         r.raise_for_status()
                         return r.json()
                     except:
                         pass
        return None

    def fetch_all_etfs(self) -> Optional[pd.DataFrame]:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        all_data = []
        page = 1
        consecutive_errors = 0
        
        logger.info("📡 正在更新全市场 ETF 数据...")
        
        while page <= 200 and consecutive_errors < 3:
            # 使用全口径参数
            fs_param = "b:MK0021,b:MK0022,b:MK0023,b:MK0024,m:1 t:2,m:1 t:23,m:0 t:6,m:0 t:80"
            
            params = {
                "pn": str(page),
                "pz": "100",
                "po": "1",
                "np": "1",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": "2",
                "invt": "2",
                "fid": "f3",
                "fs": fs_param,
                "fields": "f12,f14,f2,f3,f4,f5,f6,f7,f8,f15,f16,f17,f18",
                "_": str(int(time.time() * 1000))
            }
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "http://quote.eastmoney.com/",
            }
            
            data = self._safe_request(url, params, headers, max_retries=3)
            
            if not data or data.get('rc') != 0 or 'data' not in data or 'diff' not in data['data']:
                consecutive_errors += 1
                if consecutive_errors >= 3: break
                continue
            
            consecutive_errors = 0
            items = data['data']['diff']
            if not items: break
            all_data.extend(items)
            
            if len(items) < 100: break
            page += 1
            time.sleep(0.5) 
        
        self._close_session()
        
        if not all_data: return None
        
        df = pd.DataFrame(all_data)
        rename_map = {
            'f12': 'code', 'f14': 'name', 'f2': 'close', 'f3': 'pct_change',
            'f4': 'change', 'f5': 'volume', 'f6': 'amount', 'f7': 'amplitude',
            'f8': 'turnover_rate', 'f17': 'open', 'f15': 'high', 'f16': 'low',
        }
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
        
        if 'code' in df.columns:
            df['code'] = df['code'].astype(str).str.strip().str.lower().str.replace(r'^(sh|sz)', '', regex=True)
            df = df.drop_duplicates(subset=['code'], keep='first')
            return df.set_index('code')
        else:
            return None

    def init_spot_data(self) -> bool:
        today = get_beijing_time().strftime("%Y-%m-%d")
        if self.spot_data_cache is not None and self.spot_data_date == today: return True
        df = self.fetch_all_etfs()
        if df is not None and not df.empty:
            self.spot_data_cache = df
            self.spot_data_date = today
            return True
        return False

    def update_single(self, fund_code: str) -> bool:
        if self.spot_data_cache is None:
            if not self.init_spot_data(): return False
        
        code = str(fund_code).strip().lower().replace('sh', '').replace('sz', '')
        
        if code not in self.spot_data_cache.index:
            logger.warning(f"⚠️ 未找到 {fund_code}")
            return False
        
        try:
            row = self.spot_data_cache.loc[code]
            today = pd.Timestamp(get_beijing_time().date())
            
            def to_float(x):
                try: return float(x) if x and x != '-' else 0.0
                except: return 0.0
            
            new_data = {
                'date': today,
                'open': to_float(row.get('open')),
                'high': to_float(row.get('high')),
                'low': to_float(row.get('low')),
                'close': to_float(row.get('close')),
                'volume': to_float(row.get('volume')),
                'amount': to_float(row.get('amount')),
                'amplitude': to_float(row.get('amplitude')),
                'pct_change': to_float(row.get('pct_change')),
                'change': to_float(row.get('change')),
                'turnover_rate': to_float(row.get('turnover_rate')),
                'fetch_time': get_beijing_time().strftime("%Y-%m-%d %H:%M:%S"),
                'source': 'eastmoney_spot'
            }
            
            df_new = pd.DataFrame([new_data])
            df_new.set_index('date', inplace=True)
            
            path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
            if os.path.exists(path):
                try:
                    df_old = pd.read_csv(path, index_col='date', parse_dates=['date'])
                    if today in df_old.index:
                        df_old.update(df_new)
                        df_final = df_old
                    else:
                        df_final = pd.concat([df_old, df_new])
                    df_final = df_final[~df_final.index.duplicated(keep='last')].sort_index()
                except:
                    df_final = df_new
            else:
                df_final = df_new
            
            df_final = df_final.reindex(columns=self.UNIFIED_COLUMNS)
            df_final.to_csv(path)
            return True
            
        except Exception as e:
            logger.error(f"❌ {fund_code} 处理失败: {e}")
            return False

    def get_fund_history(self, fund_code: str) -> pd.DataFrame:
        """
        读取本地缓存的基金历史数据
        供 main.py 的 IC 分析使用
        """
        code = str(fund_code).strip().lower().replace('sh', '').replace('sz', '')
        path = os.path.join(self.DATA_DIR, f"{code}.csv")
        
        if not os.path.exists(path):
            logger.warning(f"⚠️ 本地无数据，尝试抓取 {fund_code}...")
            if not self.update_single(fund_code):
                return pd.DataFrame()
        
        try:
            df = pd.read_csv(path)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            df.sort_index(inplace=True)
            
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        except Exception as e:
            logger.error(f"❌ 读取历史数据失败 {fund_code}: {e}")
            return pd.DataFrame()

    def run(self, funds: List[dict]):
        self.total_funds = len(funds)
        self.success_count = 0
        
        logger.info("🔍 正在初始化...")
        test = self._safe_request("https://push2.eastmoney.com/api/qt/clist/get", 
                                  {"pn":"1","pz":"1","fs":"m:1 t:2"}, {}, max_retries=2)
        if not test:
            logger.error("❌ 无法连接网络")
            return 0

        if not self.init_spot_data(): return 0
        
        for i, fund in enumerate(funds, 1):
            code = str(fund.get('code', '')).strip()
            if not code: continue
            if self.update_single(code):
                self.success_count += 1
            if i % 50 == 0:
                 logger.info(f"📊 进度: {i}/{self.total_funds}, 成功: {self.success_count}")
        return self.success_count

# ===================== 主入口 =====================
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 DataFetcher V23.6 (Env Vars Support)")
    print("=" * 60)
    
    funds = []
    if os.path.exists('config.yaml'):
        with open('config.yaml', 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
            funds = cfg.get('funds', [])
    else:
        logger.warning("⚠️ 使用测试数据 (config.yaml 未找到)")
        funds = [{'code': '510300', 'name': '沪深300ETF'}, {'code': '510050', 'name': '上证50ETF'}]
    
    if not funds:
        print("❌ 基金列表为空")
        sys.exit(1)
    
    fetcher = DataFetcher()
    success = fetcher.run(funds)
    
    print(f"\n{'=' * 60}")
    print(f"🏁 完成: {success}/{len(funds)}")
    print(f"{'=' * 60}")
    
    sys.exit(0 if success > 0 else 1)
