import pandas as pd
import numpy as np
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
    print("❌ 请先安装依赖: pip install curl_cffi pandas pyyaml numpy")
    sys.exit(1)

# ===================== 配置加载 =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY", "")
if not SCRAPERAPI_KEY:
    try:
        import settings
        SCRAPERAPI_KEY = getattr(settings, 'SCRAPERAPI_KEY', "")
    except ImportError: pass

ALLOW_DIRECT_FALLBACK = True 

if not SCRAPERAPI_KEY:
    logger.warning("⚠️ 未检测到 SCRAPERAPI_KEY，将仅使用直连模式")
else:
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
        self.target_codes = [] 
        
    def _get_scraperapi_proxy(self) -> str:
        return f"http://scraperapi:{SCRAPERAPI_KEY}@proxy-server.scraperapi.com:8001"

    def _create_session(self, use_proxy: bool = True):
        self._close_session()
        try:
            if use_proxy and SCRAPERAPI_KEY:
                proxy_url = self._get_scraperapi_proxy()
                self.session = cffi_requests.Session(
                    impersonate="chrome120",
                    proxies={"http": proxy_url, "https": proxy_url},
                    timeout=15,
                    verify=False 
                )
            else:
                self.session = cffi_requests.Session(
                    impersonate="chrome120",
                    timeout=15 
                )
        except Exception as e:
            logger.error(f"❌ 创建 session 失败: {e}")
            raise

    def _close_session(self):
        if self.session:
            try: self.session.close()
            except: pass
            self.session = None
            gc.collect()

    def _safe_request(self, url: str, params: dict, headers: dict, max_retries: int = 2) -> Optional[dict]:
        use_proxy_first = True if SCRAPERAPI_KEY else False
        if self.session is None:
            self._create_session(use_proxy=use_proxy_first)

        for attempt in range(max_retries + 1):
            try:
                if not self.session: raise Exception("Session Lost")
                r = self.session.get(url, params=params, headers=headers)
                
                if r.status_code == 403:
                    logger.warning("⚠️ 403 Forbidden (代理额度耗尽)")
                    if ALLOW_DIRECT_FALLBACK:
                         self._create_session(use_proxy=False)
                         r = self.session.get(url, params=params, headers=headers)
                         r.raise_for_status()
                         return r.json()
                    return None
                        
                r.raise_for_status()
                return r.json()
            except (ProxyError, Timeout, RequestException, Exception) as e:
                if ALLOW_DIRECT_FALLBACK:
                     self._create_session(use_proxy=False)
                else:
                     time.sleep(1)
        
        logger.error("❌ 所有尝试均失败")
        return None

    def fetch_specific_etfs(self, codes: List[str]) -> Optional[pd.DataFrame]:
        url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
        
        if not codes: return None

        secids_list = []
        for code in codes:
            c = str(code).strip()
            if c.startswith('5') or c.startswith('6'):
                secids_list.append(f"1.{c}")
            else:
                secids_list.append(f"0.{c}")
        
        secids_str = ",".join(secids_list)
        
        logger.info(f"📡 正在精准抓取 {len(codes)} 只基金数据...")
        
        params = {
            "fltt": "2",
            "invt": "2",  # 🟢 修复 1：核心参数！保证返回实时价格而非 "-"
            "secids": secids_str,
            "fields": "f12,f14,f2,f3,f4,f5,f6,f7,f8,f15,f16,f17,f18",
            "_": str(int(time.time() * 1000))
        }
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        
        data = self._safe_request(url, params, headers, max_retries=3)
        
        if not data or 'diff' not in data.get('data', {}):
            logger.error("❌ 获取失败: 数据为空")
            return None
        
        items = data['data']['diff']
        logger.info(f"✅ 成功获取 {len(items)} 条数据")
        
        df = pd.DataFrame(items)
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
        
        if self.target_codes:
            df = self.fetch_specific_etfs(self.target_codes)
        else:
            return False

        if df is not None and not df.empty:
            self.spot_data_cache = df
            self.spot_data_date = today
            return True
        return False

    def update_single(self, fund_code: str) -> bool:
        if self.spot_data_cache is None:
            if not self.target_codes:
                self.target_codes = [fund_code]
            if not self.init_spot_data(): return False
        
        code = str(fund_code).strip().lower().replace('sh', '').replace('sz', '')
        
        if code not in self.spot_data_cache.index:
            df_single = self.fetch_specific_etfs([code])
            if df_single is not None and not df_single.empty:
                 self.spot_data_cache = pd.concat([self.spot_data_cache, df_single])
            else:
                 logger.error(f"❌ 无法找到 {fund_code}")
                 return False
        
        try:
            row = self.spot_data_cache.loc[code]
            today = pd.Timestamp(get_beijing_time().date())
            
            # 🟢 修复 2：禁止将空值或 "-" 转化为 0.0
            def to_float(x):
                try:
                    if x is None or str(x).strip() == '-' or str(x).strip() == '':
                        return np.nan
                    return float(x)
                except:
                    return np.nan
            
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
            
            # 🟢 修复 3：价格为 NaN 或 0 时，绝不入库，防止污染历史
            if pd.isna(new_data['close']) or new_data['close'] <= 0.001:
                logger.warning(f"⚠️ {fund_code} 价格数据无效(可能停牌或非交易时间)，跳过保存")
                return False

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
        code = str(fund_code).strip().lower().replace('sh', '').replace('sz', '')
        path = os.path.join(self.DATA_DIR, f"{code}.csv")
        
        if not os.path.exists(path):
            self.target_codes.append(fund_code)
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
            
            # 🟢 修复 4：自动排毒，删掉之前保存进去的 0.0 错误行
            df = df[df['close'] > 0.001]
            
            return df
        except Exception as e:
            logger.error(f"❌ 读取历史数据失败 {fund_code}: {e}")
            return pd.DataFrame()

    def get_market_net_flow(self) -> float:
        try:
            url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
            params = {
                "fltt": "2", "secids": "1.000001,0.399001", "fields": "f62",
                "_": str(int(time.time() * 1000))
            }
            headers = {"User-Agent": "Mozilla/5.0"}
            data = self._safe_request(url, params, headers)
            if not data or 'diff' not in data.get('data', {}): return 0.0
            
            total_flow = 0.0
            for item in data['data']['diff']:
                total_flow += float(item.get('f62', 0))
            return round(total_flow / 100000000, 2)
        except Exception as e:
            logger.error(f"❌ 获取宏观资金流失败: {e}")
            return 0.0

    def run(self, funds: List[dict]):
        self.total_funds = len(funds)
        self.success_count = 0
        
        self.target_codes = [str(f.get('code')).strip() for f in funds if f.get('code')]
        
        logger.info("🔍 正在初始化...")
        flow = self.get_market_net_flow()
        logger.info(f"💰 [Macro] 全市场主力净流入: {flow} 亿")

        if not self.init_spot_data(): 
            logger.error("❌ 初始化抓取失败")
            return 0
        
        for i, fund in enumerate(funds, 1):
            code = str(fund.get('code', '')).strip()
            if not code: continue
            if self.update_single(code):
                self.success_count += 1
            if i % 50 == 0:
                 logger.info(f"📊 进度: {i}/{self.total_funds}, 成功: {self.success_count}")
        return self.success_count

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 DataFetcher V24.4 (Anti-Poison Data Fix)")
    print("=" * 60)
    
    funds = []
    if os.path.exists('config.yaml'):
        try:
            with open('config.yaml', 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
                funds = cfg.get('funds', [])
        except Exception as e:
            logger.error(f"Config load error: {e}")
    
    if not funds:
        logger.warning("⚠️ config.yaml 未找到，使用默认测试数据")
        funds = [{'code': '510300', 'name': '沪深300ETF'}, {'code': '510050', 'name': '上证50ETF'}]
    
    print(f"📋 计划精准抓取 {len(funds)} 只基金...")
    
    fetcher = DataFetcher()
    success = fetcher.run(funds)
    
    print(f"\n{'=' * 60}")
    print(f"🏁 完成: {success}/{len(funds)}")
    print(f"{'=' * 60}")
    
    sys.exit(0 if success > 0 else 1)
