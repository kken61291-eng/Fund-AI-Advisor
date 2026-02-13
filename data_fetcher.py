import pandas as pd
import time
import random
import os
import yaml
import logging
import gc
import sys
from datetime import datetime, time as dt_time
from typing import Optional, List, Dict

# 检查依赖
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("❌ 请先安装依赖: pip install curl_cffi pandas pyyaml")
    sys.exit(1)

# ===================== 配置 =====================
def get_beijing_time():
    from datetime import timezone, timedelta
    return datetime.now(timezone(timedelta(hours=8)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_proxy() -> Optional[Dict[str, str]]:
    """
    获取代理配置 - 硬编码版本
    """
    # [硬编码代理] 格式: http://user:pass@host:port
    proxy = "http://typembrv:kx2q7wpv1dd4@31.59.20.176:6754"
    
    logger.info(f"🌐 使用代理: http://***@31.59.20.176:6754")
    return {"http": proxy, "https": proxy}

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
        self.proxy = get_proxy()
        
        # 统计
        self.total_funds = 0
        self.success_count = 0

    def _create_session(self):
        """创建新 session"""
        if self.session:
            try:
                self.session.close()
            except:
                pass
        
        try:
            self.session = cffi_requests.Session(
                impersonate="chrome120",
                proxies=self.proxy,
                timeout=30
            )
            # 随机延迟，模拟人类
            time.sleep(random.uniform(1, 3))
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
        """带重试的安全请求"""
        for attempt in range(max_retries):
            try:
                if not self.session:
                    self._create_session()
                
                r = self.session.get(url, params=params, headers=headers)
                r.raise_for_status()
                return r.json()
                
            except Exception as e:
                err_msg = str(e)[:100]
                logger.warning(f"⚠️ 请求失败 ({attempt+1}/{max_retries}): {err_msg}")
                self._close_session()
                
                if attempt < max_retries - 1:
                    wait = random.uniform(3, 8) * (attempt + 1)
                    logger.info(f"⏳ 等待 {wait:.1f}s 后重试...")
                    time.sleep(wait)
                else:
                    logger.error(f"❌ 请求最终失败")
                    return None
        
        return None

    def fetch_all_etfs(self) -> Optional[pd.DataFrame]:
        """获取全市场 ETF 数据"""
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        
        all_data = []
        page = 1
        
        while page <= 200:  # 安全上限
            logger.info(f"📄 获取第 {page} 页...")
            
            params = {
                "pn": str(page),
                "pz": "100",
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
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "http://quote.eastmoney.com/",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            }
            
            data = self._safe_request(url, params, headers)
            
            if not data or data.get('rc') != 0 or 'data' not in data or 'diff' not in data['data']:
                if page == 1:
                    logger.error("❌ 第一页获取失败，检查代理配置")
                    return None
                break
            
            items = data['data']['diff']
            if not items:
                break
                
            all_data.extend(items)
            logger.info(f"   ✅ 本页 {len(items)} 条，累计 {len(all_data)} 条")
            
            if len(items) < 100:
                break
            
            page += 1
            time.sleep(random.uniform(2, 5))
        
        self._close_session()
        
        if not all_data:
            return None
        
        # 处理数据
        df = pd.DataFrame(all_data)
        
        rename_map = {
            'f12': 'code', 'f14': 'name', 'f2': 'close', 'f3': 'pct_change',
            'f4': 'change', 'f5': 'volume', 'f6': 'amount', 'f7': 'amplitude',
            'f8': 'turnover_rate', 'f17': 'open', 'f15': 'high', 'f16': 'low',
        }
        
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
        
        # 清理代码格式
        df['code'] = df['code'].astype(str).str.strip().str.lower().str.replace(r'^(sh|sz)', '', regex=True)
        df = df.drop_duplicates(subset=['code'], keep='first')
        
        logger.info(f"✅ 共获取 {len(df)} 只 ETF")
        return df.set_index('code')

    def init_spot_data(self) -> bool:
        """初始化数据缓存"""
        today = get_beijing_time().strftime("%Y-%m-%d")
        
        if self.spot_data_cache is not None and self.spot_data_date == today:
            logger.info("✅ 使用缓存数据")
            return True
        
        df = self.fetch_all_etfs()
        if df is not None and not df.empty:
            self.spot_data_cache = df
            self.spot_data_date = today
            return True
        
        return False

    def update_single(self, fund_code: str) -> bool:
        """更新单个基金"""
        if self.spot_data_cache is None:
            if not self.init_spot_data():
                return False
        
        code = str(fund_code).strip().lower().replace('sh', '').replace('sz', '')
        
        if code not in self.spot_data_cache.index:
            logger.warning(f"⚠️ 未找到 {fund_code}")
            return False
        
        try:
            row = self.spot_data_cache.loc[code]
            today = pd.Timestamp(get_beijing_time().date())
            
            def to_float(x):
                try:
                    return float(x) if x and x != '-' else 0.0
                except:
                    return 0.0
            
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
            
            # 合并历史数据
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
            
            # 标准化并保存
            df_final = df_final.reindex(columns=self.UNIFIED_COLUMNS)
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 
                       'amplitude', 'pct_change', 'change', 'turnover_rate']:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
            
            df_final.to_csv(path)
            logger.info(f"💾 {fund_code} 成功 (收盘: {new_data['close']:.3f})")
            return True
            
        except Exception as e:
            logger.error(f"❌ {fund_code} 处理失败: {e}")
            return False

    def run(self, funds: List[dict]):
        """批量运行"""
        self.total_funds = len(funds)
        self.success_count = 0
        
        if not self.init_spot_data():
            logger.error("❌ 初始化失败，退出")
            return 0
        
        for i, fund in enumerate(funds, 1):
            code = str(fund.get('code', '')).strip()
            name = fund.get('name', 'Unknown')
            
            if not code or len(code) < 6:
                continue
            
            logger.info(f"🔄 [{i}/{self.total_funds}] {name} ({code})")
            
            if self.update_single(code):
                self.success_count += 1
            
            if i % 10 == 0 or i == self.total_funds:
                logger.info(f"📊 进度: {i}/{self.total_funds}, 成功: {self.success_count}")
        
        return self.success_count

# ===================== 主入口 =====================
if __name__ == "__main__":
    print("=" * 50)
    print("🚀 DataFetcher V21.1 - 东财 ETF 数据获取")
    print("🌐 代理: 31.59.20.176:6754")
    print("=" * 50)
    
    # 加载配置
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
            funds = cfg.get('funds', [])
    except Exception as e:
        logger.error(f"读取 config.yaml 失败: {e}")
        funds = []
    
    if not funds:
        print("❌ 未找到基金列表，请检查 config.yaml")
        sys.exit(1)
    
    # 运行
    fetcher = DataFetcher()
    success = fetcher.run(funds)
    
    print(f"\n{'=' * 50}")
    print(f"🏁 完成: {success}/{len(funds)} ({success/len(funds)*100:.1f}%)")
    print(f"{'=' * 50}")
    
    sys.exit(0 if success > 0 else 1)
