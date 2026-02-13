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
from dataclasses import dataclass

# 检查依赖
try:
    from curl_cffi import requests as cffi_requests
    from curl_cffi.requests.exceptions import RequestException, ProxyError
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

@dataclass
class Proxy:
    """Oxylabs 代理配置"""
    username: str      # user-abcfs_BETxs-country-US
    password: str      # xN+zq6oo+gn+
    country: str       # US, GB, DE 等
    host: str = "dc.oxylabs.io"
    port: int = 8000
    
    def get_proxy_url(self):
        # Oxylabs 格式: http://user-xxx-country-CC:pass@dc.oxylabs.io:8000
        return f"http://{self.username}:{self.password}@{self.host}:{self.port}"
    
    def to_dict(self):
        url = self.get_proxy_url()
        return {"http": url, "https": url}
    
    def __str__(self):
        return f"🇺🇸 {self.country} via Oxylabs"

# [Oxylabs 代理池] 不同国家代码
PROXY_POOL = [
    Proxy("user-abcfs_BETxs-country-US", "xN+zq6oo+gn+", "US"),
    Proxy("user-abcfs_BETxs-country-GB", "xN+zq6oo+gn+", "GB"),
    Proxy("user-abcfs_BETxs-country-DE", "xN+zq6oo+gn+", "DE"),
    Proxy("user-abcfs_BETxs-country-FR", "xN+zq6oo+gn+", "FR"),
    Proxy("user-abcfs_BETxs-country-JP", "xN+zq6oo+gn+", "JP"),
    Proxy("user-abcfs_BETxs-country-SG", "xN+zq6oo+gn+", "SG"),
    Proxy("user-abcfs_BETxs-country-NL", "xN+zq6oo+gn+", "NL"),
    Proxy("user-abcfs_BETxs-country-CA", "xN+zq6oo+gn+", "CA"),
    Proxy("user-abcfs_BETxs-country-AU", "xN+zq6oo+gn+", "AU"),
    Proxy("user-abcfs_BETxs-country-GB", "xN+zq6oo+gn+", "GB-London"),
]

class ProxyManager:
    """代理管理器"""
    
    def __init__(self, proxies: List[Proxy]):
        self.proxies = proxies
        self.current_index = 0
        self.failed_proxies: set = set()
        self.success_proxies: set = set()
        
    def get_next_proxy(self) -> Optional[Proxy]:
        """获取下一个可用代理"""
        attempts = 0
        while attempts < len(self.proxies):
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            
            if proxy.username in self.failed_proxies:
                attempts += 1
                continue
            
            return proxy
            attempts += 1
        
        # 全部失败过，重置
        if self.failed_proxies:
            logger.warning("🔄 重置失败代理列表...")
            self.failed_proxies.clear()
            return self.get_next_proxy()
        
        return None
    
    def mark_failed(self, proxy: Proxy):
        """标记代理为失败"""
        logger.warning(f"❌ 代理失败: {proxy.country}")
        self.failed_proxies.add(proxy.username)
    
    def mark_success(self, proxy: Proxy):
        """标记代理为成功"""
        if proxy.username not in self.success_proxies:
            self.success_proxies.add(proxy.username)
            logger.info(f"✅ 代理可用: {proxy.country}")

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
        self.proxy_manager = ProxyManager(PROXY_POOL)
        self.current_proxy: Optional[Proxy] = None
        
        self.total_funds = 0
        self.success_count = 0

    def _create_session(self, proxy: Proxy):
        """创建 session"""
        if self.session:
            try:
                self.session.close()
            except:
                pass
        
        try:
            self.current_proxy = proxy
            proxy_dict = proxy.to_dict()
            
            # 日志（隐藏密码）
            safe_url = f"http://{proxy.username}:***@{proxy.host}:{proxy.port}"
            logger.info(f"🌐 使用代理: {safe_url} ({proxy.country})")
            
            self.session = cffi_requests.Session(
                impersonate="chrome120",
                timeout=30
            )
            self.session.proxies = proxy_dict
            
            time.sleep(random.uniform(0.5, 1.5))
            
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

    def _test_proxy(self, proxy: Proxy) -> bool:
        """测试 Oxylabs 代理"""
        try:
            logger.info(f"🧪 测试代理: {proxy.country}")
            self._create_session(proxy)
            
            # 用 Oxylabs 的测试接口
            test_url = "https://ip.oxylabs.io/location"
            r = self.session.get(test_url, timeout=15)
            
            if r.status_code == 200:
                location = r.text.strip()
                logger.info(f"✅ 代理生效! 位置: {location}")
                return True
            else:
                logger.warning(f"⚠️ 测试状态码: {r.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 代理测试失败: {str(e)[:150]}")
            return False
        finally:
            self._close_session()

    def _safe_request(self, url: str, params: dict, headers: dict, max_proxy_retries: int = 5) -> Optional[dict]:
        """带代理切换的请求"""
        
        for proxy_attempt in range(max_proxy_retries):
            proxy = self.proxy_manager.get_next_proxy()
            if not proxy:
                logger.error("❌ 没有可用代理")
                return None
            
            # 测试代理
            if not self._test_proxy(proxy):
                self.proxy_manager.mark_failed(proxy)
                continue
            
            self.proxy_manager.mark_success(proxy)
            
            # 重试 2 次
            for attempt in range(2):
                try:
                    logger.debug(f"请求: {url[:60]}...")
                    r = self.session.get(url, params=params, headers=headers, timeout=25)
                    r.raise_for_status()
                    return r.json()
                    
                except ProxyError as e:
                    logger.error(f"❌ 代理错误: {e}")
                    break
                    
                except Exception as e:
                    err_msg = str(e)[:120]
                    logger.warning(f"⚠️ 请求失败 ({attempt+1}/2): {err_msg}")
                    
                    if attempt == 0:
                        time.sleep(random.uniform(2, 4))
                    else:
                        self.proxy_manager.mark_failed(proxy)
                        break
            
            self._close_session()
            time.sleep(random.uniform(3, 6))
        
        logger.error(f"❌ 已尝试 {max_proxy_retries} 个代理")
        return None

    def fetch_all_etfs(self) -> Optional[pd.DataFrame]:
        """获取 ETF 数据"""
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        
        all_data = []
        page = 1
        consecutive_errors = 0
        
        while page <= 200 and consecutive_errors < 3:
            logger.info(f"📄 第 {page} 页...")
            
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
            
            if not data or data.get('rc') != 0:
                consecutive_errors += 1
                logger.warning(f"⚠️ 数据异常 ({consecutive_errors}/3)")
                if consecutive_errors >= 3:
                    break
                continue
            
            consecutive_errors = 0
            items = data['data']['diff']
            
            if not items:
                break
                
            all_data.extend(items)
            logger.info(f"   ✅ {len(items)} 条，累计 {len(all_data)}")
            
            if len(items) < 100:
                break
            
            page += 1
            time.sleep(random.uniform(1, 3))
        
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
        df['code'] = df['code'].astype(str).str.strip().str.lower().str.replace(r'^(sh|sz)', '', regex=True)
        df = df.drop_duplicates(subset=['code'], keep='first')
        
        logger.info(f"✅ 共 {len(df)} 只 ETF")
        return df.set_index('code')

    def init_spot_data(self) -> bool:
        """初始化"""
        today = get_beijing_time().strftime("%Y-%m-%d")
        
        if self.spot_data_cache is not None and self.spot_data_date == today:
            return True
        
        df = self.fetch_all_etfs()
        if df is not None and not df.empty:
            self.spot_data_cache = df
            self.spot_data_date = today
            return True
        
        return False

    def update_single(self, fund_code: str) -> bool:
        """更新单个"""
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
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 
                       'amplitude', 'pct_change', 'change', 'turnover_rate']:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
            
            df_final.to_csv(path)
            logger.info(f"💾 {fund_code} 成功")
            return True
            
        except Exception as e:
            logger.error(f"❌ {fund_code} 失败: {e}")
            return False

    def run(self, funds: List[dict]):
        """批量运行"""
        self.total_funds = len(funds)
        self.success_count = 0
        
        if not self.init_spot_data():
            logger.error("❌ 初始化失败")
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
        
        # 统计
        logger.info(f"📈 代理统计: {len(self.proxy_manager.success_proxies)}/{len(PROXY_POOL)} 个可用")
        
        return self.success_count

# ===================== 主入口 =====================
if __name__ == "__main__":
    print("=" * 60)
    print("🚀 DataFetcher V24.0 - Oxylabs 代理版")
    print(f"🌐 代理池: {len(PROXY_POOL)} 个国家节点")
    print("   dc.oxylabs.io:8000")
    print("=" * 60)
    
    # 测试模式
    test_mode = os.environ.get('TEST_PROXY', 'false').lower() == 'true'
    
    if test_mode:
        print("\n🧪 代理测试模式")
        pm = ProxyManager(PROXY_POOL)
        for i, p in enumerate(PROXY_POOL):
            fetcher = DataFetcher()
            ok = fetcher._test_proxy(p)
            pm.mark_success(p) if ok else pm.mark_failed(p)
            print(f"   {i+1}. {p.country} - {'✅' if ok else '❌'}")
        sys.exit(0)
    
    # 正常模式
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
            funds = cfg.get('funds', [])
    except Exception as e:
        logger.error(f"读取配置失败: {e}")
        funds = []
    
    if not funds:
        print("❌ 未找到基金列表")
        sys.exit(1)
    
    fetcher = DataFetcher()
    success = fetcher.run(funds)
    
    print(f"\n{'=' * 60}")
    print(f"🏁 完成: {success}/{len(funds)}")
    print(f"{'=' * 60}")
    
    sys.exit(0 if success > 0 else 1)
