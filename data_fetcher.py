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
    """代理配置"""
    host: str
    port: int
    user: str
    password: str
    location: str = ""
    
    def __str__(self):
        return f"http://{self.user}:{self.password}@{self.host}:{self.port}"
    
    def to_dict(self):
        url = str(self)
        return {"http": url, "https": url}

# [代理池] 多个代理配置，按优先级排序
PROXY_POOL = [
    Proxy("31.59.20.176", 6754, "typembrv", "kx2q7wpv1dd4", "🇬🇧伦敦"),
    Proxy("23.95.150.145", 6114, "typembrv", "kx2q7wpv1dd4", "🇬🇧伦敦"),
    Proxy("198.23.239.134", 6540, "typembrv", "kx2q7wpv1dd4", "🇺🇸水牛"),
    Proxy("45.38.107.97", 6014, "typembrv", "kx2q7wpv1dd4", "🇺🇸水牛"),
    Proxy("107.172.163.27", 6543, "typembrv", "kx2q7wpv1dd4", "🇬🇧伦敦"),
    Proxy("198.105.121.200", 6462, "typembrv", "kx2q7wpv1dd4", "🇺🇸布卢明戴尔"),
    Proxy("64.137.96.74", 6641, "typembrv", "kx2q7wpv1dd4", "🇬🇧伦敦金融城"),
    Proxy("216.10.27.159", 6837, "typembrv", "kx2q7wpv1dd4", "🇪🇸马德里"),
    Proxy("23.26.71.145", 5628, "typembrv", "kx2q7wpv1dd4", "🇺🇸达拉斯"),
    Proxy("23.229.19.94", 8689, "typembrv", "kx2q7wpv1dd4", "🇺🇸奥勒姆"),
]

class ProxyManager:
    """代理管理器：自动轮询和故障转移"""
    
    def __init__(self, proxies: List[Proxy]):
        self.proxies = proxies
        self.current_index = 0
        self.failed_proxies: set = set()  # 记录失败的代理
        self.last_used: Dict[str, float] = {}  # 记录上次使用时间
        
    def get_next_proxy(self) -> Optional[Proxy]:
        """获取下一个可用代理"""
        attempts = 0
        while attempts < len(self.proxies):
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            
            # 跳过已失败的代理
            if proxy.host in self.failed_proxies:
                attempts += 1
                continue
            
            # 检查冷却时间（同一代理至少间隔 2 秒）
            last_time = self.last_used.get(proxy.host, 0)
            if time.time() - last_time < 2:
                time.sleep(2)
            
            self.last_used[proxy.host] = time.time()
            return proxy
            
            attempts += 1
        
        # 所有代理都失败了，重置并再试一次
        if self.failed_proxies:
            logger.warning("🔄 所有代理都失败过，重置失败列表重试...")
            self.failed_proxies.clear()
            return self.get_next_proxy()
        
        return None
    
    def mark_failed(self, proxy: Proxy):
        """标记代理为失败"""
        logger.warning(f"❌ 代理 {proxy.location} {proxy.host} 标记为失败")
        self.failed_proxies.add(proxy.host)
        self._close_session()
    
    def _close_session(self):
        """清理 session"""
        gc.collect()

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
        
        # 统计
        self.total_funds = 0
        self.success_count = 0

    def _create_session(self, proxy: Proxy):
        """使用指定代理创建 session"""
        if self.session:
            try:
                self.session.close()
            except:
                pass
        
        try:
            self.current_proxy = proxy
            self.session = cffi_requests.Session(
                impersonate="chrome120",
                proxies=proxy.to_dict(),
                timeout=30
            )
            logger.info(f"🌐 使用代理: {proxy.location} {proxy.host}:{proxy.port}")
            time.sleep(random.uniform(0.5, 2))
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

    def _safe_request(self, url: str, params: dict, headers: dict, max_proxy_retries: int = 3) -> Optional[dict]:
        """
        带代理切换的重试机制
        每个代理最多试 2 次，总共最多换 max_proxy_retries 个代理
        """
        proxy_attempts = 0
        
        while proxy_attempts < max_proxy_retries:
            # 获取新代理
            proxy = self.proxy_manager.get_next_proxy()
            if not proxy:
                logger.error("❌ 没有可用代理")
                return None
            
            # 用此代理创建 session
            self._create_session(proxy)
            
            # 此代理重试 2 次
            for attempt in range(2):
                try:
                    r = self.session.get(url, params=params, headers=headers, timeout=25)
                    r.raise_for_status()
                    return r.json()
                    
                except ProxyError as e:
                    logger.warning(f"⚠️ 代理错误: {str(e)[:80]}")
                    break  # 换代理
                    
                except Exception as e:
                    err_msg = str(e)[:100]
                    logger.warning(f"⚠️ 请求失败 ({attempt+1}/2): {err_msg}")
                    
                    if attempt == 0:
                        time.sleep(random.uniform(2, 5))
                    else:
                        # 两次都失败，标记代理
                        self.proxy_manager.mark_failed(proxy)
                        break
            
            proxy_attempts += 1
            self._close_session()
            time.sleep(random.uniform(3, 6))
        
        logger.error(f"❌ 已尝试 {max_proxy_retries} 个代理，全部失败")
        return None

    def fetch_all_etfs(self) -> Optional[pd.DataFrame]:
        """获取全市场 ETF 数据"""
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        
        all_data = []
        page = 1
        consecutive_errors = 0  # 连续错误计数
        
        while page <= 200 and consecutive_errors < 3:
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
            
            data = self._safe_request(url, params, headers, max_proxy_retries=5)
            
            if not data or data.get('rc') != 0 or 'data' not in data or 'diff' not in data['data']:
                consecutive_errors += 1
                logger.warning(f"⚠️ 第 {page} 页数据异常 (连续错误 {consecutive_errors}/3)")
                if consecutive_errors >= 3:
                    logger.error("❌ 连续 3 页错误，终止获取")
                    break
                continue
            
            # 成功，重置错误计数
            consecutive_errors = 0
            items = data['data']['diff']
            
            if not items:
                break
                
            all_data.extend(items)
            logger.info(f"   ✅ 本页 {len(items)} 条，累计 {len(all_data)} 条")
            
            if len(items) < 100:
                break
            
            page += 1
            time.sleep(random.uniform(1, 3))  # 页间延迟
        
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
    print("=" * 60)
    print("🚀 DataFetcher V22.0 - 东财 ETF 数据获取")
    print(f"🌐 代理池: {len(PROXY_POOL)} 个节点")
    for i, p in enumerate(PROXY_POOL[:3], 1):
        print(f"   {i}. {p.location} {p.host}:{p.port}")
    print(f"   ... 等共 {len(PROXY_POOL)} 个")
    print("=" * 60)
    
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
    
    print(f"\n{'=' * 60}")
    print(f"🏁 完成: {success}/{len(funds)} ({success/len(funds)*100:.1f}%)")
    print(f"{'=' * 60}")
    
    sys.exit(0 if success > 0 else 1)
