import akshare as ak
import pandas as pd
import os
import time
from datetime import datetime
from utils import logger, retry

try:
    import tushare as ts
except ImportError:
    ts = None

class ValuationEngine:
    def __init__(self):
        self.cn_10y_yield = None
        self.tushare_api = None
        
        token = os.getenv("TUSHARE_TOKEN")
        if token and ts:
            try:
                self.tushare_api = ts.pro_api(token)
            except: pass

        self.INDEX_MAP = {
            "沪深300":      {"em": "sh000300", "sina": "sh000300", "ts": "000300.SH"},
            "中证红利":      {"em": "sz399922", "sina": "sz399922", "ts": "399922.SZ"},
            "中证煤炭":      {"em": "sz399998", "sina": "sz399998", "ts": "399998.SZ"},
            "全指证券公司":  {"em": "sz399975", "sina": "sz399975", "ts": "399975.SZ"},
            "中华半导体":    {"em": "sz399989", "sina": "sz399989", "ts": "399989.SZ"},
            "全指半导体":    {"em": "sz399989", "sina": "sz399989", "ts": "399989.SZ"},
            "中证传媒":      {"em": "sz399971", "sina": "sz399971", "ts": "399971.SZ"}
        }

    @retry(retries=2, delay=2)
    def _get_bond_yield(self):
        if self.cn_10y_yield: return self.cn_10y_yield
        try:
            df = ak.bond_zh_us_rate()
            val = df['中国国债收益率10年'].iloc[-1]
            self.cn_10y_yield = val
            return val
        except: return 2.3

    @retry(retries=2, delay=1)
    def _fetch_history_data(self, index_name):
        codes = self.INDEX_MAP.get(index_name)
        if not codes: return None

        try:
            df = ak.stock_zh_index_daily_em(symbol=codes['em'])
            return df['close']
        except Exception: pass

        try:
            time.sleep(1)
            df = ak.stock_zh_index_daily(symbol=codes['sina'])
            return df['close']
        except Exception: pass

        if self.tushare_api:
            try:
                time.sleep(1)
                end = datetime.now().strftime("%Y%m%d")
                start = (datetime.now() - pd.Timedelta(days=2000)).strftime("%Y%m%d")
                df = self.tushare_api.index_daily(ts_code=codes['ts'], start_date=start, end_date=end)
                if not df.empty: return df.sort_values('trade_date')['close']
            except Exception: pass

        return None

    def get_valuation_status(self, index_name, strategy_type):
        if not index_name or strategy_type == 'commodity':
            return 1.0, "非权益类(默认适中)"

        try:
            history = self._fetch_history_data(index_name)
            if history is None or len(history) < 100: return 1.0, "数据源全线不可用"

            current = history.iloc[-1]
            percentile = (history.tail(1250) < current).mean()
            p_str = f"{int(percentile*100)}%"
            
            if strategy_type == 'core':
                if percentile < 0.20: return 1.5, f"极度低估(分位{p_str})"
                if percentile < 0.40: return 1.2, f"低估(分位{p_str})"
                if percentile > 0.80: return 0.5, f"高估(分位{p_str})"
                if percentile > 0.90: return 0.0, f"极度高估(分位{p_str})"
                return 1.0, "估值适中"
            elif strategy_type == 'satellite':
                if percentile < 0.10: return 1.2, "黄金坑(左侧)"
                if percentile > 0.85: return 0.0, "泡沫预警"
                return 1.0, "估值允许"
            elif strategy_type == 'dividend':
                if percentile < 0.10: return 2.0, f"历史大底(分位{p_str})"
                if percentile < 0.30: return 1.5, f"低估区域(分位{p_str})"
                if percentile > 0.80: return 0.0, f"性价比低(分位{p_str})"
                return 1.0, "红利适中"
            return 1.0, "逻辑未匹配"
        except Exception as e:
            logger.warning(f"估值错误 {index_name}: {e}")
            return 1.0, "估值未知"
