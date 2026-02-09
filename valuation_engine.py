import akshare as ak
import pandas as pd
import os
import time
import random
from datetime import datetime
from utils import logger, retry

class ValuationEngine:
    def __init__(self):
        # [V15.12] 同步反爬策略，防止估值数据获取失败
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
        ]
        
        # 核心指数代码映射 (东财代码)
        self.INDEX_MAP = {
            "沪深300": "sh000300",
            "中证500": "sh000905",
            "创业板指": "sz399006",
            "科创50": "sh000688",
            "恒生科技": "hkHSTECH", # 注意：港股数据可能需要特殊处理
            "中证红利": "sz399922",
            "中证煤炭": "sz399998",
            "全指证券公司": "sz399975",
            "中华半导体": "sz399989",
            "纳斯达克100": "us.NDX", # 美股
        }

    def _get_headers(self):
        return {"User-Agent": random.choice(self.user_agents)}

    @retry(retries=2, delay=3)
    def _fetch_index_history(self, index_code):
        """
        获取指数历史数据用于计算分位
        """
        try:
            # 增加随机延时，保护 IP
            time.sleep(random.uniform(1.5, 3.0))
            
            # 处理不同市场的代码格式
            if index_code.startswith("us."):
                # 美股暂不处理估值，返回 None
                return None
            elif index_code.startswith("hk"):
                # 港股暂略
                return None
            
            # A股指数
            df = ak.stock_zh_index_daily_em(symbol=index_code)
            return df['close']
            
        except Exception as e:
            logger.warning(f"⚠️ 估值数据获取受阻 {index_code}: {e}")
            return None

    def get_valuation_status(self, index_name, strategy_type):
        """
        计算估值分位并返回调节系数
        返回: (multiplier, description)
        """
        # 1. 对于商品、美股或未配置指数的，默认适中
        if not index_name or index_name not in self.INDEX_MAP:
            return 1.0, "非A股宽基/无数据(默认适中)"

        try:
            index_code = self.INDEX_MAP[index_name]
            history_series = self._fetch_index_history(index_code)
            
            # 如果获取失败，降级处理
            if history_series is None or len(history_series) < 250:
                return 1.0, "数据源受限(降级适中)"

            # 2. 计算分位点 (Percentile)
            # 取过去 5 年 (约 1250 个交易日) 数据
            window_data = history_series.tail(1250)
            current_price = window_data.iloc[-1]
            
            # 计算当前价格在历史区间的百分位
            # (注：严谨的估值应该用PE/PB，但免费源PE数据极难获取，此处用"价格分位"近似替代"估值分位")
            # 逻辑：指数长期向上，价格分位虽有偏差，但能反映相对位置
            low_val = window_data.min()
            high_val = window_data.max()
            
            if high_val == low_val:
                percentile = 0.5
            else:
                percentile = (current_price - low_val) / (high_val - low_val)
            
            p_str = f"{int(percentile*100)}%"
            
            # 3. 根据策略类型返回系数
            if strategy_type == 'core': # 核心资产：低吸高抛
                if percentile < 0.20: return 1.5, f"极度低估(分位{p_str})"
                if percentile < 0.40: return 1.2, f"低估(分位{p_str})"
                if percentile > 0.80: return 0.5, f"高估(分位{p_str})"
                if percentile > 0.90: return 0.0, f"极度高估(分位{p_str})"
                return 1.0, f"估值适中(分位{p_str})"
                
            elif strategy_type == 'satellite': # 卫星策略：右侧为主，不轻易左侧
                if percentile > 0.85: return 0.0, f"泡沫预警(分位{p_str})"
                return 1.0, f"估值允许(分位{p_str})"
                
            elif strategy_type == 'dividend': # 红利策略：极度厌恶高估
                if percentile > 0.70: return 0.0, f"红利高估(分位{p_str})"
                if percentile < 0.30: return 1.5, f"红利黄金坑(分位{p_str})"
                return 1.0, f"估值适中(分位{p_str})"

            return 1.0, "策略未定义"

        except Exception as e:
            logger.error(f"估值计算异常: {e}")
            return 1.0, "计算错误(默认适中)"
