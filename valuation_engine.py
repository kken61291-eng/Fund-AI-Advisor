import pandas as pd
import numpy as np
from utils import logger

class ValuationEngine:
    def __init__(self):
        pass

    def get_valuation_status(self, fund_code, current_data):
        """
        [零网络版] 直接利用已获取的 ETF 历史数据计算估值分位
        
        Args:
            fund_code: ETF 代码 (仅用于日志)
            current_data: 包含历史数据的 DataFrame (由 DataFetcher 提供)
            
        Returns: 
            (multiplier, description)
        """
        try:
            # 1. 数据校验
            if current_data is None or current_data.empty:
                return 1.0, "数据缺失"
            
            # 使用 'close' 列，如果没有则尝试兼容
            if 'close' in current_data.columns:
                history_series = current_data['close']
            elif '收盘' in current_data.columns:
                history_series = current_data['收盘']
            else:
                return 1.0, "数据列错误"

            # 2. 确保数据长度足够 (至少半年，理想3-5年)
            if len(history_series) < 120:
                return 1.0, "新股/数据不足"

            # 3. 计算分位点 (Percentile)
            # 取过去 5 年 (约 1250 个交易日) 或 所有可用数据
            window_len = min(1250, len(history_series))
            window_data = history_series.tail(window_len)
            
            current_price = window_data.iloc[-1]
            low_val = window_data.min()
            high_val = window_data.max()
            
            # 极值保护
            if high_val <= low_val:
                percentile = 0.5
            else:
                percentile = (current_price - low_val) / (high_val - low_val)
            
            p_str = f"{int(percentile*100)}%"
            
            # 4. 通用估值策略矩阵
            # 逻辑：分位越低，越倾向于低估（买入系数 > 1）；分位越高，越倾向于高估（买入系数 < 1）
            if percentile < 0.10: 
                return 1.6, f"极度低估(分位{p_str})"
            elif percentile < 0.25: 
                return 1.3, f"低估区间(分位{p_str})"
            elif percentile < 0.40: 
                return 1.1, f"相对偏低(分位{p_str})"
            elif percentile > 0.85: 
                return 0.5, f"高估限流(分位{p_str})"
            elif percentile > 0.95: 
                return 0.0, f"极度泡沫(分位{p_str})"
            else:
                return 1.0, f"估值适中(分位{p_str})"

        except Exception as e:
            logger.error(f"估值计算异常 {fund_code}: {e}")
            return 1.0, "计算错误"
