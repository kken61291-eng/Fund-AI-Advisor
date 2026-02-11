import akshare as ak
import pandas as pd
import numpy as np
import os
import time
import random
from datetime import datetime
from utils import logger, retry

class ValuationEngine:
    def __init__(self):
        # [V15.20] 增强型反爬 User-Agent 池
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
        ]

    def _get_headers(self):
        return {"User-Agent": random.choice(self.user_agents)}

    @retry(retries=2, delay=2)
    def _fetch_data_hybrid(self, index_code, fund_code):
        """
        [混合获取策略] 
        1. 优先尝试获取 A 股指数数据
        2. 如果是跨境/商品/债券，或指数获取失败，则回退获取 ETF 自身历史复权价格
        """
        # -----------------------------------------------
        # 1. 尝试获取 A 股标准指数 (sh/sz 开头且为数字)
        # -----------------------------------------------
        if index_code and (index_code.startswith("sh") or index_code.startswith("sz")) and index_code[2:].isdigit():
            try:
                # 随机延时防止封禁
                time.sleep(random.uniform(0.5, 1.5))
                df = ak.stock_zh_index_daily_em(symbol=index_code)
                if df is not None and not df.empty:
                    return df['close']
            except Exception:
                logger.warning(f"⚠️ 指数数据 {index_code} 获取失败，尝试切换至 ETF 历史走势...")

        # -----------------------------------------------
        # 2. 回退策略：获取 ETF 基金自身的历史复权净值
        # 适用：美股(NDX/SPX)、港股、商品(豆粕)、债券
        # -----------------------------------------------
        if fund_code:
            try:
                time.sleep(random.uniform(0.5, 1.5))
                # 获取 ETF 历史行情 (前复权) - 能够完美反映美股/商品 ETF 的实际涨跌幅分位
                df = ak.fund_etf_hist_em(symbol=fund_code, adjust="qfq")
                if df is not None and not df.empty:
                    # 确保日期列为 datetime
                    df['日期'] = pd.to_datetime(df['日期'])
                    df.set_index('日期', inplace=True)
                    return df['收盘']
            except Exception as e:
                logger.error(f"❌ ETF 数据 {fund_code} 获取彻底失败: {e}")
        
        return None

    def get_valuation_status(self, index_name, strategy_type, fund_code=None):
        """
        计算估值/价格分位并返回调节系数
        Args:
            index_name: 指数代码 (来自 config, 如 sh000300 或 NDX)
            strategy_type: 策略类型 (core/satellite/dividend/hedge)
            fund_code: ETF 代码 (用于数据兜底)
        
        Returns: (multiplier, description)
        """
        try:
            # 1. 获取历史数据序列 (混合模式)
            history_series = self._fetch_data_hybrid(index_name, fund_code)
            
            # 数据校验 (至少需要约 1 年数据，理想 3-5 年)
            if history_series is None or len(history_series) < 120:
                return 1.0, "数据不足(默认适中)"

            # 2. 计算分位点 (Percentile)
            # 取过去 5 年 (约 1250 个交易日) 或 所有可用数据
            window_len = min(1250, len(history_series))
            window_data = history_series.tail(window_len)
            
            current_price = window_data.iloc[-1]
            low_val = window_data.min()
            high_val = window_data.max()
            
            if high_val <= low_val:
                percentile = 0.5
            else:
                percentile = (current_price - low_val) / (high_val - low_val)
            
            p_str = f"{int(percentile*100)}%"
            
            # 3. 根据策略类型返回系数 (V15.20 策略矩阵)
            
            # --- A. 核心资产 (Core) ---
            # 逻辑：以囤积筹码为主，低估时大举买入，高估时适度止盈或停止定投
            if strategy_type == 'core': 
                if percentile < 0.15: return 1.8, f"极度低估(分位{p_str})"
                if percentile < 0.30: return 1.5, f"低估机会(分位{p_str})"
                if percentile < 0.40: return 1.2, f"相对低估(分位{p_str})"
                if percentile > 0.85: return 0.5, f"高估限流(分位{p_str})"
                if percentile > 0.95: return 0.0, f"极度泡沫(分位{p_str})"
                return 1.0, f"估值适中(分位{p_str})"
                
            # --- B. 卫星进攻 (Satellite) ---
            # 逻辑：右侧交易属性强，允许一定溢价，但极端泡沫必须刹车
            elif strategy_type == 'satellite': 
                if percentile > 0.90: return 0.0, f"泡沫预警(分位{p_str})"
                if percentile > 0.80: return 0.5, f"高位防守(分位{p_str})"
                # 卫星策略在低估时不宜过重仓位(防止阴跌)，保持定投即可
                return 1.0, f"估值允许(分位{p_str})"
                
            # --- C. 红利防守 (Dividend) ---
            # 逻辑：对价格极度敏感，必须买得便宜才有高股息率
            elif strategy_type == 'dividend': 
                if percentile > 0.70: return 0.5, f"红利高估(分位{p_str})"
                if percentile > 0.85: return 0.0, f"股息率低(分位{p_str})"
                if percentile < 0.20: return 1.5, f"黄金股息(分位{p_str})"
                return 1.0, f"估值适中(分位{p_str})"

            # --- D. 宏观对冲 (Hedge) ---
            # 逻辑：黄金/商品/债券。通常具有强周期或避险属性，采用均值回归逻辑
            elif strategy_type == 'hedge':
                if percentile > 0.90: return 0.0, f"历史高位(分位{p_str})" # 比如金价历史新高时暂缓
                if percentile < 0.15: return 1.5, f"周期底部(分位{p_str})"
                return 1.0, f"配置区间(分位{p_str})"

            return 1.0, f"策略默认(分位{p_str})"

        except Exception as e:
            logger.error(f"估值计算异常 {index_name}/{fund_code}: {e}")
            return 1.0, "计算错误(默认适中)"
