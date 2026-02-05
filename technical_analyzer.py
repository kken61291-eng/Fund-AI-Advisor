import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import SMAIndicator, MACD
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator
from utils import logger

class TechnicalAnalyzer:
    @staticmethod
    def calculate_indicators(data_dict):
        try:
            df = data_dict['daily'].copy()
            weekly_df = data_dict['weekly'].copy()
            if len(df) < 60: return None
            
            # --- 1. 基础指标 ---
            current_price = df['close'].iloc[-1]
            close_series = df['close']
            high_series = df['high']
            low_series = df['low']
            vol_series = df['volume']
            
            # RSI & MACD
            rsi_series = RSIIndicator(close_series, window=14).rsi()
            rsi = rsi_series.iloc[-1]
            
            macd = MACD(close_series)
            macd_diff = macd.macd_diff().iloc[-1]
            macd_trend = "金叉" if (macd_diff > 0 and macd.macd().iloc[-1] > macd.macd_signal().iloc[-1]) else ("死叉" if macd_diff < 0 else "震荡")
            
            # OBV
            obv_slope = 0
            if vol_series.sum() > 0:
                try:
                    obv = OnBalanceVolumeIndicator(close_series, vol_series).on_balance_volume()
                    if len(obv) >= 6:
                        prev = obv.iloc[-6]
                        if prev != 0: obv_slope = (obv.iloc[-1] - prev) / abs(prev) * 100
                except: pass

            # 周线趋势
            trend_weekly = "NEUTRAL"
            if len(weekly_df) >= 20:
                w_ma20 = SMAIndicator(weekly_df['close'], window=20).sma_indicator().iloc[-1]
                trend_weekly = "UP" if weekly_df['close'].iloc[-1] > w_ma20 else "DOWN"

            # --- 2. [V12.1 新增] 风控官专用指标 ---
            
            # A. 布林带位置 (%B)
            bb = BollingerBands(close_series, window=20, window_dev=2)
            bb_pband = bb.bollinger_pband().iloc[-1]  # %B
            bb_wband = bb.bollinger_wband().iloc[-1]  # 带宽 (用于判断变盘)
            
            # B. 量比 (Volume Ratio)
            # 今日成交量 / 过去5日均量
            vol_ma5 = vol_series.rolling(window=5).mean().iloc[-1]
            vol_ratio = vol_series.iloc[-1] / vol_ma5 if vol_ma5 > 0 else 1.0
            
            # C. RSI 背离检测 (简单的顶背离)
            # 逻辑：价格创20日新高，但RSI没有创20日新高
            price_high_20 = high_series.iloc[-20:].max()
            rsi_high_20 = rsi_series.iloc[-20:].max()
            is_new_high_price = (high_series.iloc[-1] >= price_high_20)
            is_new_high_rsi = (rsi >= rsi_high_20)
            
            divergence_signal = "无"
            if is_new_high_price and not is_new_high_rsi:
                divergence_signal = "顶背离(钝化)"

            # --- 3. 打分逻辑 (保持核心稳定，新指标用于AI修正，不直接干扰基准分) ---
            score = 50
            # RSI得分
            if rsi < 30: score += 25
            elif rsi < 40: score += 10
            elif rsi > 80: score -= 25
            elif rsi > 70: score -= 15
            # 趋势得分
            if trend_weekly == "UP": score += 20
            else: score -= 20
            # 乖离率得分
            ma20 = SMAIndicator(close_series, window=20).sma_indicator().iloc[-1]
            bias_20 = (current_price - ma20) / ma20 * 100
            if bias_20 < -7: score += 15
            if bias_20 > 15: score -= 15
            # MACD得分
            if macd_diff > 0 and macd_trend == "金叉": score += 10
            elif macd_diff < 0: score -= 10
            # OBV得分
            if obv_slope > 5: score += 10
            elif obv_slope < -5: score -= 10

            return {
                "price": current_price, 
                "quant_score": int(max(0, min(100, score))),
                "rsi": round(rsi, 2), 
                "bias_20": round(bias_20, 2),
                "trend_weekly": trend_weekly, 
                "macd": {"diff": round(macd_diff, 3), "trend": macd_trend},
                "flow": {"obv_slope": round(obv_slope, 2)},
                # 新增指标包
                "risk_factors": {
                    "bollinger_pct_b": round(bb_pband, 2), # >1 超买, <0 超卖
                    "vol_ratio": round(vol_ratio, 2),      # 量比
                    "divergence": divergence_signal        # 背离状态
                },
                "quant_reasons": []
            }
        except Exception as e:
            logger.error(f"指标计算错误: {e}")
            return None
