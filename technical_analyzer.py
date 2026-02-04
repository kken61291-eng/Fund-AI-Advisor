import pandas as pd
# [修复] 移除了 import pandas_ta，因为它会导致 ModuleNotFoundError 且后续逻辑并未用到
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import SMAIndicator, MACD
from ta.volatility import AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator
from utils import logger

class TechnicalAnalyzer:
    @staticmethod
    def calculate_indicators(data_dict):
        """
        V10.0 技术指标计算 (双核版: MACD + KDJ + OBV)
        """
        try:
            df = data_dict['daily'].copy()
            weekly_df = data_dict['weekly'].copy()
            
            # 数据太少无法计算指标
            if len(df) < 60: return None

            current_price = df['close'].iloc[-1]
            
            # --- 1. 基础指标 (Base) ---
            # RSI (相对强弱)
            rsi = RSIIndicator(df['close'], window=14).rsi().iloc[-1]
            
            # MA20 & Bias (乖离率)
            ma20 = SMAIndicator(df['close'], window=20).sma_indicator().iloc[-1]
            bias_20 = (current_price - ma20) / ma20 * 100
            
            # ATR (波动率)
            atr = AverageTrueRange(df['high'], df['low'], df['close'], window=14).average_true_range().iloc[-1]
            
            # --- 2. V10.0 进阶指标 (Advanced) ---
            
            # MACD (趋势确认)
            # 逻辑: 只有金叉且红柱放大才是真强势
            macd_obj = MACD(df['close'])
            macd_line = macd_obj.macd().iloc[-1]     # DIF
            macd_signal = macd_obj.macd_signal().iloc[-1] # DEA
            macd_hist = macd_obj.macd_diff().iloc[-1] # MACD柱
            
            # KDJ (通过 Stochastic Oscillator 模拟)
            # KDJ 的 K 和 D 对应 stoch 和 stoch_signal
            stoch = StochasticOscillator(df['high'], df['low'], df['close'], window=9, smooth_window=3)
            k_val = stoch.stoch().iloc[-1]
            d_val = stoch.stoch_signal().iloc[-1]
            # 手动计算 J 值: J = 3K - 2D
            j_val = 3 * k_val - 2 * d_val

            # 资金流向代理 (OBV 能量潮)
            # 使用 OBV 的 5日斜率来判断资金是在流入还是流出
            obv = OnBalanceVolumeIndicator(df['close'], df['volume']).on_balance_volume()
            if len(obv) >= 6:
                obv_curr = obv.iloc[-1]
                obv_prev = obv.iloc[-6] # 取5天前
                # 防止除以0
                if obv_prev != 0:
                    obv_slope = (obv_curr - obv_prev) / abs(obv_prev) * 100 
                else:
                    obv_slope = 0
            else:
                obv_slope = 0
            
            # --- 3. 真·周线趋势 (Weekly Trend) ---
            trend_weekly = "NEUTRAL"
            if len(weekly_df) >= 20:
                w_ma20 = SMAIndicator(weekly_df['close'], window=20).sma_indicator().iloc[-1]
                w_price = weekly_df['close'].iloc[-1]
                trend_weekly = "UP" if w_price > w_ma20 else "DOWN"

            # --- 4. 综合打分模型 (Scoring V10) ---
            score = 50
            
            # A. RSI 评分 (超买超卖)
            if rsi < 30: score += 25  # 黄金坑
            elif rsi < 40: score += 10
            elif rsi > 80: score -= 25 # 极度超买
            elif rsi > 70: score -= 15
            
            # B. 趋势评分 (周线定方向)
            if trend_weekly == "UP": score += 20
            else: score -= 20
            
            # C. 乖离率评分 (均值回归)
            if bias_20 < -7: score += 15 # 深跌必反弹
            if bias_20 > 15: score -= 15 # 涨太多必回调
            
            # D. MACD 评分 (趋势增强)
            if macd_hist > 0: # 红柱
                if macd_line > macd_signal: score += 10 # 金叉状态
            else: # 绿柱
                score -= 10
            
            # E. 资金流向评分 (OBV)
            if obv_slope > 5: score += 10 # 资金大幅流入
            elif obv_slope < -5: score -= 10 # 资金大幅出逃

            # F. KDJ 评分 (短线情绪)
            # J值 < 0 为极度超卖，J > 100 为极度超买
            if j_val < 0: score += 5 
            if j_val > 100: score -= 5

            return {
                "price": current_price,
                "quant_score": int(max(0, min(100, score))),
                "rsi": round(rsi, 2),
                "bias_20": round(bias_20, 2),
                "trend_weekly": trend_weekly,
                "atr": round(atr, 3),
                "macd": {
                    "diff": round(macd_hist, 3), 
                    "trend": "金叉" if (macd_hist > 0 and macd_line > macd_signal) else ("死叉" if macd_hist < 0 else "震荡")
                },
                "kdj": {"j": round(j_val, 2)},
                "flow": {"obv_slope": round(obv_slope, 2)}
            }
        except Exception as e:
            logger.error(f"指标计算错误: {e}")
            return None
