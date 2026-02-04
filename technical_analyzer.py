import pandas as pd
import pandas_ta as ta # 需要 pip install pandas_ta，或者使用 ta 库
from ta.momentum import RSIIndicator, StochRSIIndicator
from ta.trend import SMAIndicator, MACD
from ta.volatility import AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator
from utils import logger

class TechnicalAnalyzer:
    @staticmethod
    def calculate_indicators(data_dict):
        try:
            df = data_dict['daily'].copy()
            weekly_df = data_dict['weekly'].copy()
            
            if len(df) < 60: return None

            current_price = df['close'].iloc[-1]
            
            # 1. 基础指标 (保留 V9 逻辑)
            rsi = RSIIndicator(df['close'], window=14).rsi().iloc[-1]
            
            ma20 = SMAIndicator(df['close'], window=20).sma_indicator().iloc[-1]
            bias_20 = (current_price - ma20) / ma20 * 100
            
            atr = AverageTrueRange(df['high'], df['low'], df['close'], window=14).average_true_range().iloc[-1]
            
            # 2. V10.0 新增指标
            # MACD (趋势确认)
            macd_obj = MACD(df['close'])
            macd_line = macd_obj.macd().iloc[-1]
            macd_signal = macd_obj.macd_signal().iloc[-1]
            macd_hist = macd_obj.macd_diff().iloc[-1]
            
            # KDJ (短线超买超卖) - 使用 StochRSI 模拟或手写，这里用 ta 库通用的 Stochastic Oscillator
            # KDJ 计算较为复杂，这里使用 KD 指标替代，逻辑类似
            from ta.momentum import StochasticOscillator
            stoch = StochasticOscillator(df['high'], df['low'], df['close'], window=9, smooth_window=3)
            k_val = stoch.stoch().iloc[-1]
            d_val = stoch.stoch_signal().iloc[-1]
            j_val = 3 * k_val - 2 * d_val # 手动计算 J 值

            # 资金流向代理 (OBV 能量潮)
            obv = OnBalanceVolumeIndicator(df['close'], df['volume']).on_balance_volume()
            obv_slope = (obv.iloc[-1] - obv.iloc[-5]) / abs(obv.iloc[-5]) * 100 # 5日OBV斜率
            
            # 3. 真·周线趋势
            if len(weekly_df) >= 20:
                w_ma20 = SMAIndicator(weekly_df['close'], window=20).sma_indicator().iloc[-1]
                w_price = weekly_df['close'].iloc[-1]
                trend_weekly = "UP" if w_price > w_ma20 else "DOWN"
            else:
                trend_weekly = "NEUTRAL"

            # 4. 综合打分 (V10 升级版算法)
            score = 50
            
            # RSI 评分 (反转逻辑)
            if rsi < 30: score += 25
            elif rsi < 40: score += 10
            elif rsi > 70: score -= 25
            elif rsi > 60: score -= 10
            
            # 周线评分 (趋势逻辑)
            if trend_weekly == "UP": score += 20
            else: score -= 20
            
            # 乖离率评分
            if bias_20 < -5: score += 15
            if bias_20 > 10: score -= 15
            
            # MACD 评分 (金叉/死叉)
            if macd_hist > 0 and macd_line > macd_signal: score += 10 # 多头增强
            if macd_hist < 0: score -= 10 # 空头压制
            
            # 资金流向评分
            if obv_slope > 2: score += 10 # 资金大幅流入
            if obv_slope < -2: score -= 10 # 资金出逃

            # KDJ 评分 (超短线)
            if j_val < 0: score += 5 # 极度超卖
            if j_val > 100: score -= 5 # 极度超买

            return {
                "price": current_price,
                "quant_score": int(max(0, min(100, score))),
                "rsi": round(rsi, 2),
                "bias_20": round(bias_20, 2),
                "trend_weekly": trend_weekly,
                "atr": round(atr, 3),
                "macd": {"diff": round(macd_hist, 3), "trend": "金叉" if macd_hist > 0 else "死叉"},
                "kdj": {"j": round(j_val, 2)},
                "flow": {"obv_slope": round(obv_slope, 2)}
            }
        except Exception as e:
            logger.error(f"指标计算错误: {e}")
            return None
