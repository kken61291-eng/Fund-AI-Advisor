import pandas as pd
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import SMAIndicator, MACD
from ta.volatility import AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator
from utils import logger

class TechnicalAnalyzer:
    @staticmethod
    def calculate_indicators(data_dict):
        """
        V10.9 稳定版: 修复 KeyErrors，确保所有指标字段齐全
        """
        try:
            df = data_dict['daily'].copy()
            weekly_df = data_dict['weekly'].copy()
            
            if len(df) < 60: return None

            current_price = df['close'].iloc[-1]
            
            # --- 指标计算 ---
            rsi = RSIIndicator(df['close'], window=14).rsi().iloc[-1]
            ma20 = SMAIndicator(df['close'], window=20).sma_indicator().iloc[-1]
            bias_20 = (current_price - ma20) / ma20 * 100
            atr = AverageTrueRange(df['high'], df['low'], df['close'], window=14).average_true_range().iloc[-1]
            
            # MACD
            macd_obj = MACD(df['close'])
            macd_line = macd_obj.macd().iloc[-1]
            macd_signal = macd_obj.macd_signal().iloc[-1]
            macd_hist = macd_obj.macd_diff().iloc[-1]
            
            # KDJ (Stoch)
            stoch = StochasticOscillator(df['high'], df['low'], df['close'], window=9, smooth_window=3)
            # 计算 J 值 (3K - 2D)
            k = stoch.stoch().iloc[-1]
            d = stoch.stoch_signal().iloc[-1]
            j_val = 3 * k - 2 * d

            # OBV 资金流
            obv_slope = 0
            if 'volume' in df.columns and df['volume'].sum() > 0:
                try:
                    obv = OnBalanceVolumeIndicator(df['close'], df['volume']).on_balance_volume()
                    if len(obv) >= 6:
                        prev = obv.iloc[-6]
                        if prev != 0:
                            obv_slope = (obv.iloc[-1] - prev) / abs(prev) * 100
                except: pass
            
            # 周线趋势
            trend_weekly = "NEUTRAL"
            if len(weekly_df) >= 20:
                w_ma20 = SMAIndicator(weekly_df['close'], window=20).sma_indicator().iloc[-1]
                trend_weekly = "UP" if weekly_df['close'].iloc[-1] > w_ma20 else "DOWN"

            # --- 打分逻辑 ---
            score = 50
            # RSI
            if rsi < 30: score += 25
            elif rsi < 40: score += 10
            elif rsi > 80: score -= 25
            elif rsi > 70: score -= 15
            
            # 趋势
            if trend_weekly == "UP": score += 20
            else: score -= 20
            
            # 乖离
            if bias_20 < -7: score += 15
            if bias_20 > 15: score -= 15
            
            # MACD
            if macd_hist > 0 and macd_line > macd_signal: score += 10
            elif macd_hist < 0: score -= 10
            
            # OBV
            if obv_slope > 5: score += 10
            elif obv_slope < -5: score -= 10

            # KDJ
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
                "flow": {"obv_slope": round(obv_slope, 2)},
                # [核心修复] 初始化空列表，防止 main.py 报错
                "quant_reasons": [] 
            }
        except Exception as e:
            logger.error(f"指标计算错误: {e}")
            return None
