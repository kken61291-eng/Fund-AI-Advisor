import pandas as pd
import ta
from utils import logger

class TechnicalAnalyzer:
    @staticmethod
    def calculate_indicators(df_dict):
        daily_df = df_dict.get('daily')
        weekly_df = df_dict.get('weekly')

        if daily_df is None or daily_df.empty or len(daily_df) < 60:
            return None

        try:
            # 1. 基础指标
            close = daily_df['close'].iloc[-1]
            high = daily_df['high'] if 'high' in daily_df.columns else daily_df['close']
            low = daily_df['low'] if 'low' in daily_df.columns else daily_df['close']
            
            rsi = ta.momentum.RSIIndicator(daily_df['close'], window=14).rsi().iloc[-1]
            ma20 = ta.trend.SMAIndicator(daily_df['close'], window=20).sma_indicator().iloc[-1]
            bias_20 = (close - ma20) / ma20 * 100
            
            # 2. 波动率 ATR
            atr_indicator = ta.volatility.AverageTrueRange(high, low, daily_df['close'], window=14)
            current_atr = atr_indicator.average_true_range().iloc[-1]
            atr_mean = atr_indicator.average_true_range().rolling(60).mean().iloc[-1]
            
            volatility_high = False
            if current_atr > atr_mean * 1.5:
                volatility_high = True

            # 3. 趋势判定
            trend_daily = "BULL" if close > ma20 else "BEAR"
            trend_weekly = "UNKNOWN"
            if weekly_df is not None and len(weekly_df) > 20:
                ma20_weekly = ta.trend.SMAIndicator(weekly_df['close'], window=20).sma_indicator().iloc[-1]
                trend_weekly = "UP" if weekly_df['close'].iloc[-1] > ma20_weekly else "DOWN"

            # 4. 评分卡
            score = 0
            reasons = []

            # A. 趋势
            if trend_weekly == "UP":
                score += 40
                reasons.append("周线多头(+40)")
            elif trend_weekly == "DOWN":
                score -= 20 
                reasons.append("周线空头(-20)")
            
            # B. 超卖
            if rsi < 30: 
                score += 40
                reasons.append("RSI极度超卖(+40)")
            elif rsi < 40:
                score += 20
                reasons.append("RSI弱势区(+20)")
            elif rsi > 70:
                score -= 30
                reasons.append("RSI超买(-30)")
            
            # C. 乖离
            if bias_20 < -5:
                score += 15
                reasons.append("乖离深跌(+15)")
            elif bias_20 > 5:
                score -= 10
                reasons.append("乖离过大(-10)")

            # D. 日线结构
            if trend_daily == "BULL" and rsi < 60:
                score += 20
                reasons.append("日线健康多头(+20)")
            elif trend_daily == "BEAR" and rsi > 40:
                score -= 10
                reasons.append("日线空头反抽(-10)")

            # E. 波动率风控
            if volatility_high:
                score *= 0.8
                reasons.append("⚠️[高波动]评分打折")

            # F. 下跌中继风控
            if trend_weekly == "DOWN" and trend_daily == "BULL":
                score = min(score, 45)
                reasons.append("⚠️[风控]下跌中继限制")
            
            signal = "WAIT"
            if score >= 85: signal = "STRONG_BUY"
            elif score >= 60: signal = "BUY"
            elif score <= 15: signal = "SELL"
            
            return {
                "price": round(close, 4),
                "rsi": round(rsi, 2),
                "bias_20": round(bias_20, 2),
                "atr_ratio": round(current_atr/atr_mean, 2),
                "trend_daily": trend_daily,
                "trend_weekly": trend_weekly,
                "quant_score": int(score),
                "quant_signal": signal,
                "quant_reasons": reasons
            }
        except Exception as e:
            logger.error(f"指标计算错误: {e}")
            return None
