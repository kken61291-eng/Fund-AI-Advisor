import pandas as pd
import ta
from utils import logger

class TechnicalAnalyzer:
    @staticmethod
    def calculate_indicators(df_dict):
        """
        计算技术指标 (Python 侧预计算)
        """
        daily_df = df_dict.get('daily')
        weekly_df = df_dict.get('weekly')

        if daily_df is None or daily_df.empty or len(daily_df) < 30:
            return None

        try:
            # 1. 日线指标
            # RSI (14天)
            rsi = ta.momentum.RSIIndicator(daily_df['close'], window=14).rsi().iloc[-1]
            
            # 均线 (MA20)
            ma20 = ta.trend.SMAIndicator(daily_df['close'], window=20).sma_indicator().iloc[-1]
            price = daily_df['close'].iloc[-1]
            
            # 乖离率 (Bias)
            bias = (price - ma20) / ma20 * 100
            
            # 趋势判断
            trend_daily = "BULL" if price > ma20 else "BEAR"

            # 2. 周线指标 (大趋势过滤)
            trend_weekly = "UNKNOWN"
            if weekly_df is not None and not weekly_df.empty and len(weekly_df) > 20:
                ma20_weekly = ta.trend.SMAIndicator(weekly_df['close'], window=20).sma_indicator().iloc[-1]
                weekly_close = weekly_df['close'].iloc[-1]
                trend_weekly = "UP" if weekly_close > ma20_weekly else "DOWN"

            return {
                "price": round(price, 4),
                "rsi": round(rsi, 2),
                "bias_20": round(bias, 2),
                "trend_daily": trend_daily,
                "trend_weekly": trend_weekly
            }
        except Exception as e:
            logger.error(f"指标计算错误: {e}")
            return None
