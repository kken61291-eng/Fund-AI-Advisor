import pandas as pd
import numpy as np
from utils import logger

# 尝试导入 ta 库 (技术指标库)
try:
    import ta
except ImportError:
    ta = None

class TechnicalAnalyzer:
    @staticmethod
    def calculate_indicators(data):
        """
        全能技术分析器 (适配 V14 DataFrame 格式)
        输入: DataFrame (索引为 datetime, 包含 close, high, low, volume)
        输出: 包含各种指标的字典
        """
        # 1. 数据格式标准化
        if data is None or data.empty:
            return None
        
        # 兼容性处理: 如果传入的是旧版字典 {'daily': df}, 取出 daily
        if isinstance(data, dict) and 'daily' in data:
            df = data['daily']
        else:
            df = data.copy()

        # 确保按日期升序
        df = df.sort_index()
        
        close = df['close']
        high = df['high']
        low = df['low']
        volume = df['volume']

        res = {
            "price": close.iloc[-1],
            "quant_score": 50, # 初始分
            "signals": [],
            "risk_factors": {}
        }

        try:
            # --- 2. 核心指标计算 (使用 ta 库) ---
            if ta:
                # RSI (14)
                rsi_series = ta.momentum.RSIIndicator(close, window=14).rsi()
                res['rsi'] = round(rsi_series.iloc[-1], 2)

                # MACD (12, 26, 9)
                macd = ta.trend.MACD(close)
                macd_line = macd.macd()
                signal_line = macd.macd_signal()
                hist = macd.macd_diff()
                
                res['macd'] = {
                    "diff": round(hist.iloc[-1], 3),
                    "trend": "金叉" if hist.iloc[-1] > 0 and hist.iloc[-2] <= 0 else ("死叉" if hist.iloc[-1] < 0 and hist.iloc[-2] >= 0 else ("多头" if hist.iloc[-1] > 0 else "空头"))
                }

                # Bollinger Bands (20, 2)
                bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
                res['risk_factors']['bollinger_pct_b'] = round(bb.bollinger_pband().iloc[-1], 2)
                
            else:
                # 简易手动计算 (防止 ta 库缺失)
                delta = close.diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                res['rsi'] = round(100 - (100 / (1 + rs.iloc[-1])), 2)
                res['macd'] = {"trend": "未知"}
                res['risk_factors']['bollinger_pct_b'] = 0.5

            # --- 3. 资金流向 (OBV) ---
            # OBV 斜率 (近5天)
            obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
            obv_slope = (obv.iloc[-1] - obv.iloc[-6]) / obv.iloc[-6] * 100 if len(obv) > 6 else 0
            res['flow'] = {"obv_slope": round(obv_slope, 2)}

            # --- 4. 量能分析 (VR - Volume Ratio) ---
            # 简化版 VR: 上涨日成交量 / 下跌日成交量 (近26天)
            window_vr = 26
            df_vr = df.tail(window_vr+1)
            up_vol = df_vr[df_vr['close'] > df_vr['close'].shift(1)]['volume'].sum()
            down_vol = df_vr[df_vr['close'] < df_vr['close'].shift(1)]['volume'].sum()
            vr = up_vol / down_vol if down_vol > 0 else 2.0
            res['risk_factors']['vol_ratio'] = round(vr, 2)

            # --- 5. 自动生成周线趋势 (Weekly Trend) ---
            try:
                # 自动降采样为周线
                df_weekly = df.resample('W').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()
                
                if len(df_weekly) >= 5:
                    w_ma5 = df_weekly['close'].rolling(5).mean().iloc[-1]
                    w_close = df_weekly['close'].iloc[-1]
                    res['trend_weekly'] = "UP" if w_close > w_ma5 else "DOWN"
                else:
                    res['trend_weekly'] = "震荡"
            except:
                res['trend_weekly'] = "数据不足"

            # --- 6. 最终量化打分 (0-100) ---
            score = 50
            # RSI 评分
            rsi = res['rsi']
            if 40 <= rsi <= 60: score += 10
            elif rsi < 30: score += 20 (超卖反弹)
            elif rsi > 80: score -= 20 (超买风险)
            
            # 趋势评分
            if res['trend_weekly'] == "UP": score += 20
            
            # MACD 评分
            if "金叉" in res['macd'].get('trend', ''): score += 15
            elif "多头" in res['macd'].get('trend', ''): score += 5
            elif "死叉" in res['macd'].get('trend', ''): score -= 15
            
            # 量能评分
            if vr < 0.6: score -= 10 (极度缩量)
            elif 0.8 <= vr <= 1.5: score += 5 (健康)
            
            res['quant_score'] = max(0, min(100, score))

            # 背离检测
            res['risk_factors']['divergence'] = "无"
            if res['price'] > df['close'].iloc[-10:].max() and res['rsi'] < 70:
                res['risk_factors']['divergence'] = "顶背离(钝化)"

            return res

        except Exception as e:
            logger.error(f"指标计算错误: {e}")
            # 返回降级数据，防止主程序崩溃
            return {
                "price": df['close'].iloc[-1] if not df.empty else 0,
                "quant_score": 50,
                "rsi": 50,
                "macd": {"trend": "计算失败"},
                "flow": {"obv_slope": 0},
                "risk_factors": {"vol_ratio": 1.0, "divergence": "无", "bollinger_pct_b": 0.5},
                "trend_weekly": "未知"
            }
