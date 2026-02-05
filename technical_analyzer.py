import pandas as pd
import numpy as np
from utils import logger

try:
    import ta
except ImportError:
    ta = None

class TechnicalAnalyzer:
    @staticmethod
    def calculate_indicators(data):
        """
        全能技术分析器 + 技术风控官 (Technical CRO)
        """
        if data is None or data.empty:
            return None
        
        # 数据清洗
        if isinstance(data, dict) and 'daily' in data: df = data['daily']
        else: df = data.copy()
        df = df.sort_index()
        
        close = df['close']
        volume = df['volume']
        
        # --- 1. 基础指标计算 ---
        res = {
            "price": close.iloc[-1],
            "quant_score": 50,
            "risk_factors": {},
            "tech_cro_signal": "PASS", # 默认通行
            "tech_cro_comment": "技术指标正常"
        }

        try:
            # RSI & MACD & Bollinger
            if ta:
                rsi_series = ta.momentum.RSIIndicator(close, window=14).rsi()
                res['rsi'] = round(rsi_series.iloc[-1], 2)
                
                macd = ta.trend.MACD(close)
                hist = macd.macd_diff()
                res['macd'] = {
                    "diff": round(hist.iloc[-1], 3),
                    "trend": "金叉" if hist.iloc[-1] > 0 and hist.iloc[-2] <= 0 else ("死叉" if hist.iloc[-1] < 0 and hist.iloc[-2] >= 0 else ("多头" if hist.iloc[-1] > 0 else "空头"))
                }
                
                bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
                res['risk_factors']['bollinger_pct_b'] = round(bb.bollinger_pband().iloc[-1], 2)
            else:
                # 降级计算
                res['rsi'] = 50
                res['macd'] = {"trend": "未知"}
                res['risk_factors']['bollinger_pct_b'] = 0.5

            # OBV (资金流向)
            obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
            obv_slope = (obv.iloc[-1] - obv.iloc[-6]) / obv.iloc[-6] * 100 if len(obv) > 6 else 0
            res['flow'] = {"obv_slope": round(obv_slope, 2)}

            # VR (量比风控核心)
            window_vr = 26
            df_vr = df.tail(window_vr+1)
            up_vol = df_vr[df_vr['close'] > df_vr['close'].shift(1)]['volume'].sum()
            down_vol = df_vr[df_vr['close'] < df_vr['close'].shift(1)]['volume'].sum()
            vr = up_vol / down_vol if down_vol > 0 else 2.0
            res['risk_factors']['vol_ratio'] = round(vr, 2)

            # 周线趋势
            try:
                df_weekly = df.resample('W').agg({'close': 'last'}).dropna()
                if len(df_weekly) >= 5:
                    w_ma5 = df_weekly['close'].rolling(5).mean().iloc[-1]
                    res['trend_weekly'] = "UP" if df_weekly['close'].iloc[-1] > w_ma5 else "DOWN"
                else: res['trend_weekly'] = "震荡"
            except: res['trend_weekly'] = "数据不足"

            # --- 2. 技术风控官 (The Technical CRO) 介入 ---
            # 这是一个基于"硬逻辑"的一票否决系统
            cro_msgs = []
            veto_triggered = False

            # 风控规则 1: 流动性枯竭 (Liquidity Trap)
            if vr < 0.6:
                cro_msgs.append(f"⛔ 量比{vr}极低(无承接)，禁止开仓")
                veto_triggered = True

            # 风控规则 2: 顶背离 (Top Divergence)
            # 价格创近10天新高，但 RSI 却在下降
            recent_high = close.iloc[-10:].max()
            if res['price'] >= recent_high and res['rsi'] < 60 and res['rsi'] < rsi_series.iloc[-5:].max():
                cro_msgs.append("⚠️ 出现量价顶背离，建议减仓")
                res['risk_factors']['divergence'] = "顶背离"
            
            # 风控规则 3: 趋势破位 (Trend Breakdown)
            if res['trend_weekly'] == "DOWN":
                cro_msgs.append("📉 周线趋势向下，只卖不买")
                # 周线向下不一定完全禁止（可能有超跌反弹），但要扣分

            # 风控规则 4: 极端超买 (Extreme Overbought)
            if res['rsi'] > 85:
                cro_msgs.append("🔥 RSI>85 极度超买，禁止追高")
                veto_triggered = True

            # 汇总风控意见
            if veto_triggered:
                res['tech_cro_signal'] = "VETO" # 一票否决
            elif cro_msgs:
                res['tech_cro_signal'] = "WARN" # 警告
            
            if cro_msgs:
                res['tech_cro_comment'] = " | ".join(cro_msgs)
            else:
                res['tech_cro_comment'] = "✅ 技术指标健康，风控通过"

            # --- 3. 最终评分 ---
            score = 50
            if 40 <= res['rsi'] <= 60: score += 10
            elif res['rsi'] < 30: score += 20
            elif res['rsi'] > 80: score -= 20
            
            if res['trend_weekly'] == "UP": score += 20
            if "金叉" in res['macd']['trend']: score += 15
            elif "死叉" in res['macd']['trend']: score -= 15
            
            if 0.8 <= vr <= 1.5: score += 5
            elif vr < 0.6: score -= 20 # 严重扣分

            res['quant_score'] = max(0, min(100, score))
            return res

        except Exception as e:
            logger.error(f"指标计算错误: {e}")
            return None
