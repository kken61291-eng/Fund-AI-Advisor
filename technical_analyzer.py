import pandas as pd
import numpy as np
from datetime import datetime, time as dt_time
from utils import logger, get_beijing_time

# [V14.29] 适配 GitHub Actions 环境，使用 'ta' 库而非 'pandas_ta'
from ta.momentum import RSIIndicator
from ta.trend import MACD
from ta.volatility import BollingerBands
from ta.volume import OnBalanceVolumeIndicator

class TechnicalAnalyzer:
    def __init__(self):
        pass

    @staticmethod
    def _get_safe_default_indicators(error_msg="数据不足或计算错误"):
        """
        [V15.11 修复] 返回一个默认的安全字典，防止 main.py 崩溃
        默认状态：0分 + VETO (一票否决)
        """
        return {
            'rsi': 50,
            'macd': {'line': 0, 'signal': 0, 'hist': 0, 'trend': '未知'},
            'risk_factors': {'bollinger_pct_b': 0.5, 'vol_ratio': 1.0, 'divergence': '无'},
            'flow': {'obv_slope': 0},
            'trend_weekly': 'Unknown',
            'price': 0,
            'quant_score': 0,
            'final_score': 0,
            'tech_cro_signal': 'VETO',  # 默认拦截
            'tech_cro_comment': f"系统风控: {error_msg}"
        }

    @staticmethod
    def _calculate_trade_minutes(current_time):
        """
        [数学核心] 计算A股当日已交易分钟数 (全天240分钟)
        剔除午休 11:30 - 13:00
        """
        t_min = current_time.hour * 60 + current_time.minute
        
        t_open_am = 9 * 60 + 30   # 09:30 (570)
        t_close_am = 11 * 60 + 30 # 11:30 (690)
        t_open_pm = 13 * 60       # 13:00 (780)
        t_close_pm = 15 * 60      # 15:00 (900)
        
        if t_min < t_open_am:
            return 0 
        elif t_open_am <= t_min <= t_close_am:
            return t_min - t_open_am 
        elif t_close_am < t_min < t_open_pm:
            return 120 # 午休期间固定为120
        elif t_open_pm <= t_min <= t_close_pm:
            return 120 + (t_min - t_open_pm) 
        else:
            return 240 

    @staticmethod
    def calculate_indicators(df):
        # 基础数据检查
        if df is None or df.empty or len(df) < 30:
            return TechnicalAnalyzer._get_safe_default_indicators("K线数据不足(<30)")

        # [关键修复] 确保列名全小写，防止 'Volume' vs 'volume' 错误
        df.columns = [c.lower() for c in df.columns]
        
        # 再次检查关键列是否存在
        required_cols = ['close', 'volume', 'high', 'low', 'open']
        if not all(col in df.columns for col in required_cols):
             logger.error(f"❌ 数据缺少关键列: {list(df.columns)}")
             return TechnicalAnalyzer._get_safe_default_indicators("缺少关键列(OHLCV)")

        # --- [V14.28 逻辑保留] 全时段动态成交量投影 ---
        try:
            last_date = df.index[-1]
            now_bj = get_beijing_time()
            
            # 只有当K线日期是今天，且未收盘时，才进行预测
            if last_date.date() == now_bj.date() and now_bj.time() < dt_time(15, 0):
                
                trade_mins = TechnicalAnalyzer._calculate_trade_minutes(now_bj.time())
                
                if trade_mins > 15:
                    original_vol = df.iloc[-1]['volume']
                    multiplier = 240 / trade_mins
                    
                    # 保守修正系数
                    if trade_mins < 120: 
                        multiplier *= 0.9 # 上午保守
                    else:
                        multiplier *= 1.05 # 下午激进(尾盘放量)
                    
                    projected_vol = original_vol * multiplier
                    
                    # 修改数据 [修复类型警告]
                    vol_idx = df.columns.get_loc('volume')
                    df.iloc[-1, vol_idx] = float(projected_vol) 
                    
                    logger.info(f"⚖️ [动态量能投影] 交易{trade_mins}min | 乘数x{multiplier:.2f} | Vol预测: {int(original_vol)} -> {int(projected_vol)}")
                else:
                    logger.info("⏳ [动态量能投影] 开盘时间不足15分钟，跳过预测。")
                    
        except Exception as e:
            logger.warning(f"量能投影计算微瑕: {e}")
        # ---------------------------------------

        indicators = {}
        
        try:
            # 数据清洗
            df = df.ffill().bfill()
            close = df['close']
            volume = df['volume']
            current_price = close.iloc[-1]
            
            # 1. RSI (相对强弱)
            rsi_ind = RSIIndicator(close=close, window=14)
            rsi_val = rsi_ind.rsi().iloc[-1]
            indicators['rsi'] = round(rsi_val, 2)

            # 2. MACD (趋势)
            macd_ind = MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
            macd_line = macd_ind.macd().iloc[-1]
            macd_signal = macd_ind.macd_signal().iloc[-1]
            macd_hist = macd_ind.macd_diff().iloc[-1]
            
            # 判断MACD柱状图趋势
            try:
                prev_hist = macd_ind.macd_diff().iloc[-2]
                macd_trend = "金叉" if macd_hist > 0 else "死叉"
                if macd_hist > 0 and macd_hist < prev_hist:
                    macd_trend = "红柱缩短"
                elif macd_hist < 0 and macd_hist > prev_hist:
                    macd_trend = "绿柱缩短"
            except:
                macd_trend = "未知"
                
            indicators['macd'] = {
                "line": round(macd_line, 3),
                "signal": round(macd_signal, 3),
                "hist": round(macd_hist, 3),
                "trend": macd_trend
            }

            # 3. Bollinger Bands (布林带 %B)
            bb_ind = BollingerBands(close=close, window=20, window_dev=2)
            pct_b = bb_ind.bollinger_pband().iloc[-1]
            indicators['risk_factors'] = {
                "bollinger_pct_b": round(pct_b, 2)
            }

            # 4. Volume Ratio (量比 - 简化版)
            ma_vol_5 = volume.rolling(window=5).mean().iloc[-1]
            vol_ratio = volume.iloc[-1] / ma_vol_5 if ma_vol_5 > 0 else 1.0
            indicators['risk_factors']['vol_ratio'] = round(vol_ratio, 2)
            
            # 5. OBV 能量潮
            obv_ind = OnBalanceVolumeIndicator(close=close, volume=volume)
            obv = obv_ind.on_balance_volume()
            try:
                obv_slope = (obv.iloc[-1] - obv.iloc[-10]) / 10
            except:
                obv_slope = 0
            
            indicators['flow'] = {
                "obv_slope": round(obv_slope / 10000, 2) # 归一化
            }

            # 6. 趋势状态 (周线 MA5)
            df_weekly = df.resample('W').agg({'close': 'last'})
            if len(df_weekly) >= 5:
                ma5_weekly = df_weekly['close'].rolling(5).mean().iloc[-1]
                current_weekly_close = df_weekly['close'].iloc[-1]
                trend_status = "UP" if current_weekly_close > ma5_weekly else "DOWN"
            else:
                trend_status = "Unknown"
            
            indicators['trend_weekly'] = trend_status
            indicators['price'] = current_price

            # 7. 综合打分
            score = 50
            if indicators['rsi'] < 30: score += 15
            if indicators['rsi'] > 70: score -= 10
            if macd_hist > 0: score += 10
            if trend_status == "UP": score += 20
            if vol_ratio > 1.2: score += 5
            if vol_ratio < 0.6: score -= 15
            if obv_slope > 0: score += 10
            
            indicators['quant_score'] = max(0, min(100, score))
            
            # 8. CRO 信号生成 [V15.6 修复: 优先级逻辑]
            # 优先级: VETO (3) > WARN (1) > PASS (0)
            current_risk_level = 0
            cro_signal = "PASS"
            cro_reason = "技术指标正常"
            
            # 规则 A: 周线趋势 (权重 1 - 警告)
            if trend_status == "DOWN":
                if current_risk_level < 1:
                    current_risk_level = 1
                    cro_signal = "WARN"
                    cro_reason = "周线趋势向下"
            
            # 规则 B: 流动性枯竭 (权重 3 - 否决)
            if vol_ratio < 0.6: 
                if current_risk_level < 3:
                    current_risk_level = 3
                    cro_signal = "VETO"
                    cro_reason = f"流动性枯竭(VR {vol_ratio}<0.6)"
            
            # 规则 C: RSI 极度超买 (权重 3 - 否决)
            if indicators['rsi'] > 85:
                if current_risk_level < 3:
                    current_risk_level = 3
                    cro_signal = "VETO"
                    cro_reason = f"RSI极度超买({indicators['rsi']})"

            indicators['tech_cro_signal'] = cro_signal
            indicators['tech_cro_comment'] = cro_reason

            return indicators

        except Exception as e:
            logger.error(f"指标计算失败: {e}")
            return TechnicalAnalyzer._get_safe_default_indicators(f"计算异常: {str(e)[:20]}")
