import pandas as pd
import numpy as np
from datetime import datetime
from utils import logger, get_beijing_time

# 确保安装了 ta 库: pip install ta
from ta.momentum import RSIIndicator
from ta.trend import MACD, EMAIndicator, SMAIndicator, ADXIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator, VolumeWeightedAveragePrice

class TechnicalAnalyzer:
    """
    技术分析器 - V17.1 (适配 v3.5 四态架构 - 全量版)
    """
    
    def __init__(self, asset_type='ETF'):
        self.asset_type = asset_type

    def calculate_indicators(self, df):
        """
        计算全量技术指标，返回字典供 Prompt 使用
        """
        if df is None or df.empty or len(df) < 60:
            return self._get_safe_default_indicators("K线数据不足(<60)")

        try:
            # --- 1. 数据预处理 ---
            df = self._preprocess_data(df)
            if df is None: 
                return self._get_safe_default_indicators("数据预处理失败")
            
            # 提取基础序列
            close = df['close']
            high = df['high']
            low = df['low']
            volume = df['volume']
            current_price = close.iloc[-1]

            indicators = {
                'price': current_price,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # --- 2. 基础指标计算 ---
            
            # RSI (14)
            rsi = RSIIndicator(close=close, window=14).rsi().iloc[-1]
            indicators['rsi'] = round(rsi, 2)

            # 均线系统
            ema5 = EMAIndicator(close=close, window=5).ema_indicator().iloc[-1]
            ma20 = SMAIndicator(close=close, window=20).sma_indicator().iloc[-1]
            ma60 = SMAIndicator(close=close, window=60).sma_indicator().iloc[-1]
            
            indicators['moving_averages'] = {
                'EMA5': round(ema5, 3), 
                'MA20': round(ma20, 3), 
                'MA60': round(ma60, 3)
            }
            
            # MACD (12, 26, 9)
            macd = MACD(close=close)
            macd_diff = macd.macd_diff().iloc[-1]
            macd_line = macd.macd().iloc[-1]
            
            indicators['macd'] = {
                'trend': 'UP' if macd_diff > 0 else 'DOWN',
                'divergence': self._detect_macd_divergence(close, macd_line),
                'hist': round(macd_diff, 3)
            }

            # --- 3. v3.5 核心适配指标 ---

            # [A] 近期涨幅 (5日) - 用于防抢跑校验
            if len(close) >= 6:
                recent_gain = ((close.iloc[-1] - close.iloc[-5]) / close.iloc[-5]) * 100
            else:
                recent_gain = 0.0
            indicators['recent_gain'] = round(recent_gain, 2)

            # [B] 波动率状态 (ATR + 布林带)
            bb = BollingerBands(close=close)
            bb_width = bb.bollinger_wband().iloc[-1]
            atr = AverageTrueRange(high, low, close).average_true_range().iloc[-1]
            
            if bb_width > 0.20:
                vol_status = "HIGH"
            elif bb_width < 0.05:
                vol_status = "LOW" # 变盘前夕
            else:
                vol_status = "NORMAL"
                
            indicators['volatility_status'] = vol_status
            indicators['atr'] = round(atr, 3)

            # [C] 趋势打分 (Trend Score 0-100)
            indicators['quant_score'] = self._calculate_trend_score(close, rsi, macd_diff, macd_line, ma20, ma60)

            # [D] 相对强弱 (RS Rating)
            rs_score = self._calculate_rs_rating(close)
            indicators['relative_strength'] = rs_score

            # [E] 成交量状态
            vol_ma20 = volume.rolling(20).mean().iloc[-1]
            if vol_ma20 > 0:
                vol_ratio = volume.iloc[-1] / vol_ma20
            else:
                vol_ratio = 1.0
            
            vol_status_str = "NORMAL"
            if vol_ratio > 1.5: vol_status_str = "HEAVY"  # 放量
            elif vol_ratio < 0.6: vol_status_str = "DRY"  # 缩量/地量
            
            indicators['volume_analysis'] = {
                'vol_ratio': round(vol_ratio, 2),
                'status': vol_status_str
            }
            
            # 均线排列判断
            if ema5 > ma20 and ma20 > ma60:
                ma_alignment = "BULLISH"
            elif ema5 < ma20 and ma20 < ma60:
                ma_alignment = "BEARISH"
            else:
                ma_alignment = "MIXED"
            indicators['ma_alignment'] = ma_alignment

            return indicators

        except Exception as e:
            logger.error(f"❌ 技术分析计算异常: {e}", exc_info=True)
            return self._get_safe_default_indicators(str(e))

    def _calculate_trend_score(self, close, rsi, macd_hist, macd_val, ma20, ma60):
        """
        计算 0-100 的趋势分 (v3.5 核心)
        """
        score = 50
        price = close.iloc[-1]
        
        # 1. 均线分 (权重 40)
        if price > ma20: score += 10
        if price > ma60: score += 10
        if ma20 > ma60: score += 20  # 多头排列
        
        # 2. 动量分 (权重 30)
        if rsi > 50: score += 10
        if macd_hist > 0: score += 10
        if macd_val > 0: score += 10
        
        # 3. 扣分项 (弱势惩罚)
        if price < ma20: score -= 10
        if price < ma60: score -= 15
        if rsi < 30: score -= 10 # 极弱
        
        return max(0, min(100, score))

    def _calculate_rs_rating(self, close):
        """
        计算相对强弱 RS (简化版: 20日 ROC)
        """
        try:
            if len(close) > 20:
                roc_20 = (close.iloc[-1] / close.iloc[-20] - 1) * 100
                return round(roc_20, 2)
            return 0.0
        except:
            return 0.0

    def _detect_macd_divergence(self, close, macd_line):
        """
        检测 MACD 背离 (保留原逻辑)
        """
        try:
            # 简化版背离：价格新低但MACD未新低
            window = 20
            if len(close) < window: return "NONE"
            
            recent_close = close.tail(window)
            recent_macd = macd_line.tail(window)
            
            # 底背离判断
            price_min_idx = recent_close.idxmin()
            macd_min_idx = recent_macd.idxmin()
            
            # 如果价格最低点比MACD最低点发生得晚，且当前MACD高于最低点
            if price_min_idx > macd_min_idx:
                # 且当前是近期低位
                if close.iloc[-1] <= recent_close.min() * 1.02:
                    return "BOTTOM_DIVERGENCE"
            
            # 顶背离判断
            price_max_idx = recent_close.idxmax()
            macd_max_idx = recent_macd.idxmax()
            
            if price_max_idx > macd_max_idx:
                if close.iloc[-1] >= recent_close.max() * 0.98:
                    return "TOP_DIVERGENCE"
                    
            return "NONE"
        except:
            return "NONE"

    def _preprocess_data(self, df):
        """数据列名标准化"""
        if df is None or df.empty: return None
        df = df.copy()
        df.columns = [c.lower().strip() for c in df.columns]
        
        # 兼容 akshare 的 amount/volume
        if 'amount' in df.columns and 'volume' not in df.columns:
            df.rename(columns={'amount': 'volume'}, inplace=True)
            
        required = ['close', 'high', 'low', 'volume']
        if not all(col in df.columns for col in required):
            logger.error(f"数据缺失关键列: {df.columns}")
            return None
            
        return df

    def _get_safe_default_indicators(self, msg):
        """兜底返回"""
        return {
            'error': msg, 
            'quant_score': 0, 
            'rsi': 50, 
            'recent_gain': 0,
            'volatility_status': 'NORMAL', 
            'relative_strength': 0,
            'macd': {'trend': 'FLAT', 'divergence': 'NONE', 'hist': 0},
            'volume_analysis': {'vol_ratio': 1.0, 'status': 'NORMAL'},
            'ma_alignment': 'MIXED',
            'moving_averages': {'EMA5': 0, 'MA20': 0, 'MA60': 0}
        }
