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
    技术分析器 - V17.2 (适配 v3.5 四态架构 - CRO风控升级全量版)
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

            # [C] 趋势打分 (Trend Score 0-100) - 已接入CRO风控惩罚模型
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

            # --- 4. 风险收益评估指标 (新增部分) ---
            
            # 使用60日最高点作为理论目标价
            target_price = high.rolling(window=60, min_periods=1).max().iloc[-1]
            # 使用20日最低点作为理论止损价
            stop_loss = low.rolling(window=20, min_periods=1).min().iloc[-1]

            # 边缘情况处理：如果当前价格已经突破60日新高，利用ATR向外延展2倍作为新目标
            if target_price <= current_price:
                target_price = current_price + (atr * 2)

            # 边缘情况处理：如果当前价格已经跌破20日新低，利用ATR向下延展2倍作为极限止损
            if stop_loss >= current_price:
                stop_loss = current_price - (atr * 2)

            # 计算向上空间(%)和向下风险(%)
            upside_space = ((target_price - current_price) / current_price) * 100
            downside_risk = ((current_price - stop_loss) / current_price) * 100

            # 计算盈亏比 (避免除以0的情况)
            if downside_risk > 0:
                ratio = upside_space / downside_risk
            else:
                ratio = 999.0  # 代表技术面测算的下行风险极小

            indicators['risk_reward'] = {
                'target_price': round(target_price, 3),
                'stop_loss': round(stop_loss, 3),
                'upside_space_pct': round(upside_space, 2),
                'downside_risk_pct': round(downside_risk, 2),
                'ratio': round(ratio, 2)
            }

            return indicators

        except Exception as e:
            logger.error(f"❌ 技术分析计算异常: {e}", exc_info=True)
            return self._get_safe_default_indicators(str(e))

    def _calculate_trend_score(self, close, rsi, macd_hist, macd_val, ma20, ma60):
        """
        计算 0-100 的趋势分 (v3.5 CRO升级版)
        核心改动：引入非线性风控惩罚机制，解决“高位超买标的满分溢出”的致命缺陷。
        """
        score = 50
        price = close.iloc[-1]
        
        # 1. 基础结构分 (满分30)：奖励均线多头形态，但不给予过度溢价
        if price > ma20: score += 10
        if price > ma60: score += 10
        if ma20 > ma60: score += 10
        
        # 2. 动量质量分 (满分20)：仅在健康区间给予奖励
        # 修正原先只要RSI>50就无脑加分的逻辑，改为健康上涨区间才加分
        if 50 < rsi <= 75: score += 10
        if macd_val > 0: score += 5
        if macd_hist > 0: score += 5
        
        # 3. CRO 核心风控：非线性极致惩罚项 (直接击穿底分)
        
        # [核心惩罚 A] 乖离率 (Bias) 测算：严惩脱离均线的高位加速
        bias_20 = ((price - ma20) / ma20) * 100
        if bias_20 > 15:
            score -= 40  # 极度超买，直接剥夺满分可能，防追高核按钮
        elif bias_20 > 8:
            score -= 15  # 高度警惕，限制得分上限
            
        # [核心惩罚 B] RSI 极端情绪惩罚：严惩山顶狂热与深渊极寒
        if rsi > 85:
            score -= 40  # 情绪沸腾，散户接盘期，重度扣分
        elif rsi > 75:
            score -= 20
        elif rsi < 30:
            score -= 20  # 极度弱势，动能衰竭
        elif rsi < 40:
            score -= 10
            
        # [核心惩罚 C] 均线破位惩罚：趋势反转的左侧确认
        if price < ma20: score -= 15
        if price < ma60: score -= 20
        
        return int(max(0, min(100, score)))

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
            'moving_averages': {'EMA5': 0, 'MA20': 0, 'MA60': 0},
            'risk_reward': { # 新增默认兜底数据
                'target_price': 0.0,
                'stop_loss': 0.0,
                'upside_space_pct': 0.0,
                'downside_risk_pct': 0.0,
                'ratio': 0.0
            }
        }
