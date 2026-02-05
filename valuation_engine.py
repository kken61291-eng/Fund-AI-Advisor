import akshare as ak
import pandas as pd
from datetime import datetime
from utils import logger, retry

class ValuationEngine:
    def __init__(self):
        self.cn_10y_yield = None
        
        # [V13.1] 静态映射表: 将通俗名称映射为官方指数代码
        # 必须使用中证(CSI)或国证代码
        self.INDEX_MAP = {
            "沪深300": "000300",
            "中证红利": "000922", 
            "中证煤炭": "399998",
            "全指证券公司": "399975",
            # 半导体指数较多，选用 "全指半导体" 作为行业锚
            "中华半导体": "H30184", 
            "全指半导体": "H30184",
            "半导体": "H30184"
        }

    @retry(retries=2)
    def _get_bond_yield(self):
        """获取中国10年期国债收益率"""
        if self.cn_10y_yield: return self.cn_10y_yield
        try:
            df = ak.bond_zh_us_rate()
            # 确保列名存在
            col = '中国国债收益率10年'
            if col in df.columns:
                val = df[col].iloc[-1]
                self.cn_10y_yield = val
                return val
            return 2.3
        except Exception as e:
            logger.warning(f"国债收益率获取失败: {e}")
            return 2.3

    @retry(retries=1)
    def get_valuation_status(self, index_name, strategy_type):
        """
        核心功能: 计算估值状态
        """
        # 1. 非权益类跳过
        if not index_name or strategy_type == 'commodity':
            return 1.0, "非权益类(默认适中)"

        # 2. 获取映射代码
        index_code = self.INDEX_MAP.get(index_name)
        if not index_code:
            logger.warning(f"未知指数名称: {index_name}，无法估值")
            return 1.0, "无估值锚"

        try:
            # 3. 使用中证官网接口 (最稳)
            # 接口: stock_zh_index_value_csindex
            # 返回列通常含: 日期, 市盈率1(PE-TTM), 市盈率2(LYR), 市净率1(PB)
            df = ak.stock_zh_index_value_csindex(symbol=index_code)
            
            if df.empty: return 1.0, "数据为空"
            
            # 中证接口列名可能是 "市盈率1" 或 "市盈率(PE)"，需做防御性处理
            pe_col = None
            for col in ["市盈率1", "市盈率(PE)", "PE1", "市盈率"]:
                if col in df.columns:
                    pe_col = col
                    break
            
            if not pe_col:
                return 1.0, "PE列缺失"

            # 4. 计算百分位
            current_pe = pd.to_numeric(df[pe_col], errors='coerce').iloc[-1]
            
            # 取近 5 年 (约1250个交易日)
            history = pd.to_numeric(df[pe_col], errors='coerce').tail(1250).dropna()
            
            if len(history) < 100:
                return 1.0, "历史数据不足"

            percentile = (history < current_pe).mean() # 0.0 - 1.0
            
            # 5. 策略逻辑 (V13.0)
            p_str = f"{int(percentile*100)}%"
            
            if strategy_type == 'core':
                if percentile < 0.20: return 1.5, f"极度低估(分位{p_str})"
                if percentile < 0.40: return 1.2, f"低估(分位{p_str})"
                if percentile > 0.80: return 0.5, f"高估(分位{p_str})"
                if percentile > 0.90: return 0.0, f"极度高估(分位{p_str})"
                return 1.0, "估值适中"

            elif strategy_type == 'satellite':
                if percentile < 0.10: return 1.2, "黄金坑(左侧)"
                if percentile > 0.85: return 0.0, "泡沫预警"
                return 1.0, "估值允许"
            
            elif strategy_type == 'dividend':
                bond = self._get_bond_yield()
                # 股息率 ≈ 1/PE (简化)
                div_yield = (1 / current_pe) * 100
                spread = div_yield - bond
                
                if spread > 2.5: return 2.0, f"历史机会(息差{spread:.1f}%)"
                if spread > 1.5: return 1.5, f"高性价比(息差{spread:.1f}%)"
                if spread < 0: return 0.0, f"性价比消失(息差{spread:.1f}%)"
                return 1.0, "红利适中"

            return 1.0, "逻辑未匹配"

        except Exception as e:
            logger.warning(f"估值计算异常 {index_name}({index_code}): {e}")
            return 1.0, "估值未知"
