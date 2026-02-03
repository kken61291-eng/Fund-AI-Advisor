import google.generativeai as genai
import os
import json
import time
from utils import retry, logger

class NewsAnalyst:
    def __init__(self):
        # ... 初始化代码保持不变 (模型优先级列表) ...
        self.models_priority = ['gemini-1.5-flash', 'gemini-pro'] # 推荐 1.5-flash 处理结构化数据

    def analyze_fund(self, fund_info, tech_indicators, market_sentiment, news_titles):
        """
        Map 阶段：单只基金分析
        """
        # 构建极简的 Prompt (省 Token 神器)
        prompt = f"""
        角色：量化交易员。
        任务：分析基金【{fund_info['name']}】。
        
        【硬数据 (Python已计算)】
        1. 趋势: 日线{tech_indicators['trend']} | 周线{tech_indicators.get('weekly_trend', 'N/A')} (周线向下时慎做多)
        2. 动能: RSI={tech_indicators['rsi']} (超卖<30, 超买>70)
        3. 估值: 溢价率={tech_indicators.get('premium', 0)}% (正为溢价，>3%高危)
        
        【软数据 (舆情)】
        新闻标题: {str(news_titles)}
        宏观环境: {market_sentiment}
        
        请输出 JSON 格式决策：
        {{
            "signal": "BUY/SELL/HOLD",
            "position_adjust": "0.0 to 1.0",
            "reason": "简短理由(50字内)"
        }}
        """
        # ... 调用 API 代码保持不变 ...

    def generate_portfolio_summary(self, all_funds_analysis):
        """
        Reduce 阶段：生成总日报
        """
        # 将上面每只基金的 JSON 结果拼起来，让 AI 写个总结
        pass
