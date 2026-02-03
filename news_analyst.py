import requests
import xml.etree.ElementTree as ET
import os
import json
import time
from openai import OpenAI
from utils import retry, logger

class NewsAnalyst:
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL", "https://api.siliconflow.cn/v1") 
        self.model_name = os.getenv("LLM_MODEL", "deepseek-ai/DeepSeek-V3") 

        if not self.api_key:
            logger.warning("⚠️ 未检测到 LLM_API_KEY，AI 分析功能将跳过")
            self.client = None
        else:
            self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)

    @retry(retries=3)
    def fetch_news_titles(self, keyword):
        """抓取新闻"""
        if "红利" in keyword: search_q = "中证红利 股息率"
        elif "白酒" in keyword: search_q = "白酒 茅台 批发价"
        elif "纳斯达克" in keyword: search_q = "美联储 纳斯达克 降息"
        elif "黄金" in keyword: search_q = "黄金 避险 美元指数"
        elif "医疗" in keyword: search_q = "医药 集采 创新药"
        else: search_q = keyword + " 行业分析"

        url = f"https://news.google.com/rss/search?q={search_q} when:2d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            titles = [item.find('title').text for item in root.findall('.//item')[:10]]
            return titles
        except:
            return []

    def analyze_fund_v4(self, fund_name, tech_data, market_ctx, news_titles):
        """
        V5.0 绝对收益操盘引擎
        """
        if not self.client:
            return {"thesis": "未配置API", "action_advice": "观望", "confidence": 0, "pros":"", "cons":"", "glossary": {}}

        if not tech_data:
            return {"thesis": "数据不足", "action_advice": "观望", "confidence": 0, "pros":"", "cons":"", "glossary": {}}

        news_text = "; ".join(news_titles) if news_titles else "无重大特异性新闻"
        
        prompt = f"""
        # Role
        你是一位追求**绝对收益 (Absolute Return)** 的顶级对冲基金操盘手。你的唯一目标是**最大化利润**。你极度厌恶回撤，只有当**盈亏比 (Reward/Risk Ratio)** 超过 3:1 时，你才会扣动扳机。

        # Task
        深度分析标的【{fund_name}】，给出实战操作建议。

        # Data Input
        - **技术形态**: 周线趋势:{tech_data['trend_weekly']} | 日线趋势:{tech_data['trend_daily']}
        - **量化指标**: RSI={tech_data['rsi']} (低位钝化不等于底，高位钝化不等于顶); 乖离率={tech_data['bias_20']}%
        - **市场环境**: 北向资金:{market_ctx.get('north_label','未知')}; 舆情:{news_text}

        # Output Requirements (Strict JSON)
        请输出 JSON，字段如下：
        
        1. **thesis (逻辑核心)**: 100字左右。直击要害。判断当前是“主力洗盘”、“下跌中继”还是“主升浪”？**不要废话**，直接说能不能涨。
        2. **action_advice (交易指令)**: 必须从 [强力买入, 买入, 观望, 卖出, 坚决清仓] 中选一个。
        3. **confidence (信心系数)**: 0-10 分。10分代表“哪怕借钱也要买”，0分代表“垃圾”。(8分以上我们才会重仓)。
        4. **pros (利多)**: 2-3个最硬核的上涨理由。
        5. **cons (利空)**: 2-3个最致命的下跌风险。
        6. **risk_warning (风控线)**: 明确的止损位或离场条件。
        7. **glossary (名词解释)**: **非常重要**。请提取你在 `thesis`, `pros`, `cons` 中使用过的 1-2 个专业术语（如“底背离”、“乖离率”、“流动性陷阱”等），用**小白能听懂的大白话**解释它。
           格式: {{"术语1": "人话解释1", "术语2": "人话解释2"}}

        # Example Output
        {{
            "thesis": "当前属于周线级别的上涨中继。日线回踩20日均线不破，且RSI在50上方金叉，这是经典的'老鸭头'形态。叠加白酒批价企稳的利好，主力洗盘结束迹象明显，是绝佳的倒车接人机会。",
            "action_advice": "强力买入",
            "confidence": 9,
            "pros": "日线缩量回调到位; 北向资金连续3日净流入",
            "cons": "上方60日线有一定抛压",
            "risk_warning": "若有效跌破20日均线，逻辑证伪，立即离场。",
            "glossary": {{
                "老鸭头": "一种经典的技术形态，指股价经过上涨后短暂回调，像鸭头一样，随后通常会有一波大涨。",
                "缩量回调": "股价下跌但成交量变小，说明大家都不舍得卖，主力洗盘概率大。"
            }}
        }}
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": "You are a legendary trader. Output valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.2 
            )
            
            content = response.choices[0].message.content
            content = content.replace('```json', '').replace('```', '').strip()
            return json.loads(content)
            
        except Exception as e:
            logger.error(f"LLM 调用失败: {e}")
            return {"thesis": "AI 思考超时", "action_advice": "观望", "confidence": 0, "pros": "", "glossary": {}}
