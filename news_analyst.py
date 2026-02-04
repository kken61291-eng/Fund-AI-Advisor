import os
import json
import requests
import xml.etree.ElementTree as ET
from openai import OpenAI
from utils import retry, logger

class NewsAnalyst:
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL", "https://api.siliconflow.cn/v1") 
        self.model_name = os.getenv("LLM_MODEL", "Pro/moonshotai/Kimi-K2.5") 
        self.client = OpenAI(api_key=self.api_key, base_url=self.base_url) if self.api_key else None

    @retry(retries=2)
    def fetch_news_titles(self, keyword):
        # 保持 V11.0 的精准搜索逻辑
        search_q = keyword + " 行业分析"
        if "红利" in keyword: search_q = "A股 红利指数 股息率"
        elif "美股" in keyword: search_q = "美联储 降息 纳斯达克 宏观"
        elif "半导体" in keyword: search_q = "半导体 周期 涨价"
        elif "黄金" in keyword: search_q = "黄金 避险 美元指数"
        
        url = f"https://news.google.com/rss/search?q={search_q} when:2d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            return [item.find('title').text for item in root.findall('.//item')[:5]]
        except: return []

    def analyze_fund_v4(self, fund_name, tech, market_ctx, news):
        # 保持 V11.0 的逻辑修正层不变
        if not self.client: return {"comment": "AI Offline", "risk_alert": "", "adjustment": 0}

        tech_context = f"""
        - 量化基准分: {tech['quant_score']} (0-100)
        - 周线趋势: {tech['trend_weekly']}
        - MACD形态: {tech['macd']['trend']}
        - 资金流向(OBV): {tech['flow']['obv_slope']}
        - RSI: {tech['rsi']}
        """

        prompt = f"""
        # Role: 20年经验首席风控官
        # Data
        - 标的: {fund_name}
        - 宏观: {str(market_ctx)}
        - 技术: {tech_context}
        - 舆情: {str(news)}
        # Task: 逻辑审计与评分修正
        # Rules
        - 价涨量缩/OBV流出 -> 扣分(-30~-50)
        - 宏观极差但评分高 -> 扣分(-20)
        - 形态完美 -> 0或+5
        # Output JSON
        {{
            "comment": "80字犀利点评",
            "risk_alert": "20字致命风险",
            "adjustment": (整数)
        }}
        """

        try:
            res = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}, 
                temperature=0.3
            )
            data = json.loads(res.choices[0].message.content)
            if 'adjustment' not in data: data['adjustment'] = 0
            return data
        except Exception as e:
            logger.error(f"AI 分析错误: {e}")
            return {"comment": "AI服务异常", "risk_alert": "无", "adjustment": 0}

    def review_report(self, summary):
        if not self.client: return "<p>CIO Offline</p>"
        
        prompt = f"""
        # Role: 基金首席投资官 (CIO)
        # Plan
        {summary}
        # Task
        以“鎏金岁月”的高贵、严谨风格，对今日策略进行最终盖章。
        # Output HTML (无markdown)
        结构:
        <div class='cio-seal'>CIO APPROVED</div>
        <h3>CIO 战略审计</h3>
        <p><strong>宏观定调：</strong>...</p>
        <p><strong>板块逻辑：</strong>...</p>
        <p class='warning'><strong>最终裁决：</strong>...</p>
        """
        try:
            res = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5
            )
            return res.choices[0].message.content.strip().replace('```html', '').replace('```', '')
        except: return "CIO Audit Failed."
