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
        if not self.client: return {"comment": "AI Offline", "risk_alert": ""}

        # 详细的上下文
        tech_context = f"""
        - 评分: {tech['quant_score']} (0-100)
        - 周线: {tech['trend_weekly']}
        - RSI: {tech['rsi']}
        - MACD: {tech['macd']['trend']}
        - 资金OBV: {tech['flow']['obv_slope']}
        """

        prompt = f"""
        # Role: 20年经验首席宏观策略师 (反人性、犀利)
        # Context
        - 标的: {fund_name}
        - 宏观: {market_ctx}
        - 技术: {tech_context}
        - 舆情: {str(news)}
        # Task
        输出微型研报 (JSON):
        1. "comment" (80字): 深度解析量价与消息的背离。主力是在诱多还是洗盘？给出明确多空方向。
        2. "risk_alert" (20字): 最致命的风险点。
        """

        try:
            res = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}, 
                temperature=0.4
            )
            return json.loads(res.choices[0].message.content)
        except Exception as e:
            logger.error(f"AI 分析错误: {e}")
            return {"comment": "AI服务异常", "risk_alert": "无"}

    def review_report(self, summary):
        if not self.client: return "<p>CIO Offline</p>"
        
        prompt = f"""
        # Role: 基金首席投资官 (CIO)
        # Plan
        {summary}
        # Task
        上帝视角审计。检查宏观一致性、板块轮动逻辑。
        # Output
        输出 HTML (不含markdown标记):
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
            content = res.choices[0].message.content.strip()
            # 清洗 markdown 标记，防止页面乱码
            return content.replace('```html', '').replace('```', '')
        except: return "CIO Audit Failed."
