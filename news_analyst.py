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
        # 搜索逻辑保持不变，确保获取足够的信息源
        search_q = keyword + " 行业分析"
        if "红利" in keyword: search_q = "A股 红利指数 股息率"
        elif "美股" in keyword: search_q = "美联储 降息 纳斯达克 宏观"
        elif "半导体" in keyword: search_q = "半导体 周期 涨价"
        elif "黄金" in keyword: search_q = "黄金 避险 美元指数"
        
        url = f"https://news.google.com/rss/search?q={search_q} when:2d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            # 获取前 5 条，保证信息量足够 AI 归纳
            return [item.find('title').text for item in root.findall('.//item')[:5]]
        except: return []

    def analyze_fund_v4(self, fund_name, tech, market_ctx, news):
        """
        V10.9 深度分析模式：不计成本，只求洞察
        """
        if not self.client: return {"comment": "AI Offline", "risk_alert": ""}

        # 构建极其详细的上下文
        tech_context = f"""
        - 综合评分: {tech['quant_score']} (0-100)
        - 长期趋势(周线): {tech['trend_weekly']}
        - 短期RSI: {tech['rsi']} (30超卖, 70超买)
        - MACD结构: {tech['macd']['trend']} (Diff: {tech['macd']['diff']})
        - 资金博弈(OBV斜率): {tech['flow']['obv_slope']} (正值为流入)
        - 乖离率(Bias): {tech['bias_20']}%
        """

        # Deep Logic Prompt
        prompt = f"""
        # Role
        你是一位在华尔街顶级对冲基金工作超过20年的**首席宏观策略师**。你以**冷酷、犀利、反人性**的判断著称。你从不废话，只谈本质。

        # Context
        - 标的: {fund_name}
        - 宏观环境: {market_ctx}
        - 技术面画像: {tech_context}
        - 实时舆情: {str(news)}

        # Task
        请综合上述“数学事实”与“新闻舆情”，输出一份微型研报（JSON格式）。

        # Requirements
        1. **comment (深度逻辑)**: 
           - 限 80 字以内。
           - 必须解释**“量价形态”与“消息面”是否背离**？
           - 例如：如果利好满天飞但 OBV 流出，必须指出是“主力借利好出货”。
           - 拒绝模棱两可，必须给出明确的多空倾向（如“典型的诱多”、“倒车接人良机”）。
        
        2. **risk_alert (致命风险)**:
           - 限 20 字以内。
           - 指出当前最不可忽视的一个风险点（如：汇率贬值、技术破位、获利盘回吐）。

        # JSON Format
        {{
            "comment": "...",
            "risk_alert": "..."
        }}
        """

        try:
            res = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}, 
                temperature=0.4 # 稍微增加一点创造性，避免死板
            )
            return json.loads(res.choices[0].message.content)
        except Exception as e:
            logger.error(f"AI 分析错误: {e}")
            return {"comment": "AI分析服务暂时不可用", "risk_alert": "模型响应超时"}

    def review_report(self, summary):
        """
        V10.9 CIO 审计：全局视角的战略验证
        """
        if not self.client: return "<p>CIO Offline</p>"
        
        prompt = f"""
        # Role
        你是基金的 **CIO (首席投资官)**。这是交易员提交的今日操作计划，请进行最终审计。

        # Daily Plan
        {summary}

        # Task
        以**上帝视角**审视这份计划：
        1. **一致性检查**：我们的操作是否与当前的宏观环境（如北向资金流向）冲突？
        2. **板块轮动**：资金是在流向防御板块（红利/黄金）还是进攻板块（科技/券商）？这暗示了什么？
        3. **最终裁决**：给出一句总结性的评价，风格要犀利，直击要害。

        # Output
        直接输出一段 HTML 代码 (不包含 ```html 标记)。
        结构：
        <h3>CIO 战略审计 (V10.9)</h3>
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
