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
        
        # 移除 when:2d，让 AI 自己判断时效性
        url = f"https://news.google.com/rss/search?q={search_q}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            return [item.find('title').text for item in root.findall('.//item')[:5]]
        except: return []

    def analyze_fund_v4(self, fund_name, tech, market_ctx, news):
        """
        微观审计：加入新闻权重判断
        """
        if not self.client: return {"comment": "AI Offline", "risk_alert": "", "adjustment": 0}

        tech_context = f"基准分:{tech['quant_score']}, 周线:{tech['trend_weekly']}, MACD:{tech['macd']['trend']}, OBV斜率:{tech['flow']['obv_slope']}"

        prompt = f"""
        # Role: 资深风控官 (Risk Officer)
        # Task: 结合新闻时效性，判断量价逻辑的真实性。
        
        # Data
        - 标的: {fund_name}
        - 宏观新闻流: {str(market_ctx)} (注意新闻后的时间标签!)
        - 个股舆情: {str(news)}
        - 技术面: {tech_context}

        # 核心逻辑 (News Weighting Logic)
        1. **看时间戳**：如果是"[3天前]"的新闻，无论标题多惊悚，**影响力归零**或视为**利好兑现**。
        2. **看预期差**：如果是老生常谈的消息（如"预计降息"），市场早就反应过了，不应加分。
        3. **看背离**：如果利好新闻满天飞（且是新鲜的），但OBV在流出，判定为**诱多陷阱**，重罚。

        # Output JSON
        {{
            "comment": "80字以内的深度洞察。明确指出新闻是'新鲜利好'还是'过期噪音'。",
            "risk_alert": "20字以内风险点。",
            "adjustment": (整数 -100 到 +50) 
        }}
        """

        try:
            res = self.client.chat.completions.create(model=self.model_name, messages=[{"role":"user","content":prompt}], response_format={"type":"json_object"}, temperature=0.4)
            data = json.loads(res.choices[0].message.content)
            if 'adjustment' not in data: data['adjustment'] = 0
            return data
        except Exception as e:
            logger.error(f"AI 分析错误: {e}")
            return {"comment": "AI服务异常", "risk_alert": "无", "adjustment": 0}

    def review_report(self, summary):
        """
        V11.12 CIO: 宏观权重的精确计算
        """
        if not self.client: return "<p>CIO Offline</p>"
        
        prompt = f"""
        # Role: 首席投资官 (CIO)
        # Mission: 过滤噪音，提炼信号。
        
        # 核心能力：新闻加权 (News Impact Assessment)
        你看到的宏观新闻流中包含了时间标签（如 [3天前]）。你必须严格执行以下过滤：
        - **过期信息 (>48h)**: 视为"背景噪音"，市场已充分定价 (Priced-in)，**不可作为今日交易的激进理由**。
        - **新鲜信息 (<24h)**: 视为"交易驱动"，重点评估其对持仓的冲击。
        
        # Strategy (双轨制)
        - Core (底仓): 扛过噪音，穿越周期。
        - Satellite (卫星): 利用新鲜消息博弈，消息落地即止盈。

        # Plan
        {summary}

        # Task
        1. **宏观定调**：基于新闻的新鲜程度，判断当前是"消息真空期"还是"剧烈博弈期"。
        2. **双轨评估**：如果新闻是旧的，要求卫星仓位收缩；如果新闻是新的且超预期，允许卫星仓位进攻。
        3. **最终裁决**：给出精确指令。

        # Output HTML
        结构：
        <div class='cio-seal'>CIO APPROVED</div>
        <h3>CIO 战略审计</h3>
        <p><strong>宏观定调：</strong>[指出新闻时效性对市场的影响]</p>
        <p><strong>双轨评估：</strong>...</p>
        <p class='warning'><strong>最终裁决：</strong>...</p>
        """
        try:
            res = self.client.chat.completions.create(model=self.model_name, messages=[{"role":"user","content":prompt}], temperature=0.6)
            return res.choices[0].message.content.strip().replace('```html', '').replace('```', '')
        except: return "CIO Audit Failed."

    def advisor_review(self, summary, market_ctx):
        """
        V11.12 Sage: 利用时间差博弈
        """
        if not self.client: return ""

        prompt = f"""
        # Role: 玄铁先生 (资产配置专家)
        # Perspective: 你最擅长利用**"场外基金的滞后性"**来反向收割。

        # Context: {market_ctx}
        # Plan: {summary}

        # 核心心法：【旧闻新炒必有诈】
        - 如果你发现宏观新闻都是"3天前"的，但今天ETF在大涨，告诉基民：**"这是主力利用旧消息在诱多，场外千万别追，进去就是接盘。"**
        - 如果新闻是"今早"的突发利空，告诉基民：**"T+1跑不掉了，躺倒装死，别在恐慌底割肉。"**

        # Task
        为场外基民提供基于**信息时效性**的实战建议。
        
        # Output HTML
        结构:
        <div class='advisor-title'>🗡️ 玄铁先生·场外实战复盘</div>
        <p><strong>【势·鉴伪】：</strong>[分析新闻新鲜度与盘面的关系]</p>
        <p><strong>【术·底仓】：</strong>...</p>
        <p><strong>【断·进攻】：</strong>...</p>
        """
        try:
            res = self.client.chat.completions.create(model=self.model_name, messages=[{"role":"user","content":prompt}], temperature=0.7)
            return res.choices[0].message.content.strip().replace('```html', '').replace('```', '')
        except: return "Advisor Offline."
