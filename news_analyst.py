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

    @retry(retries=3)
    def fetch_news_titles(self, keyword):
        """抓取谷歌新闻RSS (保持 V9 逻辑)"""
        if "红利" in keyword: search_q = "A股 红利指数 股息率"
        elif "白酒" in keyword: search_q = "白酒 茅台 批发价 库存"
        elif "美股" in keyword: search_q = "美联储 降息 纳斯达克"
        elif "港股" in keyword: search_q = "恒生科技 外资流向"
        elif "医疗" in keyword: search_q = "医药集采 创新药 出海"
        elif "黄金" in keyword: search_q = "黄金价格 避险 美元"
        elif "半导体" in keyword: search_q = "半导体 周期 国产替代"
        elif "军工" in keyword: search_q = "军工 订单 地缘"
        else: search_q = keyword + " 行业分析"

        url = f"https://news.google.com/rss/search?q={search_q} when:2d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            titles = [item.find('title').text for item in root.findall('.//item')[:5]]
            return titles
        except: return []

    def analyze_fund_v4(self, fund_name, tech_data, market_ctx, news_titles):
        """(角色1：行业研究员) 针对单个标的的分析"""
        if not self.client: return {"comment": "AI 未配置", "risk_alert": ""}

        # V10 Prompt: 加入 MACD/KDJ/资金流向
        prompt = f"""
        # Role: 资深行业研究员
        # Data
        - 标的: {fund_name}
        - 评分: {tech_data['quant_score']} (0-100)
        - 资金流(OBV斜率): {tech_data['flow']['obv_slope']} (正为流入，负为流出)
        - MACD: {tech_data['macd']['trend']}
        - KDJ_J值: {tech_data['kdj']['j']} (0超卖, 100超买)
        - 舆情: {" | ".join(news_titles)}

        # Task
        输出微型研报 (JSON)。
        1. comment (60字): 结合资金流向和技术指标，判断主力意图。
        2. risk_alert (15字): 指出最大风险。
        """
        # ... (调用代码与 V9 一致，略微省略以匹配 V10 结构)
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}, temperature=0.3
            )
            return json.loads(response.choices[0].message.content)
        except: return {"comment": "数据不足", "risk_alert": ""}

    def review_report(self, report_summary):
        """
        V10 新增：(角色2：首席投资官 CIO) 
        从宏观视角审计整份报告，进行独立验证和总结。
        """
        if not self.client: return "AI Reviewer Offline."

        prompt = f"""
        # Role
        你是华尔街顶级对冲基金的 **CIO (首席投资官)**。你正审阅由量化模型生成的今日交易计划。

        # Report Content
        {report_summary}

        # Task
        请以 **顶级专业散户和基金经理** 的双重视角，对这份报告进行 **"独立验证 (Independent Verification)"**。
        1. **宏观定调**：目前的策略（进攻/防守）是否符合当前的宏观环境？
        2. **板块点评**：针对报告中提及的重点板块，验证其逻辑是否成立。
        3. **风险审计**：指出模型可能忽略的致命风险。

        # Output Format (HTML Fragment)
        请直接输出一段 HTML 代码，包含 `<h3>CIO 独立审计</h3>` 和具体的点评内容。
        使用 `<p>` 分段，关键结论用 `<strong>` 加粗。语言风格：**毒舌、犀利、客观**。
        不要客气，如果模型建议买入但市场很差，请直接抨击。
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5 # 稍微提高创造性
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"<p>CIO 审计服务暂时不可用: {e}</p>"
