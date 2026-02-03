import requests
import xml.etree.ElementTree as ET
import google.generativeai as genai
import os
import json
import time
from utils import retry, logger

class NewsAnalyst:
    def __init__(self):
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key: raise ValueError("未设置 GEMINI_API_KEY")
        genai.configure(api_key=api_key)
        # 自动降级策略
        self.models_priority = ['gemini-2.5-flash', 'gemini-1.5-flash', 'gemini-pro']

    @retry(retries=3)
    def fetch_news_titles(self, keyword):
        """抓取新闻"""
        if "红利" in keyword: search_q = "中证红利 基金"
        elif "白酒" in keyword: search_q = "白酒板块 茅台"
        elif "纳斯达克" in keyword: search_q = "纳斯达克 美股"
        elif "黄金" in keyword: search_q = "黄金价格 金价"
        else: search_q = keyword + " 基金"

        url = f"https://news.google.com/rss/search?q={search_q} when:1d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            titles = [item.find('title').text for item in root.findall('.//item')[:6]] # 限制条数省Token
            return titles
        except:
            return []

    def analyze_fund_v4(self, fund_name, tech_data, market_ctx, news_titles):
        """
        V4.0 分析引擎：基于预计算指标进行决策
        """
        if not tech_data:
            return {"thesis": "数据不足", "action_advice": "观望"}

        news_text = "; ".join(news_titles) if news_titles else "无重大新闻"
        
        # 极简 Prompt，Token 消耗极低
        prompt = f"""
        角色：量化交易员。标的：{fund_name}。
        
        【硬数据 (Python计算)】
        1. 价格趋势: 日线{tech_data['trend_daily']} | 周线{tech_data['trend_weekly']} (周线DOWN时慎做多)
        2. 动能指标: RSI={tech_data['rsi']} (超卖<35, 超买>70)
        3. 均线乖离: 偏离MA20 {tech_data['bias_20']}%
        
        【宏观与舆情】
        市场风向: {market_ctx.get('north_label','未知')} ({market_ctx.get('north_money',0)}亿)
        新闻摘要: {news_text}
        
        请输出 JSON (Strict JSON):
        {{
            "thesis": "一句话核心逻辑(含技术+消息)",
            "pros": "利多因素",
            "cons": "利空因素",
            "action_advice": "买入/卖出/观望/强力买入",
            "risk_warning": "最大风险点"
        }}
        """

        for model_name in self.models_priority:
            try:
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(prompt)
                text = response.text.replace('```json', '').replace('```', '').strip()
                return json.loads(text)
            except Exception as e:
                logger.warning(f"模型 {model_name} 失败: {e}")
                time.sleep(1)
                continue
        
        return {"thesis": "AI服务暂时不可用", "action_advice": "观望", "pros":"", "cons":""}
