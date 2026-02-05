import requests
import json
import os
import re
import time
import akshare as ak
from datetime import datetime
from utils import logger, retry

class NewsAnalyst:
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        self.model = os.getenv("LLM_MODEL", "gpt-3.5-turbo")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    @retry(retries=2, delay=2)
    def fetch_news_titles(self, keyword):
        """行业新闻抓取"""
        if not keyword: return []
        news_list = []
        try:
            df = ak.stock_news_em(symbol="要闻")
            keys = keyword.split()
            for _, row in df.iterrows():
                title = str(row.get('title', ''))
                if any(k in title for k in keys):
                    news_list.append(f"[{row.get('public_time','')[-5:]}] {title}")
            if not news_list:
                return [f"近期无'{keyword}'直接相关资讯，需参考宏观大势。"]
            return news_list[:5] 
        except Exception as e:
            logger.warning(f"行业新闻抓取失败 {keyword}: {e}")
            return ["数据源暂时不可用"]

    def _clean_json(self, text):
        try:
            match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
            if match: return match.group(1)
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match: return match.group(0)
            return text
        except: return text

    # --- 1. 底层：投委会 (针对单个标的) ---
    @retry(retries=2, delay=2)
    def analyze_fund_v4(self, fund_name, tech_indicators, macro_summary, sector_news):
        # ... (投委会逻辑保持不变，为节省篇幅，这里直接复用 V14.2 的逻辑) ...
        # 请确保此处代码与之前版本一致，包含 CGO/CRO/Chairman 的辩论 Prompt
        
        score = tech_indicators.get('quant_score', 50)
        trend = tech_indicators.get('trend_weekly', '无趋势')
        valuation = tech_indicators.get('valuation_desc', '未知')
        obv_slope = tech_indicators.get('flow', {}).get('obv_slope', 0)
        
        if obv_slope > 1.5: money_flow = "主力大幅抢筹"
        elif obv_slope > 0: money_flow = "温和流入"
        elif obv_slope < -1.5: money_flow = "主力坚决出货"
        else: money_flow = "资金流出"
        
        vol_ratio = tech_indicators.get('risk_factors', {}).get('vol_ratio', 1.0)
        if vol_ratio < 0.6: volume_status = "极度缩量"
        elif vol_ratio < 0.8: volume_status = "缩量"
        elif vol_ratio > 2.0: volume_status = "放量滞涨" if score < 40 else "放量上攻"
        else: volume_status = "量能正常"

        prompt = f"""
        你现在是【玄铁基金投委会】的会议记录员。对标的【{fund_name}】进行投资决策。

        ### 📜 投委会最高宪章
        1. **重剑无锋**：只吃周期和趋势的钱。
        2. **数据为王**：硬数据(估值/资金) 权重 > 新闻情绪。
        3. **厌恶风险**：生存第一，宁可踏空不可套牢。

        ### 📊 标的硬数据
        - 战术评分: {score}分
        - 周期估值: {valuation}
        - 资金流向: {money_flow}
        - 量能状态: {volume_status}
        - 周线趋势: {trend}

        ### 🌍 情报
        - 宏观: {macro_summary[:200]}
        - 行业: {str(sector_news)[:500]}

        ### 🗣️ 模拟委员发言
        **1. 🦊 CGO (多头):** 贪婪，找利好，强调资金流入或低估。
        **2. 🐻 CRO (空头):** 恐惧，找背离，强调缩量或利好出尽。
        **3. ⚖️ 主席 (裁决):** 听取辩论，结合硬数据权重，给出最终修正分(-30~+30)和定调。

        必须返回 JSON:
        {{
            "bull_view": "CGO观点(30字)",
            "bear_view": "CRO观点(30字)",
            "chairman_conclusion": "主席裁决(50字)",
            "adjustment": 整数,
            "risk_alert": "无"或"风险内容"
        }}
        """

        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "max_tokens": 1000
        }
        
        try:
            response = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if response.status_code != 200: return self._fallback_result()
            data = json.loads(self._clean_json(response.json()['choices'][0]['message']['content']))
            return {
                "bull_say": data.get("bull_view", "观点模糊"),
                "bear_say": data.get("bear_view", "风险不明"),
                "comment": data.get("chairman_conclusion", "需人工复核"),
                "adjustment": int(data.get("adjustment", 0)),
                "risk_alert": data.get("risk_alert", "无")
            }
        except Exception as e:
            logger.error(f"投委会崩溃 {fund_name}: {e}")
            return self._fallback_result()

    def _fallback_result(self):
        return {"bull_say": "数据不足", "bear_say": "风险未知", "comment": "API异常，维持原判", "adjustment": 0, "risk_alert": "API Error"}

    # --- 2. 中层：CIO 战略审计 (针对全市场汇总) ---
    @retry(retries=2, delay=2)
    def review_report(self, report_text):
        """
        CIO 视角：审核所有投委会的决定是否符合宏观逻辑
        """
        prompt = f"""
        你是【玄铁量化】的首席投资官 (CIO)。你刚收到了各板块投委会的决策汇总。
        请进行【战略审计】，输出一段 HTML 格式的总结。

        【投委会决策汇总】
        {report_text}

        【你的任务】
        1. **宏观定调**：当前市场是进攻期、防御期还是震荡期？
        2. **双轨评估**：
           - 核心仓(沪深300/红利)的决策是否稳健？
           - 卫星仓(科技/周期)的决策是否过于激进？
        3. **最终裁决**：指出上述决策中你认为最正确的一个，和风险最大的一个。
        4. **仓位建议**：给出一个总仓位建议 (0-100%)。

        请用 HTML 格式输出 (不包含 ```html 标记)，结构如下：
        <p><b>宏观定调：</b>...</p>
        <p><b>双轨评估：</b>...</p>
        <p><b>最终裁决：</b>...</p>
        <p><b>总仓位建议：</b>...</p>
        """
        
        return self._call_llm_text(prompt, "CIO 战略审计")

    # --- 3. 顶层：玄铁先生复盘 (哲学视角) ---
    @retry(retries=2, delay=2)
    def advisor_review(self, report_text, macro_str):
        """
        玄铁先生视角：重剑无锋，大巧不工
        """
        prompt = f"""
        你是【玄铁先生】，一位崇尚"重剑无锋"投资哲学的隐世高手。
        请阅读今日的宏观面和投委会决议，写一段【场外实战复盘】。

        【宏观面】{macro_str}
        【决议表】{report_text}

        请用三个段落进行点评 (HTML格式)：
        1. **【势·验证】**：目前的市场趋势（势）是否明确？如果是缩量上涨，痛斥其为诱多；如果是放量大跌，指出其为黄金坑。
        2. **【术·底仓】**：点评红利或核心资产的配置逻辑。强调"不败"的重要性。
        3. **【断·进攻】**：点评进攻板块（如传媒、科技）。告诫不要追高，要像猎人一样耐心等待。

        风格要求：语言犀利，多用比喻，带有武侠和哲学气息，不讲废话。
        输出格式：
        <h4>【势·验证】</h4><p>...</p>
        <h4>【术·底仓】</h4><p>...</p>
        <h4>【断·进攻】</h4><p>...</p>
        """
        return self._call_llm_text(prompt, "玄铁先生复盘")

    def _call_llm_text(self, prompt, task_name):
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.5,
            "max_tokens": 1200
        }
        try:
            response = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if response.status_code == 200:
                return response.json()['choices'][0]['message']['content']
            return f"{task_name} 生成失败: API Error"
        except Exception as e:
            logger.error(f"{task_name} 失败: {e}")
            return f"{task_name} 暂时缺席 (网络波动)"
