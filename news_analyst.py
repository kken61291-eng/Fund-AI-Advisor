import requests
import json
import os
import re
from datetime import datetime
from utils import logger, retry

class NewsAnalyst:
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-V3.2"      
        self.model_strategic = "Pro/deepseek-ai/DeepSeek-R1"  

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.cls_headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.cls.cn/telegraph",
            "Origin": "https://www.cls.cn"
        }

    def _format_short_time(self, time_str):
        try:
            if str(time_str).isdigit():
                dt = datetime.fromtimestamp(int(time_str))
                return dt.strftime("%m-%d %H:%M")
            if len(str(time_str)) > 10:
                dt = datetime.strptime(str(time_str), "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%m-%d %H:%M")
            return str(time_str)
        except:
            return str(time_str)[:11]

    def _fetch_eastmoney_news(self):
        try:
            import akshare as ak
            df = ak.stock_news_em(symbol="要闻")
            raw_list = []
            for _, row in df.iterrows():
                title = str(row.get('title', ''))[:40]
                raw_list.append(f"[{str(row.get('public_time',''))[5:16]}] (东财) {title}")
            return raw_list[:5]
        except:
            return []

    def _fetch_cls_telegraph(self):
        raw_list = []
        url = "https://www.cls.cn/nodeapi/telegraphList"
        params = {"rn": 20, "sv": 7755}
        try:
            resp = requests.get(url, headers=self.cls_headers, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if "data" in data and "roll_data" in data["data"]:
                    for item in data["data"]["roll_data"]:
                        title = item.get("title", "")
                        content = item.get("content", "")
                        txt = title if title else content[:50]
                        time_str = self._format_short_time(item.get("ctime", 0))
                        raw_list.append(f"[{time_str}] (财社) {txt}")
        except Exception as e:
            logger.warning(f"财社源微瑕: {e}")
        return raw_list

    @retry(retries=2, delay=2)
    def fetch_news_titles(self, keywords_str):
        l1 = self._fetch_cls_telegraph()
        l2 = self._fetch_eastmoney_news()
        all_n = l1 + l2
        hits = []
        keys = keywords_str.split()
        seen = set()
        for n in all_n:
            clean_n = n.split(']')[-1].strip()
            if clean_n in seen: continue
            seen.add(clean_n)
            if any(k in n for k in keys):
                hits.append(n)
        return hits[:8] if hits else l1[:3]

    def _clean_json(self, text):
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
        code_match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
        if code_match:
            return code_match.group(1)
        obj_match = re.search(r'\{.*\}', text, re.DOTALL)
        if obj_match:
            return obj_match.group(0)
        return "{}"
    
    def _clean_html(self, text):
        text = text.replace("```html", "").replace("```", "").strip()
        return text

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro, news, risk):
        fuse_level = risk['fuse_level']
        fuse_msg = risk['risk_msg']
        trend_score = tech.get('quant_score', 50)
        rsi = tech.get('rsi', 50)
        macd = tech.get('macd', {})
        dif = macd.get('line', 0)
        dea = macd.get('signal', 0)
        hist = macd.get('hist', 0)
        vol_ratio = tech.get('risk_factors', {}).get('vol_ratio', 1.0)
        
        prompt = f"""
        【系统任务】
        你现在是玄铁量化基金的投研系统。请模拟 CGO(动量)、CRO(风控)、CIO(总监) 三位专家的辩论过程，并输出最终决策 JSON。
        
        【输入数据】
        标的: {fund_name}
        技术因子:
        - 趋势强度: {trend_score} (0-100)
        - RSI(14): {rsi}
        - MACD: DIF={dif}, DEA={dea}, Hist={hist}
        - 成交量偏离(VR): {vol_ratio}
        
        风险因子:
        - 熔断等级: {fuse_level} (0-3，>=2为限制交易)
        - 风控指令: {fuse_msg}
        
        舆情因子:
        - 相关新闻: {str(news)[:400]}

        --- 角色定义 ---
        1. **CGO (动量策略分析师)**
           - 核心职能: 右侧交易信号识别、赔率测算。
           - 纪律: 若趋势强度<50，直接输出HOLD。禁止模糊表述。

        2. **CRO (风控合规官)**
           - 核心职能: 左侧风险扫描、压力测试。
           - 纪律: 必须证明"为什么现在不该做"。禁止与CGO妥协。

        3. **CIO (投资总监)**
           - 核心职能: 战术裁决、仓位配置。
           - 纪律: 决策必须明确，禁止"观望"。

        【输出格式-严格JSON】
        请只输出 JSON，不要包含 Markdown 格式标记。确保 JSON 格式合法。
        {{
            "bull_view": "CGO观点 (50字以内)",
            "bear_view": "CRO观点 (50字以内)",
            "chairman_conclusion": "CIO裁决 (80字以内)",
            "adjustment": 整数数值 (-30 到 +30)
        }}
        """
        
        payload = {
            "model": self.model_tactical,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "max_tokens": 1000,
            "response_format": {"type": "json_object"}
        }
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            
            if resp.status_code != 200:
                logger.error(f"⚠️ API Error {resp.status_code}: {resp.text}")
                return {"bull_view": "API Error", "bear_view": "API Error", "comment": "API Error", "adjustment": 0}
            
            data = resp.json()
            if isinstance(data, str): data = json.loads(data)
            content = data['choices'][0]['message']['content']
            
            cleaned_json = self._clean_json(content)
            result = json.loads(cleaned_json)
            
            if "chairman_conclusion" in result and "comment" not in result:
                result["comment"] = result["chairman_conclusion"]
            return result
        except Exception as e:
            logger.error(f"AI Analysis Failed {fund_name}: {e}")
            raise e

    @retry(retries=2, delay=5)
    def review_report(self, report_text, macro_str):
        """
        [CIO 升级版]：合并了之前的宏观策略师职能
        CIO 现在全权负责宏观周期定位和微观账户管理
        """
        prompt = f"""
        【系统角色】
        你是玄铁量化基金的 **CIO (首席投资官)**。
        你现在拥有最高决策权，负责整合宏观周期与微观交易。
        
        【输入数据】
        1. 宏观新闻流: {macro_str[:600]}
        2. 基金持仓与交易报告: 
        {report_text}
        
        【任务要求 - 必须使用 DeepSeek-R1 思维链】
        1. **宏观定调**: 首先判断当前处于什么周期（库存/信用/情绪）？今天的宏观新闻说明了什么？
        2. **归因分析**: 今天的交易决策（买入/卖出）是否符合当前的宏观定调？
        3. **战略指令**: 给明天的交易定下基调（进攻/防御/游击）。
        
        【输出格式-HTML】
        <div class="cio-memo">
            <h3 style="border-left: 4px solid #1a237e; padding-left: 10px;">宏观与周期定调</h3>
            <p>(100字: 结合新闻流，判断当前市场所处的宏观象限。)</p>
            
            <h3 style="border-left: 4px solid #1a237e; padding-left: 10px;">交易归因审计</h3>
            <p>(100字: 点评今日的交易是否理智，是否符合宏观大势。)</p>
            
            <h3 style="border-left: 4px solid #d32f2f; padding-left: 10px;">CIO 总攻令</h3>
            <p>(80字: 下达明确的战略指令，如“全线进攻”、“防守反击”或“空仓避险”。)</p>
        </div>
        """
        
        payload = {
            "model": self.model_strategic, 
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4000,
            "temperature": 0.3 
        }
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            data = resp.json()
            if isinstance(data, str): data = json.loads(data)
            content = data['choices'][0]['message']['content']
            return self._clean_html(content)
        except:
            return "<p>CIO 正在进行深度战略审计...</p>"

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        """
        [顾问升级版]：独立审计员 (The Auditor)
        他不再写宏观报告，而是作为"红军"去挑战 CIO 的决策。
        他会模拟"自行搜索"（利用R1的知识库），寻找被忽略的风险。
        """
        prompt = f"""
        【系统角色】
        你是玄铁量化基金的 **独立顾问 (The Auditor)**。
        你的职责不是附和 CIO，而是**质疑**和**验证**。你怀疑目前的新闻源可能不完整。
        
        【输入数据】
        CIO看到的宏观面: {macro_str[:500]}
        CIO批准的交易: {report_text}
        
        【任务要求 - 模拟实盘验证】
        请调动你内部的知识库（模拟自行搜索近期市场热点），进行以下“红军对抗”测试：
        1. **盲点扫描**: 现在的市场有没有什么大事（如美联储动态、地缘政治、行业突发）是上述输入中**没提到**的？
        2. **逻辑漏洞**: CIO 的决策是否存在逻辑硬伤？（比如宏观利空却在做多？）
        3. **实盘推演**: 如果明天大盘暴跌 2%，目前的策略会发生什么？
        
        【输出格式-HTML结构化】
        <div class="advisor-report" style="background: #1a1a1a; padding: 15px; border: 1px dashed #ffd700;">
            <h4 style="color: #ffd700;">🕵️ 独立审计报告 (Red Team)</h4>
            
            <p><strong>[盲点警示]</strong>: <br>
            (指出可能被忽略的市场风险或新闻线索，模拟你的独立调研结果。)</p>
            
            <p><strong>[逻辑压力测试]</strong>: <br>
            (针对今日交易的质疑。例如："CIO在加仓半导体，但忽略了...")</p>
            
            <p><strong>[最终验证结论]</strong>: <br>
            (通过/有保留通过/建议驳回)</p>
        </div>
        """
        
        payload = {
            "model": self.model_strategic,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4000,
            "temperature": 0.5 # 温度稍高，增加发散性思维，模拟"搜索"
        }
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            data = resp.json()
            if isinstance(data, str): data = json.loads(data)
            content = data['choices'][0]['message']['content']
            return self._clean_html(content)
        except:
            return "<p>独立顾问正在进行场外尽调...</p>"
