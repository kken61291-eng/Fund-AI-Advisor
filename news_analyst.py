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
        if code_match: return code_match.group(1)
        obj_match = re.search(r'\{.*\}', text, re.DOTALL)
        if obj_match: return obj_match.group(0)
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
        - 相关新闻: {str(news)[:2000]}

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
        # 注入当前日期，强制 AI 关注当下
        current_date = datetime.now().strftime("%Y年%m月%d日")
        
        prompt = f"""
        【系统角色】
        你是玄铁量化基金的 **CIO (首席投资官)**。
        今天是 **{current_date}**。
        你全权负责整合宏观周期与微观交易。
        
        【输入数据】
        1. 今日宏观快讯 (来源: 实时抓取): 
        {macro_str[:800]}
        
        2. 基金持仓与交易报告: 
        {report_text}
        
        【任务要求 - 必须使用 DeepSeek-R1 思维链】
        1. **宏观定调**: 基于【今日宏观快讯】判断当前市场情绪（进攻/防御/震荡）。**禁止编造快讯中未提及的新闻。**
        2. **归因分析**: 今天的交易决策是否符合上述宏观定调？
        3. **战略指令**: 给明天的交易定下基调。
        
        【输出格式-HTML】
        <div class="cio-memo">
            <h3 style="border-left: 4px solid #1a237e; padding-left: 10px;">宏观与周期定调 ({current_date})</h3>
            <p>(100字: 仅基于提供的快讯进行总结，不要引用过时数据。)</p>
            
            <h3 style="border-left: 4px solid #1a237e; padding-left: 10px;">交易归因审计</h3>
            <p>(100字: 点评今日交易的合理性。)</p>
            
            <h3 style="border-left: 4px solid #d32f2f; padding-left: 10px;">CIO 总攻令</h3>
            <p>(80字: 明确的战术指令。)</p>
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
        # 注入当前日期
        current_date = datetime.now().strftime("%Y年%m月%d日")
        
        prompt = f"""
        【系统角色】
        你是玄铁量化基金的 **独立审计顾问 (The Auditor)**。
        今天是 **{current_date}**。
        你的职责是基于**已知的真实信息**，寻找 CIO 决策中的逻辑漏洞。
        
        【输入数据】
        1. 真实捕捉到的宏观新闻: 
        {macro_str[:800]}
        
        2. CIO批准的交易: 
        {report_text}
        
        【任务要求 - 红军对抗】
        请进行严格的逻辑审计：
        1. **信息完备性检查**: CIO 是否忽略了【真实捕捉到的宏观新闻】中的某条重磅利空/利好？（如果新闻列表为空或无重磅，请指出“当前缺乏关键宏观指引，建议谨慎”。**严禁编造新闻**。）
        2. **逻辑一致性**: CIO 的交易方向是否与新闻情绪背离？（例如：新闻利空却做多）
        3. **实盘推演**: 基于当前持仓，如果明日大盘波动，风险点在哪里？
        
        【输出格式-HTML结构化】
        <div class="advisor-report" style="background: #1a1a1a; padding: 15px; border: 1px dashed #ffd700;">
            <h4 style="color: #ffd700;">🕵️ 独立审计报告 ({current_date})</h4>
            
            <p><strong>[盲点警示]</strong>: <br>
            (基于输入新闻的客观检查。若无重大遗漏，回答“基于现有资讯，未发现明显盲点”。)</p>
            
            <p><strong>[逻辑压力测试]</strong>: <br>
            (针对今日交易的质疑。)</p>
            
            <p><strong>[最终验证结论]</strong>: <br>
            (通过/有保留通过/建议驳回)</p>
        </div>
        """
        
        payload = {
            "model": self.model_strategic,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4000,
            # [关键修改] 降低温度，抑制幻觉
            "temperature": 0.2 
        }
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            data = resp.json()
            if isinstance(data, str): data = json.loads(data)
            content = data['choices'][0]['message']['content']
            return self._clean_html(content)
        except:
            return "<p>独立顾问正在进行场外尽调...</p>"
