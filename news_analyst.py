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
        
        # [V15.6 模型分层]
        # 战术执行 (快思考): V3.2 Pro - 负责 CGO/CRO/CIO 实时信号
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-V3.2"     
        
        # 战略推理 (慢思考): R1 Pro - 负责 宏观策略/复盘审计
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
        # [增强] 提取并打印思维链 (如果 R1 被意外调用到此处)
        think_match = re.search(r'<think>(.*?)</think>', text, re.DOTALL)
        if think_match:
            logger.info(f"🧠 [R1思维链]: {think_match.group(1).strip()[:100]}...") 
        
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
        try:
            match = re.search(r'\{.*\}', text, re.DOTALL)
            return match.group(0) if match else "{}"
        except: return "{}"

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro, news, risk):
        """
        [战术层] 实时信号生成
        模型: DeepSeek-V3.2 (极速版)
        角色: CGO + CRO + CIO (三位一体)
        """
        # 数据解构与格式化
        fuse_level = risk['fuse_level']
        fuse_msg = risk['risk_msg']
        
        trend_score = tech.get('quant_score', 50) # 近似趋势强度
        rsi = tech.get('rsi', 50)
        macd = tech.get('macd', {})
        dif = macd.get('line', 0)
        dea = macd.get('signal', 0)
        hist = macd.get('hist', 0)
        vol_ratio = tech.get('risk_factors', {}).get('vol_ratio', 1.0)
        
        # 构建机构级 Prompt (融合 CGO/CRO/CIO)
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
           - 分析框架: 确认趋势(均线/MACD) -> 验证动量(RSI) -> 确认量能(VR>1.2)。
           - 纪律: 若趋势强度<50，直接输出HOLD。禁止模糊表述。

        2. **CRO (风控合规官)**
           - 核心职能: 左侧风险扫描、压力测试。
           - 压力测试: 熔断硬约束(>=2否决)、流动性折价(VR<0.6)、技术背离。
           - 纪律: 必须证明"为什么现在不该做"。禁止与CGO妥协。

        3. **CIO (投资总监)**
           - 核心职能: 战术裁决、仓位配置。
           - 决策矩阵: 
             - 胜率<40% 或 赔率<1:1.5 -> 否决
             - CRO风险等级=CRITICAL -> 否决
             - 胜率>60% 且 风险可控 -> 批准
           - 纪律: 决策必须明确，禁止"观望"。

        【输出格式-严格JSON】
        {{
            "bull_view": "CGO观点 (50字): 动量质量评估与赔率测算。无废话。",
            "bear_view": "CRO观点 (50字): 风险压力测试结果。无废话。",
            "chairman_conclusion": "CIO裁决 (80字): 最终决策逻辑(胜率x赔率)。明确仓位建议与止损位。",
            "adjustment": 整数数值 (-30 到 +30)
        }}
        """
        
        payload = {
            "model": self.model_tactical, # V3
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2, # 低温，确保结构化和纪律性
            "max_tokens": 1000,
            "response_format": {"type": "json_object"}
        }
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if resp.status_code != 200:
                logger.error(f"API Error: {resp.text[:100]}")
                raise Exception(f"API {resp.status_code}")
                
            data = resp.json()
            if isinstance(data, str): data = json.loads(data)
            content = data['choices'][0]['message']['content']
            
            # 键名映射 (兼容 main.py)
            result = json.loads(self._clean_json(content))
            if "chairman_conclusion" in result and "comment" not in result:
                result["comment"] = result["chairman_conclusion"]
            return result
        except Exception as e:
            logger.error(f"AI战术分析异常 {fund_name}: {e}")
            raise e

    @retry(retries=2, delay=5)
    def review_report(self, report_text):
        """
        [战略层] 机构级复盘备忘录
        模型: DeepSeek-R1 (深度推理)
        角色: CIO (战略审计版)
        """
        prompt = f"""
        【系统角色】
        你是玄铁量化基金的 **CIO (投资总监)**。
        请撰写一份【机构级市场复盘备忘录】 (CIO Memo)。
        
        【输入数据】
        全市场交易汇总:
        {report_text}
        
        【深度分析要求 - 必须使用 DeepSeek-R1 思维链】
        1. **收益归因**: 拆解Alpha来源（择时/选股/风格），识别是"运气"还是"能力"。
        2. **风险归因**: 风险主要来自系统性暴露(Beta)还是特异性风险？是否在预算内？
        3. **策略失效检测**: 当前市场Regime（如高波/低波/震荡）是否导致策略暂时失效？
        
        【输出格式-HTML】
        <div class="cio-memo">
            <h3 style="border-left: 4px solid #1a237e; padding-left: 10px;">宏观环境审视</h3>
            <p>(100字: 流动性评估与风险偏好审计。)</p>
            
            <h3 style="border-left: 4px solid #1a237e; padding-left: 10px;">收益与风险归因</h3>
            <p>(100字: 基于数据的归因分析。拆解Alpha来源。)</p>
            
            <h3 style="border-left: 4px solid #d32f2f; padding-left: 10px;">CIO战术指令</h3>
            <p>(80字: 总仓位控制、风险敞口调整与明日重点监控阈值。)</p>
        </div>
        """
        
        payload = {
            "model": self.model_strategic, # R1
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 3000,
            "temperature": 0.3 # 适度严谨
        }
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            data = resp.json()
            if isinstance(data, str): data = json.loads(data)
            content = data['choices'][0]['message']['content']
            
            # 记录思维链日志
            think_match = re.search(r'<think>(.*?)</think>', content, re.DOTALL)
            if think_match:
                logger.info(f"🧠 [CIO深度归因]: {think_match.group(1).strip()[:200]}...")
            
            clean_content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL)
            return self._clean_html(clean_content)
        except:
            return "<p>CIO 正在进行深度战略审计...</p>"

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        """
        [战略层] 周期策略报告
        模型: DeepSeek-R1 (深度推理)
        角色: 首席宏观策略师
        """
        prompt = f"""
        【系统角色】
        你是玄铁量化基金的 **首席宏观策略师**。
        你使用DeepSeek-R1的深度推理能力，识别非线性关系与预期差。
        
        【输入数据】
        宏观背景: {macro_str[:400]}
        市场数据: {report_text}
        
        【推理要求 - 必须使用 DeepSeek-R1 思维链】
        1. **周期定位**: 当前处于三周期（库存/信用/货币）的什么阶段？历史对标？
        2. **预期差识别**: 市场当前price in了什么宏观假设？哪些存在修正风险？
        3. **策略映射**: 基于周期位置，最优配置策略是什么？（哑铃/杠铃/卫星）
        
        【输出格式-HTML结构化】
        <div class="macro-report">
            <h4 style="color: #ffd700;">【势·周期定位】</h4>
            <p>(100字: 库存/信用/货币周期定位。历史对标。)</p>
            
            <h4 style="color: #ffd700;">【术·预期差分析】</h4>
            <p>(100字: 市场隐含假设与潜在修正风险点。)</p>
            
            <h4 style="color: #ffd700;">【断·战略配置】</h4>
            <p>(80字: 基于周期的配置框架与战术偏离建议。)</p>
        </div>
        """
        
        payload = {
            "model": self.model_strategic, # R1
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 3000,
            "temperature": 0.4 # 允许非线性推理
        }
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            data = resp.json()
            if isinstance(data, str): data = json.loads(data)
            content = data['choices'][0]['message']['content']
            
            # 记录思维链日志
            think_match = re.search(r'<think>(.*?)</think>', content, re.DOTALL)
            if think_match:
                logger.info(f"🧠 [策略师推演]: {think_match.group(1).strip()[:200]}...")
            
            clean_content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL)
            return self._clean_html(clean_content)
        except:
            return "<p>首席策略师正在闭关推演...</p>"
            
    def _clean_html(self, text):
        text = text.replace("```html", "").replace("```", "").strip()
        return text
