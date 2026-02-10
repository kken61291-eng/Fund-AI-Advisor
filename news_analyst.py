import requests
import json
import os
import re
import akshare as ak
import time
import random
from datetime import datetime
from utils import logger, retry, get_beijing_time

class NewsAnalyst:
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        # 战术执行 (快思考): V3.2 - 负责 CGO/CRO/CIO 实时信号
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-V3.2"      
        # 战略推理 (慢思考): R1 - 负责 宏观策略/复盘审计
        self.model_strategic = "Pro/deepseek-ai/DeepSeek-R1"  

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # [RAG] 加载板块实战经验库
        self.knowledge_base = self._load_knowledge_base()

    def _load_knowledge_base(self):
        """加载 JSON 经验库，若不存在则返回空"""
        try:
            if os.path.exists('knowledge_base.json'):
                with open('knowledge_base.json', 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.warning(f"⚠️ 无法加载经验库: {e}")
            return {}

    def _fetch_live_patch(self):
        try:
            time.sleep(1)
            df = ak.stock_news_em(symbol="要闻")
            news = []
            for i in range(min(5, len(df))):
                title = str(df.iloc[i].get('新闻标题') or df.iloc[i].get('title'))
                t = str(df.iloc[i].get('发布时间') or df.iloc[i].get('public_time'))
                if len(t) > 10: t = t[5:16] 
                news.append(f"[{t}] {title} (Live)")
            return news
        except:
            return []

    def get_market_context(self, max_length=20000):
        news_lines = []
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        file_path = f"data_news/news_{today_str}.jsonl"
        
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            item = json.loads(line)
                            t_str = str(item.get('time', ''))
                            if len(t_str) > 10: t_str = t_str[5:16]
                            news_lines.append(f"[{t_str}] {item.get('title')}")
                        except: pass
            except Exception as e:
                logger.error(f"读取新闻缓存失败: {e}")
        
        live_news = self._fetch_live_patch()
        if live_news:
            news_lines.extend(live_news)
            
        unique_news = []
        seen = set()
        for n in reversed(news_lines):
            if n not in seen:
                seen.add(n)
                unique_news.append(n)
        
        final_text = "\n".join(unique_news)
        
        if len(final_text) > max_length:
            return final_text[:max_length] + "\n...(早期消息已截断)"
        
        return final_text if final_text else "今日暂无重大新闻。"

    def _clean_json(self, text):
        try:
            text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
            code_match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
            if code_match: return code_match.group(1)
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1: return text[start:end+1]
            return "{}"
        except: return "{}"
    
    def _clean_html(self, text):
        text = text.replace("```html", "").replace("```", "").strip()
        return text

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro, news, risk, strategy_type="core"):
        """
        [战术层] 联邦投委会辩论系统 (V3.2) - RAG 增强版
        """
        # 1. 检索 RAG 经验
        kb_data = self.knowledge_base.get(strategy_type, {})
        expert_rules = "\n".join([f"- {r}" for r in kb_data.get('rules', [])])
        if not expert_rules: expert_rules = "- 无特殊经验，按常规逻辑分析。"

        # 2. 数据解构
        fuse_level = risk['fuse_level']
        fuse_msg = risk['risk_msg']
        trend_score = tech.get('quant_score', 50)
        rsi = tech.get('rsi', 50)
        macd = tech.get('macd', {})
        macd_trend = macd.get('trend', '未知')
        macd_hist = macd.get('hist', 0)
        vol_ratio = tech.get('risk_factors', {}).get('vol_ratio', 1.0)
        
        rsi_zone = "超买(>70)" if rsi > 70 else "超卖(<30)" if rsi < 30 else "中性(30-70)"
        vol_signal = "放量(>1.2)" if vol_ratio > 1.2 else "缩量(<0.8)" if vol_ratio < 0.8 else "常态(0.8-1.2)"
        fuse_veto = "TRUE" if fuse_level >= 2 else "FALSE"

        # [修改点] Prompt 品牌名称替换为 "鹊知风"
        prompt = f"""
        【系统架构】鹊知风投委会 | RAG增强模式
        
        【标的信息】
        标的: {fund_name} (策略类型: {strategy_type})
        趋势强度: {trend_score}/100 | RSI: {rsi}({rsi_zone}) | MACD: {macd_trend} | VR: {vol_ratio}({vol_signal})
        熔断状态: Level{fuse_level} | 硬约束: {fuse_msg}
        
        【💀 鹊知风实战经验库 (RAG Knowledge)】
        (请务必优先遵守以下经验，甚至可以覆盖技术指标的结论！)
        {expert_rules}
        
        【舆情因子】
        {str(news)[:15000]}

        【角色指令】
        **CGO (进攻)**: 引用经验库中的进攻逻辑。若经验库提示"忽略超买/忽略缩量"，则必须执行，寻找做多理由。
        **CRO (防守)**: 引用经验库中的防守逻辑。若经验库提示"忽略拥挤度"，则不要用拥挤度作为反对理由。
        **CIO (裁决)**: 
        - 你的最高指令是"知行合一"。
        - 如果技术指标显示卖出，但【经验库】提示这是"假摔/洗盘"，请裁决 HOLD 或 EXECUTE。
        - 如果是跨境ETF (纳指/日经)，严禁使用"缩量/VR低"作为拒绝理由 (根据经验库)。
        
        【决策矩阵】
        - EXECUTE: 趋势强且符合经验库逻辑。
        - REJECT: 触发硬性熔断，或逻辑完全破位。
        - HOLD: 其他情况。

        【输出格式-严格JSON】
        {{
            "bull_view": "CGO观点 (80字)",
            "bear_view": "CRO观点 (80字)",
            "chairman_conclusion": "CIO裁决 (100字): 必须明确引用'经验库'中的规则来支持你的决定。",
            "decision": "EXECUTE|REJECT|HOLD",
            "position_pct": "具体仓位%",
            "adjustment": -30到+30,
            "confidence": 0-100,
            "key_risk": "风险点"
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
                logger.error(f"API Error {resp.status_code}: {resp.text}")
                return {"bull_view": "API Error", "bear_view": "API Error", "comment": "API Error", "adjustment": 0}
            
            content = resp.json()['choices'][0]['message']['content']
            result = json.loads(self._clean_json(content))
            
            # 硬约束：代码层强制执行熔断
            if fuse_level >= 2:
                result['decision'] = 'REJECT'
                result['adjustment'] = -30
                result['position_pct'] = '0%'
                result['chairman_conclusion'] = f'[系统熔断] 熔断等级{fuse_level}触发。AI原话: {result.get("chairman_conclusion", "")}'
                result['confidence'] = 100

            if "chairman_conclusion" in result and "comment" not in result:
                result["comment"] = result["chairman_conclusion"]
            return result
        except Exception as e:
            logger.error(f"AI Analysis Failed {fund_name}: {e}")
            return {"bull_view": "解析失败", "bear_view": "解析失败", "comment": "JSON Error", "adjustment": 0}

    @retry(retries=2, delay=5)
    def review_report(self, report_text, macro_str):
        """
        [战略层] CIO 复盘备忘录 (R1) - 完整 HTML 版
        """
        current_date = datetime.now().strftime("%Y年%m月%d日")
        # [修改点] 角色名替换为 "鹊知风CIO"
        prompt = f"""
        【系统角色】鹊知风CIO | 机构级复盘备忘录 | 日期: {current_date}
        
        【输入数据】
        宏观环境: {macro_str[:2000]}
        交易明细: {report_text[:3000]}
        
        【深度推理任务-必须使用思维链】
        
        任务1: 精确归因计算 (请展示计算逻辑)
        - 择时贡献: 仓位调整带来的潜在收益/亏损
        - 选股贡献: 标的选择带来的影响
        - 风格贡献: 价值/成长因子暴露
        - 运气成分: 无法解释的残差
        
        任务2: 策略适配评估
        - 基于近5日表现，判断当前市场Regime(高波/低波/震荡)
        - 当前策略是否适配? 若不适配，切换成本是多少?
        
        【输出格式-HTML结构化】
        <div class="cio-memo">
            <h3 style="border-left: 4px solid #1a237e; padding-left: 10px;">宏观环境审视</h3>
            <p>流动性评分[X/10] | 风险偏好评估。关键事件影响。[100字]</p>
            
            <h3 style="border-left: 4px solid #1a237e; padding-left: 10px;">收益与风险归因 (精确计算)</h3>
            <p>超额收益 = 择时[X%] + 选股[Y%] + 风格[Z%] + 运气[W%]</p>
            <p>核心驱动: [最大贡献因子] | 异常点: [需解释]</p>
            
            <h3 style="border-left: 4px solid #d32f2f; padding-left: 10px;">CIO战术指令</h3>
            <p>总仓位[具体%] | 风险预算消耗[X/Y] | 明日监控[具体阈值] | 交易纪律。</p>
            
            <h3 style="border-left: 4px solid #d32f2f; padding-left: 10px;">策略状态评估</h3>
            <p>当前Regime[高波/低波/震荡] | 策略适配度[高/中/低]。是否降速[是/否]。</p>
        </div>
        """
        return self._call_r1(prompt)

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        """
        [审计层] Red Team 顾问 (R1) - 完整 HTML 版
        """
        current_date = datetime.now().strftime("%Y年%m月%d日")
        # [修改点] 角色名替换为 "鹊知风Red Team"
        prompt = f"""
        【系统角色】鹊知风Red Team | 独立审计顾问 | 日期: {current_date}
        【任务目标】通过结构化质疑，发现CIO决策中的认知偏差与逻辑漏洞。
        
        【输入数据】
        宏观数据: {macro_str[:2000]}
        CIO交易: {report_text[:3000]}
        
        【强制纪律】
        1. **必须找到至少1个** CIO的逻辑漏洞或数据盲区。
        2. 禁止无原则通过，评分>=80时必须附带"保留意见"。
        3. 若总分<60，必须直接驳回，并明确"重新提交条件"。

        【五问压力测试-必须逐一打分(0-20分)】
        Q1: 确认偏误检测? (CIO是否只看了利好忽略了利空?)
        Q2: 归因谬误检测? (收益是能力还是运气?)
        Q3: 宏观错配检测? (微观操作是否逆宏观大势?)
        Q4: 流动性幻觉检测? (成交量是否支撑?)
        Q5: 尾部风险盲区? (如果明天大跌2%，策略会怎样?)
        
        【输出格式-HTML结构化】
        <div class="red-team-report">
            <h4 style="color: #c62828;">【盲点警示 (必须至少1条)】</h4>
            <p>风险点: [具体描述] | 概率: [高/中/低] | 潜在影响: [量化评估]</p>
            
            <h4 style="color: #c62828;">【五问评分】</h4>
            <p>Q1确认偏误: [X]/20 | 证据: ...</p>
            <p>Q3宏观错配: [X]/20 | 证据: ...</p>
            <p>Q5尾部盲区: [X]/20 | 证据: ...</p>
            
            <h4 style="color: #c62828;">【验证结论】</h4>
            <p>总分: [SUM]/100 | 结论: [通过/有条件通过/驳回]</p>
            <p>强制修正建议: [若<80分，列出必须修正项]</p>
        </div>
        """
        return self._call_r1(prompt)

    def _call_r1(self, prompt):
        payload = {
            "model": self.model_strategic, 
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 4000,
            "temperature": 0.3 
        }
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            content = resp.json()['choices'][0]['message']['content']
            return self._clean_html(content)
        except:
            return "<p>分析生成中...</p>"
