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
        # 战术执行 (快思考): V3.2 - 负责 CGO/CRO/CIO 实时信号 (低延迟，结构化强)
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-V3.2"      
        # 战略推理 (慢思考): R1 - 负责 宏观策略/复盘审计 (深度归因，非线性推理)
        self.model_strategic = "Pro/deepseek-ai/DeepSeek-R1"  

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

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
    def analyze_fund_v5(self, fund_name, tech, macro, news, risk):
        """
        [战术层] 联邦投委会辩论系统 (V3.2) - 机构级对抗版 (Ultimate)
        """
        # 数据解构与预计算 (数据锚定)
        fuse_level = risk['fuse_level']
        fuse_msg = risk['risk_msg']
        trend_score = tech.get('quant_score', 50)
        rsi = tech.get('rsi', 50)
        macd = tech.get('macd', {})
        macd_trend = macd.get('trend', '未知')
        macd_hist = macd.get('hist', 0)
        vol_ratio = tech.get('risk_factors', {}).get('vol_ratio', 1.0)
        
        # [关键] 计算量化阈值，强制模型引用
        rsi_zone = "超买(>70)" if rsi > 70 else "超卖(<30)" if rsi < 30 else "中性(30-70)"
        vol_signal = "放量(>1.2)" if vol_ratio > 1.2 else "缩量(<0.8)" if vol_ratio < 0.8 else "常态(0.8-1.2)"
        fuse_veto = "TRUE" if fuse_level >= 2 else "FALSE"

        prompt = f"""
        【系统架构】玄铁量化投委会 | 零和博弈机制
        
        【市场数据】
        标的: {fund_name}
        趋势强度: {trend_score}/100 | RSI: {rsi}({rsi_zone}) | MACD: {macd_trend}(Hist:{macd_hist}) | VR: {vol_ratio}({vol_signal})
        熔断状态: Level{fuse_level} | 硬约束: {fuse_msg} | VETO触发: {fuse_veto}
        
        【舆情因子】
        {str(news)[:15000]}

        【对抗机制-必须遵守】
        1. CGO与CRO立场必须对立，禁止达成共识。
        2. 若 VETO触发=TRUE，CRO必须无条件否决，CGO必须承认失败。
        3. CIO裁决必须明确：EXECUTE(执行)/REJECT(否决)/HOLD(观望)，禁止模糊表述。
        
        【角色指令】
        
        **CGO (动量策略师)** - 进攻方
        - 任务: 寻找做多信号。必须引用具体数据锚点 (如"趋势>60", "VR>1.2")。
        - 纪律: 若趋势<50，必须承认"无势可借"。

        **CRO (风控官)** - 防守方  
        - 任务: 执行压力测试。必须引用具体数据锚点 (如"RSI超买", "熔断Level").
        - 纪律: 若 fuse_level>=2，无需讨论其他，直接否决。

        **CIO (投资总监)** - 裁决者
        - 决策矩阵:
          - EXECUTE: 仅当 趋势>=60 AND RSI中性 AND VR>1.0 AND 无熔断。
          - REJECT: 若 熔断>=2 OR RSI>75 OR 趋势<40。
          - HOLD: 其他情况。
        - 仓位指令:
          - 强信号(趋势>75): 15-20%
          - 中信号(趋势60-75): 8-12%
          - 弱信号: 0-5%
        
        【输出格式-强制JSON，禁止省略字段】
        {{
            "bull_view": "CGO观点 (80字): 引用[趋势{trend_score}/RSI{rsi}/VR{vol_ratio}]，阐述进攻逻辑。",
            "bear_view": "CRO观点 (80字): 引用[熔断{fuse_level}/RSI{rsi_zone}/VR{vol_signal}]，阐述防守逻辑。",
            "chairman_conclusion": "CIO裁决 (100字): 综合多空，给出 EXECUTE/REJECT/HOLD 指令及具体理由。",
            "decision": "EXECUTE|REJECT|HOLD",
            "position_pct": "具体仓位% (EXECUTE时必填)",
            "adjustment": 整数数值 (-30 到 +30),
            "confidence": 0-100整数,
            "key_risk": "最大单一风险点 (15字)"
        }}
        """
        
        payload = {
            "model": self.model_tactical,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2, # 降低温度，确保纪律执行
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
            
            # [关键修复] 硬约束验证层 - Code Level Enforcement
            # 如果触发熔断，不管 AI 说什么，直接强行覆盖结果
            if fuse_level >= 2:
                result['decision'] = 'REJECT'
                result['adjustment'] = -30
                result['position_pct'] = '0%'
                result['chairman_conclusion'] = f'[系统熔断] 熔断等级{fuse_level}触发，系统强制否决交易。AI原话: {result.get("chairman_conclusion", "")}'
                result['confidence'] = 100
                logger.warning(f"🛡️ [硬约束触发] {fund_name} 熔断等级{fuse_level} -> 强制 REJECT")

            # 兼容旧版字段
            if "chairman_conclusion" in result and "comment" not in result:
                result["comment"] = result["chairman_conclusion"]
                
            return result
        except Exception as e:
            logger.error(f"AI Analysis Failed {fund_name}: {e}")
            return {"bull_view": "解析失败", "bear_view": "解析失败", "comment": "JSON Error", "adjustment": 0}

    @retry(retries=2, delay=5)
    def review_report(self, report_text, macro_str):
        """
        [战略层] CIO 复盘备忘录 (R1) - 深度归因版
        """
        current_date = datetime.now().strftime("%Y年%m月%d日")
        prompt = f"""
        【系统角色】玄铁量化CIO | 机构级复盘备忘录 | 日期: {current_date}
        
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
        [审计层] Red Team 顾问 (R1) - 强制对抗版
        """
        current_date = datetime.now().strftime("%Y年%m月%d日")
        prompt = f"""
        【系统角色】玄铁量化Red Team | 独立审计顾问 | 日期: {current_date}
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
