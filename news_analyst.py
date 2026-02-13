import requests
import json
import os
import re
import time
from datetime import datetime, timedelta
from utils import logger, retry, get_beijing_time

# 引入所有版本的配置，确保新旧逻辑都能跑
from prompts_config import (
    TACTICAL_IC_PROMPT, 
    STRATEGIC_CIO_REPORT_PROMPT, 
    RED_TEAM_AUDIT_PROMPT, 
    RISK_CONTROL_VETO_PROMPT, 
    EVENT_TIER_DEFINITIONS
)

class NewsAnalyst:
    """
    新闻分析师 - V19.6 认知对抗版 (Cognitive Adversarial Model)
    包含：
    1. v3.5/v19.4 旧接口 (analyze_fund_v5) - 保留兼容性
    2. v19.6 新接口 (analyze_fund_tactical_v6) - IC 提案
    3. v19.6 新接口 (run_risk_committee_veto) - 风控终审
    """
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        
        # 战术层(IC)用快模型 (DeepSeek-V3.2)
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-R1"      
        # 战略层(Risk/CIO)用慢模型 (DeepSeek-R1)
        self.model_strategic = "Pro/deepseek-ai/DeepSeek-R1"    

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def extract_event_info(self, news_text):
        """
        [工具] 从新闻中提取事件信息
        返回: (days_to_event, event_tier)
        """
        days_to_event = "NULL"
        event_tier = "TIER_C"
        
        try:
            # 1. 简单的正则提取 "N天后"
            # 示例: "3天后召开会议" -> 3
            match = re.search(r'(\d+)\s*天后', news_text)
            if match:
                days_to_event = int(match.group(1))
            
            # 2. 简单的关键词定级
            s_keywords = ["议息", "五年规划", "中央", "重磅"]
            a_keywords = ["大会", "发布", "财报", "数据"]
            
            if any(k in news_text for k in s_keywords):
                event_tier = "TIER_S"
            elif any(k in news_text for k in a_keywords):
                event_tier = "TIER_A"
                if days_to_event == "NULL": days_to_event = 5 # 默认赋值
                
        except Exception as e:
            logger.warning(f"事件提取失败: {e}")
            
        return days_to_event, event_tier

    def get_market_context(self, max_length=35000): 
        """读取本地新闻文件"""
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        possible_paths = [f"data_news/news_{today_str}.jsonl", f"news_{today_str}.jsonl"]
        
        target_file = None
        for p in possible_paths:
            if os.path.exists(p):
                target_file = p
                break
        
        if not target_file:
            return "【系统提示】本地新闻库缺失，请先运行 news_crawler.py。"

        news_candidates = []
        try:
            with open(target_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        item = json.loads(line)
                        title = item.get('title', '').strip()
                        content = item.get('content', '').strip()
                        source = item.get('source', 'Local')
                        time_str = str(item.get('time', ''))[:16]
                        
                        entry = f"[{time_str}] [{source}] {title}"
                        if len(content) > 30 and content != title:
                            entry += f"\n   (摘要: {content[:100]}...)"
                        news_candidates.append(entry)
                    except: pass
        except Exception as e:
            logger.error(f"读取新闻文件出错: {e}")

        if not news_candidates: return "本地新闻文件为空。"

        # 倒序排列，优先保留最新新闻
        news_candidates.reverse()
        return "\n".join(news_candidates[:80])

    def _clean_json(self, text):
        try:
            text = re.sub(r'```json\s*', '', text)
            text = re.sub(r'```', '', text)
            # 兼容 DeepSeek R1 的思维链标记
            text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
            start, end = text.find('{'), text.rfind('}')
            if start != -1 and end != -1: return text[start:end+1]
            return "{}"
        except: return "{}"

    # ======================================================
    # v19.6 新增核心方法 (Cognitive Adversarial)
    # ======================================================

    @retry(retries=1, delay=2)
    def analyze_fund_tactical_v6(self, fund_name, tech, macro_data, news_text, risk, strategy_type="core"):
        """
        [Phase 1] 战术层 IC 提案 (v19.6)
        使用 TACTICAL_IC_PROMPT (包含 Technical/CGO/CRO 三方辩论)
        """
        # 1. 准备数据
        trend_score = tech.get('quant_score', 0)
        days_to_event, event_tier = self.extract_event_info(news_text)

        # 2. 构造 Prompt
        try:
            prompt = TACTICAL_IC_PROMPT.format(
                fund_name=fund_name, 
                trend_score=trend_score, 
                rsi=tech.get('rsi', 50),
                volatility_status=tech.get('volatility_status', '-'),
                recent_gain=tech.get('recent_gain', 0),
                net_flow=f"{macro_data.get('net_flow', 0)}亿",
                leader_status=macro_data.get('leader_status', 'UNKNOWN'),
                days_to_event=days_to_event,
                event_tier=event_tier,
                news_content=str(news_text)[:8000] # 适度截断，V3.2 上下文足够
            )
        except Exception as e:
            logger.error(f"IC Prompt构造失败: {e}")
            return None

        # 3. 调用 API (使用战术模型 V3.2)
        payload = {
            "model": self.model_tactical, 
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.4, # 稍微增加温度，鼓励角色扮演和辩论
            "max_tokens": 2000, 
            "response_format": {"type": "json_object"}
        }
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if resp.status_code != 200: return None
            
            result = json.loads(self._clean_json(resp.json()['choices'][0]['message']['content']))
            
            # 注入中间变量供后续使用
            result['days_to_event'] = days_to_event
            return result
        except Exception as e:
            logger.error(f"IC Analysis Failed {fund_name}: {e}")
            return None

    @retry(retries=2, delay=5)
    def run_risk_committee_veto(self, candidates):
        """
        [Phase 2] 风控委员会终审 (v19.6)
        对 PROPOSE_EXECUTE 的标的进行压力测试
        """
        if not candidates:
            return {"approved_list": [], "rejected_log": [], "risk_summary": "无提案提交"}

        # 构造候选人列表文本
        candidates_str = json.dumps(candidates, indent=2, ensure_ascii=False)
        
        prompt = RISK_CONTROL_VETO_PROMPT.format(
            candidate_count=len(candidates),
            candidates_context=candidates_str
        )
        
        # 调用 API (使用战略模型 R1，需要深度思考)
        payload = {
            "model": self.model_strategic, 
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2, # 低温，保持理性
            "max_tokens": 2000
        }
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            raw_text = resp.json()['choices'][0]['message']['content']
            
            # R1 有时不会输出纯 JSON，尝试清洗
            return json.loads(self._clean_json(raw_text))
        except Exception as e:
            logger.error(f"Risk Veto Failed: {e}")
            # 兜底：如果风控挂了，为了安全起见，假设全被驳回 (或者根据需求改为全部通过)
            return {
                "approved_list": [], 
                "rejected_log": [{"code": "ALL", "reason": "风控服务超时，触发安全熔断"}], 
                "risk_summary": "System Error"
            }

    @retry(retries=2, delay=5)
    def generate_cio_strategy(self, current_date, risk_report_json):
        """
        [Phase 3] CIO 战略定调 (v19.6)
        """
        prompt = STRATEGIC_CIO_REPORT_PROMPT.format(
            current_date=current_date,
            risk_committee_json=json.dumps(risk_report_json, indent=2, ensure_ascii=False)
        )
        # 直接返回 HTML 文本
        return self._call_r1_text(prompt)

    # ======================================================
    # 旧版兼容方法 (保留不删，防止报错)
    # ======================================================

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro_data, news_text, risk, strategy_type="core"):
        """
        [Deprecation Warning] 旧版单体分析接口 (v19.4)
        如果 main.py 回退到旧版，此方法依然可用
        """
        # 如果 macro_data 为空，给一个默认空字典
        if macro_data is None: macro_data = {}

        fuse_level = risk.get('fuse_level', 0)
        fuse_msg = risk.get('risk_msg', '')
        trend_score = tech.get('quant_score', 0)
        days_to_event, event_tier = self.extract_event_info(news_text)

        # 这里的 TACTICAL_IC_PROMPT 已经是 v19.6 的新版了
        # 如果旧版 main.py 调用这个，可能会因为输出字段不匹配 (debate_transcript) 而导致 UI 显示不全
        # 但为了保证代码不报错，我们依然执行流程
        try:
            prompt = TACTICAL_IC_PROMPT.format(
                fund_name=fund_name, 
                trend_score=trend_score, 
                rsi=tech.get('rsi', 50),
                volatility_status=tech.get('volatility_status', 'NORMAL'),
                recent_gain=tech.get('recent_gain', 0),
                net_flow=f"{macro_data.get('net_flow', 0)}亿",
                leader_status=macro_data.get('leader_status', 'UNKNOWN'),
                days_to_event=days_to_event,
                event_tier=event_tier,
                news_content=str(news_text)[:12000]
            )
        except Exception as e:
            return self._get_fallback_result()
        
        payload = {
            "model": self.model_tactical, 
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1, 
            "max_tokens": 1200, 
            "response_format": {"type": "json_object"}
        }
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if resp.status_code != 200: return self._get_fallback_result()
            
            result = json.loads(self._clean_json(resp.json()['choices'][0]['message']['content']))
            if 'trend_analysis' not in result: result['trend_analysis'] = {}
            result['trend_analysis']['days_to_event'] = days_to_event
            return result
        except Exception:
            return self._get_fallback_result()

    def _get_fallback_result(self):
        return {
            "decision": "HOLD", 
            "adjustment": 0, 
            "strategy_meta": {"mode": "WAIT", "rationale": "系统故障兜底"},
            "position_size": 0
        }

    @retry(retries=2, delay=5)
    def review_report(self, report_text, macro_str):
        # 旧版 CIO 接口
        prompt = STRATEGIC_CIO_REPORT_PROMPT.format(
            current_date=datetime.now().strftime("%Y年%m月%d日"), 
            macro_str=str(macro_str)[:2000] if macro_str else "无", 
            report_text=str(report_text)[:3000],
            # v19.6 的 CIO Prompt 可能不需要 report_text 参数，这里为了兼容不做强校验
            risk_committee_json="{}" 
        )
        # 注意：如果 STRATEGIC_CIO_REPORT_PROMPT 格式变了，这里可能会报错
        # 建议使用 v19.6 的 generate_cio_strategy 替代
        return self._call_r1_text(prompt)

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        # 旧版 Red Team 接口
        prompt = RED_TEAM_AUDIT_PROMPT.format(
            current_date=datetime.now().strftime("%Y年%m月%d日"), 
            macro_str=str(macro_str)[:2000], 
            report_text=str(report_text)[:3000]
        )
        return self._call_r1_text(prompt)

    def _call_r1_text(self, prompt):
        payload = {"model": self.model_strategic, "messages": [{"role": "user", "content": prompt}], "max_tokens": 4000, "temperature": 0.3}
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            return resp.json()['choices'][0]['message']['content'].replace("```html", "").replace("```", "").strip()
        except: return "<p>分析生成中...</p>"
