import requests
import json
import os
import re
import time
from datetime import datetime, timedelta
from utils import logger, retry, get_beijing_time

# 【关键修改】移除了 RED_TEAM_AUDIT_PROMPT，防止 ImportError
from prompts_config import (
    TACTICAL_IC_PROMPT, 
    STRATEGIC_CIO_REPORT_PROMPT, 
    RISK_CONTROL_VETO_PROMPT, 
    EVENT_TIER_DEFINITIONS
)

class NewsAnalyst:
    """
    新闻分析师 - V19.6 认知对抗版
    """
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-R1"      
        self.model_strategic = "Pro/deepseek-ai/DeepSeek-R1"    

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def extract_event_info(self, news_text):
        days_to_event = "NULL"
        event_tier = "TIER_C"
        try:
            match = re.search(r'(\d+)\s*天后', news_text)
            if match: days_to_event = int(match.group(1))
            
            s_keywords = ["议息", "五年规划", "中央", "重磅"]
            a_keywords = ["大会", "发布", "财报", "数据"]
            
            if any(k in news_text for k in s_keywords): event_tier = "TIER_S"
            elif any(k in news_text for k in a_keywords):
                event_tier = "TIER_A"
                if days_to_event == "NULL": days_to_event = 5
        except Exception as e:
            logger.warning(f"事件提取失败: {e}")
        return days_to_event, event_tier

    def get_market_context(self, max_length=35000): 
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        possible_paths = [f"data_news/news_{today_str}.jsonl", f"news_{today_str}.jsonl"]
        
        target_file = None
        for p in possible_paths:
            if os.path.exists(p):
                target_file = p
                break
        
        if not target_file: return "【系统提示】本地新闻库缺失，请先运行 news_crawler.py。"

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
        news_candidates.reverse()
        return "\n".join(news_candidates[:80])

    def _clean_json(self, text):
        try:
            text = re.sub(r'```json\s*', '', text)
            text = re.sub(r'```', '', text)
            text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
            start, end = text.find('{'), text.rfind('}')
            if start != -1 and end != -1: return text[start:end+1]
            return "{}"
        except: return "{}"

    @retry(retries=1, delay=2)
    def analyze_fund_tactical_v6(self, fund_name, tech, macro_data, news_text, risk, strategy_type="core"):
        trend_score = tech.get('quant_score', 0)
        days_to_event, event_tier = self.extract_event_info(news_text)

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
                news_content=str(news_text)[:8000]
            )
        except Exception as e:
            logger.error(f"IC Prompt构造失败: {e}")
            return None

        payload = {
            "model": self.model_tactical, 
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.4,
            "max_tokens": 2000, 
            "response_format": {"type": "json_object"}
        }
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if resp.status_code != 200: return None
            result = json.loads(self._clean_json(resp.json()['choices'][0]['message']['content']))
            result['days_to_event'] = days_to_event
            return result
        except Exception as e:
            logger.error(f"IC Analysis Failed {fund_name}: {e}")
            return None

    @retry(retries=2, delay=5)
    def run_risk_committee_veto(self, candidates):
        if not candidates: return {"approved_list": [], "rejected_log": [], "risk_summary": "无提案提交"}

        candidates_str = json.dumps(candidates, indent=2, ensure_ascii=False)
        prompt = RISK_CONTROL_VETO_PROMPT.format(
            candidate_count=len(candidates),
            candidates_context=candidates_str
        )
        
        payload = {"model": self.model_strategic, "messages": [{"role": "user", "content": prompt}], "temperature": 0.2, "max_tokens": 2000}
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            raw_text = resp.json()['choices'][0]['message']['content']
            return json.loads(self._clean_json(raw_text))
        except Exception as e:
            logger.error(f"Risk Veto Failed: {e}")
            return {"approved_list": [], "rejected_log": [{"code": "ALL", "reason": "风控服务超时"}], "risk_summary": "System Error"}

    @retry(retries=2, delay=5)
    def generate_cio_strategy(self, current_date, risk_report_json):
        prompt = STRATEGIC_CIO_REPORT_PROMPT.format(
            current_date=current_date,
            risk_committee_json=json.dumps(risk_report_json, indent=2, ensure_ascii=False)
        )
        return self._call_r1_text(prompt)

    # --- 兼容性方法 (移除对 RED_TEAM_AUDIT_PROMPT 的依赖) ---
    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro_data, news_text, risk, strategy_type="core"):
        # 如果旧版 main.py 调用此方法，重定向到 v6 或提供默认值
        return self.analyze_fund_tactical_v6(fund_name, tech, macro_data, news_text, risk, strategy_type)

    @retry(retries=2, delay=5)
    def review_report(self, report_text, macro_str):
        # 兜底：使用 CIO 模板
        prompt = STRATEGIC_CIO_REPORT_PROMPT.format(
            current_date=datetime.now().strftime("%Y-%m-%d"), 
            risk_committee_json=json.dumps({"summary": report_text}, ensure_ascii=False)
        )
        return self._call_r1_text(prompt)

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        # 【修改】不再使用 RED_TEAM_AUDIT_PROMPT，改为返回空字符串
        return ""

    def _call_r1_text(self, prompt):
        payload = {"model": self.model_strategic, "messages": [{"role": "user", "content": prompt}], "max_tokens": 4000, "temperature": 0.3}
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            return resp.json()['choices'][0]['message']['content'].replace("```html", "").replace("```", "").strip()
        except: return "<p>分析生成中...</p>"
