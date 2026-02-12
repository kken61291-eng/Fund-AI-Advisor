import requests
import json
import os
import re
from datetime import datetime, timedelta
from utils import logger, retry, get_beijing_time
from prompts_config import TACTICAL_IC_PROMPT, STRATEGIC_CIO_REPORT_PROMPT, RED_TEAM_AUDIT_PROMPT, EVENT_TIER_DEFINITIONS

class NewsAnalyst:
    """
    新闻分析师 - V3.5 适配版
    新增：事件提取(Event Extraction)、Prompt动态填充
    """
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        # 请根据实际部署修改模型名称
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-V3.2"      
        self.model_strategic = "Pro/deepseek-ai/DeepSeek-R1"    

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def extract_event_info(self, news_text):
        """
        [v3.5] 从新闻中提取事件信息
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
            # 只要新闻中包含 S 级或 A 级关键词
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
            text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
            start, end = text.find('{'), text.rfind('}')
            if start != -1 and end != -1: return text[start:end+1]
            return "{}"
        except: return "{}"

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro_data, news_text, risk, strategy_type="core"):
        """
        [v3.5 核心] 战术层分析
        """
        # [🔥关键修复] 如果 macro_data 为空，给一个默认空字典，防止 .get() 报错
        if macro_data is None:
            macro_data = {}

        # 1. 准备数据
        fuse_level = risk.get('fuse_level', 0)
        fuse_msg = risk.get('risk_msg', '')
        
        # 提取技术指标
        trend_score = tech.get('quant_score', 0)
        vol_status = tech.get('volatility_status', 'NORMAL')
        recent_gain = tech.get('recent_gain', 0)
        rs_score = tech.get('relative_strength', 0)
        
        # 提取宏观数据 (现在安全了)
        net_flow = macro_data.get('net_flow', 0)
        leader_status = macro_data.get('leader_status', 'UNKNOWN')
        
        # 提取事件信息
        days_to_event, event_tier = self.extract_event_info(news_text)

        # 2. 构造 Prompt
        # 必须严格对应 prompts_config.py 中的 TACTICAL_IC_PROMPT 占位符
        try:
            prompt = TACTICAL_IC_PROMPT.format(
                fund_name=fund_name, 
                strategy_type=strategy_type,
                trend_score=trend_score, 
                fuse_level=fuse_level, 
                fuse_msg=fuse_msg,
                
                # 技术面
                rsi=tech.get('rsi', 50), 
                macd_trend=tech.get('macd', {}).get('trend', '-'), 
                volume_status=tech.get('volume_analysis', {}).get('status', 'NORMAL'),
                
                # v3.5 新增字段
                days_to_event=days_to_event,
                event_tier=event_tier,
                volatility_status=vol_status,
                recent_gain=recent_gain,
                relative_strength=rs_score,
                net_flow=f"{net_flow}亿",
                leader_status=leader_status,
                
                # 新闻内容
                news_content=str(news_text)[:12000]
            )
        except Exception as e:
            logger.error(f"Prompt 构造异常: {e}")
            return self._get_fallback_result()
        
        # 3. 调用 API
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
            
            # 将 days_to_event 注入回结果，方便 StrategyEngine 使用
            if 'trend_analysis' not in result: result['trend_analysis'] = {}
            result['trend_analysis']['days_to_event'] = days_to_event
            
            return result
        except Exception as e:
            logger.error(f"AI Analysis Failed {fund_name}: {e}")
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
        prompt = STRATEGIC_CIO_REPORT_PROMPT.format(
            current_date=datetime.now().strftime("%Y年%m月%d日"), 
            macro_str=str(macro_str)[:2000], 
            report_text=str(report_text)[:3000]
        )
        return self._call_r1(prompt)

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        prompt = RED_TEAM_AUDIT_PROMPT.format(
            current_date=datetime.now().strftime("%Y年%m月%d日"), 
            macro_str=str(macro_str)[:2000], 
            report_text=str(report_text)[:3000]
        )
        return self._call_r1(prompt)

    def _call_r1(self, prompt):
        payload = {"model": self.model_strategic, "messages": [{"role": "user", "content": prompt}], "max_tokens": 4000, "temperature": 0.3}
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            return resp.json()['choices'][0]['message']['content'].replace("```html", "").replace("```", "").strip()
        except: return "<p>分析生成中...</p>"
