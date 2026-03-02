import requests
import json
import os
import re
import akshare as ak
import time
import random
import pandas as pd
from datetime import datetime
from utils import logger, retry, get_beijing_time

class NewsAnalyst:
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        # 战术执行 (快思考): V3.2 - 负责 CGO/CRO/CIO 实时信号
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-R1"       
        # 战略推理 (慢思考): R1 - 负责 宏观复盘/逻辑审计
        self.model_strategic = "Pro/deepseek-ai/DeepSeek-R1"    

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    # ========================================================
    # 🟢 1. 数据读取模块 (重写，极其稳健)
    # ========================================================
    def _clean_time(self, t_str):
        """统一时间格式为 MM-DD HH:MM"""
        try:
            if len(str(t_str)) >= 16:
                return str(t_str)[5:16]
            return str(t_str)
        except: return ""

    def get_market_context(self, max_length=15000): 
        """
        直接读取本地文件 (由 news_loader.py 生成)
        """
        news_candidates = []
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        
        # 🟢 我们直接读刚才跑完 loader 存入的这个文件
        file_path = f"data_news/news_{today_str}.jsonl"
        
        logger.info(f"📂 正在读取本地新闻缓存: {file_path}")
        
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            item = json.loads(line)
                            title = str(item.get('title', '')).strip()
                            if not title or len(title) < 2: continue
                                
                            raw_time = item.get('time', '')
                            t_str = self._clean_time(raw_time)
                            
                            source = item.get('source', 'Local')
                            src_tag = "[EM]" if source == "EastMoney" else ("[CLS]" if source == "CLS" else "[Local]")
                            
                            content = str(item.get('content') or item.get('digest') or "").strip()
                            
                            # 构建文本块
                            news_entry = f"[{t_str}] {src_tag} {title}"
                            if len(content) > 10 and content != title:
                                # 限制单条新闻长度，防止爆 Token
                                content_truncated = content[:200] + "..." if len(content) > 200 else content
                                news_entry += f"\n   (摘要: {content_truncated})"
                                
                            # 将原始时间戳存入元组，方便精准排序
                            news_candidates.append((raw_time, news_entry, title))
                        except Exception as parse_err:
                            pass
                logger.info(f"✅ 成功从本地加载 {len(news_candidates)} 条新闻")
            except Exception as e:
                logger.error(f"❌ 读取新闻缓存彻底失败: {e}")
        else:
            logger.warning(f"⚠️ 未找到今日新闻文件 {file_path}，使用空数据。")
            return "今日暂无重大新闻。"
        
        # 如果还是空的，直接返回
        if not news_candidates:
            return "今日暂无重大新闻。"
        
        # 1. 标题去重
        unique_news = []
        seen_titles = set()
        for raw_t, entry, title in news_candidates:
            if title not in seen_titles:
                seen_titles.add(title)
                unique_news.append((raw_t, entry))
                
        # 2. 按时间倒序排列 (最新的在最上面)
        unique_news.sort(key=lambda x: x[0], reverse=True)
        
        # 3. 拼接并控制最大长度
        final_list = []
        current_len = 0
        for _, entry in unique_news:
            item_len = len(entry)
            if current_len + item_len < max_length:
                final_list.append(entry)
                current_len += item_len + 1 
            else:
                break
                
        final_text = "\n".join(final_list)
        logger.info(f"📰 最终投喂给 AI 的新闻字数: {len(final_text)}")
        return final_text

    # ========================================================
    # 🟢 2. LLM 分析模块 (v19.6 架构)
    # ========================================================
    def _clean_json(self, text):
        try:
            text = re.sub(r'```json\s*', '', text)
            text = re.sub(r'```', '', text)
            text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1:
                text = text[start:end+1]
            return text
        except: return "{}"

    @retry(retries=1, delay=2)
    def analyze_fund_tactical_v6(self, fund_name, tech, macro, news, risk, strategy_type="core"):
        """
        [Phase 1] 战术海选 (Tactical Selection) - V6
        """
        trend_score = tech.get('quant_score', 50)
        
        prompt = f"""
        【系统架构】鹊知风 IC 战术海选 (v6)
        
        【标的信息】
        标的: {fund_name} ({strategy_type})
        量化评分: {trend_score}/100 | RSI: {tech.get('rsi', 50)} | MACD趋势: {tech.get('macd',{}).get('trend','-')}
        
        【市场环境】
        资金流向: {macro.get('net_flow', 0)} 亿
        新闻摘要:
        {str(news)[:8000]}
        
        【任务】
        请扮演三位委员 (CGO 进攻, CRO 风控, CIO 决策) 进行简短辩论。
        1. CGO: 寻找做多理由 (结合新闻催化剂)。
        2. CRO: 寻找否决理由 (风险与新闻利空)。
        3. CIO: 给出最终判决 (PROPOSE_EXECUTE / HOLD / REJECT)。
        
        【输出 JSON】
        {{
            "debate_transcript": {{
                "CGO": "...",
                "CRO": "...",
                "CIO": "..."
            }},
            "chairman_verdict": {{
                "final_decision": "PROPOSE_EXECUTE", 
                "logic_weighting": "..."
            }},
            "strategy_meta": {{
                "mode": "TREND",
                "rationale": "..."
            }},
            "days_to_event": "NULL"
        }}
        """
        
        try:
            payload = {
                "model": self.model_tactical,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "response_format": {"type": "json_object"}
            }
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=60)
            if resp.status_code != 200: return None
            
            content = resp.json()['choices'][0]['message']['content']
            return json.loads(self._clean_json(content))
        except Exception as e:
            logger.error(f"IC Tactical V6 Failed: {e}")
            return None

    @retry(retries=1, delay=5)
    def run_risk_committee_veto(self, candidates):
        """
        [Phase 2] 风控委员会终审 (Risk Committee Veto)
        """
        if not candidates:
            return {"approved_list": [], "rejected_log": []}
            
        candidate_str = json.dumps(candidates, ensure_ascii=False, indent=2)
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        prompt = f"""
        【系统角色】鹊知风 风控委员会 (Risk Committee) | 终审环节
        日期: {current_date}
        
        【待审提案】
        {candidate_str}
        
        【终审纪律】
        作为首席风控官 (CRO)，你拥有"一票否决权"。请审查上述提案：
        1. 宏观对冲: 如果全市场大跌，是否所有提案都在买入进攻型资产？(如果是，必须否决部分)
        2. 逻辑自洽: 提案理由是否牵强附会？
        
        【输出 JSON】
        {{
            "approved_list": [
                {{"code": "xxxxxx", "reason": "逻辑扎实，风险可控"}}, ...
            ],
            "rejected_log": [
                {{"code": "xxxxxx", "reason": "逆势接飞刀，否决"}}
            ],
            "risk_summary": "整体风控评价..."
        }}
        """
        
        try:
            payload = {
                "model": self.model_strategic, 
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 2000
            }
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=120)
            content = resp.json()['choices'][0]['message']['content']
            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL)
            return json.loads(self._clean_json(content))
        except Exception as e:
            logger.error(f"Risk Veto Failed: {e}")
            approved = [{"code": c['code'], "reason": "风控服务离线，默认放行"} for c in candidates]
            return {"approved_list": approved, "rejected_log": [], "risk_summary": "Risk API Error"}

    def generate_cio_strategy(self, date_str, risk_report):
        """
        [Phase 3] 生成 CIO 战略报告 HTML
        """
        risk_summary = risk_report.get('risk_summary', '无')
        html = f"""
        <div style="background-color: #f8f9fa; padding: 15px; border-left: 5px solid #2c3e50; margin-bottom: 20px;">
            <h3 style="margin-top: 0; color: #2c3e50;">🧠 CIO 战略定调 ({date_str})</h3>
            <p><strong>风控综述：</strong>{risk_summary}</p>
            <p style="font-size: 0.9em; color: #666;">* 本报告由 DeepSeek-R1 生成，基于认知对抗模型 v19.6</p>
        </div>
        """
        return html
