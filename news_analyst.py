import requests
import json
import os
import re
import time
from datetime import datetime
from utils import logger, retry, get_beijing_time
from prompts_config import TACTICAL_IC_PROMPT, STRATEGIC_CIO_REPORT_PROMPT, RED_TEAM_AUDIT_PROMPT

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

    def get_market_context(self, max_length=35000): 
        """
        [核心逻辑 - 修改] 强制读取本地新闻文件
        不再进行联网抓取，只读 news_crawler.py 生成的文件
        """
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        
        # 1. 寻找本地文件 (兼容两种路径)
        possible_paths = [
            f"data_news/news_{today_str}.jsonl",
            f"news_{today_str}.jsonl"
        ]
        
        target_file = None
        for p in possible_paths:
            if os.path.exists(p):
                target_file = p
                break
        
        # 2. 如果没找到文件，返回警告
        if not target_file:
            logger.warning(f"⚠️ 未找到今日新闻文件: {possible_paths}")
            return "【系统提示】本地新闻库缺失，请先运行 news_crawler.py。当前仅基于技术面分析。"

        logger.info(f"📂 正在加载本地新闻: {target_file}")
        
        # 3. 读取并解析
        news_candidates = []
        try:
            with open(target_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        item = json.loads(line)
                        title = item.get('title', '').strip()
                        content = item.get('content', '').strip()
                        source = item.get('source', 'Local')
                        # 截取时间字符串，只要 HH:MM
                        time_str = str(item.get('time', ''))
                        if len(time_str) > 16: time_str = time_str[5:16]
                        
                        if len(title) < 2: continue
                        
                        # 格式化
                        entry = f"[{time_str}] [{source}] {title}"
                        # 如果有摘要且不重复，加上摘要
                        if len(content) > 30 and content != title:
                            entry += f"\n   (摘要: {content[:150]}...)"
                            
                        news_candidates.append(entry)
                    except: pass
        except Exception as e:
            logger.error(f"读取新闻文件出错: {e}")

        if not news_candidates:
            return "本地新闻文件内容为空。"

        # 4. 截断 (保留前 30 条，防止 Token 溢出)
        return "\n".join(news_candidates[:30])

    def _clean_json(self, text):
        try:
            text = re.sub(r'```json\s*', '', text)
            text = re.sub(r'```', '', text)
            text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
            start, end = text.find('{'), text.rfind('}')
            if start != -1 and end != -1: text = text[start:end+1]
            return re.sub(r',\s*([\]}])', r'\1', text)
        except: return "{}"

    def _apply_logic_guardian(self, res, tech):
        """逻辑守卫：修正幻觉"""
        try:
            # 1. 仓位限制
            stage = res.get('trend_analysis', {}).get('stage', 'UNCLEAR')
            thresholds = {"START": 50, "ACCELERATING": 80, "EXHAUSTION": 20, "REVERSAL": 0}
            if stage in thresholds:
                current_adj = res.get('adjustment', 0)
                if current_adj > thresholds[stage]:
                     res['adjustment'] = thresholds[stage]

            # 2. 背离强制
            div_type = tech.get('macd', {}).get('divergence', 'NONE')
            if div_type == "TOP_DIVERGENCE" and res.get('decision') == 'EXECUTE':
                res['decision'] = 'HOLD'
                res['adjustment'] = 0
        except: pass
        return res

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro, news, risk, strategy_type="core"):
        """
        [战术层] V3.2 生产版调用 - 全量指标投喂
        """
        fuse_level, fuse_msg = risk['fuse_level'], risk['risk_msg']
        
        # 提取指标
        rsi = tech.get('rsi', 50)
        trend_str = tech.get('trend_strength', {})
        adx = trend_str.get('adx', 0)
        trend_type = trend_str.get('trend_type', 'UNCLEAR')
        ma_align = tech.get('ma_alignment', 'MIXED')
        
        # 构造扩展上下文
        extended_tech_context = f"""
        【V17.0 高级量化全景】
        1. 趋势雷达: ADX={adx} (趋势强度), 类型={trend_type}, 均线排列={ma_align}
        2. MACD深度: 趋势={tech.get('macd', {}).get('trend', '-')}, 结构背离={tech.get('macd', {}).get('divergence', 'NONE')}
        3. 量价结构: 量比={tech.get('volume_analysis', {}).get('vol_ratio', 1.0)}
        """

        # 确保 news 不为空，避免 AI 瞎编
        safe_news = news if news and len(news) > 10 else "【注意】今日无本地新闻数据，请严格基于技术指标分析。"

        prompt = TACTICAL_IC_PROMPT.format(
            fund_name=fund_name, strategy_type=strategy_type,
            trend_score=tech.get('quant_score', 50), fuse_level=fuse_level, fuse_msg=fuse_msg,
            rsi=rsi, macd_trend=f"{tech.get('macd', {}).get('trend', '-')} (背离:{tech.get('macd', {}).get('divergence', 'NONE')})", 
            volume_status="N/A",   
            ma5_status=f"{ma_align} (ADX:{adx})",               
            ma20_status="N/A",
            ma60_status="N/A",
            news_content=f"{extended_tech_context}\n\n【本地新闻摘要】\n{str(safe_news)[:15000]}"
        )
        
        payload = {
            "model": self.model_tactical, "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1, "max_tokens": 1200, "response_format": {"type": "json_object"}
        }
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if resp.status_code != 200: return self._get_fallback_result()
            
            result = json.loads(self._clean_json(resp.json()['choices'][0]['message']['content']))
            result = self._apply_logic_guardian(result, tech)
            if fuse_level >= 2:
                result['decision'], result['adjustment'] = 'REJECT', -100
                result['chairman_conclusion'] = f'[系统熔断] {fuse_msg}'
            return result
        except Exception as e:
            logger.error(f"AI Analysis Failed {fund_name}: {e}")
            return self._get_fallback_result()

    def _get_fallback_result(self):
        return {"decision": "HOLD", "adjustment": 0, "trend_analysis": {"stage": "UNCLEAR"}}

    @retry(retries=2, delay=5)
    def review_report(self, report_text, macro_str):
        # 确保 macro_str 不为空
        safe_macro = macro_str if macro_str and len(macro_str) > 10 else "暂无新闻数据。"
        prompt = STRATEGIC_CIO_REPORT_PROMPT.format(current_date=datetime.now().strftime("%Y年%m月%d日"), macro_str=safe_macro[:2500], report_text=report_text[:3000])
        return self._call_r1(prompt)

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        safe_macro = macro_str if macro_str and len(macro_str) > 10 else "暂无新闻数据。"
        prompt = RED_TEAM_AUDIT_PROMPT.format(current_date=datetime.now().strftime("%Y年%m月%d日"), macro_str=safe_macro[:2500], report_text=report_text[:3000])
        return self._call_r1(prompt)

    def _call_r1(self, prompt):
        payload = {"model": self.model_strategic, "messages": [{"role": "user", "content": prompt}], "max_tokens": 4000, "temperature": 0.3}
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            return resp.json()['choices'][0]['message']['content'].replace("```html", "").replace("```", "").strip()
        except: return "<p>分析生成中...</p>"
