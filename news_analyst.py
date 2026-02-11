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
# 导入 v3.2 配置文件
from prompts_config import TACTICAL_IC_PROMPT, STRATEGIC_CIO_REPORT_PROMPT, RED_TEAM_AUDIT_PROMPT

class NewsAnalyst:
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        # 战术执行 (快思考): V3.2 - 负责 CGO/CRO/CIO 实时信号
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-V3.2"      
        # 战略推理 (慢思考): R1 - 负责 宏观复盘/逻辑审计
        self.model_strategic = "Pro/deepseek-ai/DeepSeek-R1"   

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def _clean_time(self, t_str):
        """统一时间格式为 MM-DD HH:MM"""
        try:
            if len(str(t_str)) >= 16:
                return str(t_str)[5:16]
            return str(t_str)
        except: return ""

    def _fetch_live_patch(self):
        """[7x24全球财经电报] - 双源抓取 (EastMoney + CLS)"""
        news_list = []
        # 1. 东方财富
        try:
            df_em = ak.stock_telegraph_em()
            if df_em is not None and not df_em.empty:
                for i in range(min(50, len(df_em))):
                    title, content = str(df_em.iloc[i].get('title') or ''), str(df_em.iloc[i].get('content') or '')
                    t = self._clean_time(df_em.iloc[i].get('public_time'))
                    if self._is_valid_news(title):
                        item_str = f"[{t}] [EM] {title}"
                        if len(content) > 10 and content != title: item_str += f"\n   (摘要: {content[:300]})"
                        news_list.append(item_str)
        except Exception as e: logger.warning(f"Live EM fetch error: {e}")

        # 2. 财联社
        try:
            df_cls = ak.stock_telegraph_cls()
            if df_cls is not None and not df_cls.empty:
                for i in range(min(50, len(df_cls))):
                    title, content = str(df_cls.iloc[i].get('title') or ''), str(df_cls.iloc[i].get('content') or '')
                    raw_t = df_cls.iloc[i].get('ctime', df_cls.iloc[i].get('publish_time'))
                    try:
                        t = datetime.fromtimestamp(int(raw_t)).strftime("%m-%d %H:%M") if str(raw_t).isdigit() else self._clean_time(raw_t)
                    except: t = ""
                    if not title and content: title = content[:30] + "..."
                    if self._is_valid_news(title):
                        item_str = f"[{t}] [CLS] {title}"
                        if len(content) > 10 and content != title: item_str += f"\n   (摘要: {content[:300]})"
                        news_list.append(item_str)
        except Exception as e: logger.warning(f"Live CLS fetch error: {e}")
        return news_list

    def _is_valid_news(self, title):
        return bool(title and len(title) >= 2)

    def get_market_context(self, max_length=35000): 
        """[核心逻辑] 收集 -> 去重 -> 排序 -> 截断"""
        news_candidates = []
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        file_path = f"data_news/news_{today_str}.jsonl"
        
        live_news = self._fetch_live_patch()
        if live_news: news_candidates.extend(live_news)
            
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            item = json.loads(line)
                            title = str(item.get('title', ''))
                            if not self._is_valid_news(title): continue
                            t_str, source = self._clean_time(item.get('time', '')), item.get('source', 'Local')
                            src_tag = "[EM]" if source == "EastMoney" else ("[CLS]" if source == "CLS" else "[Local]")
                            content = str(item.get('content') or item.get('digest') or "")
                            news_entry = f"[{t_str}] {src_tag} {title}"
                            if len(content) > 10: news_entry += f"\n   (摘要: {content[:300]})"
                            news_candidates.append(news_entry)
                        except: pass
            except Exception as e: logger.error(f"读取新闻缓存失败: {e}")
        
        unique_news, seen = [], set()
        for n in news_candidates:
            title_part = n.split('] ', 2)[-1].split('\n')[0]
            if title_part not in seen:
                seen.add(title_part); unique_news.append(n)
        
        try: unique_news.sort(key=lambda x: x[:17], reverse=True)
        except: pass 
        
        final_list, current_len = [], 0
        for news_item in unique_news:
            if current_len + len(news_item) < max_length:
                final_list.append(news_item); current_len += len(news_item) + 1 
            else: break
        
        return "\n".join(final_list) if final_list else "今日暂无重大新闻。"

    def _clean_json(self, text):
        try:
            text = re.sub(r'```json\s*', '', text)
            text = re.sub(r'```', '', text)
            text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
            start, end = text.find('{'), text.rfind('}')
            if start != -1 and end != -1: text = text[start:end+1]
            return re.sub(r',\s*([\]}])', r'\1', text)
        except: return "{}"

    # ============================================
    # v3.2 逻辑守卫 (Logic Guardian) - 核心后处理校验
    # ============================================
    def _apply_logic_guardian(self, res, tech):
        """强制执行 v3.2 后处理校验规则，修正 AI 幻觉"""
        try:
            # 规则 1: 趋势阶段与仓位匹配强制修正
            stage = res.get('trend_analysis', {}).get('stage', 'UNCLEAR')
            pos_size_str = str(res.get('position_size', '0%')).replace('%', '')
            try: pos_size = float(pos_size_str)
            except: pos_size = 0.0

            # 阈值定义
            thresholds = {"START": (0, 50), "ACCELERATING": (0, 80), "EXHAUSTION": (0, 20), "REVERSAL": (0, 0)}
            if stage in thresholds:
                min_p, max_p = thresholds[stage]
                if pos_size > max_p:
                    logger.warning(f"🚨 [逻辑守卫] {stage}阶段仓位{pos_size}%超限，强制修正至{max_p}%")
                    res['position_size'] = f"{max_p}%"
                    res['adjustment'] = min(res.get('adjustment', 0), 20) # 限制加仓幅度

            # 规则 2: 背离响应强制化
            divergence_type = res.get('trend_analysis', {}).get('divergence', {}).get('type', 'NONE')
            if divergence_type == "BEARISH_TOP":
                if res.get('decision') == "EXECUTE":
                    logger.warning(f"🚨 [逻辑守卫] 发现顶背离，强制撤销买入指令")
                    res['decision'] = "HOLD"
                    res['adjustment'] = min(res.get('adjustment', 0), 0)

            # 规则 3: 乖离率硬闸门
            bias_alert = res.get('cro_audit', {}).get('bias_alert', False)
            if bias_alert and res.get('adjustment', 0) > 0:
                logger.warning(f"🚨 [逻辑守卫] 乖离率过高，禁止加仓")
                res['adjustment'] = 0
                res['decision'] = "HOLD"

            # 规则 4: 趋势失效位缺失补全
            if not res.get('trend_analysis', {}).get('key_levels', {}).get('invalidation'):
                res['trend_analysis']['key_levels']['invalidation'] = "20日均线破位"

        except Exception as e:
            logger.error(f"逻辑守卫执行异常: {e}")
        return res

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro, news, risk, strategy_type="core"):
        """[战术层] v3.2 生产版调用"""
        fuse_level, fuse_msg = risk['fuse_level'], risk['risk_msg']
        
        prompt = TACTICAL_IC_PROMPT.format(
            fund_name=fund_name, strategy_type=strategy_type,
            trend_score=tech.get('quant_score', 50), fuse_level=fuse_level, fuse_msg=fuse_msg,
            rsi=tech.get('rsi', 50), macd_trend=tech.get('macd', {}).get('trend', '-'),
            volume_status="放量" if tech.get('risk_factors',{}).get('vol_ratio',1) > 1.5 else "缩量",
            ma5_status="向上" if tech.get('ma5_slope', 0) > 0 else "向下",
            ma20_status="支撑" if tech.get('price', 0) > tech.get('ma20', 0) else "破位",
            ma60_status="多头" if tech.get('ma20', 0) > tech.get('ma60', 0) else "空头",
            news_content=str(news)[:25000]
        )
        
        payload = {
            "model": self.model_tactical, "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1, "max_tokens": 1200, "response_format": {"type": "json_object"}
        }
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if resp.status_code != 200: return self._get_fallback_result()
            
            result = json.loads(self._clean_json(resp.json()['choices'][0]['message']['content']))
            
            # 执行逻辑守卫
            result = self._apply_logic_guardian(result, tech)

            # 强制执行熔断逻辑
            if fuse_level >= 2:
                result['decision'], result['adjustment'] = 'REJECT', -100
                result['chairman_conclusion'] = f'[系统熔断] {fuse_msg} - 强制离场。'

            return result
        except Exception as e:
            logger.error(f"AI Analysis Failed {fund_name}: {e}")
            return self._get_fallback_result()

    def _get_fallback_result(self):
        return {"decision": "HOLD", "adjustment": 0, "trend_analysis": {"stage": "UNCLEAR"}}

    @retry(retries=2, delay=5)
    def review_report(self, report_text, macro_str):
        prompt = STRATEGIC_CIO_REPORT_PROMPT.format(current_date=datetime.now().strftime("%Y年%m月%d日"), macro_str=macro_str[:2500], report_text=report_text[:3000])
        return self._call_r1(prompt)

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        prompt = RED_TEAM_AUDIT_PROMPT.format(current_date=datetime.now().strftime("%Y年%m月%d日"), macro_str=macro_str[:2500], report_text=report_text[:3000])
        return self._call_r1(prompt)

    def _call_r1(self, prompt):
        payload = {"model": self.model_strategic, "messages": [{"role": "user", "content": prompt}], "max_tokens": 4000, "temperature": 0.3}
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=180)
            return resp.json()['choices'][0]['message']['content'].replace("```html", "").replace("```", "").strip()
        except: return "<p>分析生成中...</p>"
