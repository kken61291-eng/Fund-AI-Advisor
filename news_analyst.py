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

    def _clean_time(self, t_str):
        try:
            if len(str(t_str)) >= 16: return str(t_str)[5:16]
            return str(t_str)
        except: return ""

    def _fetch_live_patch(self):
        news_list = []
        # 1. 东方财富
        try:
            if hasattr(ak, 'stock_info_global_em'): df_em = ak.stock_info_global_em()
            else: df_em = None
            if df_em is not None and not df_em.empty:
                for i in range(min(50, len(df_em))):
                    title = str(df_em.iloc[i].get('title') or '')
                    t = self._clean_time(df_em.iloc[i].get('public_time') or df_em.iloc[i].get('publish_time'))
                    if self._is_valid_news(title): news_list.append(f"[{t}] [EM] {title}")
        except: pass

        # 2. 财联社
        try:
            if hasattr(ak, 'stock_info_global_cls'): df_cls = ak.stock_info_global_cls()
            elif hasattr(ak, 'stock_telegraph_cls'): df_cls = ak.stock_telegraph_cls()
            else: df_cls = None
            if df_cls is not None and not df_cls.empty:
                for i in range(min(50, len(df_cls))):
                    title = str(df_cls.iloc[i].get('title') or '')
                    t = self._clean_time(df_cls.iloc[i].get('time') or df_cls.iloc[i].get('publish_time'))
                    if self._is_valid_news(title): news_list.append(f"[{t}] [CLS] {title}")
        except: pass
        return news_list

    def _is_valid_news(self, title):
        return bool(title and len(title) >= 2)

    def get_market_context(self, max_length=35000):
        news_candidates = []
        live_news = self._fetch_live_patch()
        if live_news: news_candidates.extend(live_news)
        
        # 去重与截断
        unique_news, seen = [], set()
        for n in news_candidates:
            if n not in seen:
                seen.add(n); unique_news.append(n)
        
        return "\n".join(unique_news[:50]) if unique_news else "今日暂无重大新闻。"

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
            
        return res

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro, news, risk, strategy_type="core"):
        """
        [战术层] V3.2 生产版调用 - 全量指标投喂
        """
        fuse_level, fuse_msg = risk['fuse_level'], risk['risk_msg']
        
        # --- 1. 提取全量 V17.0 指标 ---
        rsi = tech.get('rsi', 50)
        
        # 趋势强度
        trend_str = tech.get('trend_strength', {})
        adx = trend_str.get('adx', 0)
        trend_type = trend_str.get('trend_type', 'UNCLEAR')
        ma_align = tech.get('ma_alignment', 'MIXED')
        
        # 均线位置
        key_lvls = tech.get('key_levels', {})
        ma20_status = "支撑(价>MA20)" if key_lvls.get('above_ma20') else "破位(价<MA20)"
        ma60_status = "多头(价>MA60)" if key_lvls.get('above_ma60') else "空头(价<MA60)"
        
        # MACD & 背离
        macd_info = tech.get('macd', {})
        macd_trend = macd_info.get('trend', '-')
        macd_div = macd_info.get('divergence', 'NONE')
        
        # 波动率 & 布林
        atr_pct = tech.get('volatility', {}).get('atr_percent', 0)
        boll_sqz = "是" if tech.get('bollinger', {}).get('squeeze') else "否"
        
        # 量能
        vol_info = tech.get('volume_analysis', {})
        vol_ratio = vol_info.get('vol_ratio', 1.0)
        vwap_status = "上方" if vol_info.get('above_vwap') else "下方"
        price_vol_div = vol_info.get('price_vol_divergence', 'NORMAL')

        # --- 2. 构造扩展上下文 ---
        extended_tech_context = f"""
        【V17.0 高级量化全景】
        1. 趋势雷达: ADX={adx} (趋势强度), 类型={trend_type}, 均线排列={ma_align}
        2. 波动状态: ATR波动率={atr_pct}%, 布林带收窄={boll_sqz}
        3. MACD深度: 趋势={macd_trend}, 结构背离={macd_div}
        4. 量价结构: 量比={vol_ratio}, 价格位置={vwap_status}VWAP成本线, 量价背离={price_vol_div}
        """

        prompt = TACTICAL_IC_PROMPT.format(
            fund_name=fund_name, strategy_type=strategy_type,
            trend_score=tech.get('quant_score', 50), fuse_level=fuse_level, fuse_msg=fuse_msg,
            rsi=rsi, macd_trend=f"{macd_trend} (背离:{macd_div})", 
            volume_status=f"量比{vol_ratio} ({price_vol_div})",   
            ma5_status=f"{ma_align} (ADX:{adx})",               
            ma20_status=ma20_status,
            ma60_status=ma60_status,
            news_content=f"{extended_tech_context}\n\n【实时新闻】\n{str(news)[:20000]}"
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
