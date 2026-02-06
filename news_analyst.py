import requests
import json
import os
import re
import time
import akshare as ak
import pandas as pd
from datetime import datetime
from utils import logger, retry

class NewsAnalyst:
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        self.model = os.getenv("LLM_MODEL", "gpt-3.5-turbo")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.cls_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
        raw_list = []
        try:
            df = ak.stock_news_em(symbol="要闻")
            junk_words = ["汇总", "集锦", "收评", "早报", "公告", "提示", "复盘"]
            for _, row in df.iterrows():
                title = str(row.get('title', ''))
                raw_time = str(row.get('public_time', ''))
                if any(jw in title for jw in junk_words): continue
                time_str = self._format_short_time(raw_time)
                raw_list.append({
                    "text": f"[{time_str}] (东财) {title}",
                    "pure_title": title,
                    "timestamp": raw_time
                })
        except Exception as e:
            logger.warning(f"东财源微瑕: {e}")
        return raw_list

    def _fetch_cls_telegraph(self):
        raw_list = []
        url = "https://www.cls.cn/nodeapi/telegraphList"
        params = {"rn": 30, "sv": 7755}
        try:
            resp = requests.get(url, headers=self.cls_headers, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if "data" in data and "roll_data" in data["data"]:
                    items = data["data"]["roll_data"]
                    for item in items:
                        title = item.get("title", "")
                        content = item.get("content", "")
                        ctime = item.get("ctime", 0)
                        display_text = title if title else content[:50].replace("\n", " ")
                        if not display_text: continue
                        time_str = self._format_short_time(ctime)
                        raw_list.append({
                            "text": f"[{time_str}] (财社) {display_text}",
                            "pure_title": display_text,
                            "timestamp": ctime
                        })
        except Exception as e:
            logger.warning(f"财社直连微瑕: {e}")
        return raw_list

    @retry(retries=2, delay=2)
    def fetch_news_titles(self, keywords_str):
        if not keywords_str: return []
        keys = keywords_str.split()
        pool_em = self._fetch_eastmoney_news()
        pool_cls = self._fetch_cls_telegraph()
        all_news_items = pool_cls + pool_em
        
        hit_list = []
        fallback_list = []
        seen_titles = set()

        for item in all_news_items:
            clean_t = item['pure_title'].replace(" ", "")[:10]
            if clean_t in seen_titles: continue
            seen_titles.add(clean_t)
            if len(fallback_list) < 5: fallback_list.append(item['text'])
            if any(k in item['pure_title'] for k in keys):
                hit_list.append(item['text'])

        if not hit_list and len(keys) > 0:
            try:
                sector_key = keys[0]
                df_sector = ak.stock_news_em(symbol=sector_key)
                for _, row in df_sector.iterrows():
                    title = str(row.get('title', ''))
                    time_str = self._format_short_time(str(row.get('public_time', '')))
                    hit_list.append(f"[{time_str}] (板块) {title}")
                    if len(hit_list) >= 3: break
            except:
                pass

        final_list = hit_list[:10] if hit_list else [f"[市场背景] {x}" for x in fallback_list[:4]]
        logger.info(f"📰 [情报融合] 关键词:{keys} | 财社:{len(pool_cls)} | 东财:{len(pool_em)} | 命中:{len(hit_list)}")
        for n in final_list: logger.info(f"  > {n}")
        return final_list

    def _clean_json(self, text):
        try:
            match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
            if match: return match.group(1)
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match: return match.group(0)
            return text
        except: return text

    @retry(retries=2, delay=2)
    def analyze_fund_v4(self, fund_name, tech_indicators, macro_summary, sector_news):
        # 1. 基础数据提取
        score = tech_indicators.get('quant_score', 50) # 这个分数要藏起来
        trend = tech_indicators.get('trend_weekly', '无趋势')
        valuation = tech_indicators.get('valuation_desc', '未知')
        
        # 2. 资金与量能
        obv_slope = tech_indicators.get('flow', {}).get('obv_slope', 0)
        money_flow = "资金抢筹" if obv_slope > 1.0 else ("资金出逃" if obv_slope < -1.0 else "存量博弈")
        
        vol_ratio = tech_indicators.get('risk_factors', {}).get('vol_ratio', 1.0)
        if vol_ratio < 0.6: volume_status = "流动性枯竭 (极度缩量)"
        elif vol_ratio < 0.8: volume_status = "缩量回调"
        elif vol_ratio > 2.0: volume_status = "放量分歧/突破"
        else: volume_status = "温和"

        # 3. 战术三件套
        rsi = tech_indicators.get('rsi', 50)
        macd_data = tech_indicators.get('macd', {})
        macd_status = macd_data.get('trend', '未知')
        macd_hist = macd_data.get('hist', 0)
        pct_b = tech_indicators.get('risk_factors', {}).get('bollinger_pct_b', 0.5)
        
        if pct_b > 1.0: bollinger_status = "突破上轨 (极端强势)"
        elif pct_b > 0.8: bollinger_status = "触及压力位"
        elif pct_b < 0.0: bollinger_status = "跌破下轨 (极端弱势)"
        elif pct_b < 0.2: bollinger_status = "触及支撑位"
        else: bollinger_status = "中轨震荡"

        # [V14.33] 双盲辩论版 Prompt
        # 核心改动：移除了“综合评分”字段，新增了“机密档案”字段
        prompt = f"""
        你现在是【玄铁联邦投委会】的决策现场。
        请基于以下【实盘全息档案】和【自查情报】，组织一场高水平的 **"盲评" (Blind Review)** 辩证会议。

        📁 **公开·全息档案 (Public Holographic Data)**:
        [注意：CGO和CRO不可见模型评分，必须自行判断]
        -------------------------------------------
        【趋势定性】
        - 标的: {fund_name}
        - 周线趋势: {trend} (决定长期方向)
        - 估值状态: {valuation}

        【时机信号 (关键)】
        - MACD状态: {macd_status} (Hist: {macd_hist})
          * 金叉=进攻; 死叉=防守; 柱状缩短=变盘。
        - RSI (14): {rsi}
          * >70超买; <30超卖; 50震荡。
        - 布林位置: {bollinger_status} (PctB: {pct_b})

        【资金与量能】
        - 资金意图: {money_flow} (OBV斜率: {obv_slope:.2f})
        - 量能状态: {volume_status} (VR: {vol_ratio})
        -------------------------------------------

        📰 **自查情报 (Intelligence)**:
        - 宏观: {macro_summary[:600]}
        - 行业: {str(sector_news)[:600]}

        🔒 **【CIO专享·机密档案】(Confidential)**:
        - 量化模型基础分: {score} 分
        - (此分数仅CIO可见，用于最终校准)

        --- 🏛️ 参会人员与任务 ---

        1. **🦊 CGO (首席增长官)** - [盲评模式]
           - **状态**: 你不知道模型打了多少分。
           - **任务**: 仅凭RSI/MACD/OBV等指标寻找做多机会。
           - **规则**: 如果数据很烂（如MACD死叉+缩量），不要强行看多，诚实说"目前无机会"。

        2. **🐻 CRO (首席风控官)** - [盲评模式]
           - **状态**: 你也不知道模型打了多少分。
           - **任务**: 仅凭数据寻找风险点（背离/超买/压力位）。
           - **规则**: 如果数据完美（量价齐升+低估），诚实说"安全"。

        3. **⚖️ CIO (首席投资官)** - [上帝视角]
           - **状态**: 你拥有【机密档案】，知道基础分是 {score}。
           - **任务**: 
             1. 听取两人的"盲评"，判断市场情绪与模型分数是否背离？
             2. **决策**: 
                - 如果基础分很高(>70)但CGO觉得没机会 -> 说明模型可能失真 -> **大幅扣分**。
                - 如果基础分很低(<30)但CRO觉得很安全 -> 说明可能有左侧机会 -> **适当加分**。
             3. **输出**: 给出【策略修正分】，决定最终买卖方向。

        --- 输出要求 (JSON) ---
        {{
            "bull_view": "CGO: (基于指标盲评)... 观点 (30字)",
            "bear_view": "CRO: (基于指标盲评)... 观点 (30字)",
            "chairman_conclusion": "CIO: [参考基础分{score}，结合辩论]... 最终修正 (50字)",
            "adjustment": 整数数值 (-30 到 +30),
            "risk_alert": "核心风险点"
        }}
        """

        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.4, 
            "max_tokens": 1200
        }
        
        try:
            logger.info(f"🧠 [联邦辩论] {fund_name} 投委会(盲评模式)召开中...")
            response = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            
            if response.status_code != 200: 
                logger.error(f"API Error: {response.text}")
                return self._fallback_result(sector_news)
                
            raw_content = response.json()['choices'][0]['message']['content']
            logger.info(f"📝 [会议纪要 {fund_name}]:\n{raw_content}")
            data = json.loads(self._clean_json(raw_content))
            return {
                "bull_say": data.get("bull_view", "..."),
                "bear_say": data.get("bear_view", "..."),
                "comment": data.get("chairman_conclusion", "需人工介入"),
                "adjustment": int(data.get("adjustment", 0)),
                "risk_alert": data.get("risk_alert", "无"),
                "used_news": sector_news 
            }
        except Exception as e:
            logger.error(f"投委会崩溃 {fund_name}: {e}")
            return self._fallback_result(sector_news)

    def _fallback_result(self, news):
        return {"bull_say": "数据缺失", "bear_say": "风险未知", "comment": "连接中断", "adjustment": 0, "risk_alert": "API Error", "used_news": news}

    @retry(retries=2, delay=2)
    def review_report(self, report_text):
        prompt = f"""
        你是【玄铁量化】的 **CIO**。
        请对以下汇总进行【战略审计】，输出 HTML。
        
        【汇总】{report_text}

        输出模板：
        <div class="cio-section">
            <h3 style="border-left: 4px solid #d32f2f; padding-left: 10px;">宏观定调</h3>
            <p>...</p>
            <h3 style="border-left: 4px solid #d32f2f; padding-left: 10px;">双轨审计</h3>
            <p>...</p>
            <h3 style="border-left: 4px solid #d32f2f; padding-left: 10px;">CIO指令</h3>
            <p>...</p>
        </div>
        """
        return self._call_llm_text(prompt, "CIO 战略审计")

    @retry(retries=2, delay=2)
    def advisor_review(self, report_text, macro_str):
        prompt = f"""
        你是 **【玄铁先生】**，一位冷峻的市场哲学家。
        请写一段【场外实战复盘】 (HTML)。

        【宏观】{macro_str[:1500]} 
        【决议】{report_text}

        请透过现象看本质。输出：
        <div class="advisor-section">
            <h4 style="color: #ffd700;">【势·验证】</h4><p>...</p>
            <h4 style="color: #ffd700;">【术·底仓】</h4><p>...</p>
            <h4 style="color: #ffd700;">【断·进攻】</h4><p>...</p>
        </div>
        """
        return self._call_llm_text(prompt, "玄铁先生复盘")

    def _call_llm_text(self, prompt, task_name):
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.5,
            "max_tokens": 1500
        }
        try:
            response = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if response.status_code == 200:
                raw_text = response.json()['choices'][0]['message']['content']
                clean_text = raw_text.replace("```html", "").replace("```", "").strip()
                return clean_text
            return f"{task_name} 生成失败: API Error"
        except Exception as e:
            logger.error(f"{task_name} 失败: {e}")
            return f"{task_name} 暂时缺席 (网络波动)"
