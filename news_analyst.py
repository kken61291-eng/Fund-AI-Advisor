import requests
import json
import os
import re
import time
import akshare as ak
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

    def _format_short_time(self, time_str):
        try:
            dt = datetime.strptime(str(time_str), "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%m-%d %H:%M")
        except:
            s = str(time_str)
            if len(s) > 10: return s[5:16]
            return s

    @retry(retries=2, delay=2)
    def fetch_news_titles(self, keywords_str):
        """
        [V14.28] 关键词矩阵搜索 + 强制日志打印
        """
        if not keywords_str: return []
        
        keys = keywords_str.split()
        news_list = []
        fallback_list = [] 
        
        try:
            # 1. 获取要闻 (数据源)
            df = ak.stock_news_em(symbol="要闻")
            junk_words = ["汇总", "集锦", "收评", "早报", "公告", "提示", "复盘"]
            
            for _, row in df.iterrows():
                title = str(row.get('title', ''))
                raw_time = str(row.get('public_time', ''))
                
                if any(jw in title for jw in junk_words): continue
                
                time_str = self._format_short_time(raw_time)
                item = f"[{time_str}] {title}"
                
                # 收集备选 (取前5条)
                if len(fallback_list) < 5:
                    fallback_list.append(item)

                # OR 关系匹配
                if any(k in title for k in keys):
                    news_list.append(item)
            
            # [新增] 尝试获取板块新闻 (如果关键词没搜到)
            if not news_list and len(keys) > 0:
                try:
                    # 尝试用第一个关键词作为板块去搜 (例如 '半导体')
                    sector_key = keys[0]
                    df_sector = ak.stock_news_em(symbol=sector_key) # 某些版本支持
                    for _, row in df_sector.iterrows():
                        title = str(row.get('title', ''))
                        if any(jw in title for jw in junk_words): continue
                        news_list.append(f"[板块] {title}")
                        if len(news_list) >= 3: break
                except:
                    pass

            final_list = news_list[:8] if news_list else [f"[市场背景] {x}" for x in fallback_list[:3]]
            
            # [V14.28 核心] 强制打印新闻到日志
            logger.info(f"📰 [情报检索] 关键词:{keys} | 命中:{len(news_list)}")
            for n in final_list:
                logger.info(f"  > {n}")
                
            return final_list
            
        except Exception as e:
            logger.warning(f"关键词搜索微瑕: {e}")
            return ["数据源波动，参考宏观面。"]

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
        score = tech_indicators.get('quant_score', 50)
        trend = tech_indicators.get('trend_weekly', '无趋势')
        valuation = tech_indicators.get('valuation_desc', '未知')
        obv_slope = tech_indicators.get('flow', {}).get('obv_slope', 0)
        
        money_flow = "资金抢筹" if obv_slope > 1.0 else ("资金出逃" if obv_slope < -1.0 else "存量博弈")
        vol_ratio = tech_indicators.get('risk_factors', {}).get('vol_ratio', 1.0)
        
        if vol_ratio < 0.6: volume_status = "流动性枯竭"
        elif vol_ratio < 0.8: volume_status = "缩量"
        elif vol_ratio > 2.0: volume_status = "放量分歧"
        else: volume_status = "温和"

        # [V14.26] 联邦投委会人设增强版 Prompt
        prompt = f"""
        你现在是【玄铁联邦投委会】的决策现场。
        请基于以下【实盘档案】和【自查情报】，组织一场高水平的辩证会议。

        📁 **实盘档案 (Hard Data)**:
        - 标的: {fund_name}
        - 技术评分: {score} (基础分)
        - 估值状态: {valuation}
        - 资金流向: {money_flow} (OBV斜率: {obv_slope:.2f})
        - 量能状态: {volume_status} (VR: {vol_ratio})
        - 周线趋势: {trend}

        📰 **自查情报 (Intelligence)**:
        - 宏观背景: {macro_summary[:600]}
        - 行业动态: {str(sector_news)[:600]}

        --- 🏛️ 参会人员与人设 ---

        1. **🦊 CGO (首席增长官)**
           - **背景**: 华尔街动量交易员，信仰"趋势为王"和"强者恒强"。
           - **任务**: 挖掘上涨逻辑。但如果【趋势DOWN】或【流动性枯竭】，你必须诚实地承认"风口已过"，不能强行看多。
           - **行为**: 必须引用具体的【新闻】或【资金数据】来佐证观点。

        2. **🐻 CRO (首席风控官)**
           - **背景**: 资深宏观策略师，信仰"均值回归"和"安全边际"。
           - **任务**: 泼冷水。但如果【量价齐升】且【估值低廉】，你必须承认"安全垫足够"，不能为了反对而反对。
           - **行为**: 重点审查【背离】和【宏观压制】。

        3. **⚖️ CIO (首席投资官/裁判)**
           - **背景**: 绝对理性的决策机器。
           - **任务**: 
             1. 听取两人的辩论，判断谁更符合当下的【实盘数据】。
             2. **独立验证**: 如果CGO说"量能健康"但VR<0.6，你要无情驳斥。
             3. **收敛结论**: 给出最终的【策略修正分】(Adjustment)，并在加分/减分的基础上决定攻守方向。

        --- 输出要求 (JSON) ---
        {{
            "bull_view": "CGO: (引用数据/新闻)... 观点 (30字)",
            "bear_view": "CRO: (引用风险点)... 观点 (30字)",
            "chairman_conclusion": "CIO: [判决理由]... 最终修正 (50字)",
            "adjustment": 整数数值 (-30 到 +30),
            "risk_alert": "核心风险点"
        }}
        """

        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.4, # 适度创造性，保持逻辑严密
            "max_tokens": 1200
        }
        
        try:
            logger.info(f"🧠 [联邦辩论] {fund_name} 投委会召开中...")
            response = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            
            if response.status_code != 200: 
                logger.error(f"API Error: {response.text}")
                return self._fallback_result(sector_news)
                
            raw_content = response.json()['choices'][0]['message']['content']
            
            # [V14.26] 打印原始辩论记录
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

    # --- CIO 战略审计 ---
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

    # --- 玄铁先生复盘 ---
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
