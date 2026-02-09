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
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-V3.2"      
        self.model_strategic = "Pro/deepseek-ai/DeepSeek-R1"  

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        # 这里不需要复杂的 User-Agent，因为主要读本地文件，补丁抓取用 akshare 即可

    def _fetch_live_patch(self):
        """
        [补丁] 现场抓取最新的 5 条新闻，防止漏掉最近 1 小时的突发
        """
        try:
            time.sleep(1)
            df = ak.stock_news_em(symbol="要闻")
            news = []
            for i in range(min(5, len(df))):
                title = str(df.iloc[i].get('新闻标题') or df.iloc[i].get('title'))
                t = str(df.iloc[i].get('发布时间') or df.iloc[i].get('public_time'))
                if len(t) > 10: t = t[5:16] # MM-DD HH:MM
                news.append(f"[{t}] {title} (Live)")
            return news
        except:
            return []

    def get_market_context(self, max_length=20000):
        """
        [核心] 获取全天候市场舆情上下文
        来源 = 本地积攒的 JSONL (过去24小时) + 现场抓取的 Live (最近1小时)
        """
        news_lines = []
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        file_path = f"data_news/news_{today_str}.jsonl"
        
        # 1. 读取本地积攒的数据
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            item = json.loads(line)
                            t_str = str(item.get('time', ''))
                            if len(t_str) > 10: t_str = t_str[5:16] # MM-DD HH:MM
                            # 格式: [09:30] 标题
                            news_lines.append(f"[{t_str}] {item.get('title')}")
                        except: pass
            except Exception as e:
                logger.error(f"读取新闻缓存失败: {e}")
        
        # 2. 获取实时补丁 (Live Patch)
        live_news = self._fetch_live_patch()
        if live_news:
            news_lines.extend(live_news)
            
        # 3. 去重 (防止 Live 和 本地 重复)
        # 使用 dict.fromkeys 保留顺序去重（后出现的覆盖先出现的，或者反之）
        # 这里简单处理：转 set 再转回 list 可能会乱序，所以用 list 倒序保留
        unique_news = []
        seen = set()
        # 倒序遍历（假设越后面越新），这样保留的是最新的
        for n in reversed(news_lines):
            if n not in seen:
                seen.add(n)
                unique_news.append(n)
        
        # 结果是倒序的（最新在最前），符合 LLM 阅读习惯
        final_text = "\n".join(unique_news)
        
        logger.info(f"📖 构建舆情上下文: {len(unique_news)} 条新闻, 总长度 {len(final_text)}")
        
        if len(final_text) > max_length:
            return final_text[:max_length] + "\n...(早期消息已截断)"
        
        return final_text if final_text else "今日暂无重大新闻。"

    # --- 以下保持之前的 JSON 清洗和 LLM 调用逻辑不变 ---

    def _clean_json(self, text):
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
        code_match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
        if code_match: return code_match.group(1)
        obj_match = re.search(r'\{.*\}', text, re.DOTALL)
        if obj_match: return obj_match.group(0)
        return "{}"
    
    def _clean_html(self, text):
        text = text.replace("```html", "").replace("```", "").strip()
        return text

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro, news, risk):
        """
        [战术层] 联邦投委会辩论系统
        """
        fuse_level = risk['fuse_level']
        fuse_msg = risk['risk_msg']
        trend_score = tech.get('quant_score', 50)
        rsi = tech.get('rsi', 50)
        macd = tech.get('macd', {})
        vol_ratio = tech.get('risk_factors', {}).get('vol_ratio', 1.0)
        
        prompt = f"""
        【系统任务】
        你现在是玄铁量化基金的投研系统。请模拟 CGO(动量)、CRO(风控)、CIO(总监) 三位专家的辩论过程，并输出最终决策 JSON。
        
        【输入数据】
        标的: {fund_name}
        技术因子:
        - 趋势强度: {trend_score} (0-100)
        - RSI(14): {rsi}
        - MACD: {macd.get('trend', '未知')}
        - 成交量偏离(VR): {vol_ratio}
        
        风险因子:
        - 熔断等级: {fuse_level} (0-3，>=2为限制交易)
        - 风控指令: {fuse_msg}
        
        舆情因子 (全量上下文):
        - 市场消息流: 
        {str(news)[:15000]}  <-- [修改] 扩大到 15000 字，允许模型读取全天新闻

        --- 角色定义 ---
        1. **CGO**: 寻找右侧交易机会。若趋势强度<50，直接输出HOLD。
        2. **CRO**: 证明"为什么现在不该做"。若熔断等级>=2，必须否决。
        3. **CIO**: 基于"胜率×赔率"做最终裁决。决策必须明确。

        【输出格式-严格JSON】
        {{
            "bull_view": "CGO观点 (50字以内)",
            "bear_view": "CRO观点 (50字以内)",
            "chairman_conclusion": "CIO裁决 (80字以内)",
            "adjustment": 整数数值 (-30 到 +30)
        }}
        """
        
        payload = {
            "model": self.model_tactical,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "max_tokens": 1000,
            "response_format": {"type": "json_object"}
        }
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if resp.status_code != 200:
                return {"bull_view": "API Error", "bear_view": "API Error", "comment": "API Error", "adjustment": 0}
            
            content = resp.json()['choices'][0]['message']['content']
            result = json.loads(self._clean_json(content))
            
            if "chairman_conclusion" in result and "comment" not in result:
                result["comment"] = result["chairman_conclusion"]
            return result
        except Exception as e:
            logger.error(f"AI Analysis Failed {fund_name}: {e}")
            raise e

    # ... (review_report 和 advisor_review 保持之前修正过的 R1 版本不变) ...
    # 确保它们接收 macro_str 参数并注入 current_date
    
    @retry(retries=2, delay=5)
    def review_report(self, report_text, macro_str):
        current_date = datetime.now().strftime("%Y年%m月%d日")
        prompt = f"""
        【系统角色】CIO (首席投资官) | 日期: {current_date}
        【输入数据】
        1. 全天候宏观舆情: {macro_str[:2000]}
        2. 交易报告: {report_text}
        
        【任务】使用 DeepSeek-R1 思维链进行宏观定调、归因分析和战略指令下达。
        """
        # ... (后续代码同前) ...
        # 此处省略具体实现，请复用上一轮提供的 R1 提示词逻辑
        return self._call_r1(prompt) # 假设封装个 _call_r1，实际请把代码填回去

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        current_date = datetime.now().strftime("%Y年%m月%d日")
        prompt = f"""
        【系统角色】独立审计顾问 (Red Team) | 日期: {current_date}
        【输入数据】
        1. 全天候宏观舆情: {macro_str[:2000]}
        2. CIO交易: {report_text}
        
        【任务】盲点警示、逻辑压力测试、最终验证。
        """
        # ... (后续代码同前) ...
        return self._call_r1(prompt)

    # 辅助方法：为了减少重复代码
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
