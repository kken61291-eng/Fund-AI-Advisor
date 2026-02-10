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
        """
        [7x24全球财经电报] - 双源抓取 (EastMoney + CLS)
        """
        news_list = []
        
        # 1. 东方财富
        try:
            df_em = ak.stock_telegraph_em()
            if df_em is not None and not df_em.empty:
                for i in range(min(50, len(df_em))):
                    title = str(df_em.iloc[i].get('title') or '')
                    content = str(df_em.iloc[i].get('content') or '')
                    t = self._clean_time(df_em.iloc[i].get('public_time'))
                    
                    if self._is_valid_news(title):
                        item_str = f"[{t}] [EM] {title}"
                        if len(content) > 10 and content != title:
                            item_str += f"\n   (摘要: {content[:300]})"
                        news_list.append(item_str)
        except Exception as e:
            logger.warning(f"Live EM fetch error: {e}")

        # 2. 财联社
        try:
            df_cls = ak.stock_telegraph_cls()
            if df_cls is not None and not df_cls.empty:
                for i in range(min(50, len(df_cls))):
                    title = str(df_cls.iloc[i].get('title') or '')
                    content = str(df_cls.iloc[i].get('content') or '')
                    raw_t = df_cls.iloc[i].get('ctime', df_cls.iloc[i].get('publish_time'))
                    
                    try:
                        if str(raw_t).isdigit():
                            dt = datetime.fromtimestamp(int(raw_t))
                            t = dt.strftime("%m-%d %H:%M")
                        else:
                            t = self._clean_time(raw_t)
                    except: t = ""

                    if not title and content: title = content[:30] + "..."

                    if self._is_valid_news(title):
                        item_str = f"[{t}] [CLS] {title}"
                        if len(content) > 10 and content != title:
                            item_str += f"\n   (摘要: {content[:300]})"
                        news_list.append(item_str)
        except Exception as e:
            logger.warning(f"Live CLS fetch error: {e}")

        return news_list

    def _is_valid_news(self, title):
        if not title: return False
        if len(title) < 2: return False
        return True

    def get_market_context(self, max_length=35000): 
        """
        [核心逻辑] 收集(Local+EM+CLS) -> 去重 -> 排序 -> 截断
        """
        news_candidates = []
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        file_path = f"data_news/news_{today_str}.jsonl"
        
        # 1. 优先读取实时电报 (双源)
        live_news = self._fetch_live_patch()
        if live_news:
            news_candidates.extend(live_news)
            
        # 2. 补充本地缓存的历史新闻
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            item = json.loads(line)
                            title = str(item.get('title', ''))
                            if not self._is_valid_news(title): continue
                                
                            t_str = self._clean_time(item.get('time', ''))
                            source = item.get('source', 'Local')
                            src_tag = "[EM]" if source == "EastMoney" else ("[CLS]" if source == "CLS" else "[Local]")
                            
                            content = str(item.get('content') or item.get('digest') or "")
                            
                            news_entry = f"[{t_str}] {src_tag} {title}"
                            if len(content) > 10:
                                news_entry += f"\n   (摘要: {content[:300]})"
                                
                            news_candidates.append(news_entry)
                        except: pass
            except Exception as e:
                logger.error(f"读取新闻缓存失败: {e}")
        
        # 3. 去重 (基于标题)
        unique_news = []
        seen = set()
        for n in news_candidates:
            try:
                title_part = n.split('] ', 2)[-1].split('\n')[0]
            except:
                title_part = n.split('\n')[0]
                
            if title_part not in seen:
                seen.add(title_part)
                unique_news.append(n)
        
        # 4. 强制倒序
        try:
            unique_news.sort(key=lambda x: x[:17], reverse=True)
        except: pass 
        
        # 5. 截断
        final_list = []
        current_len = 0
        for news_item in unique_news:
            item_len = len(news_item)
            if current_len + item_len < max_length:
                final_list.append(news_item)
                current_len += item_len + 1 
            else:
                break
        
        final_text = "\n".join(final_list)
        return final_text if final_text else "今日暂无重大新闻。"

    def _clean_json(self, text):
        try:
            text = re.sub(r'```json\s*', '', text)
            text = re.sub(r'```', '', text)
            text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1:
                text = text[start:end+1]
            text = re.sub(r',\s*}', '}', text)
            text = re.sub(r',\s*]', ']', text)
            return text
        except: return "{}"
    
    def _clean_html(self, text):
        text = text.replace("```html", "").replace("```", "").strip()
        return text

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro, news, risk, strategy_type="core"):
        """
        [战术层] 联邦投委会 (V3.2) - 引入 IC 角色纪律规范 v2.0
        """
        fuse_level = risk['fuse_level']
        fuse_msg = risk['risk_msg']
        trend_score = tech.get('quant_score', 50)
        
        # [V15.20 改动] 植入 CGO/CRO/CIO 角色纪律与铁律
        prompt = f"""
        【系统架构】鹊知风投委会 (IC) | 角色纪律规范 v2.0
        当前物理位置: 日本 (Japan) | 视角: 国际投资者

        【标的信息】
        标的: {fund_name} (属性: {strategy_type})
        趋势强度: {trend_score}/100 | 熔断状态: Level{fuse_level} | 硬约束: {fuse_msg}
        技术指标: RSI={tech.get('rsi',50)} | MACD={tech.get('macd',{}).get('trend','-')}
        
        【实时舆情 (EastMoney + CLS)】
        {str(news)[:25000]}

        【角色纪律 (Strict IC Protocols)】
        
        1. 🐻 CRO (防守底线):
           - 核心: 保护本金，关注 Tail Risk (尾部风险) 和 Correlation (相关性)。
           - 铁律 (No Generic Fear): 禁止将“地缘政治”作为万能利空。地缘紧张对股票是利空，但对避险资产(黄金/能源)是**核心利好**。
           - 铁律 (Hedge over Liquidity): 当宏观风险极高时，拥有**"指标豁免权"**。即使流动性差，也必须建议配置对冲(Hedge)仓位，而非机械拒绝。

        2. 🦊 CGO (进攻锋线):
           - 核心: 寻找 Catalyst (催化剂) 和 Momentum (动量)。
           - 铁律 (No Forced Correlation): 禁止"强行关联"。必须证明新闻对该标的有 **Direct Causality** (直接营收/成本影响)。禁止 AI-washing (生硬蹭AI热点)。
           - 铁律 (Volume Confirmation): 拒绝缩量上涨。

        3. ⚖️ CIO (决策中枢):
           - 核心: 计算 Risk-Reward Ratio (盈亏比) 与 Position Sizing (仓位)。
           - 铁律: 必须给出具体的仓位调整建议 (adjustment)。
           - 视角: 必须包含 **JPY (日元)** 汇率维度及日本地缘视角的考量。

        【任务】
        仅基于提供的数据，模拟上述三位角色的辩论。
        若熔断 Level >= 2，直接执行风控清仓逻辑。

        【输出格式】
        {{
            "bull_view": "CGO观点 (聚焦赔率/催化剂/直接因果)",
            "bear_view": "CRO观点 (聚焦敞口/对冲/本金安全/非通用恐慌)",
            "chairman_conclusion": "CIO最终裁决 (包含日本视角/盈亏比计算)",
            "decision": "EXECUTE|REJECT|HOLD",
            "adjustment": -100 ~ 100 (建议仓位调整比例)
        }}
        """
        
        payload = {
            "model": self.model_tactical,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 800,
            "response_format": {"type": "json_object"}
        }
        
        try:
            resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, timeout=90)
            if resp.status_code != 200:
                return self._get_fallback_result()
            
            content = resp.json()['choices'][0]['message']['content']
            result = json.loads(self._clean_json(content))
            
            try: result['adjustment'] = int(result.get('adjustment', 0))
            except: result['adjustment'] = 0

            # 强制执行熔断逻辑
            if fuse_level >= 2:
                result['decision'] = 'REJECT'
                result['adjustment'] = -30
                result['chairman_conclusion'] = f'[系统熔断] {fuse_msg} - 强制执行风控纪律。'

            return result
        except Exception as e:
            logger.error(f"AI Analysis Failed {fund_name}: {e}")
            return self._get_fallback_result()

    def _get_fallback_result(self):
        return {"bull_view": "Error", "bear_view": "Error", "chairman_conclusion": "Offline", "decision": "HOLD", "adjustment": 0}

    @retry(retries=2, delay=5)
    def review_report(self, report_text, macro_str):
        """
        [战略层] CIO 复盘 (R1) - 策略一致性与宏观定调
        """
        current_date = datetime.now().strftime("%Y年%m月%d日")
        
        # [V15.20 改动] 引入 CIO 宏观定调纪律与策略一致性检查
        prompt = f"""
        【系统角色】鹊知风 CIO (Chief Investment Officer) | 战略复盘
        日期: {current_date} | 物理位置: 日本 (Japan)

        【输入数据】
        1. 宏观环境 (News Flow): {macro_str[:2500]}
        2. 交易决策 (IC Decisions): {report_text[:3000]}

        【战略任务】
        请撰写《每日投资复盘备忘录》，重点执行以下纪律：

        1. 宏观定调 (Macro Regime):
           - 定义今日市场情绪：恐慌(Panic) / 贪婪(Greed) / 分歧(Divergence)。
           - 必须识别主要矛盾（如：地缘政治 vs 政策宽松）。

        2. 策略一致性检查 (Strategy Consistency Check):
           - 审查投委会的操作是否精神分裂？
           - 例如：如果宏观定调为"极度恐慌"，但决策却在买入高风险小盘股，请严厉指出。
           - 检查是否忽视了日本视角（如日元汇率风险）。

        3. 风险提示 (Risk Radar):
           - 指出数据中隐含的 Tail Risk (尾部风险)。
           - 重点关注流动性陷阱和相关性崩塌。

        【输出】HTML格式 CIO 备忘录。
        """
        return self._call_r1(prompt)

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        """
        [审计层] Red Team 顾问 (R1) - 逻辑黑客与压力测试
        """
        current_date = datetime.now().strftime("%Y年%m月%d日")
        
        # [V15.20 改动] 引入 Red Team 逻辑黑客审计
        prompt = f"""
        【系统角色】鹊知风 Red Team | 独立逻辑黑客 (Logic Hacker)
        日期: {current_date}

        【输入数据】
        宏观: {macro_str[:2500]} | 交易: {report_text[:3000]}

        【审计任务】
        作为"找茬专家"，请无情地攻击 CIO 的决策逻辑。寻找 Blind Spot (盲区) 和 Overfitting (过拟合)。

        【五维压力测试 (Stress Test)】
        Q1: 决策激进性审计 (是否在接飞刀?)
        Q2: 宏观逻辑漏洞 (是否用同样的宏观理由解释完全相反的交易?)
        Q3: 仓位合理性 (是否处于"裸奔"状态，缺乏 Hedging?)
        Q4: 趋势背离风险 (是否在对抗不可逆转的趋势?)
        Q5: 情绪化交易检测 (CGO 是否存在强行关联/AI-washing?)

        【输出】HTML格式风控审计报告，必须包含"关键漏洞"和"风险评级"。
        """
        return self._call_r1(prompt)

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
