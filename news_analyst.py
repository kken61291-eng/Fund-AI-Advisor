import requests
import json
import os
import re
import time
from datetime import datetime, timedelta
from utils import logger, retry, get_beijing_time

# 🟢 [RAG 与 NLP 依赖接入]
try:
    import faiss
    import numpy as np
    from sentence_transformers import SentenceTransformer
    import jieba
    import jieba.analyse
    import pytz
    HAS_RAG_DEPS = True
except ImportError:
    HAS_RAG_DEPS = False

# 【关键修改】移除了 RED_TEAM_AUDIT_PROMPT，防止 ImportError
from prompts_config import (
    TACTICAL_IC_PROMPT, 
    STRATEGIC_CIO_REPORT_PROMPT, 
    RISK_CONTROL_VETO_PROMPT, 
    EVENT_TIER_DEFINITIONS
)

class NewsAnalyst:
    """
    新闻分析师 - V20.0 终极全息图谱版 (GraphRAG + NLP to Alpha)
    """
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        
        # 🟢 完全使用 R1 模型
        self.model_tactical = "Pro/deepseek-ai/DeepSeek-V3.2"       
        self.model_strategic = "Pro/deepseek-ai/DeepSeek-R1"    

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # 🟢 RAG 核心组件初始化
        self.has_rag = HAS_RAG_DEPS
        self.encoder = None
        self.index = None
        self.news_data = []
        self.macro_news = []

    def init_rag_system(self):
        """ 🟢 核心提效：构建基于本地新闻的向量知识库与实体图谱 """
        if not self.has_rag:
            logger.warning("⚠️ 缺少 RAG 依赖 (faiss-cpu, sentence-transformers, jieba)，自动降级为传统文本拼接模式。")
            return

        if self.index is not None:
            return # 已经初始化过

        logger.info("🧠 [RAG] 正在加载 BGE 向量模型与 NLP 实体抽取引擎...")
        try:
            self.encoder = SentenceTransformer('BAAI/bge-small-zh-v1.5')
        except Exception as e:
            logger.error(f"向量模型加载失败，关闭 RAG 功能: {e}")
            self.has_rag = False
            return

        today_str = get_beijing_time().strftime("%Y-%m-%d")
        possible_paths = [f"data_news/news_{today_str}.jsonl", f"news_{today_str}.jsonl"]
        
        target_file = None
        for p in possible_paths:
            if os.path.exists(p):
                target_file = p
                break
        
        if not target_file: 
            logger.warning(f"⚠️ [RAG] 未找到今日新闻文件，跳过图谱构建。")
            return

        texts_to_encode = []
        try:
            with open(target_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip(): continue
                    try:
                        item = json.loads(line)
                        title = str(item.get('title', '')).strip()
                        content = str(item.get('content') or item.get('digest') or "").strip()
                        if not title or len(title) < 2: continue
                        
                        raw_time = str(item.get('time', ''))
                        full_text = f"{title}。{content}"
                        
                        # 🟢 NER：基于 TF-IDF 提取核心标签，提纯信息熵
                        entities = jieba.analyse.extract_tags(full_text, topK=3)
                        
                        news_obj = {
                            "time": raw_time,
                            "title": title,
                            "content": content[:200],
                            "entities": entities
                        }
                        self.news_data.append(news_obj)
                        texts_to_encode.append(title)
                        
                        # 全局宏观新闻分流 (TIER_S 预判)
                        macro_kws = ["央行", "降息", "降准", "美联储", "重磅", "政治局", "国务院", "外汇局", "发改委"]
                        if any(k in title for k in macro_kws):
                            self.macro_news.append(news_obj)
                            
                    except Exception: pass
        except Exception as e:
            logger.error(f"[RAG] 读取新闻库报错: {e}")

        if texts_to_encode:
            logger.info(f"🧬 [RAG] 正在清洗并向量化 {len(texts_to_encode)} 条全市场新闻...")
            try:
                embeddings = self.encoder.encode(texts_to_encode, normalize_embeddings=True)
                self.index = faiss.IndexFlatIP(embeddings.shape[1]) # 内积计算余弦相似度
                self.index.add(embeddings)
                logger.info("✅ [RAG] 全息向量图谱构建完成！")
            except Exception as e:
                logger.error(f"[RAG] 构建 FAISS 索引失败: {e}")
                self.has_rag = False

    def get_fund_rag_context(self, fund_name, sector_keyword):
        """ 🟢 NLP to Alpha: 为单个基金执行精确语义检索，并计算情绪共振指数 """
        if not self.has_rag or self.index is None or self.index.ntotal == 0:
            return "无 RAG 增强数据"

        # 融合基金名称与配置表中的板块特征，实现精确狙击
        query = f"{fund_name} {sector_keyword}"
        q_emb = self.encoder.encode([query], normalize_embeddings=True)
        
        # 检索 Top 8 最相关新闻
        D, I = self.index.search(q_emb, k=8)

        sector_catalysts = []
        hype_score_accumulator = 0.0
        now = get_beijing_time()

        for idx, sim in zip(I[0], D[0]):
            if idx == -1 or sim < 0.40: continue # 过滤低相关度噪声
            news = self.news_data[idx]

            # 🟢 时间衰减权重计算 (越新的新闻效力越强)
            try:
                t_str = news['time']
                if len(t_str) == 19:
                    news_time = datetime.strptime(t_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Shanghai'))
                else:
                    news_time = datetime.strptime(t_str, "%Y-%m-%d %H:%M").replace(tzinfo=pytz.timezone('Asia/Shanghai'))
                hours_diff = (now - news_time).total_seconds() / 3600.0
                decay_weight = np.exp(-0.05 * max(0, hours_diff)) # 指数衰减
            except:
                decay_weight = 0.8

            # 累加情绪共振因子
            hype_score_accumulator += (sim * decay_weight)

            entry = f"[{news['time']}] {news['title']} (相关度:{sim:.2f}, 衰减权重:{decay_weight:.2f}) - 核心实体: {news['entities']}"
            sector_catalysts.append(entry)

        # 归一化 Hype Score (0-100)
        hype_index = min(100, int(hype_score_accumulator * 15))
        macro_str = "\n".join([f"[{m['time']}] {m['title']}" for m in self.macro_news[:3]])

        # 组装极其紧凑、结构化的全息降维面板，喂给 R1
        rag_json = {
            "Macro_Environment": macro_str if macro_str else "今日无全局性宏观异动",
            "Sector_Catalysts": sector_catalysts if sector_catalysts else ["今日无高度相关板块催化剂"],
            "Quantitative_Resonance": {
                "Hype_Score": hype_index,
                "News_Count": len(sector_catalysts),
                "System_Advice": "Hype_Score > 60 说明情绪高度共振，< 30 说明无利好支撑"
            }
        }
        return json.dumps(rag_json, ensure_ascii=False, indent=2)

    def extract_event_info(self, news_text):
        days_to_event = "NULL"
        event_tier = "TIER_C"
        try:
            match = re.search(r'(\d+)\s*天后', str(news_text))
            if match: days_to_event = int(match.group(1))
            
            s_keywords = ["议息", "五年规划", "中央", "重磅"]
            a_keywords = ["大会", "发布", "财报", "数据"]
            
            if any(k in str(news_text) for k in s_keywords): event_tier = "TIER_S"
            elif any(k in str(news_text) for k in a_keywords):
                event_tier = "TIER_A"
                if days_to_event == "NULL": days_to_event = 5
        except Exception as e:
            logger.warning(f"事件提取失败: {e}")
        return days_to_event, event_tier

    def _legacy_get_market_context(self, max_length=35000): 
        """
        保留极其稳健的本地新闻读取逻辑 (作为未安装 RAG 环境的退路)
        """
        today_str = get_beijing_time().strftime("%Y-%m-%d")
        possible_paths = [f"data_news/news_{today_str}.jsonl", f"news_{today_str}.jsonl"]
        
        target_file = None
        for p in possible_paths:
            if os.path.exists(p):
                target_file = p
                break
        
        if not target_file: 
            logger.warning(f"⚠️ 未找到今日新闻文件，尝试路径: {possible_paths}")
            return "今日暂无重大新闻。"

        news_candidates = []
        try:
            with open(target_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip(): continue
                    try:
                        item = json.loads(line)
                        title = str(item.get('title', '')).strip()
                        if not title or len(title) < 2: continue
                        
                        content = str(item.get('content') or item.get('digest') or "").strip()
                        source = str(item.get('source', 'Local')).strip()
                        
                        # 提取并格式化时间
                        raw_time = str(item.get('time', ''))
                        time_str = raw_time[:16] if len(raw_time) >= 16 else raw_time
                        
                        entry = f"[{time_str}] [{source}] {title}"
                        if len(content) > 10 and content != title:
                            # 摘要截断，防止单条新闻过长
                            entry += f"\n   (摘要: {content[:200]}...)"
                        
                        # 将 raw_time 作为排序依据一起存入
                        news_candidates.append((raw_time, entry))
                    except: pass
        except Exception as e:
            logger.error(f"读取新闻文件出错: {e}")

        if not news_candidates: 
            return "今日暂无重大新闻。"
        
        # 1. 按时间倒序排列 (最新的排在前面)
        news_candidates.sort(key=lambda x: x[0], reverse=True)
        
        # 2. 截断长度，防止超长
        final_list = []
        current_len = 0
        for _, entry in news_candidates:
            if current_len + len(entry) < max_length:
                final_list.append(entry)
                current_len += len(entry) + 1 
            else:
                break
                
        result_text = "\n".join(final_list)
        logger.info(f"📰 成功加载新闻 {len(final_list)} 条，总字数: {len(result_text)}")
        return result_text

    def get_market_context(self, max_length=35000):
        """ 🟢 挂载点：系统启动时触发构建 RAG 图谱 """
        self.init_rag_system()
        
        if not self.has_rag:
            return self._legacy_get_market_context(max_length)
            
        # 返回精简版的宏观新闻给 main.py 进行日志打印
        if self.macro_news:
            return "【全局宏观重磅】\n" + "\n".join([f"[{m['time']}] {m['title']}" for m in self.macro_news[:5]])
        return "今日暂无重大宏观新闻。"

    def _clean_json(self, text):
        try:
            text = re.sub(r'```json\s*', '', text)
            text = re.sub(r'```', '', text)
            # 清理 R1 的思考过程
            text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
            start, end = text.find('{'), text.rfind('}')
            if start != -1 and end != -1: return text[start:end+1]
            return "{}"
        except: return "{}"

    def _safe_post_stream(self, payload, timeout=600):
        """
        🟢 [防御断连核心] 使用流式接收(Stream)绕过服务器 300 秒网关静默超时。
        """
        payload['stream'] = True
        full_content = ""
        
        resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, stream=True, timeout=timeout)
        
        if resp.status_code != 200:
            raise Exception(f"HTTP Error {resp.status_code}: {resp.text}")
            
        # 逐行读取流数据，只要有数据流动，网关就不会断开 TCP 连接
        for line in resp.iter_lines():
            if line:
                line_str = line.decode('utf-8').strip()
                if line_str.startswith("data: "):
                    data_str = line_str[6:]
                    if data_str == "[DONE]": 
                        break
                    try:
                        # stream 流数据往往不包含全量 JSON，这不影响，只提取 delta
                        chunk = json.loads(data_str, strict=False)
                        if "choices" in chunk and len(chunk["choices"]) > 0:
                            delta = chunk["choices"][0].get("delta", {})
                            if "content" in delta and delta["content"]:
                                full_content += delta["content"]
                    except json.JSONDecodeError:
                        continue
                        
        if not full_content:
            raise Exception("API 返回流为空")
            
        return full_content

    @retry(retries=1, delay=2)
    def analyze_fund_tactical_v6(self, fund_name, tech, macro_data, news_text, risk, strategy_type="core", sector_keyword=""):
        trend_score = tech.get('quant_score', 0)
        days_to_event, event_tier = self.extract_event_info(news_text)

        # 🟢 提取系统计算的风报比/盈亏比数据
        risk_reward = tech.get('risk_reward', {})
        upside_space = risk_reward.get('upside_space_pct', 0.0)
        downside_risk = risk_reward.get('downside_risk_pct', 0.0)
        ratio = risk_reward.get('ratio', 0.0)

        # 🟢 [核心打击] 如果开启了 RAG，直接抛弃冗长的 news_text，获取极高密度的结构化情报 JSON
        if self.has_rag and self.index is not None:
            final_news_content = self.get_fund_rag_context(fund_name, sector_keyword)
        else:
            final_news_content = str(news_text)[:8000]

        try:
            # 【关键补齐】填补 v19.6.5 Prompt 必须的额外参数，防止 KeyError 崩溃
            prompt = TACTICAL_IC_PROMPT.format(
                market_risk_level="MEDIUM", # 此处可升级为动态水位检测
                allowed_modes="['A', 'B', 'C']",
                forbidden_modes="[]",
                max_position="15%",
                cash_ratio="30%",
                total_position_max="70%",
                fund_name=fund_name, 
                fund_code=tech.get('code', 'N/A'),
                trend_score=trend_score, 
                rsi=tech.get('rsi', 50),
                volatility_status=tech.get('volatility_status', '-'),
                recent_gain=tech.get('recent_gain', 0),
                drawdown_20d=tech.get('drawdown_20d', 5), # 降级默认值
                volume_percentile=tech.get('volume_percentile', 50),
                
                # 将测算好的风报比数据注入到 Prompt 供 CGO 裁决
                upside_space=upside_space,
                downside_risk=downside_risk,
                ratio=ratio,
                
                net_flow=f"{macro_data.get('net_flow', 0)}",
                leader_status=macro_data.get('leader_status', 'UNKNOWN'),
                sector_breadth=tech.get('sector_breadth', 50),
                days_to_event=days_to_event,
                event_tier=event_tier,
                decayed_weight=0.8, # 这里的硬编码在 RAG 内已经动态计算了，保留以防 Prompt 缺参数报错
                decay_func="exponential",
                news_content=final_news_content,
                fundamental_risk="立案调查, 财务造假, 退市风险"
            )
        except Exception as e:
            logger.error(f"IC Prompt构造失败: {e}", exc_info=True)
            return None

        payload = {
            "model": self.model_tactical, 
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.4,
            "max_tokens": 8000, 
            "response_format": {"type": "json_object"}
        }
        
        try:
            # 加入流式处理防断连
            raw_text = self._safe_post_stream(payload, timeout=600)
            # 终极防崩溃：strict=False 允许大模型在文本里输入原生换行符和制表符
            result = json.loads(self._clean_json(raw_text), strict=False)
            result['days_to_event'] = days_to_event
            return result
        except Exception as e:
            logger.error(f"IC Analysis Failed {fund_name}: {e}")
            return None

    @retry(retries=2, delay=5)
    def run_risk_committee_veto(self, candidates):
        if not candidates: return {"approved_list": [], "rejected_log": [], "risk_summary": "无提案提交"}

        candidates_str = json.dumps(candidates, indent=2, ensure_ascii=False)
        
        try:
            # 【关键补齐】V19.6.5 风控测试矩阵必须的参数
            prompt = RISK_CONTROL_VETO_PROMPT.format(
                market_risk_level="MEDIUM",
                candidate_count=len(candidates),
                emergency_override_status="NONE",
                candidates_context=candidates_str,
                assumption_break_scenario="核心逻辑被证伪或触发技术位下破"
            )
        except Exception as e:
            logger.error(f"Risk Prompt构造失败: {e}", exc_info=True)
            return {"approved_list": [], "rejected_log": [{"code": "ALL", "reason": "Prompt构建崩溃"}], "risk_summary": "Error"}

        payload = {
            "model": self.model_strategic, 
            "messages": [{"role": "user", "content": prompt}], 
            "temperature": 0.2, 
            "max_tokens": 8000
        }
        
        try:
            raw_text = self._safe_post_stream(payload, timeout=600)
            return json.loads(self._clean_json(raw_text), strict=False)
        except Exception as e:
            logger.error(f"Risk Veto Failed: {e}")
            return {"approved_list": [], "rejected_log": [{"code": "ALL", "reason": "风控服务超时"}], "risk_summary": "System Error"}

    @retry(retries=2, delay=5)
    def generate_cio_strategy(self, current_date, risk_report_json):
        try:
            prompt = STRATEGIC_CIO_REPORT_PROMPT.format(
                current_date=current_date,
                market_risk_level="MEDIUM",
                risk_level_rationale="多因子综合评估（资金流动、波动率中性）",
                allowed_modes="['A', 'B', 'C']",
                cash_ratio="30%",
                max_position="15%",
                risk_committee_json=json.dumps(risk_report_json, indent=2, ensure_ascii=False)
            )
        except Exception as e:
            logger.error(f"CIO Prompt构造失败: {e}", exc_info=True)
            return "<p>战略研判生成失败，系统降级运行。</p>"

        return self._call_r1_text(prompt)

    @retry(retries=1, delay=2)
    def analyze_fund_v5(self, fund_name, tech, macro_data, news_text, risk, strategy_type="core", sector_keyword=""):
        return self.analyze_fund_tactical_v6(fund_name, tech, macro_data, news_text, risk, strategy_type, sector_keyword)

    @retry(retries=2, delay=5)
    def review_report(self, report_text, macro_str):
        prompt = STRATEGIC_CIO_REPORT_PROMPT.format(
            current_date=datetime.now().strftime("%Y-%m-%d"), 
            market_risk_level="MEDIUM",
            risk_level_rationale="例行兜底分析",
            allowed_modes="['A', 'B', 'C']",
            cash_ratio="30%",
            max_position="15%",
            risk_committee_json=json.dumps({"summary": report_text}, ensure_ascii=False)
        )
        return self._call_r1_text(prompt)

    @retry(retries=2, delay=5)
    def advisor_review(self, report_text, macro_str):
        return ""

    def _call_r1_text(self, prompt):
        payload = {
            "model": self.model_strategic, 
            "messages": [{"role": "user", "content": prompt}], 
            "max_tokens": 8000, 
            "temperature": 0.3
        }
        try:
            raw_text = self._safe_post_stream(payload, timeout=600)
            clean_str = self._clean_json(raw_text)
            try:
                parsed = json.loads(clean_str, strict=False)
                return json.dumps(parsed, ensure_ascii=False)
            except Exception:
                return clean_str
        except Exception as e:
            logger.error(f"CIO API 调用异常: {e}")
            return "{}"
