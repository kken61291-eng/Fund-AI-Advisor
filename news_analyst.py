import requests
import json
import os
import re
import time
import logging
import difflib  # 🟢 [新增] 用于 RAG 提取结果的高精度去重
from datetime import datetime, timedelta
from utils import logger, retry, get_beijing_time

# 🟢 [静默底层烦人的网络请求日志 (修复红框刷屏)]
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("huggingface_hub").setLevel(logging.WARNING)
logging.getLogger("filelock").setLevel(logging.WARNING)

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

from prompts_config import (
    TACTICAL_IC_PROMPT, 
    STRATEGIC_CIO_REPORT_PROMPT, 
    RISK_CONTROL_VETO_PROMPT, 
    EVENT_TIER_DEFINITIONS
)

class NewsAnalyst:
    """
    新闻分析师 - V21.4 终极提纯版 (动态高精度去重 + 情绪分离度量)
    """
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        
        # 🟢 完全使用 R1/V3.2 模型
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
        
        self._has_logged_rag_sample = False

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
                embeddings = self.encoder.encode(texts_to_encode, normalize_embeddings=True, show_progress_bar=False)
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
        q_emb = self.encoder.encode([query], normalize_embeddings=True, show_progress_bar=False)
        
        # 扫描底层扩容到 50 条
        search_k = min(50, self.index.ntotal)
        D, I = self.index.search(q_emb, k=search_k)

        sector_catalysts = []
        accepted_titles = [] # 🟢 [新增] 用于存储已接纳的新闻标题，辅助去重
        hype_score_accumulator = 0.0
        valid_news_count = 0
        now = get_beijing_time()

        for idx, sim in zip(I[0], D[0]):
            if idx == -1 or sim < 0.40: continue # 过滤低相关度噪声
            news = self.news_data[idx]

            # 时间衰减权重计算
            try:
                t_str = news['time']
                if len(t_str) == 19:
                    news_time = datetime.strptime(t_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Shanghai'))
                else:
                    news_time = datetime.strptime(t_str, "%Y-%m-%d %H:%M").replace(tzinfo=pytz.timezone('Asia/Shanghai'))
                hours_diff = (now - news_time).total_seconds() / 3600.0
                decay_weight = np.exp(-0.05 * max(0, hours_diff))
            except:
                decay_weight = 0.8

            # 🟢 [核心剥离] 所有通过 0.4 阈值的新闻，全部计入热度分（说明媒体在疯狂复读，热度高）
            hype_score_accumulator += (sim * decay_weight)
            valid_news_count += 1

            # 🟢 [核心去重] 拦截废话：如果内容高度相似，不计入给 AI 的文本中
            news_title = news['title']
            is_duplicate = False
            for seen_title in accepted_titles:
                # 规则 1：子串包含关系 (如："特发信息涨停" 与 "光纤概念特发信息涨停")
                if news_title in seen_title or seen_title in news_title:
                    is_duplicate = True
                    break
                # 规则 2：语义高相似度匹配 (只要字符相似度 > 80% 则判定为重复)
                if difflib.SequenceMatcher(None, news_title, seen_title).quick_ratio() > 0.8:
                    is_duplicate = True
                    break
            
            if is_duplicate:
                continue # 发现重复，直接跳过文字拼装阶段

            # 记录唯一标题，并将情报加入 AI 投喂列表
            accepted_titles.append(news_title)
            entry = f"[{news['time']}] {news['title']} (相关度:{sim:.2f}, 衰减权重:{decay_weight:.2f}) - 核心实体: {news['entities']}"
            sector_catalysts.append(entry)

        # 归一化 Hype Score (0-100)
        hype_index = min(100, int(hype_score_accumulator * 6))
        macro_str = "\n".join([f"[{m['time']}] {m['title']}" for m in self.macro_news[:3]])

        # 组装全息面板
        rag_json = {
            "Macro_Environment": macro_str if macro_str else "今日无全局性宏观异动",
            "Sector_Catalysts": sector_catalysts if sector_catalysts else ["今日无高度相关板块催化剂"],
            "Quantitative_Resonance": {
                "Hype_Score": hype_index,
                "Total_Related_News_Scanned": valid_news_count,       # 底层发现的新闻数量（包含重复发酵）
                "Unique_Clues_Extracted": len(sector_catalysts),      # 去重后提取的精华独立线索数
                "System_Advice": f"底层侦测到 {valid_news_count} 次相关报道，去重后提炼出 {len(sector_catalysts)} 条独立线索。情绪分为 {hype_index}。"
            }
        }
        
        rag_result_str = json.dumps(rag_json, ensure_ascii=False, indent=2)
        
        if not self._has_logged_rag_sample:
            logger.info(f"🎯 [{fund_name}] RAG扫描完成 (首个示例) -> 总命中: {valid_news_count}条 | 去重后独立线索: {len(sector_catalysts)}条 | 情绪分: {hype_index}\n{rag_result_str}")
            self._has_logged_rag_sample = True
        else:
            logger.info(f"🎯 [{fund_name}] RAG扫描完成 -> 命中 {valid_news_count} 条，去重提取 {len(sector_catalysts)} 核心线索 | Hype Score: {hype_index}")
        
        return rag_result_str

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
        """ 保留极其稳健的本地新闻读取逻辑 """
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
                        
                        raw_time = str(item.get('time', ''))
                        time_str = raw_time[:16] if len(raw_time) >= 16 else raw_time
                        
                        entry = f"[{time_str}] [{source}] {title}"
                        if len(content) > 10 and content != title:
                            entry += f"\n   (摘要: {content[:200]}...)"
                        
                        news_candidates.append((raw_time, entry))
                    except: pass
        except Exception as e:
            logger.error(f"读取新闻文件出错: {e}")

        if not news_candidates: 
            return "今日暂无重大新闻。"
        
        news_candidates.sort(key=lambda x: x[0], reverse=True)
        
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
        self.init_rag_system()
        if not self.has_rag: return self._legacy_get_market_context(max_length)
        if self.macro_news:
            return "【全局宏观重磅】\n" + "\n".join([f"[{m['time']}] {m['title']}" for m in self.macro_news[:5]])
        return "今日暂无重大宏观新闻。"

    def _clean_json(self, text):
        try:
            text = re.sub(r'```json\s*', '', text)
            text = re.sub(r'```', '', text)
            text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
            start, end = text.find('{'), text.rfind('}')
            if start != -1 and end != -1: return text[start:end+1]
            return "{}"
        except: return "{}"

    def _safe_post_stream(self, payload, timeout=600):
        payload['stream'] = True
        full_content = ""
        
        resp = requests.post(f"{self.base_url}/chat/completions", headers=self.headers, json=payload, stream=True, timeout=timeout)
        
        if resp.status_code != 200:
            raise Exception(f"HTTP Error {resp.status_code}: {resp.text}")
            
        for line in resp.iter_lines():
            if line:
                line_str = line.decode('utf-8').strip()
                if line_str.startswith("data: "):
                    data_str = line_str[6:]
                    if data_str == "[DONE]": 
                        break
                    try:
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

        risk_reward = tech.get('risk_reward', {})
        upside_space = risk_reward.get('upside_space_pct', 0.0)
        downside_risk = risk_reward.get('downside_risk_pct', 0.0)
        ratio = risk_reward.get('ratio', 0.0)

        # 🟢 如果开启了 RAG，获取极高密度的结构化情报 JSON
        if self.has_rag and self.index is not None:
            final_news_content = self.get_fund_rag_context(fund_name, sector_keyword)
        else:
            final_news_content = str(news_text)[:8000]

        try:
            prompt = TACTICAL_IC_PROMPT.format(
                market_risk_level="MEDIUM", 
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
                drawdown_20d=tech.get('drawdown_20d', 5), 
                volume_percentile=tech.get('volume_percentile', 50),
                
                upside_space=upside_space,
                downside_risk=downside_risk,
                ratio=ratio,
                
                net_flow=f"{macro_data.get('net_flow', 0)}",
                leader_status=macro_data.get('leader_status', 'UNKNOWN'),
                sector_breadth=tech.get('sector_breadth', 50),
                days_to_event=days_to_event,
                event_tier=event_tier,
                decayed_weight=0.8,
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
            raw_text = self._safe_post_stream(payload, timeout=600)
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
