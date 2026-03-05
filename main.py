import yaml
import os
import threading
import time
import random
import json
import concurrent.futures  # [新增] 引入多线程并发库
from datetime import datetime

from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from technical_analyzer import TechnicalAnalyzer
from valuation_engine import ValuationEngine
from portfolio_tracker import PortfolioTracker
from market_scanner import MarketScanner
from utils import send_email, logger, LOG_FILENAME, get_beijing_time

# 导入 UI 渲染器
from ui_renderer import render_html_report_v19

# --- 全局配置 ---
TEST_MODE = False
tracker_lock = threading.Lock()

def load_config():
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"配置文件读取失败: {e}")
        return {"funds": [], "global": {"base_invest_amount": 1000, "max_daily_invest": 5000}}

def calculate_position_v19(tech, mode, final_decision, val_mult, val_desc, base_amt, max_daily, pos, strategy_type, fund_name):
    """
    V19.6.5 绝对服从型资金计算算法 (根治神经割裂)
    彻底抛弃纯依靠技术面打分的买卖映射，严格按 A/B/C/D 轨和风控决议执行！
    """
    base_score = tech.get('quant_score', 50)
    
    tactical_mult = 0
    reasons = []
    is_sell = False
    label = "观望"
    
    # 1. 核心修复：严格遵循投委会指令决定大方向，禁止技术分越权
    if final_decision == "EXECUTE":
        # 批准进攻 (A/B/C轨)
        if mode == 'A': 
            tactical_mult = 1.5; reasons.append("🚀 A轨:趋势跟随")
        elif mode == 'B': 
            tactical_mult = 1.0; reasons.append("🛡️ B轨:超跌反转")
        elif mode == 'C': 
            tactical_mult = 1.2; reasons.append("⚡ C轨:事件驱动")
        else:
            tactical_mult = 1.0; reasons.append("✅ 投委会批准")
            
    elif final_decision == "REJECT":
        # 风控否决
        tactical_mult = -1.0 if pos['shares'] > 0 else 0.0
        reasons.append("❌ 风控一票否决")
        
    else: # HOLD, HOLD_CASH, 或 D轨
        # D轨防守时间，绝不主动买入。若技术面破位(分数<30)且持有仓位，则减仓。
        if base_score < 30 and pos['shares'] > 0:
            tactical_mult = -1.0; reasons.append("🗑️ D轨:技术破位止损")
        else:
            tactical_mult = 0.0; reasons.append("☕ D轨:强制防守观望")

    # 2. 估值战略修正 (防追高，防割地板)
    if tactical_mult > 0: # 准备买入时
        if val_mult <= 0.5: 
            tactical_mult = 0; reasons.append("⚠️ 极度高估(禁止买入)")
        elif val_mult > 1.2: 
            tactical_mult *= min(val_mult, 1.5); reasons.append("💰 低估放大仓位")
    elif tactical_mult < 0: # 准备卖出时
        if val_mult > 1.2: 
            tactical_mult = 0; reasons.append("🔒 极度低估(拒绝割肉)")
            label = "低估锁仓"
            
    # 3. 规则锁仓保护 (防频繁交易)
    held_days = pos.get('held_days', 999)
    if tactical_mult < 0 and pos['shares'] > 0 and held_days < 7:
        tactical_mult = 0; reasons.append(f"⏳ T+7锁仓期({held_days}天)")
        label = "持仓观望"

    # 4. 计算最终金额
    final_amt = 0; sell_val = 0
    if tactical_mult > 0:
        final_amt = max(0, min(int(base_amt * tactical_mult), int(max_daily)))
        label = "买入"
    elif tactical_mult < 0:
        is_sell = True
        sell_val = pos['shares'] * tech.get('price', 0) * min(abs(tactical_mult), 1.0)
        label = "卖出"

    if final_decision in ["HOLD_CASH", "HOLD", "REJECT"] and pos['shares'] == 0: 
        label = "空仓"

    tech['quant_reasons'] = reasons
    tech['valuation_desc'] = val_desc
    return final_amt, label, is_sell, sell_val

def process_phase1_proposal(fund, fetcher, tracker, val_engine, analyst, market_context):
    """
    [Phase 1] 战术层提案收集
    """
    # 增加一点随机延时，防止多线程同时发起请求时撞倒 API 并发限制
    time.sleep(random.uniform(1.0, 3.0))
    
    fund_name = fund['name']; fund_code = fund['code']
    logger.info(f"🔍 [IC初审] 分析标的: {fund_name} ({fund_code})")

    try:
        data = fetcher.get_fund_history(fund_code)
        if data is None or data.empty: 
            logger.warning(f"❌ 数据获取失败: {fund_name}")
            return None
        
        analyzer = TechnicalAnalyzer(asset_type='ETF') 
        tech = analyzer.calculate_indicators(data)
        if not tech: return None
        
        val_mult, val_desc = val_engine.get_valuation_status(fund_code, data)
        
        if analyst:
            macro_payload = {"net_flow": market_context.get('net_flow', 0), "leader_status": "UNKNOWN"}
            ic_res = analyst.analyze_fund_tactical_v6(
                fund_name, tech, macro_payload, market_context.get('news_summary', ''), 
                {"fuse_level": 0}, fund.get('strategy_type', 'core')
            )
        else:
            ic_res = None

        if not ic_res:
            decision = "HOLD" if tech['quant_score'] < 70 else "PROPOSE_EXECUTE"
            ic_res = {
                "chairman_verdict": {"mode_selected": "D" if decision=="HOLD" else "A"},
                "mode_justification": "AI 离线，基于规则运行",
                "debate_transcript": {}
            }

        verdict = ic_res.get('chairman_verdict', {})
        mode = verdict.get('mode_selected', 'D')
        violation_check = ic_res.get('constraint_violation_check', {})
        
        if violation_check.get('violated') == 'TRUE':
            decision = "REJECT"
            logic_weighting = f"⚠️系统拦截: {violation_check.get('violation_details')}"
        elif mode in ['A', 'B', 'C']:
            decision = "PROPOSE_EXECUTE"
            logic_weighting = ic_res.get('mode_justification', f'确信度 {verdict.get("confidence", "-")}')
        else:
            decision = "HOLD"
            logic_weighting = "进入D轨(防御/垃圾时间)或无明确进攻信号"

        verdict['logic_weighting'] = logic_weighting
        
        proposal = {
            "name": fund_name, "code": fund_code,
            "tech": tech, "val_mult": val_mult, "val_desc": val_desc,
            "ic_res": ic_res, 
            "decision": decision, 
            "fund_obj": fund
        }
        
        logger.info(f"   -> IC初审: {decision} | 模式:{mode} | 逻辑:{logic_weighting[:20]}...")
        return proposal

    except Exception as e:
        logger.error(f"IC Process Error {fund_name}: {e}", exc_info=True)
        return None

def main():
    config = load_config()
    fetcher, tracker, val_engine = DataFetcher(), PortfolioTracker(), ValuationEngine()
    scanner = MarketScanner()
    
    tracker.confirm_trades()
    
    try: analyst = NewsAnalyst()
    except: analyst = None

    logger.info("🚀 启动 v19.7 认知对抗系统 (Cognitive Adversarial Model)...")

    market_context = {"news_summary": "无新闻", "net_flow": 0}
    all_news_seen = []
    
    if analyst:
        logger.info("📡 正在进行宏观扫描与资金流检测...")
        news_text = analyst.get_market_context()
        net_flow_val = fetcher.get_market_net_flow()
        
        market_context = {
            "news_summary": news_text,
            "net_flow": net_flow_val
        }
        all_news_seen = [line.strip() for line in news_text.split('\n') if line.strip().startswith('[')]
        logger.info(f"🌍 市场状态: 资金流 {market_context['net_flow']} 亿")

    funds = config.get('funds', [])
    if TEST_MODE and funds: 
        logger.info("🚧 测试模式：仅处理前2个标的")
        funds = funds[:2]

    # 🟢 [关键修复 1] 在多线程并发前，预先在主线程中拉取并缓存所有数据
    # 彻底杜绝 curl_cffi 在多线程环境下的竞争冲突，解决“个别板块随机报错”的问题
    logger.info("📥 [Pre-Phase] 预加载所有 ETF 行情数据...")
    fetcher.run(funds)

    # ===================================================
    # Phase 1: IC 战术投委会海选 (Proposal Collection)
    # ===================================================
    logger.info("⚔️ [Phase 1] 启动 IC 战术投委会海选 (多线程并发处理)...")
    proposals = []
    candidates_for_veto = [] 
    
    # [修改点] 开启多线程处理
    MAX_WORKERS = 5 # 默认 5 个并发，兼顾速度与防止 API 触发 429 限流
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 将所有的 fund 提交给线程池
        future_to_fund = {
            executor.submit(process_phase1_proposal, fund, fetcher, tracker, val_engine, analyst, market_context): fund
            for fund in funds
        }
        
        # 收集执行结果
        for future in concurrent.futures.as_completed(future_to_fund):
            fund = future_to_fund[future]
            try:
                p = future.result()
                if p:
                    proposals.append(p)
                    if 'EXECUTE' in p['decision'] and 'PROPOSE' in p['decision']:
                        verdict = p['ic_res'].get('chairman_verdict', {})
                        candidates_for_veto.append({
                            "code": p['code'],
                            "name": p['name'],
                            "mode": verdict.get('mode_selected', 'UNKNOWN'),
                            "reason": verdict.get('logic_weighting', '无'),
                            "key_assumption": verdict.get('key_assumption', ''),
                            "tech_score": p['tech']['quant_score']
                        })
            except Exception as e:
                logger.error(f"处理标的 {fund.get('name', 'Unknown')} 时发生多线程异常: {e}")

    # ===================================================
    # Phase 2: 风控委员会终审 (Risk Committee Veto)
    # ===================================================
    logger.info(f"⚖️ [Phase 2] 启动风控委员会终审 (待审提案: {len(candidates_for_veto)}个)...")
    
    risk_report = {"approved_list": [], "rejected_log": [], "risk_summary": "无提案提交"}
    approved_codes = []
    
    if candidates_for_veto and analyst:
        risk_report_raw = analyst.run_risk_committee_veto(candidates_for_veto)
        
        for item in risk_report_raw.get('stress_test_results', []):
            code = item.get('code')
            decision = item.get('veto_decision', 'VETO')
            reason = item.get('adjustment_reason', '')
            
            if decision in ['APPROVE', 'DEMOTE']:
                approved_codes.append(code)
                risk_report['approved_list'].append({"code": code, "reason": f"[{decision}] {reason}"})
            else:
                risk_report['rejected_log'].append({"code": code, "reason": f"[{decision}] {reason}"})
                
        logger.info(f"✅ 风控批准/降级: {len(approved_codes)} 个 | ❌ 风控驳回: {len(risk_report['rejected_log'])} 个")
    elif not candidates_for_veto:
        logger.info("👀 本轮无激进提案，跳过风控终审。")

    # ===================================================
    # Phase 3: 最终执行与报告生成 (Execution)
    # ===================================================
    logger.info("📝 [Phase 3] 生成最终执行指令...")
    
    final_results = []
    
    for p in proposals:
        code = p['code']
        raw_decision = p['decision']
        verdict = p['ic_res'].get('chairman_verdict', {})
        
        # --- 核心逻辑：风控一票否决 ---
        final_decision = raw_decision
        
        if 'PROPOSE_EXECUTE' in raw_decision:
            if code in approved_codes:
                final_decision = 'EXECUTE' 
                for item in risk_report.get('approved_list', []):
                    if item.get('code') == code:
                        verdict['logic_weighting'] += f" [✅风控终审: {item.get('reason')}]"
            else:
                final_decision = 'REJECT'  
                for item in risk_report.get('rejected_log', []):
                    if item.get('code') == code:
                        verdict['logic_weighting'] += f" [❌风控驳回: {item.get('reason')}]"
        
        calc_decision = "HOLD"
        if final_decision == "EXECUTE": calc_decision = "EXECUTE" 
        elif final_decision == "REJECT": calc_decision = "REJECT"
        elif final_decision == "HOLD_CASH": calc_decision = "HOLD_CASH"

        mode = verdict.get('mode_selected', 'D')
        
        amt, lbl, is_sell, s_val = calculate_position_v19(
            p['tech'], mode, calc_decision, p['val_mult'], p['val_desc'],
            config['global']['base_invest_amount'], config['global']['max_daily_invest'],
            tracker.get_position(code), p['fund_obj'].get('strategy_type'), p['name']
        )
        
        with tracker_lock:
            tracker.record_signal(code, lbl)
            if amt > 0: tracker.add_trade(code, p['name'], amt, p['tech']['price'])
            elif is_sell: tracker.add_trade(code, p['name'], s_val, p['tech']['price'], True)
            
        debate_str = ""
        trans = p['ic_res'].get('debate_transcript', {})
        if isinstance(trans, dict):
            for role, speech in trans.items():
                if isinstance(speech, dict):
                    stance = speech.get('stance', '')
                    content = speech.get('analysis', speech.get('odds_calculation', speech.get('tail_risk_scenario', '')))
                    debate_str += f"**{role}** ({stance}): {content}\n\n"
                else:
                    debate_str += f"**{role}**: {speech}\n\n"
        
        key_assumption = verdict.get('key_assumption', '')
        hard_stop = verdict.get('hard_stop_loss', '')
        time_stop = verdict.get('time_stop', '')
        if key_assumption or hard_stop:
            debate_str += f"---\n"
            if key_assumption: debate_str += f"**💠核心前提**: {key_assumption}\n"
            if hard_stop: debate_str += f"**🛡️硬止损**: {hard_stop} | **时限**: {time_stop}\n"
        
        ai_full_adapted = {
            "strategy_meta": {
                "mode": mode,
                "rationale": verdict.get('logic_weighting', '无逻辑')
            },
            "trend_analysis": {
                "days_to_event": p['ic_res'].get('days_to_event', 'NULL'),
                "stage": f"Tech:{p['tech']['quant_score']}分"
            },
            "execution_notes": debate_str[:800],
            "cro_risk_audit": {
                "fundamental_check": "Risk Checked" if code in approved_codes else "See Reject Log"
            }
        }

        final_results.append({
            "name": p['name'], "code": code,
            "decision": lbl, "amount": amt, "is_sell": is_sell,
            "tech": p['tech'],
            "ai_full": ai_full_adapted
        })

    # ===================================================
    # [新增排序] 按模式 A/B/C/D 依次下排，符合阅读习惯
    # ===================================================
    final_results.sort(key=lambda x: x['ai_full']['strategy_meta'].get('mode', 'D'))

    cio_html = ""
    if analyst:
        logger.info("🧠 正在生成 CIO 战略定调 (基于风控报告)...")
        try:
            raw_cio = analyst.generate_cio_strategy(
                datetime.now().strftime("%Y-%m-%d"), 
                risk_report_raw if 'risk_report_raw' in locals() else risk_report
            )
            if raw_cio:
                # 兼容性修复：剔除可能存在的大模型 Markdown 代码块标记，防止 json 解析失败
                raw_cio = raw_cio.replace('```json', '').replace('```', '').strip()
                cio_html = raw_cio
            else:
                logger.warning("CIO 战略定调返回为空，启用默认安全结构。")
                cio_html = "{}"
        except Exception as e:
            logger.error(f"CIO 战略生成请求异常: {e}")
            cio_html = "{}"
        
    html = render_html_report_v19(all_news_seen, final_results, cio_html, "") 
    
    subject_prefix = "🚧 [测试] " if TEST_MODE else "🕊️ "
    # 还原邮件名称
    send_email(f"{subject_prefix}鹊知风 v19.7 认知对抗报告", html)
    
    logger.info("✅ 运行结束，邮件已发送。")

if __name__ == "__main__": main()
