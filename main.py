import yaml
import os
import threading
import time
import random
import json
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

def calculate_position_v13(tech, ai_adj, ai_decision, val_mult, val_desc, base_amt, max_daily, pos, strategy_type, fund_name):
    """
    V13.3 核心算分逻辑 (适配 v19.6 对抗架构)
    """
    base_score = tech.get('quant_score', 50)
    try: ai_adj_int = int(ai_adj)
    except: ai_adj_int = 0

    # 1. 战术评分
    tactical_score = max(0, min(100, base_score + ai_adj_int))
    
    # 2. 执行指令过滤
    if ai_decision == "REJECT": 
        tactical_score = 0 
    elif ai_decision == "HOLD_CASH": 
        tactical_score = 0 
    elif ai_decision == "HOLD" and tactical_score >= 60: 
        tactical_score = 59
            
    # 3. 估值修正评分 (UI展示用)
    valuation_impact = 1.0
    if val_mult >= 1.3: valuation_impact = 1.1
    elif val_mult <= 0.5: valuation_impact = 0.6
    elif val_mult == 0.0: valuation_impact = 0.0
    
    final_display_score = int(tactical_score * valuation_impact)
    tech['final_score'] = max(0, min(100, final_display_score))
    tech['ai_adjustment'] = ai_adj_int
    tech['valuation_desc'] = val_desc
    
    # 4. 资金计算
    cro_signal = tech.get('tech_cro_signal', 'PASS')
    tactical_mult = 0
    reasons = []

    # 评分映射方向
    if tactical_score >= 85: tactical_mult = 2.0; reasons.append("战术:极强")
    elif tactical_score >= 70: tactical_mult = 1.0; reasons.append("战术:走强")
    elif tactical_score >= 60: tactical_mult = 0.5; reasons.append("战术:企稳")
    elif tactical_score <= 25: tactical_mult = -1.0; reasons.append("战术:破位")

    final_mult = tactical_mult
    if tactical_mult > 0: # 买入
        if val_mult < 0.5: final_mult = 0; reasons.append(f"战略:高估刹车")
        elif val_mult > 1.0: final_mult *= val_mult; reasons.append(f"战略:低估加倍")
    elif tactical_mult < 0: # 卖出
        if val_mult > 1.2: final_mult = 0; reasons.append(f"战略:底部锁仓")
        elif val_mult < 0.8: final_mult *= 1.5; reasons.append("战略:高估止损")
    else: # 震荡
        if val_mult >= 1.5 and strategy_type in ['core', 'dividend']:
            final_mult = 0.5; reasons.append(f"战略:左侧定投")

    if cro_signal == "VETO" and final_mult > 0:
        final_mult = 0; reasons.append(f"🛡️风控:否决")
    
    held_days = pos.get('held_days', 999)
    if final_mult < 0 and pos['shares'] > 0 and held_days < 7:
        final_mult = 0; reasons.append(f"规则:锁仓({held_days}天)")

    final_amt = 0; is_sell = False; sell_val = 0; label = "观望"
    if final_mult > 0:
        final_amt = max(0, min(int(base_amt * final_mult), int(max_daily)))
        label = "买入"
    elif final_mult < 0:
        is_sell = True
        sell_val = pos['shares'] * tech.get('price', 0) * min(abs(final_mult), 1.0)
        label = "卖出"
    
    if ai_decision == "HOLD_CASH": label = "空仓"

    if reasons: tech['quant_reasons'] = reasons
    return final_amt, label, is_sell, sell_val

def process_phase1_proposal(fund, fetcher, tracker, val_engine, analyst, market_context):
    """
    [Phase 1] 战术层提案收集 (适配 v19.6.5 结构)
    """
    time.sleep(random.uniform(2.0, 4.0))
    
    fund_name = fund['name']; fund_code = fund['code']
    logger.info(f"🔍 [IC初审] 分析标的: {fund_name} ({fund_code})")

    try:
        # 1. 基础数据获取
        data = fetcher.get_fund_history(fund_code)
        if data is None or data.empty: 
            logger.warning(f"❌ 数据获取失败: {fund_name}")
            return None
        
        # 2. 技术分析
        analyzer = TechnicalAnalyzer(asset_type='ETF') 
        tech = analyzer.calculate_indicators(data)
        if not tech: return None
        
        # 3. 估值分析
        val_mult, val_desc = val_engine.get_valuation_status(fund_code, data)
        
        # 4. 调用 AI IC (战术层 v19.6 接口)
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

        # 5. v19.6.5 提取 IC 初步结论
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

        # 回写供后续流程提取
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

    # 1. 环境扫描
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

    # ===================================================
    # Phase 1: IC 战术投委会海选 (Proposal Collection)
    # ===================================================
    logger.info("⚔️ [Phase 1] 启动 IC 战术投委会海选...")
    proposals = []
    candidates_for_veto = [] 
    
    for fund in funds:
        p = process_phase1_proposal(fund, fetcher, tracker, val_engine, analyst, market_context)
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

    # ===================================================
    # Phase 2: 风控委员会终审 (Risk Committee Veto)
    # ===================================================
    logger.info(f"⚖️ [Phase 2] 启动风控委员会终审 (待审提案: {len(candidates_for_veto)}个)...")
    
    risk_report = {"approved_list": [], "rejected_log": [], "risk_summary": "无提案提交"}
    approved_codes = []
    
    if candidates_for_veto and analyst:
        # v19.6.5 压力测试矩阵返回
        risk_report_raw = analyst.run_risk_committee_veto(candidates_for_veto)
        
        # 适配 V19.6.5 的 stress_test_results
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
        
        calc_decision = "PASS"
        if final_decision == "EXECUTE": calc_decision = "PASS" 
        elif final_decision == "REJECT": calc_decision = "REJECT"
        elif final_decision == "HOLD_CASH": calc_decision = "HOLD_CASH"
        
        amt, lbl, is_sell, s_val = calculate_position_v13(
            p['tech'], 0, calc_decision, p['val_mult'], p['val_desc'],
            config['global']['base_invest_amount'], config['global']['max_daily_invest'],
            tracker.get_position(code), p['fund_obj'].get('strategy_type'), p['name']
        )
        
        with tracker_lock:
            tracker.record_signal(code, lbl)
            if amt > 0: tracker.add_trade(code, p['name'], amt, p['tech']['price'])
            elif is_sell: tracker.add_trade(code, p['name'], s_val, p['tech']['price'], True)
            
        # 提取 v19.6.5 对话实录 (Technical/CGO/CRO 三方博弈)
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
        
        # 将假设前提和硬止损塞入执行笔记供 UI 展示
        key_assumption = verdict.get('key_assumption', '')
        hard_stop = verdict.get('hard_stop_loss', '')
        time_stop = verdict.get('time_stop', '')
        if key_assumption or hard_stop:
            debate_str += f"---\n"
            if key_assumption: debate_str += f"**💠核心前提**: {key_assumption}\n"
            if hard_stop: debate_str += f"**🛡️硬止损**: {hard_stop} | **时限**: {time_stop}\n"
        
        ai_full_adapted = {
            "strategy_meta": {
                "mode": verdict.get('mode_selected', 'UNKNOWN'),
                "rationale": verdict.get('logic_weighting', '无逻辑')
            },
            "trend_analysis": {
                "days_to_event": p['ic_res'].get('days_to_event', 'NULL'),
                "stage": f"Tech:{p['tech']['quant_score']}分"
            },
            "execution_notes": debate_str[:800], # 调大截断，容纳三方博弈和前提条件
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

    cio_html = ""
    if analyst:
        logger.info("🧠 正在生成 CIO 战略定调 (基于风控报告)...")
        # 直接把 raw JSON 丢过去，那边会处理结构
        cio_html = analyst.generate_cio_strategy(
            datetime.now().strftime("%Y-%m-%d"), 
            risk_report_raw if 'risk_report_raw' in locals() else risk_report
        )
        
    # 渲染最新版 HTML (引入 Variance: Zero 主题)
    html = render_html_report_v19(all_news_seen, final_results, cio_html, "") 
    
    subject_prefix = "🚧 [测试] " if TEST_MODE else "💠 "
    send_email(f"{subject_prefix}Variance: Zero 认知对抗全量化报告", html)
    
    logger.info("✅ 运行结束，邮件已发送。")

if __name__ == "__main__": main()
