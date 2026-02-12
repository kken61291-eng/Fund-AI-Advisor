import yaml
import os
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from technical_analyzer import TechnicalAnalyzer
from valuation_engine import ValuationEngine
from portfolio_tracker import PortfolioTracker
from utils import send_email, logger, LOG_FILENAME

# 【🔥关键】导入 V19 渲染器
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

def _adapt_v35_to_v19_ui(ai_res, tech):
    """
    【🔥核心修复】v3.5 数据适配层
    将 v3.5 的复杂嵌套结构 (strategy_meta, trend_analysis) 
    映射回 UI 渲染器能识别的扁平字段 (thesis, pros, cons, risk_warning)
    """
    if not ai_res: return {}

    # 1. 提取核心数据
    meta = ai_res.get('strategy_meta', {})
    trend = ai_res.get('trend_analysis', {})
    cro = ai_res.get('cro_risk_audit', {}) if 'cro_risk_audit' in ai_res else ai_res.get('cro_arbitration', {})
    
    mode = meta.get('mode', 'UNKNOWN')
    rationale = meta.get('rationale', '无核心逻辑')
    stage = trend.get('stage', 'UNKNOWN')
    
    # 2. 构造 UI 兼容字段 (Mapping)
    
    # Field A: 核心逻辑 (Thesis)
    # 组合模式、阶段和核心理由
    thesis_text = f"【模式: {mode}】 | 【阶段: {stage}】\n👉 {rationale}"
    
    # 如果是事件驱动，追加时间信息
    days = trend.get('days_to_event', 'NULL')
    if str(days) != 'NULL' and mode == 'EVENT_DRIVEN':
        thesis_text += f"\n⏳ [潜伏] 距离事件还有 {days} 天"
        if 'execution_notes' in ai_res:
            thesis_text += f"\n📝 {ai_res['execution_notes']}"

    # Field B: 利多 (Pros) -> 技术面与资金面
    pros_text = f"1. 趋势分: {tech.get('quant_score', 0)}/100\n"
    pros_text += f"2. 波动率: {tech.get('volatility_status', '-')}\n"
    if 'net_flow' in ai_res.get('trend_analysis', {}): # 某些情况可能回填了
         pros_text += f"3. 资金流: {ai_res['trend_analysis']['net_flow']}"
    
    # Field C: 风险 (Risk) -> CRO审计与利空
    risk_text = f"1. 熔断检查: {cro.get('falling_knife_check', 'PASS')}\n"
    risk_text += f"2. 基本面: {cro.get('fundamental_check', '-')}\n"
    if mode == 'EVENT_DRIVEN':
        risk_text += f"3. 防抢跑: 5日涨幅 {tech.get('recent_gain', 0)}%"

    # 3. 注入回 ai_res，欺骗旧版 UI
    ai_res['thesis'] = thesis_text      # UI 显示 "核心逻辑"
    ai_res['pros'] = pros_text          # UI 显示 "利多因子"
    ai_res['cons'] = "见风险提示"       # UI 显示 "利空因子" (占位)
    ai_res['risk_warning'] = risk_text  # UI 显示 "风控警示"
    
    return ai_res

def calculate_position_v13(tech, ai_adj, ai_decision, val_mult, val_desc, base_amt, max_daily, pos, strategy_type, fund_name):
    # 核心算分逻辑 (保持不变)
    base_score = tech.get('quant_score', 50)
    try: ai_adj_int = int(ai_adj)
    except: ai_adj_int = 0

    tactical_score = max(0, min(100, base_score + ai_adj_int))
    
    if ai_decision == "REJECT": tactical_score = 0 
    elif ai_decision == "HOLD_CASH": tactical_score = 0 # v3.5 新增状态适配
    elif ai_decision == "HOLD" and tactical_score >= 60: tactical_score = 59
            
    tech['final_score'] = tactical_score
    tech['ai_adjustment'] = ai_adj_int
    tech['valuation_desc'] = val_desc
    cro_signal = tech.get('tech_cro_signal', 'PASS')
    
    tactical_mult = 0
    reasons = []

    if tactical_score >= 85: tactical_mult = 2.0; reasons.append("战术:极强")
    elif tactical_score >= 70: tactical_mult = 1.0; reasons.append("战术:走强")
    elif tactical_score >= 60: tactical_mult = 0.5; reasons.append("战术:企稳")
    elif tactical_score <= 25: tactical_mult = -1.0; reasons.append("战术:破位")

    final_mult = tactical_mult
    if tactical_mult > 0:
        if val_mult < 0.5: final_mult = 0; reasons.append(f"战略:高估刹车")
        elif val_mult > 1.0: final_mult *= val_mult; reasons.append(f"战略:低估加倍")
    elif tactical_mult < 0:
        if val_mult > 1.2: final_mult = 0; reasons.append(f"战略:底部锁仓")
        elif val_mult < 0.8: final_mult *= 1.5; reasons.append("战略:高估止损")
    else:
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

    if reasons: tech['quant_reasons'] = reasons
    return final_amt, label, is_sell, sell_val

def process_single_fund(fund, config, fetcher, tracker, val_engine, analyst, market_context, base_amt, max_daily):
    time.sleep(random.uniform(1.5, 3.0))
    
    fund_name = fund['name']
    fund_code = fund['code']
    
    logger.info(f"🚀 [1/6] 开始分析标的: {fund_name} ({fund_code})")

    try:
        # 1. 获取数据
        data = fetcher.get_fund_history(fund_code)
        if data is None or data.empty: 
            logger.warning(f"❌ [1/6] 数据获取失败: {fund_name}")
            return None, "", []
        
        # 2. 技术分析
        analyzer_instance = TechnicalAnalyzer(asset_type='ETF') 
        tech = analyzer_instance.calculate_indicators(data)
        if not tech: 
            logger.warning(f"❌ [2/6] 技术指标计算失败: {fund_name}")
            return None, "", []
        
        # 3. 估值分析
        val_mult, val_desc = val_engine.get_valuation_status(fund_code, data)
        
        with tracker_lock: pos = tracker.get_position(fund_code)

        # 4. AI 分析
        ai_res = {}
        if analyst:
            logger.info(f"🤖 [4/6] 呼叫 AI 投委会...")
            cro_signal = tech.get('tech_cro_signal', 'PASS')
            risk_payload = {"fuse_level": 3 if cro_signal == 'VETO' else 0, "risk_msg": tech.get('tech_cro_comment', '监控')}
            
            # 构造宏观数据
            macro_payload = {
                "net_flow": 0,  
                "leader_status": "UNKNOWN"
            }
            
            ai_res = analyst.analyze_fund_v5(fund_name, tech, macro_payload, market_context, risk_payload, fund.get('strategy_type', 'core'))
            
            # 【🔥修复点】调用适配器，填充 thesis 等字段
            ai_res = _adapt_v35_to_v19_ui(ai_res, tech)
            
            # 优化日志输出，显示核心逻辑
            logic_preview = ai_res.get('strategy_meta', {}).get('rationale', 'No Rationale')[:30]
            logger.info(f"🗣️ [投委会] {ai_res.get('decision')} | 模式:{ai_res.get('strategy_meta',{}).get('mode')} | 逻辑:{logic_preview}...")

        ai_adj = ai_res.get('adjustment', 0)
        ai_decision = ai_res.get('decision', 'PASS') 
        
        # 5. 决策计算
        amt, lbl, is_sell, s_val = calculate_position_v13(tech, ai_adj, ai_decision, val_mult, val_desc, base_amt, max_daily, pos, fund.get('strategy_type'), fund_name)
        
        with tracker_lock:
            tracker.record_signal(fund_code, lbl)
            if amt > 0: 
                tracker.add_trade(fund_code, fund_name, amt, tech['price'])
            elif is_sell: 
                tracker.add_trade(fund_code, fund_name, s_val, tech['price'], True)

        # CIO 日志也增加逻辑显示
        cio_log = f"标的:{fund_name} | 模式:{ai_res.get('strategy_meta',{}).get('mode','-')} | 决策:{lbl} | 逻辑:{ai_res.get('strategy_meta',{}).get('rationale','')}"
        
        return {
            "name": fund_name, 
            "code": fund_code, 
            "index_name": fund.get('index_name'), 
            "amount": amt, 
            "sell_value": s_val, 
            "is_sell": is_sell, 
            "tech": tech, 
            "ai_analysis": ai_res # 此时 ai_res 已经包含适配后的 thesis 字段
        }, cio_log, []
    except Exception as e:
        logger.error(f"❌ Error {fund_name}: {e}", exc_info=True); return None, "", []

def main():
    config = load_config()
    fetcher, tracker, val_engine = DataFetcher(), PortfolioTracker(), ValuationEngine()
    
    tracker.confirm_trades()
    
    try: analyst = NewsAnalyst()
    except: analyst = None

    market_context = analyst.get_market_context() if analyst else "无数据"
    all_news_seen = [line.strip() for line in market_context.split('\n') if line.strip().startswith('[')]

    funds = config.get('funds', [])
    
    if TEST_MODE:
        if funds:
            logger.info(f"🚧 【测试模式开启】仅处理第一个标的: {funds[0]['name']}")
            funds = funds[:1]
        else:
            logger.error("❌ Config 中没有基金，无法测试")
            return

    results, cio_lines = [], []
    
    logger.info("🚀 启动处理 (本地模式: 新闻+数据)...")
    
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = {executor.submit(process_single_fund, f, config, fetcher, tracker, val_engine, analyst, market_context, config['global']['base_invest_amount'], config['global']['max_daily_invest']): f for f in funds}
        for f in as_completed(futures):
            res, log, _ = f.result()
            if res: 
                results.append(res); cio_lines.append(log)
                print(f"✅ 完成处理: {res['name']}") 

    if results:
        results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        full_report = "\n".join(cio_lines)
        cio_html = analyst.review_report(full_report, market_context) if analyst else ""
        advisor_html = analyst.advisor_review(full_report, market_context) if analyst else ""
        
        html = render_html_report_v19(all_news_seen, results, cio_html, advisor_html) 
        
        subject_prefix = "🚧 [测试] " if TEST_MODE else "🕊️ "
        send_email(f"{subject_prefix}鹊知风 V19.0 全量化仪表盘", html) 
        
        logger.info("✅ 运行结束，邮件已发送。")
    else:
        logger.warning("⚠️ 没有生成任何结果，请检查日志报错。")

if __name__ == "__main__": main()
