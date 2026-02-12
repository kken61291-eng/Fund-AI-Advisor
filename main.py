import yaml
import os
import threading
import time
import random
# 移除 ThreadPoolExecutor，回归单线程
# from concurrent.futures import ThreadPoolExecutor, as_completed

from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from technical_analyzer import TechnicalAnalyzer
from valuation_engine import ValuationEngine
from portfolio_tracker import PortfolioTracker
from market_scanner import MarketScanner
from utils import send_email, logger, LOG_FILENAME, get_beijing_time

# 导入 v19.3 增强版渲染器
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
    V13.1 核心算分逻辑 (适配 v3.5)
    """
    base_score = tech.get('quant_score', 50)
    try: ai_adj_int = int(ai_adj)
    except: ai_adj_int = 0

    tactical_score = max(0, min(100, base_score + ai_adj_int))
    
    # 这里的逻辑保持不变，处理 REJECT/HOLD_CASH
    if ai_decision == "REJECT": 
        tactical_score = 0 
    elif ai_decision == "HOLD_CASH": 
        tactical_score = 0 
    elif ai_decision == "HOLD" and tactical_score >= 60: 
        tactical_score = 59
            
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
    
    if ai_decision == "HOLD_CASH": label = "空仓"

    if reasons: tech['quant_reasons'] = reasons
    return final_amt, label, is_sell, sell_val

def process_single_fund(fund, config, fetcher, tracker, val_engine, analyst, market_context, base_amt, max_daily):
    # 增加间隔，防止接口封禁
    time.sleep(random.uniform(2.0, 4.0)) 
    
    fund_name = fund['name']
    fund_code = fund['code']
    
    logger.info(f"🚀 [1/6] 开始分析标的: {fund_name} ({fund_code})")

    try:
        # 1. 获取数据
        data = fetcher.get_fund_history(fund_code)
        if data is None or data.empty: 
            logger.warning(f"❌ [1/6] 数据获取失败: {fund_name}")
            return None, "", []
        
        # 2. 技术分析 (确保 technical_analyzer.py 是最新的)
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
            
            macro_payload = {
                "net_flow": market_context.get('net_flow', 0),  
                "leader_status": "UNKNOWN"
            }
            
            ai_res = analyst.analyze_fund_v5(fund_name, tech, macro_payload, market_context.get('news_summary', ''), risk_payload, fund.get('strategy_type', 'core'))
            
            mode = ai_res.get('strategy_meta', {}).get('mode', 'UNKNOWN')
            rationale_preview = ai_res.get('strategy_meta', {}).get('rationale', '无')[:20]
            logger.info(f"🗣️ [投委会] {ai_res.get('decision')} | 模式:{mode} | 逻辑:{rationale_preview}...")

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

        cio_log = f"标的:{fund_name} | 模式:{ai_res.get('strategy_meta',{}).get('mode','-')} | 决策:{lbl}"
        
        # 构造完整返回数据，供 UI 使用
        return {
            "name": fund_name, 
            "code": fund_code, 
            "index_name": fund.get('index_name'), 
            "amount": amt, 
            "sell_value": s_val, 
            "is_sell": is_sell, 
            "decision": lbl,
            "tech": tech,        # 包含所有量化指标
            "ai_full": ai_res    # 包含所有 AI 逻辑
        }, cio_log, []
    except Exception as e:
        logger.error(f"❌ Error {fund_name}: {e}", exc_info=True); return None, "", []

def main():
    config = load_config()
    fetcher, tracker, val_engine = DataFetcher(), PortfolioTracker(), ValuationEngine()
    scanner = MarketScanner()
    
    tracker.confirm_trades()
    
    try: analyst = NewsAnalyst()
    except: analyst = None

    logger.info("🚀 启动处理 (本地模式: 新闻+数据)...")

    market_context = { "news_summary": "无新闻", "net_flow": 0 }
    all_news_seen = []
    
    if analyst:
        news_text = analyst.get_market_context()
        vitality = scanner.get_market_vitality()
        market_context = {
            "news_summary": news_text,
            "net_flow": vitality.get('net_flow', 0)
        }
        all_news_seen = [line.strip() for line in news_text.split('\n') if line.strip().startswith('[')]
        logger.info(f"🌍 市场状态: 资金流 {market_context['net_flow']} 亿")

    funds = config.get('funds', [])
    
    if TEST_MODE and funds:
        logger.info(f"🚧 【测试模式开启】仅处理第一个标的")
        funds = funds[:1]

    results, cio_lines = [], []
    
    # 【修改】使用单线程顺序执行，提高稳定性，避免“H5代码乱码” (有时多线程会导致日志混入)
    for fund in funds:
        res, log, _ = process_single_fund(
            fund, config, fetcher, tracker, val_engine, analyst, 
            market_context, 
            config['global']['base_invest_amount'], 
            config['global']['max_daily_invest']
        )
        if res: 
            results.append(res)
            cio_lines.append(log)
            print(f"✅ 完成处理: {res['name']}")

    if results:
        results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        
        full_report = "\n".join(cio_lines)
        cio_html = analyst.review_report(full_report, market_context['news_summary']) if analyst else ""
        advisor_html = analyst.advisor_review(full_report, market_context['news_summary']) if analyst else ""
        
        # 调用 v19.3 渲染器
        html = render_html_report_v19(all_news_seen, results, cio_html, advisor_html) 
        
        subject_prefix = "🚧 [测试] " if TEST_MODE else "🕊️ "
        send_email(f"{subject_prefix}鹊知风 V19.3 全量化仪表盘", html) 
        
        logger.info("✅ 运行结束，邮件已发送。")
    else:
        logger.warning("⚠️ 没有生成任何结果，请检查日志报错。")

if __name__ == "__main__": main()
