import yaml
import os
import threading
import time
import random
# 保持单线程，移除并发库
# from concurrent.futures import ThreadPoolExecutor, as_completed

from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from technical_analyzer import TechnicalAnalyzer
from valuation_engine import ValuationEngine
from portfolio_tracker import PortfolioTracker
from market_scanner import MarketScanner
from utils import send_email, logger, LOG_FILENAME, get_beijing_time

# 导入 v19.3 渲染器 (UI部分)
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
    V13.2 核心算分逻辑 (逻辑闭环修复版)
    功能：
    1. 结合技术分与AI调整分
    2. 处理 AI 的特殊指令 (REJECT/HOLD_CASH)
    3. 【关键】利用估值状态修正最终显示评分，防止评分虚高
    """
    base_score = tech.get('quant_score', 50)
    try: ai_adj_int = int(ai_adj)
    except: ai_adj_int = 0

    # 1. 计算战术分 (技术 + AI)
    tactical_score = max(0, min(100, base_score + ai_adj_int))
    
    # 2. 处理 AI 否决指令
    if ai_decision == "REJECT": 
        tactical_score = 0 
    elif ai_decision == "HOLD_CASH": 
        tactical_score = 0 
    elif ai_decision == "HOLD" and tactical_score >= 60: 
        tactical_score = 59 # 压分至观望区
            
    # 3. 【新增】估值修正评分逻辑 (让UI评分与实际操作一致)
    # 即使趋势很好(100分)，如果估值太贵，分数也要降下来
    valuation_impact = 1.0
    if val_mult >= 1.3: 
        valuation_impact = 1.1   # 低估：分数上浮 10%
    elif val_mult <= 0.5: 
        valuation_impact = 0.6   # 高估：分数打 6 折
    elif val_mult == 0.0: 
        valuation_impact = 0.0   # 泡沫：分数归零
    
    final_display_score = int(tactical_score * valuation_impact)
    final_display_score = max(0, min(100, final_display_score))
    
    # 回写数据供 UI 展示
    tech['final_score'] = final_display_score
    tech['ai_adjustment'] = ai_adj_int
    tech['valuation_desc'] = val_desc
    
    # 4. 生成交易信号 (使用未修正的 tactical_score 判断方向，用 val_mult 调整金额)
    # 这样既保留了趋势的敏感度，又在金额上做了风控
    cro_signal = tech.get('tech_cro_signal', 'PASS')
    
    tactical_mult = 0
    reasons = []

    # 评分映射 (这里用原始战术分判断方向)
    if tactical_score >= 85: tactical_mult = 2.0; reasons.append("战术:极强")
    elif tactical_score >= 70: tactical_mult = 1.0; reasons.append("战术:走强")
    elif tactical_score >= 60: tactical_mult = 0.5; reasons.append("战术:企稳")
    elif tactical_score <= 25: tactical_mult = -1.0; reasons.append("战术:破位")

    # 最终倍数 = 战术倍数 * 估值系数
    final_mult = tactical_mult
    if tactical_mult > 0: # 买入逻辑
        if val_mult < 0.5: 
            final_mult = 0; reasons.append(f"战略:高估刹车") # 虽然趋势好，但太贵不买
        elif val_mult > 1.0: 
            final_mult *= val_mult; reasons.append(f"战略:低估加倍")
    elif tactical_mult < 0: # 卖出逻辑
        if val_mult > 1.2: 
            final_mult = 0; reasons.append(f"战略:底部锁仓") # 虽然破位，但太便宜不卖
        elif val_mult < 0.8: 
            final_mult *= 1.5; reasons.append("战略:高估止损")
    else: # 震荡逻辑
        if val_mult >= 1.5 and strategy_type in ['core', 'dividend']:
            final_mult = 0.5; reasons.append(f"战略:左侧定投")

    # 5. 风控与锁仓
    if cro_signal == "VETO" and final_mult > 0:
        final_mult = 0; reasons.append(f"🛡️风控:否决")
    
    held_days = pos.get('held_days', 999)
    if final_mult < 0 and pos['shares'] > 0 and held_days < 7:
        final_mult = 0; reasons.append(f"规则:锁仓({held_days}天)")

    # 6. 计算最终金额
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
    # 增加随机等待，防止 API 封禁
    time.sleep(random.uniform(2.0, 5.0)) 
    
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
        logger.info(f"📊 估值状态: {val_desc} (系数: {val_mult})")
        
        with tracker_lock: pos = tracker.get_position(fund_code)

        # 4. AI 分析
        ai_res = {}
        if analyst:
            logger.info(f"🤖 [4/6] 呼叫 AI 投委会...")
            cro_signal = tech.get('tech_cro_signal', 'PASS')
            risk_payload = {"fuse_level": 3 if cro_signal == 'VETO' else 0, "risk_msg": tech.get('tech_cro_comment', '监控')}
            
            # 构造宏观数据
            macro_payload = {
                "net_flow": market_context.get('net_flow', 0),  
                "leader_status": "UNKNOWN"
            }
            
            # 这里的 news_summary 是全市场新闻，作为上下文
            ai_res = analyst.analyze_fund_v5(fund_name, tech, macro_payload, market_context.get('news_summary', ''), risk_payload, fund.get('strategy_type', 'core'))
            
            # 日志记录 AI 核心观点
            mode = ai_res.get('strategy_meta', {}).get('mode', 'UNKNOWN')
            rationale_preview = ai_res.get('strategy_meta', {}).get('rationale', '无')[:30]
            logger.info(f"🗣️ [投委会] {ai_res.get('decision')} | 模式:{mode} | 逻辑:{rationale_preview}...")

        ai_adj = ai_res.get('adjustment', 0)
        ai_decision = ai_res.get('decision', 'PASS') 
        
        # 5. 决策计算 (应用修复后的逻辑)
        amt, lbl, is_sell, s_val = calculate_position_v13(tech, ai_adj, ai_decision, val_mult, val_desc, base_amt, max_daily, pos, fund.get('strategy_type'), fund_name)
        
        with tracker_lock:
            tracker.record_signal(fund_code, lbl)
            if amt > 0: 
                tracker.add_trade(fund_code, fund_name, amt, tech['price'])
            elif is_sell: 
                tracker.add_trade(fund_code, fund_name, s_val, tech['price'], True)

        cio_log = f"标的:{fund_name} | 模式:{ai_res.get('strategy_meta',{}).get('mode','-')} | 决策:{lbl} | 评分:{tech.get('final_score')}"
        
        return {
            "name": fund_name, 
            "code": fund_code, 
            "index_name": fund.get('index_name'), 
            "amount": amt, 
            "sell_value": s_val, 
            "is_sell": is_sell, 
            "decision": lbl,
            "tech": tech,        # 包含修复后的 final_score
            "ai_full": ai_res    # 包含完整 AI 逻辑
        }, cio_log, []
    except Exception as e:
        logger.error(f"❌ Error {fund_name}: {e}", exc_info=True); return None, "", []

def main():
    config = load_config()
    fetcher, tracker, val_engine = DataFetcher(), PortfolioTracker(), ValuationEngine()
    
    # 实例化市场扫描器
    scanner = MarketScanner()
    
    tracker.confirm_trades()
    
    try: analyst = NewsAnalyst()
    except: analyst = None

    logger.info("🚀 启动处理 (本地模式: 新闻+数据)...")

    # 1. 扫描市场
    market_context = { "news_summary": "无新闻", "net_flow": 0 }
    all_news_seen = []
    
    if analyst:
        logger.info("📡 正在获取宏观新闻与资金流向...")
        news_text = analyst.get_market_context()
        vitality = scanner.get_market_vitality() # 获取 v19.2 修复版资金流
        
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
    
    # 2. 逐个分析基金 (单线程顺序执行)
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

    # 3. 生成报告
    if results:
        # 按最终评分排序
        results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        
        full_report = "\n".join(cio_lines)
        
        # 让 CIO 思考
        cio_html = ""
        advisor_html = ""
        if analyst:
            logger.info("🧠 正在生成 CIO 战略复盘...")
            cio_html = analyst.review_report(full_report, market_context['news_summary'])
            advisor_html = analyst.advisor_review(full_report, market_context['news_summary'])
        
        # 调用 V19.3 渲染器
        html = render_html_report_v19(all_news_seen, results, cio_html, advisor_html) 
        
        subject_prefix = "🚧 [测试] " if TEST_MODE else "🕊️ "
        send_email(f"{subject_prefix}鹊知风 V19.4 全量化仪表盘", html) 
        
        logger.info("✅ 运行结束，邮件已发送。")
    else:
        logger.warning("⚠️ 没有生成任何结果，请检查日志报错。")

if __name__ == "__main__": main()
