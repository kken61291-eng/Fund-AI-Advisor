import yaml
import os
import time
from datetime import datetime
from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from market_scanner import MarketScanner
from technical_analyzer import TechnicalAnalyzer
from portfolio_tracker import PortfolioTracker
from utils import send_email, logger

def load_config():
    with open('config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def calculate_position(tech_data, base_amount, max_daily, pos_info):
    """
    💰 V7.0: 散户实战版 (含七日锁 & 成本风控)
    """
    score = tech_data['quant_score']
    weekly = tech_data['trend_weekly']
    price = tech_data['price']
    
    cost = pos_info['cost']
    shares = pos_info['shares']
    held_days = pos_info.get('held_days', 999) # 默认为很久
    
    profit_pct = 0
    has_position = shares > 0
    if has_position:
        profit_pct = (price - cost) / cost * 100
        
    # --- 1. 基础信号 ---
    multiplier = 0
    if score >= 85: multiplier = 2.0
    elif score >= 70: multiplier = 1.0
    elif score >= 60: multiplier = 0.5
    elif score <= 15: multiplier = -1.0 # 初步卖出信号
    
    reasons = []

    # --- 2. 持仓风控 (止盈/止损) ---
    if has_position:
        if profit_pct > 15 and score < 60: # 止盈线降低，保住利润
            multiplier = 0
            reasons.append(f"🔒止盈({profit_pct:.1f}%)")
        elif profit_pct < -10 and score >= 80:
            multiplier = 3.0
            max_daily *= 2.0 # 加大摊薄力度
            reasons.append(f"📉深套摊薄")

    # --- 3. 🛡️ 七日锁 (核心补丁) ---
    # 如果系统发出卖出信号 (multiplier < 0)，但持有不足 7 天
    if multiplier < 0 and has_position and held_days < 7:
        multiplier = 0 # 强制取消卖出，改为持有
        reasons.append(f"🛡️七日锁(仅持{held_days}天)-拒付1.5%赎回费")
        logger.warning(f"触发七日锁: 持有不足7天，强制取消卖出信号")

    # --- 4. 熊市防御 ---
    if weekly == "DOWN":
        if multiplier > 0: multiplier *= 0.5 # 熊市买入减半
        if multiplier < 0 and has_position and held_days >= 7: multiplier = -1.0 # 熊市清仓更坚决

    # --- 5. 执行计算 ---
    final_amount = 0
    is_sell = False
    sell_value = 0
    label = "⏸️ 观望 WAIT"

    if multiplier > 0:
        raw_amount = int(base_amount * multiplier)
        final_amount = max(0, min(raw_amount, int(max_daily)))
        if multiplier >= 2.0: label = "🔥 重仓 BUY+"
        elif multiplier >= 1.0: label = "✅ 建仓 BUY"
        else: label = "🧪 试探 ADD"

    elif multiplier < 0:
        is_sell = True
        sell_ratio = min(abs(multiplier), 1.0)
        position_value = shares * price
        sell_value = position_value * sell_ratio
        
        if (position_value - sell_value) < 50: # 剩得少就全清
            sell_value = position_value
            sell_ratio = 1.0

        if sell_ratio >= 0.99: label = "🚫 清仓 SELL ALL"
        else: label = f"✂️ 减仓 SELL {int(sell_ratio*100)}%"

    if reasons: tech_data['quant_reasons'].extend(reasons)
        
    return final_amount, label, is_sell, sell_value

def render_html_report(market_ctx, funds_results, daily_total_cap):
    invested = sum(r['amount'] for r in funds_results if r['amount'] > 0)
    
    # 简单的文本报告，聚焦结果
    html = f"""
    <html><body style="font-family:sans-serif; background:#f4f4f4; padding:20px;">
    <div style="max-width:600px; margin:0 auto; background:#fff; padding:20px;">
        <h2 style="border-bottom:2px solid #333">V7.0 散户实战版</h2>
        <p>宏观: {market_ctx.get('north_label')} | 今日投入: ¥{invested}</p>
        
        <h3>今日操作 (过滤后)</h3>
        {'<br>'.join([
            f"<div style='background:#eee; padding:10px; margin:5px; border-left:5px solid {'green' if r['amount']>0 else 'red'};'>"
            f"<b>{r['name']}</b>: {r['position_type']} "
            f"{( '¥'+str(r['amount']) if r['amount']>0 else '卖出 ¥'+str(int(r.get('sell_value',0))) )}"
            f"<br><small>{' '.join(r['tech']['quant_reasons'])}</small>"
            f"</div>"
            for r in funds_results if r['amount']>0 or r.get('is_sell')
        ]) if any(r['amount']>0 or r.get('is_sell') for r in funds_results) else "无操作 (空仓/锁仓中)"}
        
    </div></body></html>
    """
    return html

def main():
    config = load_config()
    fetcher = DataFetcher()
    scanner = MarketScanner()
    tracker = PortfolioTracker() 
    
    logger.info(">>> [V7.0] 启动 T+1 确认...")
    tracker.confirm_trades()
    
    # AI 仅作为备用，可不开启
    try: analyst = NewsAnalyst()
    except: analyst = None

    market_ctx = scanner.get_market_sentiment()
    funds_results = []
    
    BASE_AMT = config['global']['base_invest_amount']
    MAX_DAILY = config['global']['max_daily_invest']

    for fund in config['funds']:
        try:
            # 1. 数据
            data_dict = fetcher.get_fund_history(fund['code'])
            tech_indicators = TechnicalAnalyzer.calculate_indicators(data_dict)
            if not tech_indicators: continue

            # 2. 持仓 (含持有天数)
            pos_info = tracker.get_position(fund['code'])
            
            # 3. 决策 (含七日锁)
            final_amt, pos_type, is_sell, sell_amt = calculate_position(tech_indicators, BASE_AMT, MAX_DAILY, pos_info)
            
            # 4. 执行
            if final_amt > 0:
                tracker.add_trade(fund['code'], fund['name'], final_amt, tech_indicators['price'], is_sell=False)
            elif is_sell and sell_amt > 0:
                tracker.add_trade(fund['code'], fund['name'], sell_amt, tech_indicators['price'], is_sell=True)

            funds_results.append({
                "name": fund['name'], "code": fund['code'],
                "amount": final_amt, "sell_value": sell_amt,
                "position_type": pos_type, "is_sell": is_sell,
                "tech": tech_indicators
            })
            time.sleep(0.5)

        except Exception as e: logger.error(f"Error: {e}")

    if funds_results:
        funds_results.sort(key=lambda x: x['tech']['quant_score'], reverse=True)
        html_report = render_html_report(market_ctx, funds_results, MAX_DAILY)
        send_email("📊 V7.0 散户实战日报", html_report)

if __name__ == "__main__":
    main()
