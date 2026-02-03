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

def calculate_position(tech_data, base_amount, max_daily, pos_info, strategy_type):
    """
    💰 V8.0: 核心-卫星双轨策略 (Core-Satellite Strategy)
    """
    score = tech_data['quant_score']
    weekly = tech_data['trend_weekly']
    price = tech_data['price']
    
    cost = pos_info['cost']
    shares = pos_info['shares']
    held_days = pos_info.get('held_days', 999)
    
    profit_pct = 0
    has_position = shares > 0
    if has_position:
        profit_pct = (price - cost) / cost * 100
    
    # === 策略分支 ===
    is_core = (strategy_type == 'core')
    
    multiplier = 0
    reasons = []

    # 1. 评分分级 (动态资金)
    if score >= 85: 
        multiplier = 2.0  # 重仓
        reasons.append("评分极高")
    elif score >= 70: 
        multiplier = 1.0  # 标准
    elif score >= 60: 
        multiplier = 0.5  # 试探
    elif score <= 15: 
        multiplier = -1.0 # 卖出信号
    
    # 2. 核心资产特殊逻辑 (长期持有保护)
    if is_core:
        if multiplier < 0 and score > -40: # 只要不是极度崩盘
            multiplier = 0 # 忽略卖出信号，转为持有
            reasons.append("🛡️核心资产-穿越牛熊忽略波动")
        if weekly == "UP" and multiplier == 0: # 长期趋势向上，即使短期分低也保持定投
            multiplier = 0.5
            reasons.append("📈核心资产-顺势定投")

    # 3. 卫星资产特殊逻辑 (严格止盈止损)
    if not is_core:
        if profit_pct > 15 and score < 70:
            multiplier = -0.5 # 止盈一半
            reasons.append(f"💰卫星止盈({profit_pct:.1f}%)")
        if profit_pct < -8 and score < 40:
            multiplier = -1.0 # 坚决止损
            reasons.append(f"✂️卫星止损({profit_pct:.1f}%)")

    # 4. 七日锁 (ETF虽然费率低，但频繁交易仍有成本，且防止误操作)
    if multiplier < 0 and has_position and held_days < 5: # ETF T+1，且建议至少拿一周
        multiplier = 0 
        reasons.append(f"🛡️冷静期(持{held_days}天)")
        logger.warning(f"触发冷静期: 强制取消卖出")

    # 5. 熊市总控
    if weekly == "DOWN":
        if multiplier > 0: multiplier *= 0.5 # 熊市买入减半
        # 核心资产在熊市也不轻易清仓，除非深跌
        if is_core and multiplier < 0 and score > -60: multiplier = 0 

    # === 执行计算 ===
    final_amount = 0
    is_sell = False
    sell_value = 0
    label = "⏸️ 观望 HOLD"

    if multiplier > 0:
        # ETF 必须买 100 股整数倍 (大约逻辑，实际由交易软件控制，这里只给建议金额)
        # 资金分配：80分给70%，60分给40% -> 这里的 base_amount 应该是最大单笔的一半
        raw_amount = int(base_amount * multiplier)
        final_amount = max(0, min(raw_amount, int(max_daily)))
        
        if multiplier >= 2.0: label = "🔥 强力增持 (重仓)"
        elif multiplier >= 1.0: label = "✅ 标准建仓"
        else: label = "🧪 试探性买入"

    elif multiplier < 0:
        is_sell = True
        sell_ratio = min(abs(multiplier), 1.0)
        
        position_value = shares * price
        sell_value = position_value * sell_ratio
        
        if (position_value - sell_value) < 100: # 剩太少就清了
            sell_value = position_value
            sell_ratio = 1.0

        if sell_ratio >= 0.99: label = "🚫 清仓离场 (落袋)"
        else: label = f"✂️ 减仓锁定 ({int(sell_ratio*100)}%)"

    if reasons: tech_data['quant_reasons'].extend(reasons)
        
    return final_amount, label, is_sell, sell_value

def render_html_report(market_ctx, funds_results, daily_total_cap):
    """V8.0 核心卫星鎏金版 UI"""
    invested = sum(r['amount'] for r in funds_results if r['amount'] > 0)
    cash_display = f"{invested:,}"
    
    # 分组：核心 vs 卫星
    cores = [r for r in funds_results if r['strategy_type'] == 'core']
    sats = [r for r in funds_results if r['strategy_type'] == 'satellite']
    
    # 辅助渲染函数
    def render_group(title, items):
        if not items: return ""
        html_chunk = f'<div class="section-title">{title}</div>'
        for r in items:
            # 样式逻辑
            action_class = "card-wait"
            if r['amount'] > 0: action_class = "card-buy"
            elif r.get('is_sell'): action_class = "card-sell"
            
            # 操作文本
            if r['amount'] > 0: act_text = f"+¥{r['amount']:,}"
            elif r.get('is_sell'): act_text = f"卖出 ¥{int(r.get('sell_value',0)):,}"
            else: act_text = "持仓/观望"

            # AI 点评
            ai_html = ""
            if r.get('ai_analysis') and r['ai_analysis'].get('comment'):
                 ai_html = f'<div class="ai-comment"><span class="ai-label">AI:</span>{r["ai_analysis"]["comment"]}</div>'

            html_chunk += f"""
            <div class="card {action_class}">
                <div class="card-top">
                    <span>{r['name']} <span style="font-size:10px;color:#666">{r['code']}</span></span>
                    <span style="color:#D4AF37">{r['position_type']}</span>
                </div>
                <div class="card-body">
                    <div style="display:flex; justify-content:space-between; margin-bottom:10px;">
                        <span>操作: <b>{act_text}</b></span>
                        <span>评分: <b>{r['tech']['quant_score']}</b></span>
                    </div>
                    <div class="metrics">
                        <span>RSI: {r['tech']['rsi']}</span>
                        <span>Bias: {r['tech']['bias_20']}%</span>
                        <span>周线: {r['tech']['trend_weekly']}</span>
                    </div>
                    <div style="margin-top:8px;">
                        {''.join([f'<span class="reason-tag">{x}</span>' for x in r['tech']['quant_reasons']])}
                    </div>
                    {ai_html}
                </div>
            </div>
            """
        return html_chunk

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Noto+Serif+SC:wght@500;700&family=Roboto+Mono&display=swap');
            body {{ background-color: #0a0a0a; color: #e0e0e0; font-family: "Noto Serif SC", serif; margin: 0; padding: 20px; background-image: url('https://www.transparenttextures.com/patterns/cubes.png'); }}
            .container {{ max-width: 680px; margin: 0 auto; background: #141414; border: 2px solid #D4AF37; border-radius: 12px; overflow: hidden; }}
            .header {{ background: linear-gradient(180deg, #1f1f1f 0%, #141414 100%); padding: 30px; text-align: center; border-bottom: 2px solid #D4AF37; }}
            .gold-text {{ background: linear-gradient(to right, #D4AF37, #FCEabb, #D4AF37); -webkit-background-clip: text; color: transparent; font-weight: bold; }}
            .section-title {{ padding: 15px 30px; color: #D4AF37; font-size: 14px; border-bottom: 1px solid #222; background: #1a1a1a; letter-spacing: 1px; }}
            .card {{ margin: 15px 30px; background: #1c1c1c; border: 1px solid #333; border-radius: 8px; overflow: hidden; }}
            .card-buy {{ border-left: 4px solid #ff4d4f; }}
            .card-sell {{ border-left: 4px solid #52c41a; }}
            .card-wait {{ border-left: 4px solid #666; }}
            .card-top {{ padding: 10px 20px; background: #222; display: flex; justify-content: space-between; font-size: 14px; font-weight: bold; }}
            .card-body {{ padding: 15px 20px; font-size: 13px; }}
            .metrics {{ display: flex; gap: 15px; color: #888; font-size: 12px; font-family: "Roboto Mono"; }}
            .reason-tag {{ display: inline-block; background: #252525; color: #aaa; padding: 2px 6px; border-radius: 4px; font-size: 10px; margin-right: 5px; border: 1px solid #333; }}
            .ai-comment {{ margin-top: 10px; padding: 8px; background: #111; border: 1px dashed #333; color: #888; font-size: 12px; font-style: italic; }}
            .ai-label {{ color: #D4AF37; margin-right: 5px; font-style: normal; }}
            .footer {{ padding: 20px; text-align: center; color: #444; font-size: 11px; background: #0f0f0f; border-top: 1px solid #222; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1 style="font-size: 24px; margin: 0;">💰 鎏金量化·核心卫星版</h1>
                <div style="color: #888; font-size: 12px; margin-top: 5px;">V8.0 实战 ETF 策略 | {datetime.now().strftime('%Y-%m-%d')}</div>
            </div>
            
            <div style="padding: 20px; text-align: center; border-bottom: 1px solid #333;">
                <span style="color:#aaa; font-size:12px;">今日建议投入</span><br>
                <span class="gold-text" style="font-size:28px;">¥{cash_display}</span>
            </div>
            
            {render_group("🪐 核心资产 (底仓/定投)", cores)}
            {render_group("🚀 卫星资产 (波段/轮动)", sats)}
            
            <div class="footer">
                核心资产长期持有，卫星资产严格止盈止损。<br>场内 ETF 交易费率更低，资金效率更高。
            </div>
        </div>
    </body></html>
    """
    return html

def main():
    config = load_config()
    fetcher = DataFetcher()
    scanner = MarketScanner()
    tracker = PortfolioTracker() 
    
    logger.info(">>> [V8.0] 启动 T+1 确认...")
    tracker.confirm_trades()
    
    try: analyst = NewsAnalyst()
    except: analyst = None

    logger.info(">>> 启动 V8.0 核心卫星版...")
    market_ctx = scanner.get_market_sentiment()
    funds_results = []
    
    BASE_AMT = config['global']['base_invest_amount']
    MAX_DAILY = config['global']['max_daily_invest']

    for fund in config['funds']:
        try:
            logger.info(f"=== 分析 {fund['name']} ({fund['strategy_type']}) ===")
            data_dict = fetcher.get_fund_history(fund['code'])
            tech_indicators = TechnicalAnalyzer.calculate_indicators(data_dict)
            if not tech_indicators: continue

            pos_info = tracker.get_position(fund['code'])
            
            # 传入 strategy_type
            final_amt, pos_type, is_sell, sell_amt = calculate_position(
                tech_indicators, BASE_AMT, MAX_DAILY, pos_info, fund.get('strategy_type', 'satellite')
            )
            
            # AI 分析
            ai_analysis = {}
            if analyst:
                 if final_amt > 0 or is_sell or tech_indicators['quant_score'] >= 70 or tech_indicators['quant_score'] <= 30:
                    news = analyst.fetch_news_titles(fund['sector_keyword'])
                    ai_analysis = analyst.analyze_fund_v4(fund['name'], tech_indicators, market_ctx, news)

            if final_amt > 0:
                tracker.add_trade(fund['code'], fund['name'], final_amt, tech_indicators['price'], is_sell=False)
            elif is_sell and sell_amt > 0:
                tracker.add_trade(fund['code'], fund['name'], sell_amt, tech_indicators['price'], is_sell=True)

            funds_results.append({
                "name": fund['name'], "code": fund['code'],
                "amount": final_amt, "sell_value": sell_amt,
                "position_type": pos_type, "is_sell": is_sell,
                "tech": tech_indicators,
                "ai_analysis": ai_analysis,
                "strategy_type": fund.get('strategy_type', 'satellite') # 传递类型
            })
            time.sleep(1)

        except Exception as e: logger.error(f"分析失败: {e}")

    if funds_results:
        # 先按类型排序(核心在前)，再按分数
        funds_results.sort(key=lambda x: (x['strategy_type'] != 'core', -x['tech']['quant_score']))
        html_report = render_html_report(market_ctx, funds_results, MAX_DAILY)
        send_email("📊 鎏金量化·核心卫星内参", html_report)

if __name__ == "__main__":
    main()
