import yaml
import os
import time
import random
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
    💰 V8.0/V9.0 通用核心算法: 核心-卫星双轨策略
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
    
    is_core = (strategy_type == 'core')
    multiplier = 0
    reasons = []

    # 1. 评分分级
    if score >= 85: 
        multiplier = 2.0 
        reasons.append("评分极高")
    elif score >= 70: 
        multiplier = 1.0 
    elif score >= 60: 
        multiplier = 0.5 
    elif score <= 15: 
        multiplier = -1.0 
    
    # 2. 核心资产逻辑
    if is_core:
        if multiplier < 0 and score > -40: 
            multiplier = 0 
            reasons.append("🛡️核心资产-穿越牛熊")
        if weekly == "UP" and multiplier == 0:
            multiplier = 0.5
            reasons.append("📈核心资产-顺势定投")

    # 3. 卫星资产逻辑
    if not is_core:
        if profit_pct > 15 and score < 70:
            multiplier = -0.5 
            reasons.append(f"💰卫星止盈({profit_pct:.1f}%)")
        if profit_pct < -8 and score < 40:
            multiplier = -1.0 
            reasons.append(f"✂️卫星止损({profit_pct:.1f}%)")

    # 4. 七日锁
    if multiplier < 0 and has_position and held_days < 7: 
        multiplier = 0 
        reasons.append(f"🛡️冷静期(持{held_days}天)")
        logger.warning(f"触发冷静期: 强制取消卖出")

    # 5. 熊市风控
    if weekly == "DOWN":
        if multiplier > 0: multiplier *= 0.5 
        if is_core and multiplier < 0 and score > -60: multiplier = 0 

    final_amount = 0
    is_sell = False
    sell_value = 0
    label = "⏸️ 观望 HOLD"

    if multiplier > 0:
        raw_amount = int(base_amount * multiplier)
        final_amount = max(0, min(raw_amount, int(max_daily)))
        if multiplier >= 2.0: label = "🔥 强力增持"
        elif multiplier >= 1.0: label = "✅ 标准建仓"
        else: label = "🧪 试探买入"

    elif multiplier < 0:
        is_sell = True
        sell_ratio = min(abs(multiplier), 1.0)
        position_value = shares * price
        sell_value = position_value * sell_ratio
        
        if (position_value - sell_value) < 100: 
            sell_value = position_value
            sell_ratio = 1.0

        if sell_ratio >= 0.99: label = "🚫 清仓离场"
        else: label = f"✂️ 减仓锁定 ({int(sell_ratio*100)}%)"

    if reasons: tech_data['quant_reasons'].extend(reasons)
        
    return final_amount, label, is_sell, sell_value

def render_html_report(market_ctx, funds_results, daily_total_cap):
    """
    ✨ V9.1 UI: 鎏金量化·核心卫星智投系统 (暗黑高对比度版)
    """
    invested = sum(r['amount'] for r in funds_results if r['amount'] > 0)
    cash_display = f"{invested:,}"
    
    cores = [r for r in funds_results if r['strategy_type'] == 'core']
    sats = [r for r in funds_results if r['strategy_type'] == 'satellite']
    
    def render_group(title, items):
        if not items: return ""
        html_chunk = f'<div class="section-title">{title}</div>'
        for r in items:
            border_color = "#444" 
            if r['amount'] > 0: border_color = "#ff4d4f" 
            elif r.get('is_sell'): border_color = "#52c41a" 
            
            if r['amount'] > 0: 
                act_text = f"<span style='color:#ff4d4f'>+¥{r['amount']:,}</span>"
            elif r.get('is_sell'): 
                act_text = f"<span style='color:#52c41a'>卖出 ¥{int(r.get('sell_value',0)):,}</span>"
            else: 
                act_text = "<span style='color:#888'>持仓/观望</span>"

            ai_html = ""
            if r.get('ai_analysis') and r['ai_analysis'].get('comment'):
                 ai_html = f'<div class="ai-comment"><span class="ai-label">AI:</span>{r["ai_analysis"]["comment"]}</div>'

            html_chunk += f"""
            <div class="card" style="border-left: 3px solid {border_color};">
                <div class="card-header">
                    <div>
                        <span class="fund-name">{r['name']}</span>
                        <span class="fund-code">{r['code']}</span>
                    </div>
                    <div class="fund-action">{r['position_type']}</div>
                </div>
                
                <div class="card-body">
                    <div class="row">
                        <span>操作: {act_text}</span>
                        <span>评分: <b style="color:#D4AF37">{r['tech']['quant_score']}</b></span>
                    </div>
                    <div class="metrics">
                        <span>RSI: {r['tech']['rsi']}</span>
                        <span>Bias: {r['tech']['bias_20']}%</span>
                        <span>周线: {r['tech']['trend_weekly']}</span>
                    </div>
                    <div class="tags">
                        {''.join([f'<span class="tag">{x}</span>' for x in r['tech']['quant_reasons']])}
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
            body {{
                background-color: #000000; color: #e0e0e0;
                font-family: -apple-system, BlinkMacSystemFont, "Microsoft YaHei", sans-serif;
                margin: 0; padding: 20px;
            }}
            .container {{
                max-width: 600px; margin: 0 auto;
                background: #111111; border: 1px solid #333;
                border-radius: 10px; overflow: hidden;
            }}
            .header {{
                background: linear-gradient(180deg, #1a1a1a 0%, #111111 100%);
                padding: 30px; text-align: center;
                border-bottom: 2px solid #D4AF37;
            }}
            .title {{ 
                font-size: 26px; color: #D4AF37; margin: 0; font-weight: bold; 
                letter-spacing: 1px; text-transform: uppercase;
                background: linear-gradient(to right, #D4AF37, #FCEabb, #D4AF37);
                -webkit-background-clip: text; color: transparent;
            }}
            .subtitle {{ color: #666; font-size: 12px; margin-top: 8px; letter-spacing: 1px; }}
            
            .dashboard {{ padding: 20px; text-align: center; border-bottom: 1px solid #222; }}
            .money {{ font-size: 32px; color: #fff; font-weight: bold; margin: 10px 0; }}
            .macro {{ font-size: 12px; color: #888; }}
            
            .section-title {{
                padding: 15px 20px; color: #D4AF37; font-size: 14px;
                background: #0a0a0a; border-top: 1px solid #222; border-bottom: 1px solid #222;
                letter-spacing: 1px;
            }}
            
            .card {{
                margin: 15px 20px; background: #1c1c1c; 
                border-radius: 6px; overflow: hidden;
                box-shadow: 0 2px 5px rgba(0,0,0,0.5);
            }}
            .card-header {{
                padding: 12px 15px; background: #252525;
                display: flex; justify-content: space-between; align-items: center;
            }}
            .fund-name {{ font-size: 15px; font-weight: bold; color: #fff; }}
            .fund-code {{ font-size: 12px; color: #666; margin-left: 5px; }}
            .fund-action {{ font-size: 12px; color: #D4AF37; font-weight: bold; }}
            
            .card-body {{ padding: 15px; color: #ccc; }}
            .row {{ display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 13px; }}
            .metrics {{ font-size: 11px; color: #666; margin-bottom: 10px; font-family: monospace; }}
            .metrics span {{ margin-right: 10px; }}
            
            .tags {{ margin-bottom: 5px; }}
            .tag {{ 
                display: inline-block; background: #333; color: #aaa; 
                padding: 2px 6px; border-radius: 3px; font-size: 10px; 
                margin-right: 5px; margin-bottom: 3px; border: 1px solid #444; 
            }}
            
            .ai-comment {{ 
                margin-top: 10px; padding: 10px; background: #0f0f0f; 
                border: 1px dashed #444; border-radius: 4px;
                color: #999; font-size: 12px; font-style: italic; line-height: 1.5;
            }}
            .ai-label {{ color: #D4AF37; margin-right: 5px; font-style: normal; font-weight:bold; }}
            
            .footer {{ padding: 25px; text-align: center; color: #444; font-size: 11px; background: #0a0a0a; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="title">鎏金量化 · 核心卫星智投</div>
                <div class="subtitle">GILDED QUANT SYSTEM | V9.1 FINAL EDITION</div>
                <div style="font-size:10px; color:#555; margin-top:5px;">{datetime.now().strftime('%Y-%m-%d')}</div>
            </div>
            
            <div class="dashboard">
                <div class="macro">Market Context: {market_ctx.get('north_label')} {market_ctx.get('north_money')}</div>
                <div style="color:#888; font-size:12px; margin-top:10px;">建议投入 (CNY)</div>
                <div class="money">¥{cash_display}</div>
            </div>
            
            {render_group("🪐 核心资产 (底仓/定投)", cores)}
            {render_group("🚀 卫星资产 (波段/轮动)", sats)}
            
            <div class="footer">
                <strong>SYSTEM STATUS: OPERATIONAL</strong><br>
                Core Assets: Long-term Hold | Satellite Assets: Swing Trade<br>
                Powered by Kimi AI & Quantitative Math
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
    
    logger.info(">>> [V9.1] 启动 T+1 确认...")
    tracker.confirm_trades()
    
    try: analyst = NewsAnalyst()
    except: analyst = None

    logger.info(">>> 启动 V9.1 核心卫星版 (Final Edition)...")
    market_ctx = scanner.get_market_sentiment()
    funds_results = []
    
    BASE_AMT = config['global']['base_invest_amount']
    MAX_DAILY = config['global']['max_daily_invest']

    for fund in config['funds']:
        try:
            logger.info(f"=== 分析 {fund['name']} ({fund.get('strategy_type','satellite')}) ===")
            
            # 1. 获取数据 (防崩逻辑)
            data_dict = fetcher.get_fund_history(fund['code'])
            if not data_dict:
                logger.warning(f"⚠️ {fund['name']} 数据获取失败，跳过")
                continue

            # 2. 技术指标
            tech_indicators = TechnicalAnalyzer.calculate_indicators(data_dict)
            if not tech_indicators: continue

            # 3. 持仓数据
            pos_info = tracker.get_position(fund['code'])
            
            # 4. 核心计算
            final_amt, pos_type, is_sell, sell_amt = calculate_position(
                tech_indicators, BASE_AMT, MAX_DAILY, pos_info, fund.get('strategy_type', 'satellite')
            )
            
            # 5. AI 分析 (仅关键时刻调用)
            ai_analysis = {}
            if analyst:
                 if final_amt > 0 or is_sell or tech_indicators['quant_score'] >= 70 or tech_indicators['quant_score'] <= 30:
                    news = analyst.fetch_news_titles(fund['sector_keyword'])
                    ai_analysis = analyst.analyze_fund_v4(fund['name'], tech_indicators, market_ctx, news)

            # 6. 记账
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
                "strategy_type": fund.get('strategy_type', 'satellite')
            })
            
            # 随机冷却
            wait_time = random.randint(3, 6)
            logger.info(f"⏳ 冷却 {wait_time} 秒...")
            time.sleep(wait_time)

        except Exception as e:
            logger.error(f"分析失败 {fund['name']}: {e}")

    if funds_results:
        # 排序：卫星在前，核心在后；同类中按分数倒序
        funds_results.sort(key=lambda x: (x.get('strategy_type') != 'core', -x['tech']['quant_score']))
        html_report = render_html_report(market_ctx, funds_results, MAX_DAILY)
        send_email("📊 鎏金量化·核心卫星智投", html_report)

if __name__ == "__main__":
    main()
