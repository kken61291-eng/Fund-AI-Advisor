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
    V10.0 核心算法 (逻辑保持不变，确保延续性)
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
    
    # 2. 核心/卫星/七日锁/风控逻辑 (完全保留 V9.2 逻辑)
    if is_core:
        if multiplier < 0 and score > -40: multiplier = 0 
        if weekly == "UP" and multiplier == 0: multiplier = 0.5

    if not is_core:
        if profit_pct > 15 and score < 70: multiplier = -0.5 
        if profit_pct < -8 and score < 40: multiplier = -1.0 

    if multiplier < 0 and has_position and held_days < 7: 
        multiplier = 0 
        reasons.append(f"🔒冷静期({held_days}天)")

    if weekly == "DOWN":
        if multiplier > 0: multiplier *= 0.5 
        if is_core and multiplier < 0 and score > -60: multiplier = 0 

    final_amount = 0
    is_sell = False
    sell_value = 0
    label = "⏸️ 观望"

    if multiplier > 0:
        raw_amount = int(base_amount * multiplier)
        final_amount = max(0, min(raw_amount, int(max_daily)))
        if multiplier >= 2.0: label = "🔥 强力增持"
        elif multiplier >= 1.0: label = "✅ 建仓"
        else: label = "🧪 试探"

    elif multiplier < 0:
        is_sell = True
        sell_ratio = min(abs(multiplier), 1.0)
        position_value = shares * price
        sell_value = position_value * sell_ratio
        if (position_value - sell_value) < 100: 
            sell_value = position_value; sell_ratio = 1.0
        if sell_ratio >= 0.99: label = "🚫 清仓"
        else: label = f"✂️ 减仓{int(sell_ratio*100)}%"

    if reasons: tech_data['quant_reasons'].extend(reasons)
    return final_amount, label, is_sell, sell_value

def render_html_report(market_ctx, funds_results, daily_total_cap, cio_review):
    """
    ✨ V10.0 UI: 仪表盘式布局 + 信号回测 + CIO 审计
    """
    invested = sum(r['amount'] for r in funds_results if r['amount'] > 0)
    cash_display = f"{invested:,}"
    
    cores = [r for r in funds_results if r['strategy_type'] == 'core']
    sats = [r for r in funds_results if r['strategy_type'] == 'satellite']
    
    def render_history_dots(history):
        """渲染历史信号圆点"""
        html = '<div class="history-track">'
        for h in history:
            color = "#666" # 默认灰
            if h['s'] == 'B': color = "#ff4d4f" # 买入红
            elif h['s'] == 'S' or h['s'] == 'C': color = "#52c41a" # 卖出绿
            html += f'<span class="dot" style="background:{color};" title="{h["date"]}: {h["s"]}"></span>'
        html += '</div>'
        return html

    def render_group(title, items):
        if not items: return ""
        html_chunk = f'<div class="section-title">{title}</div><div class="card-grid">'
        for r in items:
            border_color = "#444" 
            if r['amount'] > 0: border_color = "#ff4d4f" 
            elif r.get('is_sell'): border_color = "#52c41a" 
            
            # 操作文本
            if r['amount'] > 0: act_text = f"<span class='act-buy'>+¥{r['amount']:,}</span>"
            elif r.get('is_sell'): act_text = f"<span class='act-sell'>卖 ¥{int(r.get('sell_value',0)):,}</span>"
            else: act_text = "<span class='act-wait'>观望</span>"

            # AI 分析
            ai_html = ""
            if r.get('ai_analysis') and r['ai_analysis'].get('comment'):
                 ai_html = f'<div class="ai-comment">{r["ai_analysis"]["comment"]} <span class="risk-tag">⚠️ {r["ai_analysis"].get("risk_alert","")}</span></div>'

            html_chunk += f"""
            <div class="card" style="border-top: 3px solid {border_color};">
                <div class="card-header">
                    <span class="fund-name">{r['name']}</span>
                    <span class="score-badge" style="background:{border_color}">{r['tech']['quant_score']}</span>
                </div>
                <div class="card-sub">{r['code']} | {r['position_type']}</div>
                
                <div class="card-body">
                    <div class="main-act">{act_text}</div>
                    
                    <div class="indicators">
                        <span>RSI:{r['tech']['rsi']}</span>
                        <span>MACD:{r['tech']['macd']['trend']}</span>
                        <span>J值:{r['tech']['kdj']['j']}</span>
                        <span>资金:{'流入' if r['tech']['flow']['obv_slope']>0 else '流出'}</span>
                    </div>
                    
                    <div class="tags">
                        {''.join([f'<span class="tag">{x}</span>' for x in r['tech']['quant_reasons']])}
                    </div>
                    
                    <div class="history-label">近10次策略验证:</div>
                    {render_history_dots(r['history'])}
                    
                    {ai_html}
                </div>
            </div>
            """
        html_chunk += "</div>" # end grid
        return html_chunk

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ background-color: #121212; color: #e0e0e0; font-family: 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 10px; }}
            .container {{ max-width: 800px; margin: 0 auto; background: #1e1e1e; border-radius: 12px; overflow: hidden; }}
            
            /* Header */
            .header {{ background: linear-gradient(135deg, #1f1f1f 0%, #000 100%); padding: 25px; text-align: center; border-bottom: 2px solid #D4AF37; }}
            .title {{ font-size: 24px; color: #D4AF37; font-weight: 800; letter-spacing: 1px; text-transform: uppercase; }}
            .subtitle {{ color: #666; font-size: 11px; margin-top: 5px; }}
            
            /* Dashboard */
            .dashboard {{ display: flex; justify-content: space-around; padding: 15px; background: #252525; border-bottom: 1px solid #333; }}
            .dash-item {{ text-align: center; }}
            .dash-val {{ font-size: 20px; font-weight: bold; color: #fff; }}
            .dash-lbl {{ font-size: 10px; color: #888; text-transform: uppercase; }}
            
            /* Grid Layout for Cards */
            .card-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 15px; padding: 15px; }}
            .section-title {{ padding: 15px 20px 0; color: #D4AF37; font-size: 14px; font-weight: bold; letter-spacing: 1px; }}
            
            /* Card Design */
            .card {{ background: #2a2a2a; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }}
            .card-header {{ padding: 12px 15px; display: flex; justify-content: space-between; align-items: center; background: #333; }}
            .fund-name {{ font-weight: bold; color: #fff; font-size: 15px; }}
            .score-badge {{ padding: 2px 8px; border-radius: 10px; color: #fff; font-size: 12px; font-weight: bold; }}
            .card-sub {{ padding: 0 15px; font-size: 10px; color: #666; margin-top: 5px; }}
            
            .card-body {{ padding: 15px; }}
            .main-act {{ font-size: 18px; font-weight: bold; margin-bottom: 10px; }}
            .act-buy {{ color: #ff4d4f; }} .act-sell {{ color: #52c41a; }} .act-wait {{ color: #888; }}
            
            .indicators {{ display: grid; grid-template-columns: 1fr 1fr; gap: 5px; font-size: 11px; color: #aaa; margin-bottom: 10px; font-family: monospace; }}
            
            .tags {{ margin-bottom: 10px; }}
            .tag {{ display: inline-block; background: #3a3a3a; color: #ccc; padding: 2px 6px; border-radius: 3px; font-size: 10px; margin-right: 4px; border: 1px solid #444; }}
            
            /* History Dots */
            .history-track {{ display: flex; gap: 3px; margin-bottom: 10px; }}
            .history-label {{ font-size: 10px; color: #555; margin-bottom: 2px; }}
            .dot {{ width: 8px; height: 8px; border-radius: 50%; display: inline-block; }}
            
            /* AI & CIO Section */
            .ai-comment {{ font-size: 12px; color: #999; line-height: 1.4; border-top: 1px dashed #444; padding-top: 8px; }}
            .risk-tag {{ color: #D4AF37; font-size: 11px; display: block; margin-top: 4px; }}
            
            .cio-section {{ margin: 20px; padding: 20px; background: #1a1a1a; border: 1px solid #D4AF37; border-radius: 8px; color: #ccc; }}
            .cio-section h3 {{ color: #D4AF37; margin-top: 0; }}
            .cio-section strong {{ color: #fff; }}
            
            .footer {{ padding: 20px; text-align: center; color: #444; font-size: 10px; background: #111; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div class="title">鎏金量化 · 双核旗舰版</div>
                <div class="subtitle">V10.0 DUAL-CORE ENGINE | MACD + KDJ + AI AUDIT</div>
                <div style="font-size:10px; color:#555; margin-top:5px;">{datetime.now().strftime('%Y-%m-%d')}</div>
            </div>
            
            <div class="dashboard">
                <div class="dash-item">
                    <div class="dash-val">{market_ctx.get('north_label')}</div>
                    <div class="dash-lbl">宏观情绪</div>
                </div>
                <div class="dash-item">
                    <div class="dash-val" style="color:#D4AF37">¥{cash_display}</div>
                    <div class="dash-lbl">建议投入</div>
                </div>
                <div class="dash-item">
                    <div class="dash-val">{len(funds_results)}</div>
                    <div class="dash-lbl">监控标的</div>
                </div>
            </div>
            
            <div class="cio-section">
                {cio_review}
            </div>
            
            {render_group("🪐 核心资产 (底仓)", cores)}
            {render_group("🚀 卫星资产 (进攻)", sats)}
            
            <div class="footer">
                SYSTEM V10.0 | POWERED BY KIMI & PANDAS-TA <br>
                HISTORY TRACKING ENABLED
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
    
    logger.info(">>> [V10.0] 启动 T+1 确认...")
    tracker.confirm_trades()
    
    try: analyst = NewsAnalyst()
    except: analyst = None

    logger.info(">>> 启动 V10.0 双核旗舰版 (MACD/KDJ/CIO)...")
    market_ctx = scanner.get_market_sentiment()
    funds_results = []
    
    # 收集简报用于 CIO 审计
    report_summary_for_cio = f"市场环境: {market_ctx}\n今日交易计划:\n"

    BASE_AMT = config['global']['base_invest_amount']
    MAX_DAILY = config['global']['max_daily_invest']

    for fund in config['funds']:
        try:
            logger.info(f"=== 分析 {fund['name']} ===")
            
            # 1. 获取数据
            data_dict = fetcher.get_fund_history(fund['code'])
            if not data_dict: continue

            # 2. V10 技术指标 (含 MACD/KDJ)
            tech_indicators = TechnicalAnalyzer.calculate_indicators(data_dict)
            if not tech_indicators: continue

            pos_info = tracker.get_position(fund['code'])
            
            # 3. 策略计算
            final_amt, pos_type, is_sell, sell_amt = calculate_position(
                tech_indicators, BASE_AMT, MAX_DAILY, pos_info, fund.get('strategy_type', 'satellite')
            )
            
            # 4. 记录信号回测
            tracker.record_signal(fund['code'], pos_type)
            history = tracker.get_signal_history(fund['code'])
            
            # 5. AI 分析员 (Analyst)
            ai_analysis = {}
            if analyst and (final_amt > 0 or is_sell or tech_indicators['quant_score'] >= 70 or tech_indicators['quant_score'] <= 30):
                news = analyst.fetch_news_titles(fund['sector_keyword'])
                ai_analysis = analyst.analyze_fund_v4(fund['name'], tech_indicators, market_ctx, news)

            # 6. 记账
            if final_amt > 0:
                tracker.add_trade(fund['code'], fund['name'], final_amt, tech_indicators['price'], is_sell=False)
            elif is_sell and sell_amt > 0:
                tracker.add_trade(fund['code'], fund['name'], sell_amt, tech_indicators['price'], is_sell=True)
            
            # 7. 汇总给 CIO
            action_str = f"买入{final_amt}" if final_amt > 0 else (f"卖出{sell_amt}" if is_sell else "观望")
            report_summary_for_cio += f"- {fund['name']}: {action_str} (评分{tech_indicators['quant_score']})\n"

            funds_results.append({
                "name": fund['name'], "code": fund['code'],
                "amount": final_amt, "sell_value": sell_amt,
                "position_type": pos_type, "is_sell": is_sell,
                "tech": tech_indicators,
                "ai_analysis": ai_analysis,
                "strategy_type": fund.get('strategy_type', 'satellite'),
                "history": history # 传递历史记录给 UI
            })
            
            wait_time = random.randint(3, 6)
            time.sleep(wait_time)

        except Exception as e:
            logger.error(f"分析失败 {fund['name']}: {e}")

    # 8. 召唤 CIO 进行总审计
    cio_review = ""
    if analyst and funds_results:
        logger.info(">>> 正在进行 CIO 独立审计...")
        cio_review = analyst.review_report(report_summary_for_cio)

    if funds_results:
        funds_results.sort(key=lambda x: (x.get('strategy_type') != 'core', -x['tech']['quant_score']))
        html_report = render_html_report(market_ctx, funds_results, MAX_DAILY, cio_review)
        send_email("📊 鎏金量化 V10.0 旗舰战报", html_report)

if __name__ == "__main__":
    main()
