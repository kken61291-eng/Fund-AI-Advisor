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

def calculate_position_v11(tech_data, ai_adjustment, base_amount, max_daily, pos_info, strategy_type):
    """
    V11.0 神经量化融合引擎
    """
    # 1. 基础分 (Quant)
    base_score = tech_data['quant_score']
    
    # 2. 最终分 (Fusion) = 基础分 + AI修正
    # 限制最终分在 0-100 之间
    final_score = max(0, min(100, base_score + ai_adjustment))
    
    # 将最终分写回 tech_data 以便 UI 展示
    tech_data['final_score'] = final_score
    tech_data['ai_adjustment'] = ai_adjustment

    weekly = tech_data['trend_weekly']
    shares = pos_info['shares']
    held_days = pos_info.get('held_days', 999)
    
    is_core = (strategy_type == 'core')
    multiplier = 0
    reasons = []

    # 3. 基于最终分的决策逻辑
    if final_score >= 85: 
        multiplier = 2.0; reasons.append("极高确信")
    elif final_score >= 70: 
        multiplier = 1.0
    elif final_score >= 60: 
        multiplier = 0.5
    elif final_score <= 20: # 稍微放宽卖出阈值，因为AI可能会扣分很狠
        multiplier = -1.0 
    
    # 策略微调 (核心/卫星)
    if is_core:
        if multiplier < 0 and final_score > -40: multiplier = 0 
        if weekly == "UP" and multiplier == 0: multiplier = 0.5

    if not is_core:
        cost = pos_info['cost']
        if shares > 0:
            pct = (tech_data['price'] - cost) / cost * 100
            # 止盈止损逻辑也参考最终分
            if pct > 15 and final_score < 70: multiplier = -0.5 
            if pct < -8 and final_score < 40: multiplier = -1.0 

    # 七日锁
    if multiplier < 0 and shares > 0 and held_days < 7: 
        multiplier = 0; reasons.append(f"锁仓({held_days}天)")

    # 熊市防御
    if weekly == "DOWN":
        if multiplier > 0: multiplier *= 0.5 
        if is_core and multiplier < 0 and final_score > -60: multiplier = 0 

    final_amount = 0
    is_sell = False
    sell_value = 0
    label = "观望"

    if multiplier > 0:
        final_amount = max(0, min(int(base_amount * multiplier), int(max_daily)))
        label = "买入"
    elif multiplier < 0:
        is_sell = True
        sell_ratio = min(abs(multiplier), 1.0)
        sell_value = shares * tech_data['price'] * sell_ratio
        label = "卖出"

    # 记录原因
    if reasons:
        if 'quant_reasons' not in tech_data: tech_data['quant_reasons'] = []
        tech_data['quant_reasons'].extend(reasons)
        
    return final_amount, label, is_sell, sell_value

def render_html_report(market_ctx, funds_results, daily_total_cap, cio_review):
    """
    V11.0 UI: 鎏金岁月 (Gilded Age) 主题
    """
    def render_dots(hist):
        h = ""
        for x in hist:
            c = "#d32f2f" if x['s']=='B' else ("#388e3c" if x['s'] in ['S','C'] else "#555")
            h += f'<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:{c};margin-right:3px;box-shadow:0 0 2px rgba(0,0,0,0.5);" title="{x["date"]}"></span>'
        return h

    rows = ""
    for r in funds_results:
        # 配色逻辑：涨红跌绿，但使用更有质感的颜色
        # 红: #d32f2f (朱砂), 绿: #388e3c (翡翠), 灰: #424242
        if r['amount'] > 0: 
            border_color = "#d32f2f"
            bg_gradient = "linear-gradient(90deg, rgba(60,10,10,0.8) 0%, rgba(30,30,30,0.8) 100%)"
        elif r.get('is_sell'): 
            border_color = "#388e3c"
            bg_gradient = "linear-gradient(90deg, rgba(10,40,10,0.8) 0%, rgba(30,30,30,0.8) 100%)"
        else: 
            border_color = "#555"
            bg_gradient = "linear-gradient(90deg, rgba(30,30,30,0.8) 0%, rgba(20,20,20,0.8) 100%)"

        act = f"<span style='color:#ff8a80;font-weight:bold'>+{r['amount']:,}</span>" if r['amount']>0 else (f"<span style='color:#a5d6a7;font-weight:bold'>-{int(r.get('sell_value',0)):,}</span>" if r.get('is_sell') else "<span style='color:#aaa'>HOLD</span>")
        
        reasons_list = r['tech'].get('quant_reasons', [])
        reasons = " ".join([f"<span style='border:1px solid #555;padding:0 3px;font-size:9px;border-radius:2px;color:#888;'>{x}</span>" for x in reasons_list])
        
        # 分数展示逻辑：显示修正过程
        base = r['tech']['quant_score']
        adj = r['tech'].get('ai_adjustment', 0)
        final = r['tech'].get('final_score', base)
        
        score_html = f"{final}"
        if adj != 0:
            color = "#ff8a80" if adj < 0 else "#a5d6a7"
            score_html += f" <span style='font-size:10px;color:{color};'>({adj:+})</span>"

        ai_txt = ""
        ai_data = r.get('ai_analysis', {})
        if ai_data.get('comment'):
            ai_txt = f"""
            <div style='font-size:12px;color:#d7ccc8;margin-top:10px;padding:8px;background:rgba(0,0,0,0.3);border-radius:4px;border-left:2px solid #D4AF37;'>
                <div style='margin-bottom:4px;'><strong style='color:#D4AF37'>✦ 洞察:</strong> {ai_data.get('comment')}</div>
                <div><strong style='color:#ef5350'>⚡ 风险:</strong> {ai_data.get('risk_alert')}</div>
            </div>
            """

        rows += f"""
        <div style="background:{bg_gradient};border-left:4px solid {border_color};margin-bottom:15px;padding:15px;border-radius:8px;box-shadow:0 4px 8px rgba(0,0,0,0.5);border-top:1px solid #333;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
                <div>
                    <span style="font-size:18px;font-weight:bold;color:#f0e6d2;font-family:'Times New Roman',serif;">{r['name']}</span>
                    <span style="font-size:12px;color:#9ca3af;margin-left:5px;">{r['code']}</span>
                </div>
                <div style="text-align:right;">
                    <div style="color:#D4AF37;font-weight:bold;font-size:16px;text-shadow:0 0 5px rgba(212,175,55,0.3);">{score_html}</div>
                    <div style="font-size:9px;color:#666;">NEURO-SCORE</div>
                </div>
            </div>
            
            <div style="display:flex;justify-content:space-between;color:#e0e0e0;font-size:15px;margin-bottom:8px;border-bottom:1px solid #444;padding-bottom:8px;">
                <span style="font-weight:bold;color:#D4AF37;">{r['position_type']}</span>
                <span style="font-family:'Courier New',monospace;">{act}</span>
            </div>
            
            <div style="display:grid;grid-template-columns:repeat(4, 1fr);gap:5px;font-size:11px;color:#bdbdbd;font-family:'Courier New',monospace;margin-bottom:8px;">
                <span>RSI: {r['tech']['rsi']}</span>
                <span>MACD: {r['tech']['macd']['trend']}</span>
                <span>OBV: {'流入' if r['tech']['flow']['obv_slope']>0 else '流出'}</span>
                <span>Wkly: {r['tech']['trend_weekly']}</span>
            </div>
            
            <div style="margin-bottom:8px;">{reasons}</div>
            <div style="margin-top:5px;">{render_dots(r.get('history',[]))}</div>
            {ai_txt}
        </div>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
        body {{ 
            background: linear-gradient(135deg, #1a0505 0%, #000000 100%); 
            color: #f0e6d2; 
            font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; 
            max-width: 640px; margin: 0 auto; padding: 20px;
            background-attachment: fixed;
        }}
        .header {{ text-align: center; border-bottom: 2px solid #D4AF37; padding-bottom: 20px; margin-bottom: 25px; }}
        .title {{ 
            color: #D4AF37; margin: 0; font-size: 28px; letter-spacing: 2px; font-weight: 300; 
            text-transform: uppercase; font-family: 'Times New Roman', serif;
            text-shadow: 0 2px 4px rgba(0,0,0,0.8);
        }}
        .subtitle {{ font-size: 11px; color: #8d6e63; margin-top: 8px; letter-spacing: 1px; }}
        .macro-tag {{ 
            background: #3e2723; color: #ffccbc; padding: 4px 10px; border-radius: 20px; 
            font-size: 12px; display: inline-block; margin-top: 10px; border: 1px solid #5d4037;
        }}
        .cio-paper {{ 
            background: #151515; padding: 20px; border: 1px solid #5d4037; border-radius: 4px; 
            margin-bottom: 25px; font-size: 14px; line-height: 1.6; color: #d7ccc8;
            box-shadow: inset 0 0 20px rgba(0,0,0,0.8); position: relative;
        }}
        .cio-seal {{
            position: absolute; top: 10px; right: 10px; border: 2px solid #D4AF37; color: #D4AF37;
            padding: 2px 8px; font-size: 10px; transform: rotate(-15deg); font-weight: bold; opacity: 0.7;
        }}
        .footer {{ text-align: center; font-size: 10px; color: #5d4037; margin-top: 40px; font-family: serif; }}
    </style>
    </head>
    <body>
        <div class="header">
            <h1 class="title">GILDED QUANT</h1>
            <div class="subtitle">V11.0 NEURO-FUSION ENGINE</div>
            <div class="macro-tag">宏观情绪: {market_ctx.get('north_label')}</div>
        </div>
        
        <div class="cio-paper">
            <div class="cio-seal">CONFIDENTIAL</div>
            {cio_review}
        </div>
        
        {rows}
        
        <div class="footer">
            EST. 2026 | POWERED BY KIMI & YAHOO FINANCE <br>
            "In Math We Trust, By AI We Verify."
        </div>
    </body></html>
    """

def main():
    config = load_config()
    fetcher = DataFetcher()
    scanner = MarketScanner()
    tracker = PortfolioTracker() 
    
    logger.info(">>> [V11.0] 启动神经量化融合引擎 (Gilded Age)...")
    tracker.confirm_trades()
    
    try: analyst = NewsAnalyst()
    except: analyst = None

    market_ctx = scanner.get_market_sentiment()
    funds_results = []
    
    cio_summary_lines = [f"市场环境: {market_ctx}"]
    
    BASE_AMT = config['global']['base_invest_amount']
    MAX_DAILY = config['global']['max_daily_invest']

    for fund in config['funds']:
        try:
            logger.info(f"Analyzing {fund['name']}...")
            data = fetcher.get_fund_history(fund['code'])
            if not data: continue

            tech = TechnicalAnalyzer.calculate_indicators(data)
            if not tech: continue

            pos = tracker.get_position(fund['code'])
            
            # --- V11.0 核心改变：先问 AI，再做决策 ---
            # 为了节省 Token，依然保留触发门槛，但门槛放低
            ai_adjustment = 0
            ai_res = {}
            
            # 触发条件：有持仓 OR 评分极端 OR 评分不错(>60)想买入时需要审计
            need_ai = (pos['shares'] > 0) or (tech['quant_score'] >= 60) or (tech['quant_score'] <= 35)
            
            if analyst and need_ai:
                news = analyst.fetch_news_titles(fund['sector_keyword'])
                ai_res = analyst.analyze_fund_v4(fund['name'], tech, market_ctx, news)
                ai_adjustment = ai_res.get('adjustment', 0)
            
            # 传入 ai_adjustment 进行最终决策计算
            amt, lbl, is_sell, s_val = calculate_position_v11(
                tech, ai_adjustment, BASE_AMT, MAX_DAILY, pos, fund.get('strategy_type')
            )
            
            tracker.record_signal(fund['code'], lbl)
            
            if amt > 0: tracker.add_trade(fund['code'], fund['name'], amt, tech['price'])
            elif is_sell: tracker.add_trade(fund['code'], fund['name'], s_val, tech['price'], True)

            # 汇总给 CIO
            act_str = f"买{amt}" if amt>0 else ("卖" if is_sell else "停")
            cio_summary_lines.append(f"- {fund['name']}: {act_str} (基准:{tech['quant_score']}->修正:{tech['final_score']})")

            funds_results.append({
                "name": fund['name'], "code": fund['code'], "amount": amt, "sell_value": s_val,
                "position_type": lbl, "is_sell": is_sell, "tech": tech, "ai_analysis": ai_res, 
                "history": tracker.get_signal_history(fund['code'])
            })
            
            time.sleep(0.1) 

        except Exception as e:
            logger.error(f"Err {fund['name']}: {e}")

    cio_review = ""
    if analyst and funds_results:
        logger.info(">>> CIO Final Seal...")
        cio_review = analyst.review_report("\n".join(cio_summary_lines))

    if funds_results:
        # 按最终得分排序
        funds_results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        html = render_html_report(market_ctx, funds_results, MAX_DAILY, cio_review)
        send_email("🏆 鎏金量化 V11.0 战略内参", html)

if __name__ == "__main__":
    main()
