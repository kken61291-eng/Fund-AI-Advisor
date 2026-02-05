import yaml
import os
import time
from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from market_scanner import MarketScanner
from technical_analyzer import TechnicalAnalyzer
from portfolio_tracker import PortfolioTracker
from utils import send_email, logger

def load_config():
    with open('config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def calculate_position_v11(tech, ai_adjustment, base_amount, max_daily, pos_info, strategy_type):
    # --- 保持 V11.0 核心算法不变 ---
    base_score = tech['quant_score']
    final_score = max(0, min(100, base_score + ai_adjustment))
    
    tech['final_score'] = final_score
    tech['ai_adjustment'] = ai_adjustment

    weekly = tech['trend_weekly']
    shares = pos_info['shares']
    held_days = pos_info.get('held_days', 999)
    
    is_core = (strategy_type == 'core')
    multiplier = 0
    reasons = []

    if final_score >= 85: multiplier = 2.0; reasons.append("极高确信")
    elif final_score >= 70: multiplier = 1.0
    elif final_score >= 60: multiplier = 0.5
    elif final_score <= 20: multiplier = -1.0 
    
    if is_core:
        if multiplier < 0 and final_score > -40: multiplier = 0 
        if weekly == "UP" and multiplier == 0: multiplier = 0.5

    if not is_core:
        cost = pos_info['cost']
        if shares > 0:
            pct = (tech['price'] - cost) / cost * 100
            if pct > 15 and final_score < 70: multiplier = -0.5 
            if pct < -8 and final_score < 40: multiplier = -1.0 

    if multiplier < 0 and shares > 0 and held_days < 7: 
        multiplier = 0; reasons.append(f"锁仓({held_days}天)")

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
        sell_value = shares * tech['price'] * sell_ratio
        label = "卖出"

    if reasons:
        if 'quant_reasons' not in tech: tech['quant_reasons'] = []
        tech['quant_reasons'].extend(reasons)
        
    return final_amount, label, is_sell, sell_value

def render_html_report(macro_news_list, funds_results, daily_total_cap, cio_review, advisor_review):
    """
    V12.3 UI: 适配新指标 (量比/背离/布林带)
    """
    # 宏观新闻 HTML
    macro_html = ""
    for news in macro_news_list:
        macro_html += f"""
        <div style="font-size:12px;color:#eeeeee;margin-bottom:6px;border-bottom:1px dashed #5d4037;padding-bottom:4px;line-height:1.4;">
            <span style="color:#ffb74d;margin-right:5px;font-weight:bold;">●</span>{news['title']} 
            <span style="color:#bdbdbd;float:right;font-size:10px;">[{news['source']}]</span>
        </div>
        """

    def render_dots(hist):
        h = ""
        for x in hist:
            c = "#d32f2f" if x['s']=='B' else ("#388e3c" if x['s'] in ['S','C'] else "#555")
            h += f'<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:{c};margin-right:3px;box-shadow:0 0 2px rgba(0,0,0,0.5);" title="{x["date"]}"></span>'
        return h

    rows = ""
    for r in funds_results:
        # 颜色逻辑
        if r['amount'] > 0: 
            border_color = "#d32f2f"
            bg_gradient = "linear-gradient(90deg, rgba(60,10,10,0.9) 0%, rgba(20,20,20,0.95) 100%)"
        elif r.get('is_sell'): 
            border_color = "#388e3c"
            bg_gradient = "linear-gradient(90deg, rgba(10,40,10,0.9) 0%, rgba(20,20,20,0.95) 100%)"
        else: 
            border_color = "#555"
            bg_gradient = "linear-gradient(90deg, rgba(30,30,30,0.9) 0%, rgba(15,15,15,0.95) 100%)"

        act = f"<span style='color:#ff8a80;font-weight:bold'>+{r['amount']:,}</span>" if r['amount']>0 else (f"<span style='color:#a5d6a7;font-weight:bold'>-{int(r.get('sell_value',0)):,}</span>" if r.get('is_sell') else "<span style='color:#777'>HOLD</span>")
        
        reasons_list = r['tech'].get('quant_reasons', [])
        reasons = " ".join([f"<span style='border:1px solid #555;padding:0 3px;font-size:9px;border-radius:2px;color:#888;'>{x}</span>" for x in reasons_list])
        
        # 分数展示
        base = r['tech']['quant_score']
        adj = r['tech'].get('ai_adjustment', 0)
        final = r['tech'].get('final_score', base)
        
        score_html = f"{final}"
        if adj != 0:
            color = "#ff8a80" if adj < 0 else "#a5d6a7"
            score_html += f" <span style='font-size:10px;color:{color};'>({adj:+})</span>"

        # AI 点评
        ai_txt = ""
        ai_data = r.get('ai_analysis', {})
        if ai_data.get('comment'):
            ai_txt = f"""
            <div style='font-size:12px;color:#d7ccc8;margin-top:10px;padding:8px;background:rgba(0,0,0,0.3);border-radius:4px;border-left:2px solid #ffb74d;'>
                <div style='margin-bottom:4px;'><strong style='color:#ffb74d'>✦ 洞察:</strong> {ai_data.get('comment')}</div>
                <div><strong style='color:#ef5350'>⚡ 风险:</strong> {ai_data.get('risk_alert')}</div>
            </div>
            """

        # --- [V12.3] 新指标数据处理 ---
        risk = r['tech'].get('risk_factors', {})
        vol_ratio = risk.get('vol_ratio', 1.0)
        pct_b = risk.get('bollinger_pct_b', 0.5)
        divergence = risk.get('divergence', '无')

        # 样式逻辑
        vol_style = "color:#ffb74d;" if vol_ratio < 0.8 else ("color:#ff8a80;" if vol_ratio > 2.0 else "color:#bbb;") # 缩量黄，放量红
        div_style = "color:#ef5350;font-weight:bold;" if "顶背离" in str(divergence) else ("color:#a5d6a7;" if "底背离" in str(divergence) else "color:#bbb;")
        bb_style = "color:#e040fb;" if pct_b > 1.0 else ("color:#a5d6a7;" if pct_b < 0 else "color:#bbb;")

        rows += f"""
        <div style="background:{bg_gradient};border-left:4px solid {border_color};margin-bottom:15px;padding:15px;border-radius:6px;box-shadow:0 4px 10px rgba(0,0,0,0.6);border-top:1px solid #333;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
                <div>
                    <span style="font-size:18px;font-weight:bold;color:#f0e6d2;font-family:'Times New Roman',serif;">{r['name']}</span>
                    <span style="font-size:12px;color:#9ca3af;margin-left:5px;">{r['code']}</span>
                </div>
                <div style="text-align:right;">
                    <div style="color:#ffb74d;font-weight:bold;font-size:16px;text-shadow:0 0 5px rgba(255,183,77,0.3);">{score_html}</div>
                    <div style="font-size:9px;color:#666;">XUANTIE SCORE</div>
                </div>
            </div>
            
            <div style="display:flex;justify-content:space-between;color:#e0e0e0;font-size:15px;margin-bottom:8px;border-bottom:1px solid #444;padding-bottom:8px;">
                <span style="font-weight:bold;color:#ffb74d;">{r['position_type']}</span>
                <span style="font-family:'Courier New',monospace;">{act}</span>
            </div>

            <div style="display:grid;grid-template-columns:repeat(4, 1fr);gap:5px;font-size:11px;color:#bdbdbd;font-family:'Courier New',monospace;margin-bottom:4px;">
                <span>RSI: {r['tech']['rsi']}</span>
                <span>MACD: {r['tech']['macd']['trend']}</span>
                <span>OBV: {'流入' if r['tech']['flow']['obv_slope']>0 else '流出'}</span>
                <span>Wkly: {r['tech']['trend_weekly']}</span>
            </div>
            
            <div style="display:grid;grid-template-columns:repeat(3, 1fr);gap:5px;font-size:11px;color:#bdbdbd;font-family:'Courier New',monospace;margin-bottom:8px;border-top:1px dashed #333;padding-top:4px;">
                <span style="{vol_style}">VR(量比): {vol_ratio}</span>
                <span style="{bb_style}">%B(布林): {pct_b}</span>
                <span style="{div_style}">Div: {divergence}</span>
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
    <meta charset="utf-8">
    <style>
        body {{ 
            background: #0a0a0a;
            color: #f0e6d2; 
            font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; 
            max-width: 660px; margin: 0 auto; padding: 20px;
        }}
        .main-container {{
            border: 2px solid #3e2723;
            border-top: 5px solid #ffb74d;
            border-radius: 4px;
            padding: 20px;
            background: linear-gradient(180deg, #1b1b1b 0%, #000000 100%);
            box-shadow: 0 10px 30px rgba(0,0,0,0.8);
        }}
        .header {{ text-align: center; border-bottom: 2px solid #3e2723; padding-bottom: 20px; margin-bottom: 25px; }}
        .title {{ 
            color: #ffb74d; margin: 0; font-size: 32px; letter-spacing: 3px; font-weight: 800; 
            text-transform: uppercase; font-family: 'Times New Roman', serif;
            text-shadow: 0 2px 10px rgba(0,0,0,0.9);
            background: -webkit-linear-gradient(#fff, #ffb74d); -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        }}
        .subtitle {{ font-size: 11px; color: #8d6e63; margin-top: 8px; letter-spacing: 2px; text-transform: uppercase; }}
        
        .macro-panel {{
            background: rgba(30, 30, 30, 0.6); 
            border: 1px solid #3e2723;
            border-radius: 4px;
            padding: 15px;
            margin-top: 20px;
            text-align: left;
        }}
        .macro-title {{
            font-size: 11px; color: #ffb74d; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 1px;
            border-bottom: 1px solid #3e2723; padding-bottom: 4px; font-weight: bold;
        }}

        .cio-paper {{ 
            background: #121212; padding: 20px; border: 1px solid #3e2723; border-radius: 2px; 
            margin-bottom: 25px; font-size: 14px; line-height: 1.6; color: #d7ccc8;
            box-shadow: inset 0 0 30px rgba(0,0,0,0.8); position: relative;
        }}
        .cio-seal {{
            position: absolute; top: 10px; right: 10px; 
            border: 2px solid #ffb74d; color: #ffb74d;
            padding: 5px 15px; font-size: 14px; 
            transform: rotate(-10deg); font-weight: 900; 
            opacity: 0.9; 
            text-shadow: 0 0 10px rgba(255, 183, 77, 0.2);
            letter-spacing: 2px;
        }}
        
        .advisor-paper {{
            background: #1a1a1a; 
            border-left: 4px solid #5d4037; 
            padding: 20px; 
            margin-bottom: 25px; 
            font-size: 14px; line-height: 1.6; color: #e0e0e0;
            background-image: repeating-linear-gradient(45deg, rgba(255,255,255,0.02) 0px, rgba(255,255,255,0.02) 1px, transparent 1px, transparent 5px);
        }}
        .advisor-title {{
            color: #bdbdbd; font-weight: bold; font-size: 16px; margin-bottom: 10px;
            font-family: 'Times New Roman', serif; border-bottom: 1px solid #5d4037; padding-bottom: 5px;
        }}

        .footer {{ text-align: center; font-size: 10px; color: #4e342e; margin-top: 40px; font-family: serif; }}
    </style>
    </head>
    <body>
        <div class="main-container">
            <div class="header">
                <h1 class="title">XUANTIE QUANT</h1>
                <div class="subtitle">HEAVY SWORD, NO EDGE | V12.3 OMNI-EYE</div>
                
                <div class="macro-panel">
                    <div class="macro-title">GLOBAL MACRO RADAR</div>
                    {macro_html}
                </div>
            </div>
            
            <div class="cio-paper">
                <div class="cio-seal">CIO APPROVED</div>
                {cio_review}
            </div>
            
            <div class="advisor-paper">
                {advisor_review}
            </div>
            
            {rows}
            
            <div class="footer">
                EST. 2026 | POWERED BY CAILIAN & JINSHI DATA <br>
                "In Math We Trust, By AI We Verify."
            </div>
        </div>
    </body></html>
    """

def main():
    config = load_config()
    fetcher = DataFetcher()
    scanner = MarketScanner()
    tracker = PortfolioTracker() 
    
    logger.info(">>> [V12.3] 启动玄铁量化 (Xuantie Quant)...")
    tracker.confirm_trades()
    
    try: analyst = NewsAnalyst()
    except: analyst = None

    # V12.0: 全视之眼 - 多源宏观获取
    macro_news_list = scanner.get_macro_news()
    market_ctx_str = " | ".join([f"{n['title']}" for n in macro_news_list])
    
    funds_results = []
    cio_summary_lines = [f"市场环境: {market_ctx_str}"]
    
    BASE_AMT = config['global']['base_invest_amount']
    MAX_DAILY = config['global']['max_daily_invest']

    for fund in config['funds']:
        try:
            logger.info(f"Analyzing {fund['name']}...")
            data = fetcher.get_fund_history(fund['code'])
            if not data: continue

            # V12.1: 计算包含量比、背离的新指标
            tech = TechnicalAnalyzer.calculate_indicators(data)
            if not tech: continue

            pos = tracker.get_position(fund['code'])
            
            ai_adjustment = 0
            ai_res = {}
            
            need_ai = (pos['shares'] > 0) or (tech['quant_score'] >= 60) or (tech['quant_score'] <= 35)
            
            if analyst and need_ai:
                news = analyst.fetch_news_titles(fund['sector_keyword'])
                # V12.2: 注入板块逻辑链进行分析
                ai_res = analyst.analyze_fund_v4(fund['name'], tech, market_ctx_str, news)
                ai_adjustment = ai_res.get('adjustment', 0)
            
            amt, lbl, is_sell, s_val = calculate_position_v11(
                tech, ai_adjustment, BASE_AMT, MAX_DAILY, pos, fund.get('strategy_type')
            )
            
            tracker.record_signal(fund['code'], lbl)
            
            if amt > 0: tracker.add_trade(fund['code'], fund['name'], amt, tech['price'])
            elif is_sell: tracker.add_trade(fund['code'], fund['name'], s_val, tech['price'], True)

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
    advisor_review = ""
    if analyst and funds_results:
        logger.info(">>> CIO & Xuantie Master Auditing...")
        cio_review = analyst.review_report("\n".join(cio_summary_lines))
        advisor_review = analyst.advisor_review("\n".join(cio_summary_lines), market_ctx_str)

    if funds_results:
        funds_results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        html = render_html_report(macro_news_list, funds_results, MAX_DAILY, cio_review, advisor_review)
        send_email("🗡️ 玄铁量化 V12.3 全视之眼手谕", html)

if __name__ == "__main__":
    main()
