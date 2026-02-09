import yaml
import os
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from market_scanner import MarketScanner
from technical_analyzer import TechnicalAnalyzer
from valuation_engine import ValuationEngine
from portfolio_tracker import PortfolioTracker
from utils import send_email, logger

# --- Global Configuration ---
DEBUG_MODE = True  # [Modified] Set to True to enable verbose logging & force AI analysis
tracker_lock = threading.Lock()

def load_config():
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to read config: {e}")
        return {"funds": [], "global": {"base_invest_amount": 1000, "max_daily_invest": 5000}}

# [Core Logic - CIO Strategy]
def calculate_position_v13(tech, ai_adj, val_mult, val_desc, base_amt, max_daily, pos, strategy_type, fund_name):
    base_score = tech.get('quant_score', 50)
    
    # [Debug] Log base score calculation details
    if DEBUG_MODE:
        logger.info(f"🔍 [DEBUG] {fund_name} Base Score Details: {tech.get('quant_reasons', [])}")

    tactical_score = max(0, min(100, base_score + ai_adj))
    
    action_str = "Bullish Boost" if ai_adj > 0 else ("Bearish Cut" if ai_adj < 0 else "Neutral")
    logger.info(f"🧮 [Score {fund_name}] Tech({base_score}) + CIO({ai_adj:+d} {action_str}) = Final({tactical_score})")
    
    tech['final_score'] = tactical_score
    tech['ai_adjustment'] = ai_adj
    tech['valuation_desc'] = val_desc
    
    cro_signal = tech.get('tech_cro_signal', 'PASS')
    
    tactical_mult = 0
    reasons = []

    if tactical_score >= 85: tactical_mult = 2.0; reasons.append("Tactical:StrongBuy")
    elif tactical_score >= 70: tactical_mult = 1.0; reasons.append("Tactical:Buy")
    elif tactical_score >= 60: tactical_mult = 0.5; reasons.append("Tactical:Accumulate")
    elif tactical_score <= 25: tactical_mult = -1.0; reasons.append("Tactical:Breakdown")

    final_mult = tactical_mult
    
    # Valuation Correction
    if tactical_mult > 0:
        if val_mult < 0.5: final_mult = 0; reasons.append(f"Strat:Overvalued_Stop")
        elif val_mult > 1.0: final_mult *= val_mult; reasons.append(f"Strat:Undervalued_Boost")
    elif tactical_mult < 0:
        if val_mult > 1.2: final_mult = 0; reasons.append(f"Strat:Bottom_Lock")
        elif val_mult < 0.8: final_mult *= 1.5; reasons.append("Strat:Overvalued_Cut")
    else:
        if val_mult >= 1.5 and strategy_type in ['core', 'dividend']:
            final_mult = 0.5; reasons.append(f"Strat:Left_Side_DCA")

    # Hard Risk Control
    if cro_signal == "VETO":
        if final_mult > 0:
            final_mult = 0
            reasons.append(f"🛡️Risk:Veto_Buy")
            logger.info(f"🚫 [Risk Veto {fund_name}] Trigger: {tech.get('tech_cro_comment')}")
    
    # Lock Period
    held_days = pos.get('held_days', 999)
    if final_mult < 0 and pos['shares'] > 0 and held_days < 7:
        final_mult = 0; reasons.append(f"Rule:Lock({held_days}d)")

    final_amt = 0; is_sell = False; sell_val = 0; label = "Wait"

    if final_mult > 0:
        amt = int(base_amt * final_mult)
        final_amt = max(0, min(amt, int(max_daily)))
        label = "Buy"
    elif final_mult < 0:
        is_sell = True
        sell_ratio = min(abs(final_mult), 1.0)
        sell_val = pos['shares'] * tech.get('price', 0) * sell_ratio
        label = "Sell"

    if reasons: tech['quant_reasons'] = reasons
    return final_amt, label, is_sell, sell_val

# [UI Rendering - Kept mostly same but ensures data exists]
def render_html_report_v13(all_news, results, cio_html, advisor_html):
    # ... (Keep existing UI code, just ensure strings are UTF-8 safe)
    # For brevity, I am not repeating the huge HTML string unless you need it changed.
    # The fix logic is in the processing loop below.
    # ... 
    # (Returning the exact same HTML generation code as previous version to save space)
    # Ensure to include the `render_html_report_v13` function from your previous `main.py` here.
    
    # Placeholder for the function body provided in previous steps
    news_html = ""
    seen_titles = set()
    unique_news = []
    for n in all_news:
        if n['title'] not in seen_titles:
            unique_news.append(n)
            seen_titles.add(n['title'])
    
    unique_news.sort(key=lambda x: (not ('重磅' in x['title'] or '突发' in x['title']), x.get('time', '')), reverse=True)

    for i, news in enumerate(unique_news[:15]):
        color = "#ffb74d" if ('重磅' in news['title'] or '突发' in news['title']) else "#999"
        news_html += f"""
        <div style="font-size:11px;color:#ccc;margin-bottom:5px;border-bottom:1px dashed #333;padding-bottom:3px;">
            <span style="color:{color};margin-right:4px;">●</span>{news['title']}
            <span style="float:right;color:#666;font-size:10px;">{news.get('time','')}</span>
        </div>
        """

    def render_dots(hist):
        h = ""
        for x in hist:
            c = "#d32f2f" if x['s']=='B' else ("#388e3c" if x['s'] in ['S','C'] else "#555")
            h += f'<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:{c};margin-right:3px;box-shadow:0 0 2px rgba(0,0,0,0.5);" title="{x["date"]}"></span>'
        return h

    rows = ""
    for r in results:
        try:
            tech = r.get('tech', {})
            risk = tech.get('risk_factors', {})
            final_score = tech.get('final_score', 0)
            ai_adj = tech.get('ai_adjustment', 0)
            base_score = final_score - ai_adj 
            
            cro_signal = tech.get('tech_cro_signal', 'PASS')
            cro_comment = tech.get('tech_cro_comment', '无')
            cro_style = "color:#66bb6a;font-weight:bold;"
            if cro_signal == "VETO": cro_style = "color:#ef5350;font-weight:bold;"
            elif cro_signal == "WARN": cro_style = "color:#ffb74d;font-weight:bold;"

            cro_border_color = '#66bb6a' if cro_signal=='PASS' else '#ef5350'
            obv_text = '流入' if tech.get('flow',{}).get('obv_slope',0) > 0 else '流出'

            profit_html = ""
            pos_cost = r.get('pos_cost', 0.0)
            pos_shares = r.get('pos_shares', 0)
            current_price = tech.get('price', 0.0)
            if pos_shares > 0 and pos_cost > 0 and current_price > 0:
                profit_pct = (current_price - pos_cost) / pos_cost * 100
                profit_val = (current_price - pos_cost) * pos_shares
                p_color = "#ff5252" if profit_val > 0 else "#69f0ae" 
                profit_html = f"""<div style="font-size:12px;margin-bottom:8px;background:rgba(255,255,255,0.05);padding:4px 8px;border-radius:3px;display:flex;justify-content:space-between;"><span style="color:#aaa;">持有收益:</span><span style="color:{p_color};font-weight:bold;">{profit_val:+.1f}元 ({profit_pct:+.2f}%)</span></div>"""
            
            if r['amount'] > 0: 
                border_color = "#d32f2f"; bg_gradient = "linear-gradient(90deg, rgba(60,10,10,0.9) 0%, rgba(20,20,20,0.95) 100%)"; act_html = f"<span style='color:#ff8a80;font-weight:bold'>+{r['amount']:,}</span>"
            elif r.get('is_sell'): 
                border_color = "#388e3c"; bg_gradient = "linear-gradient(90deg, rgba(10,40,10,0.9) 0%, rgba(20,20,20,0.95) 100%)"; act_html = f"<span style='color:#a5d6a7;font-weight:bold'>-{int(r.get('sell_value',0)):,}</span>"
            else: 
                border_color = "#555"; bg_gradient = "linear-gradient(90deg, rgba(30,30,30,0.9) 0%, rgba(15,15,15,0.95) 100%)"; act_html = "<span style='color:#777'>HOLD</span>"
            
            reasons = " ".join([f"<span style='border:1px solid #555;padding:0 3px;font-size:9px;border-radius:2px;color:#888;'>{x}</span>" for x in tech.get('quant_reasons', [])])
            val_desc = tech.get('valuation_desc', 'N/A')
            val_style = "color:#ffb74d;font-weight:bold;" if "低估" in val_desc else ("color:#ef5350;font-weight:bold;" if "高估" in val_desc else "color:#bdbdbd;")

            committee_html = ""
            ai_data = r.get('ai_analysis', {})
            bull_say = ai_data.get('bull_say')
            bear_say = ai_data.get('bear_say')
            chairman = ai_data.get('comment', '无')

            if bull_say and bear_say:
                adj_color = "#ff5252" if ai_adj > 0 else ("#69f0ae" if ai_adj < 0 else "#ccc")
                committee_html = f"""
                <div style="margin-top:12px;border-top:1px solid #444;padding-top:10px;">
                    <div style="font-size:10px;color:#888;margin-bottom:6px;text-align:center;">--- 联邦投委会辩论 ---</div>
                    <div style="display:flex;gap:10px;margin-bottom:8px;">
                        <div style="flex:1;background:rgba(27,94,32,0.2);padding:8px;border-radius:4px;border-left:2px solid #66bb6a;"><div style="color:#66bb6a;font-size:11px;font-weight:bold;margin-bottom:4px;">🦊 CGO (增长)</div><div style="color:#c8e6c9;font-size:11px;line-height:1.3;font-style:italic;">"{bull_say}"</div></div>
                        <div style="flex:1;background:rgba(183,28,28,0.2);padding:8px;border-radius:4px;border-left:2px solid #ef5350;"><div style="color:#ef5350;font-size:11px;font-weight:bold;margin-bottom:4px;">🐻 CRO (风控)</div><div style="color:#ffcdd2;font-size:11px;line-height:1.3;font-style:italic;">"{bear_say}"</div></div>
                    </div>
                    <div style="background:linear-gradient(90deg, rgba(255,183,77,0.1) 0%, rgba(255,183,77,0.05) 100%);padding:10px;border-radius:4px;border:1px solid rgba(255,183,77,0.3);position:relative;">
                        <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
                            <div style="color:#ffb74d;font-size:12px;font-weight:bold;">⚖️ CIO 终审</div>
                            <div style="color:{adj_color};font-size:11px;font-weight:bold;">策略修正: {ai_adj:+d}</div>
                        </div>
                        <div style="color:#fff3e0;font-size:12px;line-height:1.4;">{chairman}</div>
                    </div>
                </div>"""

            vol_ratio = risk.get('vol_ratio', 1.0)
            vol_style = "color:#ffb74d;" if vol_ratio < 0.8 else ("color:#ff8a80;" if vol_ratio > 2.0 else "color:#bbb;")

            rows += f"""
            <div style="background:{bg_gradient};border-left:4px solid {border_color};margin-bottom:15px;padding:15px;border-radius:6px;box-shadow:0 4px 10px rgba(0,0,0,0.6);border-top:1px solid #333;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
                    <div><span style="font-size:18px;font-weight:bold;color:#f0e6d2;font-family:'Times New Roman',serif;">{r['name']}</span><span style="font-size:12px;color:#9ca3af;margin-left:5px;">{r['code']}</span></div>
                    <div style="text-align:right;">
                        <div style="color:#ffb74d;font-weight:bold;font-size:16px;text-shadow:0 0 5px rgba(255,183,77,0.3);">{final_score}</div>
                        <div style="font-size:9px;color:#aaa;">BASE:{base_score} <span style="color:{'#ff5252' if ai_adj>0 else ('#69f0ae' if ai_adj<0 else '#777')}">{ai_adj:+d}</span></div>
                    </div>
                </div>
                
                <div style="background:rgba(0,0,0,0.3);padding:6px 10px;border-radius:4px;margin-bottom:10px;display:flex;align-items:center;border-left:2px solid {cro_border_color};">
                    <span style="font-size:11px;color:#aaa;margin-right:8px;">🛡️ 技术风控:</span>
                    <span style="font-size:11px;{cro_style}">{cro_comment}</span>
                </div>

                <div style="display:flex;justify-content:space-between;color:#e0e0e0;font-size:15px;margin-bottom:5px;border-bottom:1px solid #444;padding-bottom:5px;">
                    <span style="font-weight:bold;color:#ffb74d;">{r.get('position_type')}</span><span style="font-family:'Courier New',monospace;">{act_html}</span>
                </div>
                {profit_html}
                <div style="font-size:11px;margin-bottom:8px;border-bottom:1px dashed #333;padding-bottom:5px;"><span style="color:#888;">周期定位:</span> <span style="{val_style}">{val_desc}</span></div>
                <div style="display:grid;grid-template-columns:repeat(4, 1fr);gap:5px;font-size:11px;color:#bdbdbd;font-family:'Courier New',monospace;margin-bottom:4px;">
                    <span>RSI: {tech.get('rsi','-')}</span><span>MACD: {tech.get('macd',{}).get('trend','-')}</span><span>OBV: {obv_text}</span><span>Wkly: {tech.get('trend_weekly','-')}</span>
                </div>
                <div style="display:grid;grid-template-columns:repeat(3, 1fr);gap:5px;font-size:11px;color:#bdbdbd;font-family:'Courier New',monospace;margin-bottom:8px;">
                    <span style="{vol_style}">VR: {vol_ratio}</span><span>Div: {risk.get('divergence','无')}</span><span>%B: {risk.get('bollinger_pct_b',0.5)}</span>
                </div>
                <div style="margin-bottom:8px;">{reasons}</div>
                <div style="margin-top:5px;">{render_dots(r.get('history',[]))}</div>
                {committee_html}
            </div>
            """
        except Exception as e:
            logger.error(f"Render Error {r.get('name')}: {e}")

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
        body {{ background: #0a0a0a; color: #f0e6d2; font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; max-width: 660px; margin: 0 auto; padding: 20px; }}
        .main-container {{ border: 2px solid #333; border-top: 5px solid #ffb74d; border-radius: 4px; padding: 20px; background: linear-gradient(180deg, #1b1b1b 0%, #000000 100%); }}
        .header {{ text-align: center; border-bottom: 2px solid #333; padding-bottom: 20px; margin-bottom: 25px; }}
        .title {{ color: #ffb74d; margin: 0; font-size: 32px; font-weight: 800; font-family: 'Times New Roman', serif; letter-spacing: 2px; }}
        .subtitle {{ font-size: 11px; color: #888; margin-top: 8px; text-transform: uppercase; }}
        
        .radar-panel {{ background: #111; border: 1px solid #333; border-radius: 4px; padding: 15px; margin-bottom: 25px; }}
        .radar-title {{ font-size: 14px; color: #ffb74d; font-weight: bold; margin-bottom: 12px; border-bottom: 1px solid #444; padding-bottom: 6px; letter-spacing: 1px; }}

        .cio-section {{ 
            background: linear-gradient(145deg, #1a0505, #2b0b0b); 
            border: 1px solid #5c1818; 
            border-left: 4px solid #d32f2f; 
            padding: 20px; 
            margin-bottom: 20px; 
            border-radius: 2px; 
            box-shadow: 0 4px 10px rgba(0,0,0,0.3);
        }}
        .cio-section p, .cio-section div, .cio-section h3 {{ color: #ffffff !important; line-height: 1.6; }}
        .cio-section h3 {{ color: #ffffff !important; border-bottom: 1px dashed #5c1818; padding-bottom: 5px; margin-top: 15px; margin-bottom: 8px; }}

        .advisor-section {{ 
            background: #0f0f0f; 
            border: 1px solid #d4af37; 
            border-left: 4px solid #ffd700; 
            padding: 20px; 
            margin-bottom: 30px; 
            border-radius: 4px; 
            box-shadow: 0 0 10px rgba(212, 175, 55, 0.2); 
            position: relative;
        }}
        .advisor-section * {{ color: #ffffff !important; line-height: 1.6; font-family: 'Georgia', serif; }}
        .advisor-section h4 {{ color: #ffd700 !important; margin-top: 15px; margin-bottom: 8px; border-bottom: 1px dashed #333; padding-bottom: 4px; }}

        .section-title {{ font-size: 16px; font-weight: bold; margin-bottom: 15px; color: #eee; text-transform: uppercase; letter-spacing: 1px; text-shadow: 0 1px 2px rgba(0,0,0,0.8); }}
        .footer {{ text-align: center; font-size: 10px; color: #444; margin-top: 40px; }}
    </style></head><body>
        <div class="main-container">
            <div class="header">
                <h1 class="title">XUANTIE QUANT</h1>
                <div class="subtitle">HEAVY SWORD, NO EDGE | V15.6 IRON FIST</div>
            </div>
            
            <div class="radar-panel">
                <div class="radar-title">📡 7x24 GLOBAL LIVE WIRE</div>
                {news_html}
            </div>

            <div class="cio-section">
                <div class="section-title">🛑 CIO 战略审计</div>
                {cio_html}
            </div>

            <div class="advisor-section">
                <div class="section-title" style="color: #ffd700;">🗡️ 玄铁先生·场外实战复盘</div>
                {advisor_html}
            </div>

            {rows}
            <div class="footer">EST. 2026 | POWERED BY AKSHARE & EM | V15.6</div>
        </div>
    </body></html>"""

def process_single_fund(fund, config, fetcher, scanner, tracker, val_engine, analyst, macro_str, base_amt, max_daily):
    res = None
    cio_log = ""
    used_news = []
    
    try:
        logger.info(f"Analyzing {fund['name']}...")
        
        data = fetcher.get_fund_history(fund['code'])
        if data is None or data.empty: 
            logger.warning(f"⚠️ No data for {fund['name']}")
            return None, "", []

        tech = TechnicalAnalyzer.calculate_indicators(data)
        if not tech: return None, "", []
        
        logger.info(f"📊 [Hard Data {fund['name']}] RSI:{tech.get('rsi')} | VR:{tech.get('risk_factors',{}).get('vol_ratio')}")

        try:
            val_mult, val_desc = val_engine.get_valuation_status(fund.get('index_name'), fund.get('strategy_type'))
        except:
            val_mult, val_desc = 1.0, "估值异常"

        with tracker_lock: pos = tracker.get_position(fund['code'])

        ai_adj = 0; ai_res = {}
        keyword = fund.get('sector_keyword', fund['name']) 
        
        # [V15.6 Logic] Force AI analysis if DEBUG_MODE is True
        should_run_ai = (
            pos['shares'] > 0 
            or tech['quant_score'] >= 60 
            or tech['quant_score'] <= 35 
            or DEBUG_MODE  # [Modified] Force run in debug mode
        )

        if analyst and should_run_ai:
            sector_news_list = analyst.fetch_news_titles(keyword)
            logger.info(f"📰 [News Snapshot {fund['name']}] Hit: {len(sector_news_list)}")
            
            cro_signal = tech.get('tech_cro_signal', 'PASS')
            fuse_level = 0
            if cro_signal == 'VETO': fuse_level = 3
            elif cro_signal == 'WARN': fuse_level = 1
            
            risk_payload = {
                "fuse_level": fuse_level,
                "risk_msg": tech.get('tech_cro_comment', '常规监控')
            }
            
            try:
                ai_res = analyst.analyze_fund_v5(fund['name'], tech, macro_str, sector_news_list, risk_payload)
                ai_adj = ai_res.get('adjustment', 0)
            except Exception as ai_e:
                logger.error(f"❌ AI Analysis Failed for {fund['name']}: {ai_e}")
                # Fallback to prevent crash, but log error
                ai_res = {"bull_say": "System Error", "bear_say": "Check Logs", "comment": "AI Offline", "adjustment": 0}
            
            for n_str in sector_news_list:
                if "]" in n_str:
                    t_part, title_part = n_str.split("]", 1)
                    used_news.append({"title": title_part.strip(), "time": t_part.replace("[", "").strip()})
                else:
                    used_news.append({"title": n_str, "time": ""})

        amt, lbl, is_sell, s_val = calculate_position_v13(
            tech, ai_adj, val_mult, val_desc, base_amt, max_daily, pos, fund.get('strategy_type'), fund['name']
        )
        
        with tracker_lock:
            tracker.record_signal(fund['code'], lbl)
            if amt > 0: tracker.add_trade(fund['code'], fund['name'], amt, tech['price'])
            elif is_sell: tracker.add_trade(fund['code'], fund['name'], s_val, tech['price'], True)

        bull = ai_res.get('bull_say', '无')
        bear = ai_res.get('bear_say', '无')
        cro_tech = tech.get('tech_cro_comment', '无')
        
        cio_log = f"""
【{fund['name']}】: {lbl}
- 算分: 基础{tech.get('quant_score')} + CIO修正{ai_adj:+d} = {tech.get('final_score')}
- 风控: {cro_tech}
- 辩论: 多方<{bull}> vs 空方<{bear}>
"""
        res = {
            "name": fund['name'], "code": fund['code'], 
            "amount": amt, "sell_value": s_val, "position_type": lbl, "is_sell": is_sell, 
            "tech": tech, "ai_analysis": ai_res, "history": tracker.get_signal_history(fund['code']),
            "pos_cost": pos.get('cost', 0), "pos_shares": pos.get('shares', 0)
        }
    except Exception as e:
        logger.error(f"Process Error {fund['name']}: {e}")
        if DEBUG_MODE: logger.exception(e) # Print full stack trace in debug
        return None, "", []
    return res, cio_log, used_news

def main():
    config = load_config()
    fetcher = DataFetcher()
    scanner = MarketScanner()
    tracker = PortfolioTracker()
    val_engine = ValuationEngine()
    
    logger.info(f">>> [V15.6] Startup | DEBUG_MODE={DEBUG_MODE} | Models Ready")
    tracker.confirm_trades()
    try: analyst = NewsAnalyst()
    except: analyst = None

    macro_news_list = scanner.get_macro_news()
    macro_str = " | ".join([n['title'] for n in macro_news_list])
    
    all_news_seen = []
    for n in macro_news_list:
        all_news_seen.append(n)

    results = []; cio_lines = [f"【宏观环境】: {macro_str}\n"]
    
    # Increase workers to 5 for speed, relying on V3.2 concurrency
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_fund = {executor.submit(
            process_single_fund, 
            fund, config, fetcher, scanner, tracker, val_engine, analyst, macro_str, 
            config['global']['base_invest_amount'], config['global']['max_daily_invest']
        ): fund for fund in config.get('funds', [])}
        
        for future in as_completed(future_to_fund):
            try:
                res, log, fund_news = future.result()
                if res: 
                    results.append(res)
                    cio_lines.append(log)
                    all_news_seen.extend(fund_news)
            except Exception as e: logger.error(f"Thread Error: {e}")

    if results:
        results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        full_report = "\n".join(cio_lines)
        
        cio_html = analyst.review_report(full_report) if analyst else "<p>CIO Missing</p>"
        advisor_html = analyst.advisor_review(full_report, macro_str) if analyst else "<p>Advisor Offline</p>"
        
        html = render_html_report_v13(all_news_seen, results, cio_html, advisor_html) 
        send_email("🗡️ 玄铁量化 V15.6 铁拳决议", html) 

if __name__ == "__main__": main()
