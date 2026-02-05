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

tracker_lock = threading.Lock()

def load_config():
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"配置文件读取失败: {e}")
        return {"funds": [], "global": {"base_invest_amount": 1000, "max_daily_invest": 5000}}

# [核心决策逻辑]
def calculate_position_v13(tech, ai_adj, val_mult, val_desc, base_amt, max_daily, pos, strategy_type):
    base_score = tech.get('quant_score', 50)
    tactical_score = max(0, min(100, base_score + ai_adj))
    
    tech['final_score'] = tactical_score
    tech['ai_adjustment'] = ai_adj
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

    if cro_signal == "VETO":
        if final_mult > 0:
            final_mult = 0
            reasons.append(f"🛡️风控:否决买入")
    
    held_days = pos.get('held_days', 999)
    if final_mult < 0 and pos['shares'] > 0 and held_days < 7:
        final_mult = 0; reasons.append(f"规则:锁仓({held_days}天)")

    final_amt = 0; is_sell = False; sell_val = 0; label = "观望"

    if final_mult > 0:
        amt = int(base_amt * final_mult)
        final_amt = max(0, min(amt, int(max_daily)))
        label = "买入"
    elif final_mult < 0:
        is_sell = True
        sell_ratio = min(abs(final_mult), 1.0)
        sell_val = pos['shares'] * tech.get('price', 0) * sell_ratio
        label = "卖出"

    if reasons: tech['quant_reasons'] = reasons
    return final_amt, label, is_sell, sell_val

# [UI 渲染]
def render_html_report_v13(all_news, results, cio_html, advisor_html):
    news_html = ""
    seen_titles = set()
    unique_news = []
    for n in all_news:
        if n['title'] not in seen_titles:
            unique_news.append(n)
            seen_titles.add(n['title'])
    
    # 排序：重磅优先，然后按时间倒序
    unique_news.sort(key=lambda x: (not ('重磅' in x['title'] or '突发' in x['title']), x.get('time', '')), reverse=True)

    for i, news in enumerate(unique_news[:15]):
        color = "#ffb74d" if ('重磅' in news['title'] or '突发' in news['title']) else "#bdbdbd"
        news_html += f"""
        <div style="font-size:11px;color:#eee;margin-bottom:4px;border-bottom:1px dashed #333;padding-bottom:2px;">
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
            
            cro_signal = tech.get('tech_cro_signal', 'PASS')
            cro_comment = tech.get('tech_cro_comment', '无')
            cro_style = "color:#66bb6a;font-weight:bold;"
            if cro_signal == "VETO": cro_style = "color:#ef5350;font-weight:bold;"
            elif cro_signal == "WARN": cro_style = "color:#ffb74d;font-weight:bold;"

            # [修复] 提取变量，避免 f-string 语法错误
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
                committee_html = f"""
                <div style="margin-top:12px;border-top:1px solid #444;padding-top:10px;">
                    <div style="font-size:10px;color:#888;margin-bottom:6px;text-align:center;">--- 投委会辩论实录 ---</div>
                    <div style="display:flex;gap:10px;margin-bottom:8px;">
                        <div style="flex:1;background:rgba(27,94,32,0.2);padding:8px;border-radius:4px;border-left:2px solid #66bb6a;"><div style="color:#66bb6a;font-size:11px;font-weight:bold;margin-bottom:4px;">🦊 CGO</div><div style="color:#c8e6c9;font-size:11px;line-height:1.3;font-style:italic;">"{bull_say}"</div></div>
                        <div style="flex:1;background:rgba(183,28,28,0.2);padding:8px;border-radius:4px;border-left:2px solid #ef5350;"><div style="color:#ef5350;font-size:11px;font-weight:bold;margin-bottom:4px;">🐻 CRO</div><div style="color:#ffcdd2;font-size:11px;line-height:1.3;font-style:italic;">"{bear_say}"</div></div>
                    </div>
                    <div style="background:linear-gradient(90deg, rgba(255,183,77,0.1) 0%, rgba(255,183,77,0.05) 100%);padding:10px;border-radius:4px;border:1px solid rgba(255,183,77,0.3);position:relative;">
                        <div style="color:#ffb74d;font-size:12px;font-weight:bold;margin-bottom:4px;">⚖️ 主席裁决</div><div style="color:#fff3e0;font-size:12px;line-height:1.4;">{chairman}</div>
                    </div>
                </div>"""

            vol_ratio = risk.get('vol_ratio', 1.0)
            vol_style = "color:#ffb74d;" if vol_ratio < 0.8 else ("color:#ff8a80;" if vol_ratio > 2.0 else "color:#bbb;")

            rows += f"""
            <div style="background:{bg_gradient};border-left:4px solid {border_color};margin-bottom:15px;padding:15px;border-radius:6px;box-shadow:0 4px 10px rgba(0,0,0,0.6);border-top:1px solid #333;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
                    <div><span style="font-size:18px;font-weight:bold;color:#f0e6d2;font-family:'Times New Roman',serif;">{r['name']}</span><span style="font-size:12px;color:#9ca3af;margin-left:5px;">{r['code']}</span></div>
                    <div style="text-align:right;"><div style="color:#ffb74d;font-weight:bold;font-size:16px;text-shadow:0 0 5px rgba(255,183,77,0.3);">{final_score}</div><div style="font-size:9px;color:#666;">COMMITTEE SCORE</div></div>
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
            logger.error(f"渲染错误 {r.get('name')}: {e}")

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
        body {{ background: #0a0a0a; color: #f0e6d2; font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; max-width: 660px; margin: 0 auto; padding: 20px; }}
        .main-container {{ border: 2px solid #333; border-top: 5px solid #ffb74d; border-radius: 4px; padding: 20px; background: linear-gradient(180deg, #1b1b1b 0%, #000000 100%); }}
        .header {{ text-align: center; border-bottom: 2px solid #333; padding-bottom: 20px; margin-bottom: 25px; }}
        .title {{ color: #ffb74d; margin: 0; font-size: 32px; font-weight: 800; font-family: 'Times New Roman', serif; letter-spacing: 2px; }}
        .subtitle {{ font-size: 11px; color: #888; margin-top: 8px; text-transform: uppercase; }}
        
        .radar-panel {{ background: #1a1a1a; border: 1px solid #333; border-radius: 4px; padding: 15px; margin-bottom: 20px; }}
        .radar-title {{ font-size: 14px; color: #ffb74d; font-weight: bold; margin-bottom: 10px; border-bottom: 1px solid #444; padding-bottom: 5px; }}

        .cio-section, .advisor-section {{ background: #111; border: 1px solid #333; border-left: 4px solid #5d4037; padding: 20px; margin-bottom: 20px; border-radius: 2px; }}
        .cio-section {{ border-left-color: #d32f2f; }}
        .advisor-section {{ border-left-color: #ffb74d; }}
        
        .section-title {{ font-size: 16px; font-weight: bold; margin-bottom: 15px; color: #eee; text-transform: uppercase; letter-spacing: 1px; }}
        .footer {{ text-align: center; font-size: 10px; color: #444; margin-top: 40px; }}
    </style></head><body>
        <div class="main-container">
            <div class="header">
                <h1 class="title">XUANTIE QUANT</h1>
                <div class="subtitle">HEAVY SWORD, NO EDGE | V14.16 FINAL</div>
            </div>
            
            <div class="radar-panel">
                <div class="radar-title">📡 全景情报雷达 (AI Vision)</div>
                {news_html}
            </div>

            <div class="cio-section">
                <div class="section-title">🛑 CIO 战略审计</div>
                {cio_html}
            </div>

            <div class="advisor-section">
                <div class="section-title">🗡️ 玄铁先生·实战复盘</div>
                {advisor_html}
            </div>

            {rows}
            <div class="footer">EST. 2026 | POWERED BY CAILIAN & JINSHI DATA</div>
        </div>
    </body></html>"""

def process_single_fund(fund, config, fetcher, scanner, tracker, val_engine, analyst, macro_str, base_amt, max_daily):
    res = None
    cio_log = ""
    used_news = []
    
    try:
        time.sleep(random.uniform(1.0, 3.0)) 
        logger.info(f"Analyzing {fund['name']}...")
        
        data = fetcher.get_fund_history(fund['code'])
        if data is None or data.empty: return None, "", []

        tech = TechnicalAnalyzer.calculate_indicators(data)
        if not tech: return None, "", []

        try:
            val_mult, val_desc = val_engine.get_valuation_status(fund.get('index_name'), fund.get('strategy_type'))
        except:
            val_mult, val_desc = 1.0, "估值异常"

        with tracker_lock: pos = tracker.get_position(fund['code'])

        ai_adj = 0; ai_res = {}
        keyword = fund.get('sector_keyword', fund['name']) 
        
        if analyst and (pos['shares']>0 or tech['quant_score']>=60 or tech['quant_score']<=35):
            sector_news_list = analyst.fetch_news_titles(keyword)
            ai_res = analyst.analyze_fund_v4(fund['name'], tech, macro_str, sector_news_list)
            ai_adj = ai_res.get('adjustment', 0)
            
            for n_str in sector_news_list:
                if "]" in n_str:
                    t_part, title_part = n_str.split("]", 1)
                    used_news.append({"title": title_part.strip(), "time": t_part.replace("[", "").strip()})
                else:
                    used_news.append({"title": n_str, "time": ""})

        amt, lbl, is_sell, s_val = calculate_position_v13(tech, ai_adj, val_mult, val_desc, base_amt, max_daily, pos, fund.get('strategy_type'))
        
        with tracker_lock:
            tracker.record_signal(fund['code'], lbl)
            if amt > 0: tracker.add_trade(fund['code'], fund['name'], amt, tech['price'])
            elif is_sell: tracker.add_trade(fund['code'], fund['name'], s_val, tech['price'], True)

        bull = ai_res.get('bull_say', '无')
        bear = ai_res.get('bear_say', '无')
        cro_tech = tech.get('tech_cro_comment', '无')
        
        cio_log = f"""
【{fund['name']}】: {lbl}
- 状态: 评分{tech.get('quant_score')}, {val_desc}, 投委会调整{ai_adj:+d}
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
        logger.error(f"处理错误 {fund['name']}: {e}")
        return None, "", []
    return res, cio_log, used_news

def main():
    config = load_config()
    fetcher = DataFetcher()
    scanner = MarketScanner()
    tracker = PortfolioTracker()
    val_engine = ValuationEngine()
    
    logger.info(">>> [V14.16] 启动玄铁量化 (Syntax Fix)...")
    tracker.confirm_trades()
    try: analyst = NewsAnalyst()
    except: analyst = None

    macro_news_list = scanner.get_macro_news()
    macro_str = " | ".join([n['title'] for n in macro_news_list])
    
    all_news_seen = []
    for n in macro_news_list:
        all_news_seen.append(n)

    results = []; cio_lines = [f"【宏观环境】: {macro_str}\n"]
    
    with ThreadPoolExecutor(max_workers=3) as executor:
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
            except Exception as e: logger.error(f"线程异常: {e}")

    if results:
        results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        full_report = "\n".join(cio_lines)
        
        cio_html = analyst.review_report(full_report) if analyst else "<p>CIO 缺席</p>"
        advisor_html = analyst.advisor_review(full_report, macro_str) if analyst else "<p>玄铁先生闭关中</p>"
        
        html = render_html_report_v13(all_news_seen, results, cio_html, advisor_html) 
        send_email("🗡️ 玄铁量化 V14.16 最终决议", html)

if __name__ == "__main__": main()
