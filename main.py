import yaml
import os
import threading
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from technical_analyzer import TechnicalAnalyzer
from valuation_engine import ValuationEngine
from portfolio_tracker import PortfolioTracker
from utils import send_email, logger, LOG_FILENAME

# --- 全局配置 ---
DEBUG_MODE = True  
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
    V13 核心算分逻辑 (含 CIO 一票否决权)
    """
    base_score = tech.get('quant_score', 50)
    
    if DEBUG_MODE:
        logger.info(f"🔍 [DEBUG] {fund_name} 基础分细节: {tech.get('quant_reasons', [])}")

    # 1. 初始计算：基础分 + AI微调
    tactical_score = max(0, min(100, base_score + ai_adj))
    action_str = "加分进攻" if ai_adj > 0 else ("减分防御" if ai_adj < 0 else "中性维持")
    logger.info(f"🧮 [算分 {fund_name}] 技术面({base_score}) + CIO修正({ai_adj:+d} {action_str}) = 初步分({tactical_score})")
    
    # 2. [核心逻辑] CIO 一票否决权 (Override)
    # 解决"口嫌体正直"问题：如果 AI 说 HOLD/REJECT，强制覆盖量化分数
    override_reason = ""
    original_score = tactical_score
    
    if ai_decision == "REJECT":
        # 否决：直接归零，强制空仓或卖出信号
        tactical_score = 0 
        override_reason = "⛔ CIO指令:REJECT (强制否决)"
    elif ai_decision == "HOLD":
        # 观望：如果分数在买入区(>=60)，强制压回观望区(59)
        if tactical_score >= 60:
            tactical_score = 59
            override_reason = "⏸️ CIO指令:HOLD (强制观望)"
            
    if override_reason:
        logger.warning(f"⚠️ [CIO介入 {fund_name}] 原分{original_score} -> {override_reason} -> 修正后: {tactical_score}")

    # 3. 记录最终状态
    tech['final_score'] = tactical_score
    tech['ai_adjustment'] = ai_adj
    tech['valuation_desc'] = val_desc
    cro_signal = tech.get('tech_cro_signal', 'PASS')
    
    tactical_mult = 0
    reasons = []

    # 4. 根据修正后的分数定档
    if tactical_score >= 85: tactical_mult = 2.0; reasons.append("战术:极强")
    elif tactical_score >= 70: tactical_mult = 1.0; reasons.append("战术:走强")
    elif tactical_score >= 60: tactical_mult = 0.5; reasons.append("战术:企稳")
    elif tactical_score <= 25: tactical_mult = -1.0; reasons.append("战术:破位")

    # 5. 结合估值系数
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

    # 6. 传统技术风控 (CRO)
    if cro_signal == "VETO":
        if final_mult > 0:
            final_mult = 0
            reasons.append(f"🛡️风控:否决买入")
            logger.info(f"🚫 [风控拦截 {fund_name}] 触发: {tech.get('tech_cro_comment')}")
    
    # 7. 锁仓规则
    held_days = pos.get('held_days', 999)
    if final_mult < 0 and pos['shares'] > 0 and held_days < 7:
        final_mult = 0; reasons.append(f"规则:锁仓({held_days}天)")

    # 8. 计算最终金额
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

def render_html_report_v13(all_news, results, cio_html, advisor_html):
    """
    生成完整的 HTML 邮件报告 (精准修复字体颜色，保持复古风格)
    """
    news_html = ""
    if isinstance(all_news, list):
        for i, news in enumerate(all_news[:15]):
            title = news.get('title', str(news))
            time_str = news.get('time', '')
            news_html += f"""<div style="font-size:11px;color:#ccc;margin-bottom:5px;border-bottom:1px dashed #333;padding-bottom:3px;"><span style="color:#ffb74d;margin-right:4px;">●</span>{title}<span style="float:right;color:#666;font-size:10px;">{time_str}</span></div>"""
    
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
            cro_border_color = '#66bb6a'
            if cro_signal == "VETO": 
                cro_style = "color:#ef5350;font-weight:bold;"
                cro_border_color = '#ef5350'
            elif cro_signal == "WARN": 
                cro_style = "color:#ffb74d;font-weight:bold;"
            
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
                border_color = "#d32f2f"
                bg_gradient = "linear-gradient(90deg, rgba(60,10,10,0.9) 0%, rgba(20,20,20,0.95) 100%)"
                act_html = f"<span style='color:#ff8a80;font-weight:bold'>+{r['amount']:,}</span>"
            elif r.get('is_sell'): 
                border_color = "#388e3c"
                bg_gradient = "linear-gradient(90deg, rgba(10,40,10,0.9) 0%, rgba(20,20,20,0.95) 100%)"
                act_html = f"<span style='color:#a5d6a7;font-weight:bold'>-{int(r.get('sell_value',0)):,}</span>"
            else: 
                border_color = "#555"
                bg_gradient = "linear-gradient(90deg, rgba(30,30,30,0.9) 0%, rgba(15,15,15,0.95) 100%)"
                act_html = "<span style='color:#777'>HOLD</span>"
            
            reasons = " ".join([f"<span style='border:1px solid #555;padding:0 3px;font-size:9px;border-radius:2px;color:#888;'>{x}</span>" for x in tech.get('quant_reasons', [])])
            val_desc = tech.get('valuation_desc', 'N/A')
            val_style = "color:#ffb74d;font-weight:bold;" if "低估" in val_desc else ("color:#ef5350;font-weight:bold;" if "高估" in val_desc else "color:#bdbdbd;")
            
            committee_html = ""
            ai_data = r.get('ai_analysis', {})
            bull_say = ai_data.get('bull_view', '无')
            bear_say = ai_data.get('bear_view', '无')
            chairman = ai_data.get('chairman_conclusion') or ai_data.get('comment', '无')
            
            if bull_say != '无':
                adj_color = "#ff5252" if ai_adj > 0 else ("#69f0ae" if ai_adj < 0 else "#ccc")
                committee_html = f"""<div style="margin-top:12px;border-top:1px solid #444;padding-top:10px;"><div style="font-size:10px;color:#888;margin-bottom:6px;text-align:center;">--- 联邦投委会辩论 ---</div><div style="display:flex;gap:10px;margin-bottom:8px;"><div style="flex:1;background:rgba(27,94,32,0.2);padding:8px;border-radius:4px;border-left:2px solid #66bb6a;"><div style="color:#66bb6a;font-size:11px;font-weight:bold;margin-bottom:4px;">🦊 CGO (增长)</div><div style="color:#c8e6c9;font-size:11px;line-height:1.3;font-style:italic;">"{bull_say}"</div></div><div style="flex:1;background:rgba(183,28,28,0.2);padding:8px;border-radius:4px;border-left:2px solid #ef5350;"><div style="color:#ef5350;font-size:11px;font-weight:bold;margin-bottom:4px;">🐻 CRO (风控)</div><div style="color:#ffcdd2;font-size:11px;line-height:1.3;font-style:italic;">"{bear_say}"</div></div></div><div style="background:linear-gradient(90deg, rgba(255,183,77,0.1) 0%, rgba(255,183,77,0.05) 100%);padding:10px;border-radius:4px;border:1px solid rgba(255,183,77,0.3);position:relative;"><div style="display:flex;justify-content:space-between;margin-bottom:4px;"><div style="color:#ffb74d;font-size:12px;font-weight:bold;">⚖️ CIO 终审</div><div style="color:{adj_color};font-size:11px;font-weight:bold;">策略修正: {ai_adj:+d}</div></div><div style="color:#fff3e0;font-size:12px;line-height:1.4;">{chairman}</div></div></div>"""
            
            vol_ratio = risk.get('vol_ratio', 1.0)
            vol_style = "color:#ffb74d;" if vol_ratio < 0.8 else ("color:#ff8a80;" if vol_ratio > 2.0 else "color:#bbb;")
            
            rows += f"""<div style="background:{bg_gradient};border-left:4px solid {border_color};margin-bottom:15px;padding:15px;border-radius:6px;box-shadow:0 4px 10px rgba(0,0,0,0.6);border-top:1px solid #333;"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;"><div><span style="font-size:18px;font-weight:bold;color:#f0e6d2;font-family:'Times New Roman',serif;">{r['name']}</span><span style="font-size:12px;color:#9ca3af;margin-left:5px;">{r['code']}</span></div><div style="text-align:right;"><div style="color:#ffb74d;font-weight:bold;font-size:16px;text-shadow:0 0 5px rgba(255,183,77,0.3);">{final_score}</div><div style="font-size:9px;color:#aaa;">BASE:{base_score} <span style="color:{'#ff5252' if ai_adj>0 else ('#69f0ae' if ai_adj<0 else '#777')}">{ai_adj:+d}</span></div></div></div><div style="background:rgba(0,0,0,0.3);padding:6px 10px;border-radius:4px;margin-bottom:10px;display:flex;align-items:center;border-left:2px solid {cro_border_color};"><span style="font-size:11px;color:#aaa;margin-right:8px;">🛡️ 技术风控:</span><span style="font-size:11px;{cro_style}">{cro_comment}</span></div><div style="display:flex;justify-content:space-between;color:#e0e0e0;font-size:15px;margin-bottom:5px;border-bottom:1px solid #444;padding-bottom:5px;"><span style="font-weight:bold;color:#ffb74d;">{r.get('position_type')}</span><span style="font-family:'Courier New',monospace;">{act_html}</span></div>{profit_html}<div style="font-size:11px;margin-bottom:8px;border-bottom:1px dashed #333;padding-bottom:5px;"><span style="color:#888;">周期定位:</span> <span style="{val_style}">{val_desc}</span></div><div style="display:grid;grid-template-columns:repeat(4, 1fr);gap:5px;font-size:11px;color:#bdbdbd;font-family:'Courier New',monospace;margin-bottom:4px;"><span>RSI: {tech.get('rsi','-')}</span><span>MACD: {tech.get('macd',{}).get('trend','-')}</span><span>OBV: {obv_text}</span><span>Wkly: {tech.get('trend_weekly','-')}</span></div><div style="display:grid;grid-template-columns:repeat(3, 1fr);gap:5px;font-size:11px;color:#bdbdbd;font-family:'Courier New',monospace;margin-bottom:8px;"><span style="{vol_style}">VR: {vol_ratio}</span><span>Div: {risk.get('divergence','无')}</span><span>%B: {risk.get('bollinger_pct_b',0.5)}</span></div><div style="margin-bottom:8px;">{reasons}</div><div style="margin-top:5px;">{render_dots(r.get('history',[]))}</div>{committee_html}</div>"""
        except Exception as e:
            logger.error(f"Render Error {r.get('name')}: {e}")
            
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>body {{ background: #0a0a0a; color: #f0e6d2; font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; max-width: 660px; margin: 0 auto; padding: 20px; }} .main-container {{ border: 2px solid #333; border-top: 5px solid #ffb74d; border-radius: 4px; padding: 20px; background: linear-gradient(180deg, #1b1b1b 0%, #000000 100%); }} .header {{ text-align: center; border-bottom: 2px solid #333; padding-bottom: 20px; margin-bottom: 25px; }} .title {{ color: #ffb74d; margin: 0; font-size: 32px; font-weight: 800; font-family: 'Times New Roman', serif; letter-spacing: 2px; }} .subtitle {{ font-size: 11px; color: #888; margin-top: 8px; text-transform: uppercase; }} .radar-panel {{ background: #111; border: 1px solid #333; border-radius: 4px; padding: 15px; margin-bottom: 25px; }} .radar-title {{ font-size: 14px; color: #ffb74d; font-weight: bold; margin-bottom: 12px; border-bottom: 1px solid #444; padding-bottom: 6px; letter-spacing: 1px; }} .cio-section {{ background: linear-gradient(145deg, #1a0505, #2b0b0b); border: 1px solid #5c1818; border-left: 4px solid #d32f2f; padding: 20px; margin-bottom: 20px; border-radius: 2px; box-shadow: 0 4px 10px rgba(0,0,0,0.3); }} .cio-section * {{ color: #ffffff !important; line-height: 1.6; }} .cio-section h3 {{ border-bottom: 1px dashed #5c1818; padding-bottom: 5px; margin-top: 15px; margin-bottom: 8px; display: block; width: 100%; }} .advisor-section {{ background: #0f0f0f; border: 1px solid #d4af37; border-left: 4px solid #ffd700; padding: 20px; margin-bottom: 30px; border-radius: 4px; box-shadow: 0 0 10px rgba(212, 175, 55, 0.2); position: relative; }} .advisor-section * {{ color: #ffffff !important; line-height: 1.6; font-family: 'Georgia', serif; }} .advisor-section h4 {{ color: #ffd700 !important; margin-top: 15px; margin-bottom: 8px; border-bottom: 1px dashed #333; padding-bottom: 4px; }} .section-title {{ font-size: 16px; font-weight: bold; margin-bottom: 15px; color: #eee; text-transform: uppercase; letter-spacing: 1px; text-shadow: 0 1px 2px rgba(0,0,0,0.8); }} .footer {{ text-align: center; font-size: 10px; color: #444; margin-top: 40px; }} </style></head><body><div class="main-container"><div class="header"><h1 class="title">QUEZHIFENG QUANT</h1><div class="subtitle">MAGPIE SENSES THE WIND | V15.15 FULL CONTEXT</div></div><div class="radar-panel"><div class="radar-title">📡 7x24 GLOBAL LIVE WIRE</div>{news_html}</div><div class="cio-section"><div class="section-title">🛑 CIO 战略审计</div>{cio_html}</div><div class="advisor-section"><div class="section-title" style="color: #ffd700;">🐦 鹊知风·场外实战复盘</div>{advisor_html}</div>{rows}<div class="footer">EST. 2026 | POWERED BY AKSHARE & EM | V15.15</div></div></body></html>"""

def process_single_fund(fund, config, fetcher, tracker, val_engine, analyst, market_context, base_amt, max_daily):
    res = None
    cio_log = ""
    used_news = []
    
    try:
        logger.info(f"Analyzing {fund['name']}...")
        
        # 1. 读本地数据
        data = fetcher.get_fund_history(fund['code'])
        if data is None or data.empty: 
            return None, "", []

        # 2. 技术指标
        tech = TechnicalAnalyzer.calculate_indicators(data)
        if not tech: return None, "", []
        
        # 3. 估值
        try:
            val_mult, val_desc = val_engine.get_valuation_status(fund.get('index_name'), fund.get('strategy_type'))
        except:
            val_mult, val_desc = 1.0, "估值异常"

        with tracker_lock: pos = tracker.get_position(fund['code'])

        # 4. AI 分析
        ai_adj = 0; ai_res = {}
        should_run_ai = True

        if analyst and should_run_ai:
            cro_signal = tech.get('tech_cro_signal', 'PASS')
            fuse_level = 3 if cro_signal == 'VETO' else (1 if cro_signal == 'WARN' else 0)
            
            risk_payload = {
                "fuse_level": fuse_level,
                "risk_msg": tech.get('tech_cro_comment', '常规监控')
            }
            
            try:
                ai_res = analyst.analyze_fund_v5(fund['name'], tech, None, market_context, risk_payload, fund.get('strategy_type', 'core'))
                ai_adj = ai_res.get('adjustment', 0)
            except Exception as e:
                logger.error(f"AI Analysis Failed: {e}")
                ai_res = {"bull_view": "Error", "bear_view": "Error", "comment": "Offline", "adjustment": 0}

        # 5. [关键] 提取 AI 决策并传递给算分函数
        ai_decision = ai_res.get('decision', 'PASS') 
        
        amt, lbl, is_sell, s_val = calculate_position_v13(
            tech, ai_adj, ai_decision, val_mult, val_desc, base_amt, max_daily, pos, fund.get('strategy_type'), fund['name']
        )
        
        # 6. 记账
        with tracker_lock:
            tracker.record_signal(fund['code'], lbl)
            if amt > 0: tracker.add_trade(fund['code'], fund['name'], amt, tech['price'])
            elif is_sell: tracker.add_trade(fund['code'], fund['name'], s_val, tech['price'], True)

        bull = ai_res.get('bull_view') or ai_res.get('bull_say', '无')
        bear = ai_res.get('bear_view') or ai_res.get('bear_say', '无')
        if bull != '无':
            logger.info(f"🗣️ [投委会 {fund['name']}] CGO:{bull[:20]}... | CRO:{bear[:20]}...")

        res = {
            "name": fund['name'], "code": fund['code'], 
            "amount": amt, "sell_value": s_val, "position_type": lbl, "is_sell": is_sell, 
            "tech": tech, "ai_analysis": ai_res, "history": tracker.get_signal_history(fund['code']),
            "pos_cost": pos.get('cost', 0), "pos_shares": pos.get('shares', 0)
        }
    except Exception as e:
        logger.error(f"Process Error {fund['name']}: {e}")
        return None, "", []
    return res, cio_log, used_news

def main():
    config = load_config()
    fetcher = DataFetcher()
    tracker = PortfolioTracker()
    val_engine = ValuationEngine()
    
    logger.info(f">>> [V15.15] Startup | LOCAL_MODE=True | News Source: Local Cache + Live Patch")
    tracker.confirm_trades()
    try:
        analyst = NewsAnalyst()
    except Exception:
        analyst = None

    logger.info("📖 正在构建全天候舆情上下文 (Local + Live)...")
    market_context = analyst.get_market_context() if analyst else "无新闻数据"
    logger.info(f"🌍 舆情上下文长度: {len(market_context)} 字符")
    
    all_news_seen = []
    if market_context and market_context != "今日暂无重大新闻。":
        for line in market_context.split('\n')[:20]: 
            try:
                parts = line.split('] ', 1)
                if len(parts) == 2:
                    all_news_seen.append({"time": parts[0][1:], "title": parts[1]})
                else:
                    all_news_seen.append({"title": line, "time": ""})
            except Exception:
                pass

    results = []; cio_lines = [f"【宏观环境】: (见独立审计报告)\n"]
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_fund = {executor.submit(
            process_single_fund, 
            fund, config, fetcher, tracker, val_engine, analyst, market_context, 
            config['global']['base_invest_amount'], config['global']['max_daily_invest']
        ): fund for fund in config.get('funds', [])}
        
        for future in as_completed(future_to_fund):
            try:
                res, log, _ = future.result()
                if res: 
                    results.append(res)
                    cio_lines.append(log)
            except Exception as e: logger.error(f"Thread Error: {e}")

    if results:
        results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        full_report = "\n".join(cio_lines)
        
        cio_html = analyst.review_report(full_report, market_context) if analyst else "<p>CIO Missing</p>"
        advisor_html = analyst.advisor_review(full_report, market_context) if analyst else "<p>Advisor Offline</p>"
        
        html = render_html_report_v13(all_news_seen, results, cio_html, advisor_html) 
        
        send_email("🐦 鹊知风 V15.15 铁拳决议 (Full Context)", html, attachment_path=LOG_FILENAME)

if __name__ == "__main__": main()
