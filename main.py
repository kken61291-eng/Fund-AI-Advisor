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
    V13 核心算分逻辑 (含 CIO 一票否决权 & 类型安全修复)
    """
    base_score = tech.get('quant_score', 50)
    
    try:
        ai_adj_int = int(ai_adj)
    except:
        logger.warning(f"⚠️ {fund_name} AI调整值类型错误 ({ai_adj}), 重置为0")
        ai_adj_int = 0

    # 1. 初始计算
    tactical_score = max(0, min(100, base_score + ai_adj_int))
    action_str = "加分进攻" if ai_adj_int > 0 else ("减分防御" if ai_adj_int < 0 else "中性维持")
    logger.info(f"🧮 [算分 {fund_name}] 技术面({base_score}) + CIO修正({ai_adj_int:+d} {action_str}) = 初步分({tactical_score})")
    
    # 2. CIO 一票否决权
    override_reason = ""
    original_score = tactical_score
    
    if ai_decision == "REJECT":
        tactical_score = 0 
        override_reason = "⛔ CIO指令:REJECT (强制否决)"
    elif ai_decision == "HOLD":
        if tactical_score >= 60:
            tactical_score = 59
            override_reason = "⏸️ CIO指令:HOLD (强制观望)"
            
    if override_reason:
        logger.warning(f"⚠️ [CIO介入 {fund_name}] 原分{original_score} -> {override_reason} -> 修正后: {tactical_score}")

    # 3. 记录状态
    tech['final_score'] = tactical_score
    tech['ai_adjustment'] = ai_adj_int
    tech['valuation_desc'] = val_desc
    cro_signal = tech.get('tech_cro_signal', 'PASS')
    
    tactical_mult = 0
    reasons = []

    # 4. 定档
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

    # 6. 风控
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
    生成完整的 HTML 邮件报告 (V15.17 UI优化版：深邃金融风格)
    """
    # --- 主色调定义 ---
    COLOR_GOLD = "#fab005" # 更具质感的琥珀金
    COLOR_RED = "#fa5252"  # 更现代的红色
    COLOR_GREEN = "#51cf66" # 更清透的绿色
    COLOR_TEXT_MAIN = "#e9ecef"
    COLOR_TEXT_SUB = "#adb5bd"
    COLOR_BG_MAIN = "#0f1215" # 深岩灰背景
    COLOR_BG_CARD = "#16191d" # 卡片背景
    COLOR_BORDER = "#2c3e50"  # 深蓝灰色边框

    news_html = ""
    if isinstance(all_news, list):
        for i, news in enumerate(all_news):
            if isinstance(news, dict):
                title = news.get('title', 'No Title')
                time_str = news.get('time', '')
            else:
                raw_text = str(news)
                if raw_text.startswith('[') and '] ' in raw_text:
                    parts = raw_text.split('] ', 1)
                    time_str = parts[0][1:] 
                    title = parts[1]
                else:
                    title = raw_text
                    time_str = ""
            
            # 新闻列表样式微调：颜色更柔和，边框更细
            news_html += f"""<div style="font-size:11px;color:{COLOR_TEXT_SUB};margin-bottom:5px;border-bottom:1px solid #25282c;padding-bottom:3px;"><span style="color:{COLOR_GOLD};margin-right:4px;">●</span>{title}<span style="float:right;color:#666;font-size:10px;">{time_str}</span></div>"""
    
    def render_dots(hist):
        h = ""
        for x in hist:
            # 历史点颜色优化
            c = COLOR_RED if x['s']=='B' else (COLOR_GREEN if x['s'] in ['S','C'] else "#444")
            h += f'<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:{c};margin-right:3px;box-shadow:0 0 4px {c}66;" title="{x["date"]}"></span>'
        return h

    rows = ""
    for r in results:
        try:
            tech = r.get('tech', {})
            risk = tech.get('risk_factors', {})
            final_score = tech.get('final_score', 0)
            ai_adj = int(tech.get('ai_adjustment', 0))
            base_score = final_score - ai_adj 
            cro_signal = tech.get('tech_cro_signal', 'PASS')
            cro_comment = tech.get('tech_cro_comment', '无')
            
            # 风控颜色优化
            cro_style = f"color:{COLOR_GREEN};font-weight:bold;"
            cro_border_color = COLOR_GREEN
            if cro_signal == "VETO": 
                cro_style = f"color:{COLOR_RED};font-weight:bold;"
                cro_border_color = COLOR_RED
            elif cro_signal == "WARN": 
                cro_style = f"color:{COLOR_GOLD};font-weight:bold;"
                cro_border_color = COLOR_GOLD
            
            obv_text = '流入' if tech.get('flow',{}).get('obv_slope',0) > 0 else '流出'
            
            profit_html = ""
            pos_cost = r.get('pos_cost', 0.0)
            pos_shares = r.get('pos_shares', 0)
            current_price = tech.get('price', 0.0)
            
            if pos_shares > 0 and pos_cost > 0 and current_price > 0:
                profit_pct = (current_price - pos_cost) / pos_cost * 100
                profit_val = (current_price - pos_cost) * pos_shares
                p_color = COLOR_RED if profit_val > 0 else COLOR_GREEN 
                profit_html = f"""<div style="font-size:12px;margin-bottom:8px;background:rgba(0,0,0,0.2);padding:4px 8px;border-radius:3px;display:flex;justify-content:space-between;border:1px solid #333;"><span style="color:{COLOR_TEXT_SUB};">持有收益:</span><span style="color:{p_color};font-weight:bold;">{profit_val:+.1f}元 ({profit_pct:+.2f}%)</span></div>"""
            
            # [核心优化] 卡片样式重构：移除大面积背景色，改用边框和光晕
            if r['amount'] > 0: 
                border_color = COLOR_RED
                # 使用微弱的红色光晕代替背景
                card_shadow = f"0 4px 15px rgba(0,0,0,0.5), 0 0 20px {COLOR_RED}22"
                act_html = f"<span style='color:{COLOR_RED};font-weight:bold'>+{r['amount']:,}</span>"
            elif r.get('is_sell'): 
                border_color = COLOR_GREEN
                # 使用微弱的绿色光晕
                card_shadow = f"0 4px 15px rgba(0,0,0,0.5), 0 0 20px {COLOR_GREEN}22"
                act_html = f"<span style='color:{COLOR_GREEN};font-weight:bold'>-{int(r.get('sell_value',0)):,}</span>"
            else: 
                border_color = "#444"
                # 中性灰色光晕
                card_shadow = "0 4px 15px rgba(0,0,0,0.5), 0 0 10px rgba(255,255,255,0.05)"
                act_html = f"<span style='color:{COLOR_TEXT_SUB}'>HOLD</span>"
            
            # 标签样式优化
            reasons = " ".join([f"<span style='border:1px solid #444;background:rgba(255,255,255,0.05);padding:1px 4px;font-size:9px;border-radius:3px;color:{COLOR_TEXT_SUB};'>{x}</span>" for x in tech.get('quant_reasons', [])])
            val_desc = tech.get('valuation_desc', 'N/A')
            val_style = f"color:{COLOR_GOLD};font-weight:bold;" if "低估" in val_desc else (f"color:{COLOR_RED};font-weight:bold;" if "高估" in val_desc else f"color:{COLOR_TEXT_SUB};")
            
            committee_html = ""
            ai_data = r.get('ai_analysis', {})
            bull_say = ai_data.get('bull_view', '无')
            bear_say = ai_data.get('bear_view', '无')
            chairman = ai_data.get('chairman_conclusion') or ai_data.get('comment', '无')
            
            if bull_say != '无':
                adj_color = COLOR_RED if ai_adj > 0 else (COLOR_GREEN if ai_adj < 0 else COLOR_TEXT_SUB)
                # 投委会样式优化：更深邃的背景，更细的边框
                committee_html = f"""<div style="margin-top:12px;border-top:1px solid #333;padding-top:10px;"><div style="font-size:10px;color:{COLOR_TEXT_SUB};margin-bottom:6px;text-align:center;letter-spacing:1px;">--- 联邦投委会辩论 ---</div><div style="display:flex;gap:10px;margin-bottom:8px;"><div style="flex:1;background:rgba(81, 207, 102, 0.1);padding:8px;border-radius:4px;border-left:2px solid {COLOR_GREEN};"><div style="color:{COLOR_GREEN};font-size:11px;font-weight:bold;margin-bottom:4px;">🦊 CGO (增长)</div><div style="color:#c0ebc9;font-size:11px;line-height:1.3;font-style:italic;">"{bull_say}"</div></div><div style="flex:1;background:rgba(250, 82, 82, 0.1);padding:8px;border-radius:4px;border-left:2px solid {COLOR_RED};"><div style="color:{COLOR_RED};font-size:11px;font-weight:bold;margin-bottom:4px;">🐻 CRO (风控)</div><div style="color:#ffc9c9;font-size:11px;line-height:1.3;font-style:italic;">"{bear_say}"</div></div></div><div style="background:rgba(250, 176, 5, 0.05);padding:10px;border-radius:4px;border:1px solid rgba(250, 176, 5, 0.2);position:relative;"><div style="display:flex;justify-content:space-between;margin-bottom:4px;"><div style="color:{COLOR_GOLD};font-size:12px;font-weight:bold;">⚖️ CIO 终审</div><div style="color:{adj_color};font-size:11px;font-weight:bold;">策略修正: {ai_adj:+d}</div></div><div style="color:{COLOR_TEXT_MAIN};font-size:12px;line-height:1.4;">{chairman}</div></div></div>"""
            
            vol_ratio = risk.get('vol_ratio', 1.0)
            vol_style = f"color:{COLOR_GOLD};" if vol_ratio < 0.8 else (f"color:{COLOR_RED};" if vol_ratio > 2.0 else "color:#777;")
            
            # [核心优化] 卡片容器：统一深色背景 + 3px 左边框 + 呼吸光晕
            rows += f"""<div style="background:{COLOR_BG_CARD};border-left:3px solid {border_color};margin-bottom:15px;padding:15px;border-radius:4px;box-shadow:{card_shadow};border-top:1px solid #222;"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;"><div><span style="font-size:18px;font-weight:bold;color:{COLOR_TEXT_MAIN};font-family:'Times New Roman',serif;letter-spacing:0.5px;">{r['name']}</span><span style="font-size:12px;color:{COLOR_TEXT_SUB};margin-left:5px;">{r['code']}</span></div><div style="text-align:right;"><div style="color:{COLOR_GOLD};font-weight:bold;font-size:18px;text-shadow:0 0 10px {COLOR_GOLD}44;">{final_score}</div><div style="font-size:9px;color:{COLOR_TEXT_SUB};">BASE:{base_score} <span style="color:{COLOR_RED if ai_adj>0 else (COLOR_GREEN if ai_adj<0 else COLOR_TEXT_SUB)}">{ai_adj:+d}</span></div></div></div><div style="background:rgba(0,0,0,0.3);padding:4px 8px;border-radius:4px;margin-bottom:10px;display:flex;align-items:center;border-left:2px solid {cro_border_color};"><span style="font-size:11px;color:{COLOR_TEXT_SUB};margin-right:8px;">🛡️ 技术风控:</span><span style="font-size:11px;{cro_style}">{cro_comment}</span></div><div style="display:flex;justify-content:space-between;color:{COLOR_TEXT_MAIN};font-size:15px;margin-bottom:5px;border-bottom:1px solid #333;padding-bottom:5px;"><span style="font-weight:bold;color:{COLOR_GOLD};">{r.get('position_type')}</span><span style="font-family:'Courier New',monospace;">{act_html}</span></div>{profit_html}<div style="font-size:11px;margin-bottom:8px;border-bottom:1px dashed #333;padding-bottom:5px;"><span style="color:{COLOR_TEXT_SUB};">周期定位:</span> <span style="{val_style}">{val_desc}</span></div><div style="display:grid;grid-template-columns:repeat(4, 1fr);gap:5px;font-size:11px;color:{COLOR_TEXT_SUB};font-family:'Courier New',monospace;margin-bottom:4px;"><span>RSI: {tech.get('rsi','-')}</span><span>MACD: {tech.get('macd',{}).get('trend','-')}</span><span>OBV: {obv_text}</span><span>Wkly: {tech.get('trend_weekly','-')}</span></div><div style="display:grid;grid-template-columns:repeat(3, 1fr);gap:5px;font-size:11px;color:{COLOR_TEXT_SUB};font-family:'Courier New',monospace;margin-bottom:8px;"><span style="{vol_style}">VR: {vol_ratio}</span><span>Div: {risk.get('divergence','无')}</span><span>%B: {risk.get('bollinger_pct_b',0.5)}</span></div><div style="margin-bottom:8px;">{reasons}</div><div style="margin-top:5px;">{render_dots(r.get('history',[]))}</div>{committee_html}</div>"""
        except Exception as e:
            logger.error(f"Render Error {r.get('name')}: {e}")
    
    # [Logo] 使用 GitHub Raw 链接
    logo_url = "https://raw.githubusercontent.com/kken61291-eng/Fund-AI-Advisor/main/logo.png"
    
    # [V15.17] CSS 全局优化：深色系 + 琥珀金
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><style>
    /* 全局背景改为深岩灰，文字颜色更柔和 */
    body {{ background: {COLOR_BG_MAIN}; color: {COLOR_TEXT_MAIN}; font-family: 'Segoe UI', 'Microsoft YaHei', sans-serif; max-width: 660px; margin: 0 auto; padding: 20px; }}
    /* 主容器：更平滑的深色渐变，边框光晕 */
    .main-container {{ border: 1px solid {COLOR_BORDER}; border-top: 4px solid {COLOR_GOLD}; border-radius: 6px; padding: 20px; background: linear-gradient(180deg, #14171a 0%, #0a0c0e 100%); box-shadow: 0 10px 30px rgba(0,0,0,0.5); }}
    .header {{ text-align: center; border-bottom: 1px solid {COLOR_BORDER}; padding-bottom: 20px; margin-bottom: 25px; }}
    .logo-img {{ width: 100%; height: auto; object-fit: contain; display: block; margin: 0 auto;filter: drop-shadow(0 0 5px {COLOR_GOLD}33); }}
    .subtitle {{ font-size: 11px; color: {COLOR_TEXT_SUB}; margin-top: 12px; text-transform: uppercase; letter-spacing: 2px; }}
    /* 模块面板：统一深色背景，减少杂色 */
    .radar-panel {{ background: {COLOR_BG_CARD}; border: 1px solid {COLOR_BORDER}; border-radius: 4px; padding: 15px; margin-bottom: 25px; }}
    .radar-title {{ font-size: 14px; color: {COLOR_GOLD}; font-weight: bold; margin-bottom: 12px; border-bottom: 1px solid #333; padding-bottom: 6px; letter-spacing: 1px; display:flex; align-items:center; }}
    .radar-title::before {{ content: '📡'; margin-right: 6px; font-size: 12px; }}
    
    /* CIO Section：优化为深红色调，减少刺眼感 */
    .cio-section {{ background: linear-gradient(145deg, #2a0f0f, #1a0505); border: 1px solid #5c1818; border-left: 3px solid {COLOR_RED}; padding: 20px; margin-bottom: 20px; border-radius: 4px; box-shadow: 0 5px 15px rgba(0,0,0,0.3); }}
    .cio-section * {{ color: {COLOR_TEXT_MAIN} !important; line-height: 1.6; }}
    .cio-section h3 {{ border-bottom: 1px dashed #5c1818; padding-bottom: 5px; margin-top: 15px; margin-bottom: 8px; display: block; width: 100%; color: {COLOR_GOLD} !important; }}
    /* 表格深度修复 (保持不变) */
    .cio-section table {{ width: 100%; border-collapse: collapse; margin: 15px 0; color: {COLOR_TEXT_MAIN} !important; background-color: transparent !important; font-size: 11px; }}
    .cio-section th {{ background-color: rgba(250, 176, 5, 0.1) !important; color: {COLOR_GOLD} !important; border: 1px solid #444 !important; padding: 8px; text-align: left; }}
    .cio-section td {{ border: 1px solid #333 !important; padding: 8px; background-color: rgba(0, 0, 0, 0.3) !important; }}
    
    /* 顾问 Section：优化为深金色调 */
    .advisor-section {{ background: linear-gradient(145deg, #2a220f, #1a1605); border: 1px solid {COLOR_GOLD}44; border-left: 3px solid {COLOR_GOLD}; padding: 20px; margin-bottom: 30px; border-radius: 4px; box-shadow: 0 5px 15px rgba(0,0,0,0.3); position: relative; }}
    .advisor-section * {{ color: {COLOR_TEXT_MAIN} !important; line-height: 1.6; font-family: 'Georgia', serif; }}
    .advisor-section h4 {{ color: {COLOR_GOLD} !important; margin-top: 15px; margin-bottom: 8px; border-bottom: 1px dashed #444; padding-bottom: 4px; }}
    
    .section-title {{ font-size: 16px; font-weight: bold; margin-bottom: 15px; color: {COLOR_TEXT_MAIN}; text-transform: uppercase; letter-spacing: 1px; text-shadow: 0 2px 4px rgba(0,0,0,0.8); display:flex; align-items:center; }}
    .footer {{ text-align: center; font-size: 10px; color: #555; margin-top: 40px; border-top: 1px solid #222; padding-top: 15px; }} 
    </style></head><body><div class="main-container"><div class="header"><img src="{logo_url}" alt="QUEZHIFENG QUANT" class="logo-img"><div class="subtitle">MAGPIE SENSES THE WIND | V15.17 DARK FINANCE UI</div></div><div class="radar-panel"><div class="radar-title">7x24 GLOBAL LIVE WIRE</div>{news_html}</div><div class="cio-section"><div class="section-title"><span style="margin-right:6px;">🛑</span>CIO 战略审计</div>{cio_html}</div><div class="advisor-section"><div class="section-title" style="color: {COLOR_GOLD};"><span style="margin-right:6px;">🐦</span>鹊知风·场外实战复盘</div>{advisor_html}</div>{rows}<div class="footer">EST. 2026 | POWERED BY AKSHARE & EM | V15.17</div></div></body></html>"""

def process_single_fund(fund, config, fetcher, tracker, val_engine, analyst, market_context, base_amt, max_daily):
    res = None
    cio_log = ""
    used_news = []
    
    try:
        logger.info(f"Analyzing {fund['name']}...")
        
        data = fetcher.get_fund_history(fund['code'])
        if data is None or data.empty: 
            return None, "", []

        tech = TechnicalAnalyzer.calculate_indicators(data)
        if not tech: return None, "", []
        
        try:
            val_mult, val_desc = val_engine.get_valuation_status(fund.get('index_name'), fund.get('strategy_type'))
        except:
            val_mult, val_desc = 1.0, "估值异常"

        with tracker_lock: pos = tracker.get_position(fund['code'])

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

        ai_decision = ai_res.get('decision', 'PASS') 
        
        amt, lbl, is_sell, s_val = calculate_position_v13(
            tech, ai_adj, ai_decision, val_mult, val_desc, base_amt, max_daily, pos, fund.get('strategy_type'), fund['name']
        )
        
        with tracker_lock:
            tracker.record_signal(fund['code'], lbl)
            if amt > 0: tracker.add_trade(fund['code'], fund['name'], amt, tech['price'])
            elif is_sell: tracker.add_trade(fund['code'], fund['name'], s_val, tech['price'], True)

        bull = ai_res.get('bull_view') or ai_res.get('bull_say', '无')
        bear = ai_res.get('bear_view') or ai_res.get('bear_say', '无')
        if bull != '无':
            logger.info(f"🗣️ [投委会 {fund['name']}] CGO:{bull[:20]}... | CRO:{bear[:20]}...")

        # [V15.17] 修复CIO日志记录逻辑，确保CIO能看到决策理由
        reason_str = ",".join(tech.get('quant_reasons', []))
        cio_log = f"标的:{fund['name']} | 决策:{lbl} (分:{tech['final_score']} AI:{ai_adj}) | 理由:{reason_str}"

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
    
    logger.info(f">>> [V15.17] Startup | LOCAL_MODE=True | News Source: Local Cache + Live Patch")
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
        for line in market_context.split('\n'):
            try:
                # [UI] 过滤掉摘要行，只保留标题供列表展示
                if line.strip().startswith('['):
                    all_news_seen.append(line.strip())
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
        
        send_email("🐦 鹊知风 V15.17 铁拳决议 (Dark Finance UI)", html, attachment_path=LOG_FILENAME)

if __name__ == "__main__": main()
