import yaml
import os
import threading
import json
import base64
import re  # 用于 Markdown 正则清洗
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

def clean_markdown(text):
    """
    清洗 AI 回复中可能夹带的 Markdown 标记，防止 HTML 渲染异常
    """
    if not text:
        return ""
    # 1. 移除 ```html, ```json, ``` 等代码块标签
    text = re.sub(r'```(?:html|json|markdown)?', '', text)
    # 2. 移除常见的 Markdown 加粗标记
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    # 3. 移除标题标记 (针对 ### 核心审计发现 -> 核心审计发现)
    text = re.sub(r'#+\s+', '', text)
    return text.strip()

def calculate_position_v13(tech, ai_adj, ai_decision, val_mult, val_desc, base_amt, max_daily, pos, strategy_type, fund_name):
    """
    V13 核心算分逻辑 (保持原样，不作改动)
    """
    base_score = tech.get('quant_score', 50)
    try:
        ai_adj_int = int(ai_adj)
    except:
        ai_adj_int = 0

    tactical_score = max(0, min(100, base_score + ai_adj_int))
    
    if ai_decision == "REJECT":
        tactical_score = 0 
    elif ai_decision == "HOLD" and tactical_score >= 60:
        tactical_score = 59
            
    tech['final_score'] = tactical_score
    tech['ai_adjustment'] = ai_adj_int
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

    if cro_signal == "VETO" and final_mult > 0:
        final_mult = 0; reasons.append(f"🛡️风控:否决买入")
    
    held_days = pos.get('held_days', 999)
    if final_mult < 0 and pos['shares'] > 0 and held_days < 7:
        final_mult = 0; reasons.append(f"规则:锁仓({held_days}天)")

    final_amt = 0; is_sell = False; sell_val = 0; label = "观望"
    if final_mult > 0:
        final_amt = max(0, min(int(base_amt * final_mult), int(max_daily)))
        label = "买入"
    elif final_mult < 0:
        is_sell = True
        sell_val = pos['shares'] * tech.get('price', 0) * min(abs(final_mult), 1.0)
        label = "卖出"

    if reasons: tech['quant_reasons'] = reasons
    return final_amt, label, is_sell, sell_val

def render_html_report_v13(all_news, results, cio_html, advisor_html):
    """
    生成 HTML 报告 - 适配 v3.2 嵌套字段，修正底色与 Markdown
    """
    COLOR_GOLD = "#fab005" 
    COLOR_RED = "#fa5252"  
    COLOR_GREEN = "#51cf66" 
    COLOR_TEXT_MAIN = "#e9ecef"
    COLOR_TEXT_SUB = "#adb5bd"
    COLOR_BG_MAIN = "#0f1215" 
    COLOR_BG_CARD = "#16191d" 
    
    # 清洗 R1 生成的 Markdown
    cio_html = clean_markdown(cio_html)
    advisor_html = clean_markdown(advisor_html)

    news_html = "".join([f'<div style="font-size:11px;color:{COLOR_TEXT_SUB};margin-bottom:5px;border-bottom:1px solid #25282c;padding-bottom:3px;"><span style="color:{COLOR_GOLD};margin-right:4px;">●</span>{n}</div>' for n in all_news])
    
    rows = ""
    for r in results:
        tech = r.get('tech', {})
        ai_data = r.get('ai_analysis', {})
        
        # v3.2 数据提取
        bull_say = clean_markdown(ai_data.get('cgo_proposal', {}).get('catalyst', '无明显催化'))
        bear_say = clean_markdown(ai_data.get('cro_audit', {}).get('max_drawdown_scenario', '无'))
        chairman = clean_markdown(ai_data.get('chairman_conclusion', '无结论'))

        act_style = f"background:rgba(250,82,82,0.15);color:{COLOR_RED};border:1px solid {COLOR_RED};" if r['amount'] > 0 else (f"background:rgba(81,207,102,0.15);color:{COLOR_GREEN};border:1px solid {COLOR_GREEN};" if r['is_sell'] else "background:rgba(255,255,255,0.05);color:#adb5bd;border:1px solid #495057;")
        act_text = f"⚡ 买入 {r['amount']:,}" if r['amount'] > 0 else (f"💰 卖出 {int(r['sell_value']):,}" if r['is_sell'] else "☕ 观望")

        reasons = " ".join([f"<span style='border:1px solid #444;background:rgba(255,255,255,0.05);padding:1px 4px;font-size:9px;border-radius:3px;color:{COLOR_TEXT_SUB};margin-right:3px;'>{x}</span>" for x in tech.get('quant_reasons', [])])
        
        # --- 适配新的 tech 结构用于显示 ---
        # 旧版: tech.get('risk_factors', {}).get('vol_ratio')
        # 新版: tech.get('volume_analysis', {}).get('vol_ratio')
        vol_ratio = tech.get('volume_analysis', {}).get('vol_ratio', 1.0)
        
        rows += f"""<div class="card" style="border-left:3px solid {COLOR_GOLD}; background:{COLOR_BG_CARD}; margin-bottom:15px; padding:15px; border-radius:4px;">
            <div style="display:flex;justify-content:space-between;margin-bottom:10px;align-items:center;">
                <span style="font-size:16px;font-weight:bold;color:{COLOR_TEXT_MAIN};">{r['name']}</span>
                <span style="display:inline-block;padding:3px 10px;font-size:12px;font-weight:bold;border-radius:4px;{act_style}">{act_text}</span>
            </div>
            <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                 <span style="color:{COLOR_GOLD};font-weight:bold;font-size:18px;">{tech.get('final_score', 0)}分</span>
                 <div style="font-size:11px;color:{COLOR_TEXT_SUB};padding-top:4px;">🛡️ {tech.get('tech_cro_comment','-')}</div>
            </div>
            <div class="tech-grid">
                <span>RSI: {tech.get('rsi','-')}</span>
                <span>阶段: {ai_data.get('trend_analysis', {}).get('stage','-')}</span>
                <span>VR: {vol_ratio}</span>
                <span>失效位: {ai_data.get('trend_analysis', {}).get('key_levels', {}).get('invalidation','-')}</span>
            </div>
            <div style="margin-top:8px;">{reasons}</div>
            <div style="margin-top:12px;border-top:1px solid #333;padding-top:10px;">
                <div style="border-left:2px solid {COLOR_GREEN};background:rgba(81,207,102,0.05);padding:8px;margin-bottom:5px;font-size:11px;">
                    <div style="color:{COLOR_GREEN};font-weight:bold;margin-bottom:2px;">🦊 CGO</div>
                    <div style="color:#c0ebc9;">"{bull_say}"</div>
                </div>
                <div style="border-left:2px solid {COLOR_RED};background:rgba(250,82,82,0.05);padding:8px;margin-bottom:5px;font-size:11px;">
                    <div style="color:{COLOR_RED};font-weight:bold;margin-bottom:2px;">🐻 CRO</div>
                    <div style="color:#ffc9c9;">"{bear_say}"</div>
                </div>
                <div style="background:rgba(250,176,5,0.05);padding:10px;border-radius:4px;border:1px solid rgba(250,176,5,0.2);margin-top:8px;">
                    <div style="color:{COLOR_GOLD};font-size:12px;font-weight:bold;margin-bottom:4px;">⚖️ CIO 终审</div>
                    <div style="color:{COLOR_TEXT_MAIN};font-size:12px;">{chairman}</div>
                </div>
            </div>
        </div>"""

    # --- Logo 处理 ---
    logo_src = "https://raw.githubusercontent.com/kken61291-eng/Fund-AI-Advisor/main/logo.png"
    for p in ["logo.png", "Gemini_Generated_Image_d7oeird7oeird7oe.jpg"]:
        if os.path.exists(p):
            try:
                with open(p, "rb") as f:
                    logo_src = f"data:image/{p.split('.')[-1]};base64,{base64.b64encode(f.read()).decode()}"
                break
            except: pass

    return f"""<!DOCTYPE html><html><head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{ background: {COLOR_BG_MAIN}; color: {COLOR_TEXT_MAIN}; font-family: sans-serif; margin: 0; padding: 10px; }}
        .main-container {{ max-width: 600px; margin: 0 auto; background: #0a0c0e; border: 1px solid #2c3e50; padding: 15px; border-radius: 8px; }}
        .tech-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 5px; font-size: 11px; color: {COLOR_TEXT_SUB}; }}
        .cio-content, .advisor-content {{ line-height: 1.6; font-size: 13px; color: {COLOR_TEXT_MAIN} !important; }}
        /* 强制覆盖 AI 可能生成的任何底色，确保为深色背景 */
        .cio-content *, .advisor-content * {{ background-color: transparent !important; color: inherit !important; }}
        @media (max-width: 480px) {{ .tech-grid {{ grid-template-columns: 1fr; }} .main-container {{ padding: 10px; border: none; }} }}
    </style></head><body>
    <div class="main-container">
        <div style="text-align:center; padding-bottom:20px; border-bottom:1px solid #222;">
            <img src="{logo_src}" style="width:200px; max-width:80%; display:block; margin:0 auto;">
            <div style="font-size:10px; color:{COLOR_GOLD}; letter-spacing:2px; margin-top:10px;">MAGPIE SENSES THE WIND | V15.20</div>
        </div>
        
        <div style="background:{COLOR_BG_CARD};margin-top:20px;padding:15px;border-radius:4px;margin-bottom:15px;">
            <div style="color:{COLOR_GOLD};font-weight:bold;border-bottom:1px solid #333;padding-bottom:5px;margin-bottom:10px;">📡 全球舆情雷达</div>
            {news_html}
        </div>
        <div style="background:{COLOR_BG_CARD};padding:15px;border-radius:4px;border-left:3px solid {COLOR_RED};margin-bottom:15px;">
            <div style="color:{COLOR_RED};font-weight:bold;margin-bottom:10px;">🛑 CIO 战略审计报告</div>
            <div class="cio-content">{cio_html}</div>
        </div>
        <div style="background:{COLOR_BG_CARD};padding:15px;border-radius:4px;border-left:3px solid {COLOR_GOLD};margin-bottom:15px;">
            <div style="color:{COLOR_GOLD};font-weight:bold;margin-bottom:10px;">🐦 鹊知风·趋势一致性审计</div>
            <div class="advisor-content">{advisor_html}</div>
        </div>
        {rows}
        <div style="text-align:center; color:#444; font-size:10px; margin-top:30px;">EST. 2026 | POWERED BY AI</div>
    </div></body></html>"""

def process_single_fund(fund, config, fetcher, tracker, val_engine, analyst, market_context, base_amt, max_daily):
    """
    适配 v3.2 JSON 字段提取 - 并实例化 TechnicalAnalyzer
    """
    try:
        data = fetcher.get_fund_history(fund['code'])
        if data is None or data.empty: return None, "", []
        
        # --- 修复点：实例化类后再调用 ---
        # 你的新版 technical_analyzer.py 移除了静态方法，使用了 __init__
        # 默认假设为 ETF 类型，如果需要区分股票，可在此处添加逻辑
        analyzer_instance = TechnicalAnalyzer(asset_type='ETF') 
        tech = analyzer_instance.calculate_indicators(data)
        
        if not tech: return None, []
        
        val_mult, val_desc = val_engine.get_valuation_status(
            fund.get('index_name'), 
            fund.get('strategy_type'), 
            fund.get('code')  # 传入 fund code 作为兜底数据源
        )
        with tracker_lock: pos = tracker.get_position(fund['code'])

        ai_res = {}
        if analyst:
            cro_signal = tech.get('tech_cro_signal', 'PASS')
            risk_payload = {"fuse_level": 3 if cro_signal == 'VETO' else 0, "risk_msg": tech.get('tech_cro_comment', '监控')}
            ai_res = analyst.analyze_fund_v5(fund['name'], tech, None, market_context, risk_payload, fund.get('strategy_type', 'core'))

        ai_adj = ai_res.get('adjustment', 0)
        ai_decision = ai_res.get('decision', 'PASS') 
        
        amt, lbl, is_sell, s_val = calculate_position_v13(tech, ai_adj, ai_decision, val_mult, val_desc, base_amt, max_daily, pos, fund.get('strategy_type'), fund['name'])
        
        with tracker_lock:
            tracker.record_signal(fund['code'], lbl)
            if amt > 0: tracker.add_trade(fund['code'], fund['name'], amt, tech['price'])
            elif is_sell: tracker.add_trade(fund['code'], fund['name'], s_val, tech['price'], True)

        cio_log = f"标的:{fund['name']} | 阶段:{ai_res.get('trend_analysis',{}).get('stage','-')} | 决策:{lbl}(AI:{ai_adj})"
        return {"name": fund['name'], "code": fund['code'], "amount": amt, "sell_value": s_val, "is_sell": is_sell, "tech": tech, "ai_analysis": ai_res}, cio_log, []
    except Exception as e:
        logger.error(f"Error {fund['name']}: {e}", exc_info=True); return None, "", []

def main():
    config = load_config()
    fetcher, tracker, val_engine = DataFetcher(), PortfolioTracker(), ValuationEngine()
    tracker.confirm_trades()
    try: analyst = NewsAnalyst()
    except: analyst = None

    market_context = analyst.get_market_context() if analyst else "无数据"
    all_news_seen = [line.strip() for line in market_context.split('\n') if line.strip().startswith('[')]

    results, cio_lines = [], []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_single_fund, f, config, fetcher, tracker, val_engine, analyst, market_context, config['global']['base_invest_amount'], config['global']['max_daily_invest']): f for f in config.get('funds', [])}
        for f in as_completed(futures):
            res, log, _ = f.result()
            if res: results.append(res); cio_lines.append(log)

    if results:
        results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        full_report = "\n".join(cio_lines)
        cio_html = analyst.review_report(full_report, market_context) if analyst else ""
        advisor_html = analyst.advisor_review(full_report, market_context) if analyst else ""
        html = render_html_report_v13(all_news_seen, results, cio_html, advisor_html) 
        send_email("🕊️ 鹊知风 V15.20 洞察微澜，御风而行", html, attachment_path=LOG_FILENAME)

if __name__ == "__main__": main()
