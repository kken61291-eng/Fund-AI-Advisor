import yaml
import os
import time
from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from market_scanner import MarketScanner
from technical_analyzer import TechnicalAnalyzer
from valuation_engine import ValuationEngine
from portfolio_tracker import PortfolioTracker
from utils import send_email, logger

def load_config():
    with open('config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def calculate_position_v13(tech, ai_adj, val_mult, val_desc, base_amt, max_daily, pos, strategy_type):
    # --- 保持 V13.0 逻辑不变，仅增加防御性键值检查 ---
    base_score = tech.get('quant_score', 50)
    tactical_score = max(0, min(100, base_score + ai_adj))
    
    tech['final_score'] = tactical_score
    tech['ai_adjustment'] = ai_adj
    tech['valuation_desc'] = val_desc # 确保存入估值描述
    
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

    held_days = pos.get('held_days', 999)
    if final_mult < 0 and pos['shares'] > 0 and held_days < 7:
        final_mult = 0; reasons.append(f"风控:锁仓({held_days}天)")

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

    if reasons:
        tech['quant_reasons'] = reasons

    return final_amt, label, is_sell, sell_val

def render_html_report_v13(macro_list, results, cio, advisor):
    """
    V13.2 UI升级：
    1. 高对比度配色 (解决看不清问题)
    2. 防御性渲染 (解决报错导致指标消失问题)
    """
    
    # 宏观部分：字体提亮
    macro_html = "".join([f"<div style='font-size:13px;color:#e0e0e0;margin-bottom:8px;border-bottom:1px dashed #5d4037;padding-bottom:5px;'><span style='color:#ffb74d;margin-right:5px;'>●</span> {n.get('title','')} <span style='color:#bbb;float:right;font-size:11px;'>[{n.get('source','')}]</span></div>" for n in macro_list])
    
    rows = ""
    for r in results:
        try:
            # --- 数据准备 (使用 .get() 防止报错) ---
            tech = r.get('tech', {})
            risk = tech.get('risk_factors', {})
            
            # 基础数据
            name = r.get('name', 'Unknown')
            code = r.get('code', '000000')
            score = tech.get('final_score', 0)
            
            # 交易动作
            amt = r.get('amount', 0)
            is_sell = r.get('is_sell', False)
            sell_val = int(r.get('sell_value', 0))
            pos_type = r.get('position_type', '观望')
            
            # 颜色逻辑
            if amt > 0: 
                border_color = "#e53935" # 更亮的红
                bg_color = "rgba(40, 10, 10, 0.6)"
                act_html = f"<span style='color:#ff8a80;font-weight:bold;font-size:16px'>+{amt}</span>"
            elif is_sell: 
                border_color = "#43a047" # 更亮的绿
                bg_color = "rgba(10, 30, 10, 0.6)"
                act_html = f"<span style='color:#a5d6a7;font-weight:bold;font-size:16px'>-{sell_val}</span>"
            else: 
                border_color = "#757575" # 更亮的灰
                bg_color = "rgba(30, 30, 30, 0.6)"
                act_html = "<span style='color:#bdbdbd;font-weight:bold'>HOLD</span>"

            # 估值描述 (防御性获取)
            val_desc = tech.get('valuation_desc', '暂无估值')
            if "低估" in val_desc or "机会" in val_desc: val_style = "color:#a5d6a7;font-weight:bold;"
            elif "高估" in val_desc or "泡沫" in val_desc: val_style = "color:#ef5350;font-weight:bold;"
            else: val_style = "color:#e0e0e0;" # 提亮普通文字

            # 理由标签 (提亮边框和文字)
            reasons_html = " ".join([f"<span style='border:1px solid #777;padding:2px 4px;font-size:11px;border-radius:3px;color:#eee;margin-right:4px;background:#333;'>{x}</span>" for x in tech.get('quant_reasons', [])])
            
            # 技术指标 (使用 .get 避免 Key Error)
            rsi = tech.get('rsi', '-')
            macd = tech.get('macd', {}).get('trend', '-')
            obv_slope = tech.get('flow', {}).get('obv_slope', 0)
            obv_str = '流入' if obv_slope > 0 else '流出'
            wkly = tech.get('trend_weekly', '-')
            
            # V12.3 风控指标
            vol_ratio = risk.get('vol_ratio', 1.0)
            pct_b = risk.get('bollinger_pct_b', 0.5)
            div = risk.get('divergence', '无')
            
            # AI 分析
            ai_txt = ""
            ai_comm = r.get('ai_analysis', {}).get('comment')
            if ai_comm:
                ai_txt = f"<div style='font-size:13px;color:#d7ccc8;margin-top:10px;padding:10px;background:rgba(255,255,255,0.05);border-left:3px solid #ffb74d;line-height:1.5;'><strong>✦ 洞察:</strong> {ai_comm}</div>"

            # --- HTML 组装 (增大字号，提亮颜色) ---
            rows += f"""
            <div style="background:{bg_color};border-left:5px solid {border_color};margin-bottom:20px;padding:15px;border-radius:6px;box-shadow:0 2px 5px rgba(0,0,0,0.5);border-top:1px solid #444;">
                
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
                    <div>
                        <span style="font-size:20px;font-weight:bold;color:#fff;">{name}</span>
                        <span style="font-size:12px;color:#bbb;margin-left:5px;">{code}</span>
                    </div>
                    <div style="text-align:right;">
                        <span style="color:#ffb74d;font-weight:bold;font-size:18px;">{score}</span> <span style="font-size:10px;color:#888;">分</span>
                    </div>
                </div>

                <div style="display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #555;padding-bottom:10px;margin-bottom:10px;">
                    <span style="font-size:15px;font-weight:bold;color:#ffcc80;">{pos_type}</span>
                    {act_html}
                </div>
                
                <div style="font-size:13px;margin-bottom:10px;background:#222;padding:5px;border-radius:3px;">
                    <span style="color:#bbb;">周期位置:</span> <span style="{val_style}">{val_desc}</span>
                </div>

                <div style="display:grid;grid-template-columns:repeat(4, 1fr);gap:8px;font-size:12px;color:#e0e0e0;font-family:monospace;margin-bottom:8px;">
                    <span>RSI: {rsi}</span>
                    <span>MACD: {macd}</span>
                    <span>OBV: {obv_str}</span>
                    <span>周线: {wkly}</span>
                </div>
                
                <div style="display:grid;grid-template-columns:repeat(3, 1fr);gap:8px;font-size:12px;color:#cfd8dc;font-family:monospace;margin-bottom:10px;border-top:1px dashed #444;padding-top:5px;">
                    <span>量比: {vol_ratio}</span>
                    <span>布林: {pct_b}</span>
                    <span>背离: {div}</span>
                </div>

                <div style="margin-bottom:10px;">{reasons_html}</div>
                {ai_txt}
            </div>
            """
        except Exception as e:
            logger.error(f"渲染行失败 {r.get('name')}: {e}")
            # 容错：即使渲染失败，也输出一个简单的占位符，不让邮件断开
            rows += f"<div style='padding:10px;border:1px solid red;color:red;'>渲染错误: {r.get('name', 'Unknown')}</div>"

    return f"""<!DOCTYPE html><html><body style="background:#121212;color:#e0e0e0;font-family:'Segoe UI', sans-serif;max-width:660px;margin:0 auto;padding:15px;">
    <div style="border:1px solid #444;border-top:4px solid #ffb74d;padding:20px;background:#1e1e1e;border-radius:8px;">
        <h2 style="color:#ffb74d;text-align:center;margin:0 0 5px 0;letter-spacing:1px;">玄铁量化 V13.2</h2>
        <div style="text-align:center;font-size:11px;color:#aaa;margin-bottom:20px;">CYCLE ANCHOR & OMNI-EYE</div>
        
        <div style="background:#252525;padding:12px;border-radius:4px;margin-bottom:20px;border:1px solid #333;">
            <div style="font-size:12px;color:#ffb74d;margin-bottom:8px;font-weight:bold;border-bottom:1px solid #444;padding-bottom:4px;">全球宏观情报</div>
            {macro_html}
        </div>
        
        <div style="background:#263238;padding:15px;border-left:4px solid #ffb74d;margin-bottom:20px;border-radius:2px;font-size:14px;line-height:1.6;">
            {cio}
        </div>
        
        <div style="background:#212121;border:1px dashed #555;padding:15px;margin-bottom:25px;font-size:14px;line-height:1.6;color:#ccc;">
            {advisor}
        </div>
        
        {rows}
        
        <div style="text-align:center;font-size:11px;color:#666;margin-top:30px;">
            In Math We Trust, By AI We Verify.
        </div>
    </div></body></html>"""

def main():
    config = load_config()
    fetcher = DataFetcher()
    scanner = MarketScanner()
    tracker = PortfolioTracker()
    val_engine = ValuationEngine()
    
    logger.info(">>> [V13.2] 启动玄铁量化 (High Contrast UI)...")
    tracker.confirm_trades()
    try: analyst = NewsAnalyst()
    except: analyst = None

    # 获取宏观新闻 (自带灾备)
    macro_news = scanner.get_macro_news()
    macro_str = " | ".join([n['title'] for n in macro_news])
    
    results = []
    cio_lines = [f"市场环境: {macro_str}"]
    
    for fund in config['funds']:
        try:
            logger.info(f"Analyzing {fund['name']}...")
            
            # 1. 获取数据 (失败则跳过)
            data = fetcher.get_fund_history(fund['code'])
            if not data: continue
            
            # 2. 技术分析 (计算失败则跳过)
            tech = TechnicalAnalyzer.calculate_indicators(data)
            if not tech: continue
            
            pos = tracker.get_position(fund['code'])
            
            # 3. 估值分析 (增加异常捕获，失败给默认值)
            try:
                val_mult, val_desc = val_engine.get_valuation_status(
                    fund.get('index_name'), fund.get('strategy_type')
                )
            except Exception as e:
                logger.warning(f"估值模块调用失败: {e}")
                val_mult, val_desc = 1.0, "估值获取异常"
            
            # 4. AI 分析
            ai_adj = 0
            ai_res = {}
            if analyst and (pos['shares']>0 or tech['quant_score']>=60 or tech['quant_score']<=35):
                news = analyst.fetch_news_titles(fund['sector_keyword'])
                ai_res = analyst.analyze_fund_v4(fund['name'], tech, macro_str, news)
                ai_adj = ai_res.get('adjustment', 0)

            # 5. 决策计算
            amt, lbl, is_sell, s_val = calculate_position_v13(
                tech, ai_adj, val_mult, val_desc,
                config['global']['base_invest_amount'], 
                config['global']['max_daily_invest'], 
                pos, fund.get('strategy_type')
            )
            
            # 6. 记录结果
            tracker.record_signal(fund['code'], lbl)
            if amt > 0: tracker.add_trade(fund['code'], fund['name'], amt, tech['price'])
            elif is_sell: tracker.add_trade(fund['code'], fund['name'], s_val, tech['price'], True)
            
            cio_lines.append(f"- {fund['name']}: {lbl} ({val_desc})")
            results.append({
                "name": fund['name'], "code": fund['code'], 
                "amount": amt, "sell_value": s_val, "position_type": lbl, "is_sell": is_sell, 
                "tech": tech, "ai_analysis": ai_res, # tech 字典里现在包含了 valuation_desc
                "history": tracker.get_signal_history(fund['code'])
            })
            
            time.sleep(1) 
            
        except Exception as e: 
            logger.error(f"处理基金 {fund.get('name')} 时发生未知错误: {e}")
            # 继续处理下一个基金，不要崩溃

    if results:
        results.sort(key=lambda x: -x['tech'].get('final_score', 0))
        cio = analyst.review_report("\n".join(cio_lines)) if analyst else ""
        adv = analyst.advisor_review("\n".join(cio_lines), macro_str) if analyst else ""
        
        html = render_html_report_v13(macro_news, results, cio, adv)
        send_email("🗡️ 玄铁量化 V13.2 周期手谕", html)

if __name__ == "__main__": main()
