import datetime
import re
import os
import base64
import json
from utils import logger

def _translate_term(term):
    """翻译常见的英文专业术语"""
    mapping = {
        "BULLISH": "多头向上", "BEARISH": "空头向下", "MIXED": "震荡/缠绕",
        "UP": "向上", "DOWN": "向下",
        "HIGH": "高", "MEDIUM": "中", "LOW": "低",
        "A": "A轨 (趋势进攻)", "B": "B轨 (超跌反转)", "C": "C轨 (事件驱动)", "D": "D轨 (防守观望)"
    }
    return mapping.get(str(term).upper(), str(term))

def _md_to_html(text):
    """深度 Markdown 清洗器"""
    if not text: return ""
    text = str(text)
    text = text.replace("```html", "").replace("```", "")
    text = re.sub(r'\*\*(.*?)\*\*', r'<b style="color:#00f0ff;">\1</b>', text)
    text = re.sub(r'\*(.*?)\*', r'<i style="color:#05d59e;">\1</i>', text)
    text = re.sub(r'^#+\s*(.*?)$', r'<div style="color:#00d2ff; font-weight:bold; margin-top:5px;">\1</div>', text, flags=re.MULTILINE)
    text = re.sub(r'^\s*[\-\*]\s+', '💠 ', text, flags=re.MULTILINE)
    text = text.replace('\n', '<br>')
    return text

def _parse_cio_json_to_html(text):
    """将 CIO 的 JSON 代码解析为精美的中文 HTML 阅读格式"""
    if not text: return ""
    
    json_str = text
    match = re.search(r'\{.*\}', text, flags=re.DOTALL)
    if match:
        json_str = match.group(0)
        
    try:
        data = json.loads(json_str)
        html = '<div style="font-family: -apple-system, sans-serif;">'
        
        cio_rev = data.get("cio_strategic_review", {})
        if cio_rev:
            html += f"""
            <div style="margin-bottom: 20px;">
                <div style="font-size: 14px; font-weight: bold; color: #00f0ff; margin-bottom: 8px;">📊 宏观战略定调</div>
                <ul style="list-style: none; padding-left: 0; margin: 0; color: #e0e6ed; font-size: 13px; line-height: 1.8;">
                    <li><span style="color:#8a9bb2;">市场风险水位：</span><b style="color:#ff2a6d;">{cio_rev.get('market_risk_level', '-')}</b></li>
                    <li><span style="color:#8a9bb2;">水位判定依据：</span>{cio_rev.get('risk_rationale', '-')}</li>
                    <li><span style="color:#8a9bb2;">主导战略姿态：</span><b style="color:#00f0ff;">{cio_rev.get('strategic_stance', '-')}</b></li>
                    <li><span style="color:#8a9bb2;">强制防守底线：</span>底仓现金及等价物不低于 <b>{cio_rev.get('constraints', {}).get('cash_ratio_min', '-')}</b></li>
                </ul>
            </div>
            """
            
        defense = data.get("defensive_allocation", {})
        if defense:
            html += f"""
            <div style="margin-bottom: 20px; background: rgba(0,240,255,0.05); border-left: 2px solid #00f0ff; padding: 10px;">
                <div style="font-size: 13px; font-weight: bold; color: #00f0ff; margin-bottom: 5px;">🛡️ 资产防御配置方案</div>
                <div style="font-size: 13px; color: #e0e6ed; line-height: 1.6;">
                    💠 <b>现金管理：</b>{defense.get('cash_management', '-')} <br>
                    💠 <b>对冲工具：</b>{defense.get('hedge_instruments', '-')} <br>
                    💠 <b>期权保护：</b>{defense.get('options_protection', '-')}
                </div>
            </div>
            """
            
        assump = data.get("assumption_monitoring", {})
        if assump:
            premises = assump.get("premises", [])
            premises_html = "".join([f"<li>{p}</li>" for p in premises])
            html += f"""
            <div style="margin-bottom: 15px;">
                <div style="font-size: 14px; font-weight: bold; color: #00f0ff; margin-bottom: 8px;">⚠️ 核心假设与风控监控池</div>
                <div style="font-size: 13px; color: #e0e6ed;"><b>当前策略成立的前提条件：</b></div>
                <ul style="color: #8a9bb2; font-size: 13px; line-height: 1.6; padding-left: 20px;">
                    {premises_html}
                </ul>
                <div style="font-size: 13px; color: #ff2a6d; margin-top: 5px;">
                    <b>🚨 紧急预案：</b>{assump.get('emergency_plan', '-')}
                </div>
            </div>
            """
            
        vetoes = data.get("strategic_veto_list", [])
        if vetoes:
            html += '<div style="font-size: 14px; font-weight: bold; color: #ff2a6d; margin-bottom: 8px; margin-top: 15px;">❌ 被风控系统一票否决的提案</div>'
            for v in vetoes:
                html += f"""
                <div style="font-size: 12px; border: 1px dashed #ff2a6d; padding: 8px; margin-bottom: 5px; color: #8a9bb2; background: rgba(255,42,109,0.05);">
                    <b style="color: #e0e6ed;">[{v.get('code', '-')}]</b> 否决原因：{v.get('cio_veto_reason', v.get('risk_committee_reason', '-'))} <br>
                    <i style="color: #05d59e;">替代建议：{v.get('suggested_alternative', '-')}</i>
                </div>
                """
                
        html += '</div>'
        return html
    except Exception as e:
        logger.warning(f"CIO JSON 解析失败，回退为普通文本: {e}")
        return _md_to_html(text) 

def render_html_report_v19(news_list, results, cio_review, advisor_review):
    """V20.0 UI 渲染器 - 全中文赛博美学版 (鹊知风)"""
    
    COLOR_CYBER = "#00d2ff"     
    COLOR_CYBER_SEC = "#00f0ff" 
    COLOR_RED = "#ff2a6d"       
    COLOR_GREEN = "#05d59e"     
    COLOR_TEXT_MAIN = "#e0e6ed" 
    COLOR_TEXT_SUB = "#8a9bb2"  
    COLOR_BG_MAIN = "#06090e"   
    COLOR_BG_CARD = "#0d131a"   
    COLOR_BORDER = "#1a2634"    
    
    css = f"""
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; 
               background-color: {COLOR_BG_MAIN}; margin: 0; padding: 0; color: {COLOR_TEXT_MAIN}; }}
        .wrapper {{ width: 100%; background-color: {COLOR_BG_MAIN}; padding: 20px 0; }}
        .container {{ max-width: 650px; margin: 0 auto; background-color: #080c12; 
                      border: 1px solid {COLOR_BORDER}; border-radius: 4px; overflow: hidden; 
                      box-shadow: 0 0 25px rgba(0, 210, 255, 0.1); }}
        
        .header {{ background: linear-gradient(180deg, #0d131a 0%, #06090e 100%); 
                   padding: 30px; text-align: center; color: {COLOR_TEXT_MAIN}; 
                   border-bottom: 1px solid {COLOR_BORDER}; position: relative; overflow: hidden; }}
        .header::before {{ content: ''; position: absolute; top: 0; left: 0; right: 0; height: 1px; 
                           background: linear-gradient(90deg, transparent, {COLOR_CYBER}, transparent); }}
        
        .header h1 {{ margin: 0; font-size: 24px; font-weight: 700; color: {COLOR_CYBER}; letter-spacing: 2px; }}
        .date-line {{ font-size: 12px; color: {COLOR_TEXT_SUB}; margin-top: 10px; font-family: monospace; }}
        
        .section-box {{ padding: 25px 20px; border-bottom: 1px solid {COLOR_BORDER}; }}
        .section-title {{ font-size: 15px; font-weight: bold; color: {COLOR_CYBER}; font-family: monospace;
                          border-left: 3px solid {COLOR_CYBER_SEC}; padding-left: 10px; margin-bottom: 15px; 
                          text-transform: uppercase; letter-spacing: 1px; }}
        .content-text {{ font-size: 14px; line-height: 1.7; color: {COLOR_TEXT_MAIN}; }}
        
        .fund-card {{ border: 1px solid {COLOR_BORDER}; border-radius: 4px; margin-bottom: 25px; 
                      background: {COLOR_BG_CARD}; position: relative; }}
        .fund-card::after {{ content: ''; position: absolute; left: -1px; top: 20%; bottom: 20%; width: 2px; background: {COLOR_CYBER}; box-shadow: 0 0 8px {COLOR_CYBER}; }}
        
        .card-head {{ background: rgba(0, 210, 255, 0.03); padding: 15px; 
                      display: flex; justify-content: space-between; align-items: center; 
                      border-bottom: 1px dashed {COLOR_BORDER}; }}
        .fund-name {{ font-size: 16px; font-weight: bold; color: {COLOR_TEXT_MAIN}; letter-spacing: 0.5px; }}
        .fund-code {{ font-size: 11px; color: {COLOR_TEXT_SUB}; margin-left: 8px; font-family: monospace; background: rgba(255,255,255,0.05); padding: 2px 5px; border-radius: 2px; }}
        
        .badge {{ padding: 4px 10px; border-radius: 2px; font-size: 11px; font-weight: 700; font-family: monospace;
                  color: white; border: 1px solid transparent; letter-spacing: 1px; }}
        .bg-red {{ background-color: rgba(255, 42, 109, 0.1); color: {COLOR_RED}; border-color: {COLOR_RED}; box-shadow: 0 0 10px rgba(255,42,109,0.2); }}
        .bg-green {{ background-color: rgba(5, 213, 158, 0.1); color: {COLOR_GREEN}; border-color: {COLOR_GREEN}; box-shadow: 0 0 10px rgba(5,213,158,0.2); }}
        .bg-gray {{ background-color: rgba(138, 155, 178, 0.1); color: {COLOR_TEXT_SUB}; border-color: #3b4958; }}
        
        .mode-label {{ font-size: 10px; padding: 2px 6px; border-radius: 2px; 
                       border: 1px solid {COLOR_CYBER_SEC}; margin-right: 8px; background: rgba(0,240,255,0.1); 
                       color: {COLOR_CYBER_SEC}; box-shadow: 0 0 5px rgba(0,240,255,0.3); }}
        
        .quant-grid {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; padding: 15px; background: transparent; }}
        .q-item {{ display: flex; flex-direction: column; padding: 10px; 
                   background: #090e14; border-radius: 2px; border: 1px solid #141e2a; }}
        .q-label {{ font-size: 10px; color: {COLOR_TEXT_SUB}; margin-bottom: 6px; letter-spacing: 0.5px; }}
        .q-val {{ font-size: 13px; font-weight: 600; color: {COLOR_TEXT_MAIN}; }}
        .q-val.pos {{ color: {COLOR_RED}; text-shadow: 0 0 5px rgba(255,42,109,0.4); }}
        .q-val.neg {{ color: {COLOR_GREEN}; text-shadow: 0 0 5px rgba(5,213,158,0.4); }}
        .q-val.cyber {{ color: {COLOR_CYBER}; text-shadow: 0 0 5px rgba(0,210,255,0.4); }}
        
        .logic-area {{ padding: 15px; border-top: 1px dashed {COLOR_BORDER}; background: #080c12; }}
        .logic-head {{ font-size: 12px; font-weight: bold; color: {COLOR_CYBER_SEC}; margin-bottom: 8px; }}
        .logic-body {{ font-size: 13px; color: {COLOR_TEXT_SUB}; line-height: 1.6; }}
        
        .tactical-note {{ margin-top: 12px; padding: 12px; background: rgba(0, 210, 255, 0.05); 
                          border-radius: 2px; font-size: 12px; color: #a1b8d1; 
                          border-left: 2px solid {COLOR_CYBER}; line-height: 1.6; }}
        
        .event-countdown {{ margin-top: 10px; font-size: 12px; color: {COLOR_CYBER_SEC}; font-weight: bold; }}
        
        .footer {{ text-align: center; padding: 25px; font-size: 11px; color: #4b5d73; background: transparent; letter-spacing: 1px; }}
        
        .logo-area {{ text-align: center; margin-bottom: 15px; }}
        .logo-area img {{ width: 220px; max-width: 80%; display: block; margin: 0 auto; filter: drop-shadow(0 0 10px rgba(0,210,255,0.3)); }}
        .tagline {{ font-size: 10px; color: {COLOR_CYBER}; letter-spacing: 3px; margin-top: 10px; 
                    text-transform: uppercase; opacity: 0.7; font-family: monospace; }}
    </style>
    """
    
    current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logo_path = "logo.png"
    logo_src = "https://raw.githubusercontent.com/kken61291-eng/Fund-AI-Advisor/main/logo.png"
    
    if os.path.exists(logo_path):
        try:
            with open(logo_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
                logo_src = f"data:image/png;base64,{b64}"
        except Exception as e:
            logger.error(f"Logo 嵌入失败: {e}")
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>鹊知风 - 量化分析报告</title>
        {css}
    </head>
    <body>
        <div class="wrapper">
            <div class="container">
                <div class="header">
                    <div class="logo-area">
                        <img src="{logo_src}" alt="Logo">
                    </div>
                    <h1>🦅 鹊知风全量化日报</h1>
                    <div class="date-line">系统时间: {current_date} | V20.20 洞察微澜，御风而行</div>
                    <div class="tagline">MAGPIE SENSES THE WIND</div>
                </div>
    """
    
    # 1. CIO 战略复盘 (解析 JSON)
    if cio_review:
        cio_clean = _parse_cio_json_to_html(cio_review)
        html += f"""
                <div class="section-box">
                    <div class="section-title">01 // 首席投资官(CIO)战略研判</div>
                    <div class="content-text cio-content">{cio_clean}</div>
                </div>
        """
        
    # 2. Advisor 复盘
    if advisor_review:
        advisor_clean = _md_to_html(advisor_review)
        html += f"""
                <div class="section-box" style="border-left: 2px solid {COLOR_CYBER_SEC};">
                    <div class="section-title">02 // 投顾实战复盘</div>
                    <div class="content-text advisor-content">{advisor_clean}</div>
                </div>
        """
        
    # 3. 基金卡片列表
    html += '<div class="section-box" style="background:rgba(0,0,0,0.1);">'
    html += f'<div class="section-title">03 // 战术投委会(IC)提案详情</div>'
    
    for res in results:
        name = res['name']
        code = res['code']
        decision = res.get('decision', 'HOLD')
        amount = res.get('amount', 0)
        
        tech = res.get('tech', {})
        ai_full = res.get('ai_full', {})
        meta = ai_full.get('strategy_meta', {})
        trend = ai_full.get('trend_analysis', {})
        
        score = tech.get('quant_score', 0)
        rsi = tech.get('rsi', 0)
        ma_align = _translate_term(tech.get('ma_alignment', '-'))
        vol_status = _translate_term(tech.get('volatility_status', '-'))
        recent_gain = tech.get('recent_gain', 0)
        macd_trend = _translate_term(tech.get('macd', {}).get('trend', '-'))
        
        mode = _translate_term(meta.get('mode', 'WAIT'))
        rationale = _md_to_html(meta.get('rationale', '系统未提供逻辑'))
        exec_note = _md_to_html(ai_full.get('execution_notes', ''))
        
        badge_cls, badge_txt = "bg-gray", "[观望防守]"
        if decision == "EXECUTE" or "买入" in decision:
            badge_cls, badge_txt = "bg-red", f"[做多/买入] ¥{amount:,}"
        elif decision == "SELL" or "卖出" in decision:
            badge_cls, badge_txt = "bg-green", "[做空/清仓]"
        elif decision == "HOLD_CASH" or "空仓" in decision:
            badge_cls, badge_txt = "bg-gray", "[强制空仓]"
            
        gain_cls = "pos" if recent_gain > 0 else "neg"
        
        html += f"""
        <div class="fund-card">
            <div class="card-head">
                <div>
                    <span class="mode-label">{mode}</span>
                    <span class="fund-name">{name}</span>
                    <span class="fund-code">[{code}]</span>
                </div>
                <span class="badge {badge_cls}">{badge_txt}</span>
            </div>
            
            <div class="quant-grid">
                <div class="q-item">
                    <span class="q-label">量化评分</span>
                    <span class="q-val cyber">{score}分</span>
                </div>
                <div class="q-item">
                    <span class="q-label">5日涨跌幅</span>
                    <span class="q-val {gain_cls}">{recent_gain}%</span>
                </div>
                <div class="q-item">
                    <span class="q-label">RSI(14)强弱</span>
                    <span class="q-val">{rsi}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">均线格局</span>
                    <span class="q-val">{ma_align}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">波动率水平</span>
                    <span class="q-val">{vol_status}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">MACD趋势</span>
                    <span class="q-val">{macd_trend}</span>
                </div>
            </div>
            
            <div class="logic-area">
                <div class="logic-head">>> 核心决策逻辑</div>
                <div class="logic-body">{rationale}</div>
        """
        
        if exec_note and len(exec_note) > 2:
            html += f"""
                <div class="tactical-note">
                    <div style="color:{COLOR_CYBER}; margin-bottom:5px;"><b>[技术/风控/赔率 三方博弈实录]</b></div>
                    {exec_note}
                </div>
            """
            
        days = trend.get('days_to_event', 'NULL')
        if str(days) not in ['NULL', 'None', '']:
             html += f"""
                <div class="event-countdown">
                    ⏳ 距关键事件发酵还有约 {days} 天
                </div>
            """
            
        html += """
            </div>
        </div>
        """
        
    html += '</div>'
    
    # 4. 底部新闻
    html += f"""
                <div class="section-box">
                    <div class="section-title">04 // 市场情报神经网</div>
                    <ul style="padding-left:10px; margin:0; font-size:12px; color:{COLOR_TEXT_SUB}; list-style: none;">
    """
    for news in news_list[:5]:
        clean_news = _md_to_html(news)
        if len(clean_news) > 5:
            html += f"<li style='margin-bottom:8px; border-bottom:1px dashed {COLOR_BORDER}; padding-bottom:5px;'><span style='color:{COLOR_CYBER_SEC}; margin-right:8px;'>[消息]</span>{clean_news[:100]}...</li>"
            
    html += f"""
                    </ul>
                </div>
                
                <div class="footer">
                    系统内核: FUND-AI-ADVISOR <br>
                    运行节点: KKEN61291-ENG <br>
                    © 2026 鹊知风. MAGPIE SENSES THE WIND.
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html
