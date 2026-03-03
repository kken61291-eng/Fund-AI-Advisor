import datetime
import re
import os
import base64
from utils import logger

def _md_to_html(text):
    """
    [工具] 深度 Markdown 清洗器 (防止 H5 代码残留，适配赛博风格)
    """
    if not text: return ""
    text = str(text)
    
    # 1. 基础清理
    text = text.strip()
    
    # 2. 转义 HTML
    text = text.replace("```html", "").replace("```", "")
    
    # 3. 样式化 Markdown (适配 Cyber 蓝/青配色)
    # **加粗** -> <b> 荧光青
    text = re.sub(r'\*\*(.*?)\*\*', r'<b style="color:#00f0ff;">\1</b>', text)
    # *斜体* -> <i>
    text = re.sub(r'\*(.*?)\*', r'<i style="color:#05d59e;">\1</i>', text)
    # ## 标题 -> 强调用色
    text = re.sub(r'^#+\s*(.*?)$', r'<div style="color:#00d2ff; font-weight:bold; margin-top:5px;">\1</div>', text, flags=re.MULTILINE)
    # - 列表 -> 💠
    text = re.sub(r'^\s*[\-\*]\s+', '💠 ', text, flags=re.MULTILINE)
    
    # 4. 换行处理
    text = text.replace('\n', '<br>')
    
    return text

def render_html_report_v19(news_list, results, cio_review, advisor_review):
    """
    V20.0 UI 渲染器 - Cyber & Mathematical Aesthetic (Variance: Zero)
    """
    
    # --- Variance: Zero 赛博美学配色方案 ---
    COLOR_CYBER = "#00d2ff"     # 科幻蓝 (原金色)
    COLOR_CYBER_SEC = "#00f0ff" # 荧光青
    COLOR_RED = "#ff2a6d"       # 赛博粉红 (用于买入/警示)
    COLOR_GREEN = "#05d59e"     # 矩阵绿 (用于卖出/安全)
    COLOR_TEXT_MAIN = "#e0e6ed" # 亮灰白
    COLOR_TEXT_SUB = "#8a9bb2"  # 灰蓝
    COLOR_BG_MAIN = "#06090e"   # 深邃宇宙黑
    COLOR_BG_CARD = "#0d131a"   # 模块黑
    COLOR_BORDER = "#1a2634"    # 边框线
    
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
        /* 顶部扫描线修饰 */
        .header::before {{ content: ''; position: absolute; top: 0; left: 0; right: 0; height: 1px; 
                           background: linear-gradient(90deg, transparent, {COLOR_CYBER}, transparent); }}
        
        .header h1 {{ margin: 0; font-size: 24px; font-weight: 700; color: {COLOR_CYBER}; letter-spacing: 2px; text-transform: uppercase; }}
        .date-line {{ font-size: 12px; color: {COLOR_TEXT_SUB}; margin-top: 10px; font-family: monospace; }}
        
        .section-box {{ padding: 25px 20px; border-bottom: 1px solid {COLOR_BORDER}; }}
        .section-title {{ font-size: 15px; font-weight: bold; color: {COLOR_CYBER}; font-family: monospace;
                          border-left: 3px solid {COLOR_CYBER_SEC}; padding-left: 10px; margin-bottom: 15px; 
                          text-transform: uppercase; letter-spacing: 1px; }}
        .content-text {{ font-size: 14px; line-height: 1.7; color: {COLOR_TEXT_MAIN}; }}
        
        /* 基金卡片 - 赛博风格 */
        .fund-card {{ border: 1px solid {COLOR_BORDER}; border-radius: 4px; margin-bottom: 25px; 
                      background: {COLOR_BG_CARD}; position: relative; }}
        /* 卡片左侧光晕标记 */
        .fund-card::after {{ content: ''; position: absolute; left: -1px; top: 20%; bottom: 20%; width: 2px; background: {COLOR_CYBER}; box-shadow: 0 0 8px {COLOR_CYBER}; }}
        
        .card-head {{ background: rgba(0, 210, 255, 0.03); padding: 15px; 
                      display: flex; justify-content: space-between; align-items: center; 
                      border-bottom: 1px dashed {COLOR_BORDER}; }}
        .fund-name {{ font-size: 16px; font-weight: bold; color: {COLOR_TEXT_MAIN}; letter-spacing: 0.5px; }}
        .fund-code {{ font-size: 11px; color: {COLOR_TEXT_SUB}; margin-left: 8px; font-family: monospace; background: rgba(255,255,255,0.05); padding: 2px 5px; border-radius: 2px; }}
        
        /* 徽章 - 适配赛博调 */
        .badge {{ padding: 4px 10px; border-radius: 2px; font-size: 11px; font-weight: 700; font-family: monospace;
                  color: white; text-transform: uppercase; border: 1px solid transparent; letter-spacing: 1px; }}
        .bg-red {{ background-color: rgba(255, 42, 109, 0.1); color: {COLOR_RED}; border-color: {COLOR_RED}; box-shadow: 0 0 10px rgba(255,42,109,0.2); }}
        .bg-green {{ background-color: rgba(5, 213, 158, 0.1); color: {COLOR_GREEN}; border-color: {COLOR_GREEN}; box-shadow: 0 0 10px rgba(5,213,158,0.2); }}
        .bg-gray {{ background-color: rgba(138, 155, 178, 0.1); color: {COLOR_TEXT_SUB}; border-color: #3b4958; }}
        
        /* 模式标签 (A/B/C/D轨) */
        .mode-label {{ font-size: 10px; font-family: monospace; padding: 2px 6px; border-radius: 2px; 
                       border: 1px solid {COLOR_CYBER_SEC}; margin-right: 8px; background: rgba(0,240,255,0.1); 
                       color: {COLOR_CYBER_SEC}; box-shadow: 0 0 5px rgba(0,240,255,0.3); }}
        
        /* 量化仪表盘 (Grid Layout) */
        .quant-grid {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; padding: 15px; background: transparent; }}
        .q-item {{ display: flex; flex-direction: column; padding: 10px; 
                   background: #090e14; border-radius: 2px; border: 1px solid #141e2a; }}
        .q-label {{ font-size: 9px; color: {COLOR_TEXT_SUB}; text-transform: uppercase; margin-bottom: 6px; font-family: monospace; letter-spacing: 0.5px; }}
        .q-val {{ font-size: 15px; font-weight: 600; color: {COLOR_TEXT_MAIN}; font-family: monospace; }}
        .q-val.pos {{ color: {COLOR_RED}; text-shadow: 0 0 5px rgba(255,42,109,0.4); }}
        .q-val.neg {{ color: {COLOR_GREEN}; text-shadow: 0 0 5px rgba(5,213,158,0.4); }}
        .q-val.cyber {{ color: {COLOR_CYBER}; text-shadow: 0 0 5px rgba(0,210,255,0.4); }}
        
        /* 逻辑区域 */
        .logic-area {{ padding: 15px; border-top: 1px dashed {COLOR_BORDER}; background: #080c12; }}
        .logic-head {{ font-size: 11px; font-weight: bold; color: {COLOR_CYBER_SEC}; margin-bottom: 8px; font-family: monospace; text-transform: uppercase; }}
        .logic-body {{ font-size: 13px; color: {COLOR_TEXT_SUB}; line-height: 1.6; }}
        
        /* 战术指令与博弈记录 (V19.6 特色) */
        .tactical-note {{ margin-top: 12px; padding: 12px; background: rgba(0, 210, 255, 0.05); 
                          border-radius: 2px; font-size: 12px; color: #a1b8d1; 
                          border-left: 2px solid {COLOR_CYBER}; font-family: monospace; }}
        
        /* 事件倒计时 */
        .event-countdown {{ margin-top: 10px; font-size: 11px; color: {COLOR_CYBER_SEC}; font-weight: bold; font-family: monospace; }}
        
        /* 底部 */
        .footer {{ text-align: center; padding: 25px; font-size: 10px; color: #4b5d73; background: transparent; font-family: monospace; letter-spacing: 1px; }}
        
        /* Logo 区域 */
        .logo-area {{ text-align: center; margin-bottom: 15px; }}
        .logo-area img {{ width: 220px; max-width: 80%; display: block; margin: 0 auto; filter: drop-shadow(0 0 10px rgba(0,210,255,0.3)); }}
        .tagline {{ font-size: 10px; color: {COLOR_CYBER}; letter-spacing: 3px; margin-top: 10px; 
                    text-transform: uppercase; opacity: 0.7; font-family: monospace; }}
        
        /* 强制覆盖 AI 生成内容的背景色 */
        .cio-content, .advisor-content {{ line-height: 1.7; font-size: 14px; color: {COLOR_TEXT_MAIN} !important; }}
        .cio-content *, .advisor-content * {{ background: transparent !important; color: inherit !important; border-color: {COLOR_BORDER} !important; }}
    </style>
    """
    
    current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # --- Logo 智能处理 (基于用户信息适配 GitHub 源) ---
    logo_path = "logo.png"
    # Fallback 到用户的 Fund-AI-Advisor 仓库
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
        <title>Variance: Zero - Quant Report</title>
        {css}
    </head>
    <body>
        <div class="wrapper">
            <div class="container">
                <div class="header">
                    <div class="logo-area">
                        <img src="{logo_src}" alt="Variance: Zero Logo">
                    </div>
                    <h1>💠 VARIANCE: ZERO</h1>
                    <div class="date-line">SYS.DATE: {current_date} | VER: 19.6.5-CYBER</div>
                    <div class="tagline">COGNITIVE ADVERSARIAL MODEL</div>
                </div>
    """
    
    # 1. CIO 战略复盘
    if cio_review:
        cio_clean = _md_to_html(cio_review)
        html += f"""
                <div class="section-box">
                    <div class="section-title">SYS.01 // STRATEGIC_CIO_REVIEW</div>
                    <div class="content-text cio-content">{cio_clean}</div>
                </div>
        """
        
    # 2. Advisor 复盘
    if advisor_review:
        advisor_clean = _md_to_html(advisor_review)
        html += f"""
                <div class="section-box" style="border-left: 2px solid {COLOR_CYBER_SEC};">
                    <div class="section-title">SYS.02 // ADVISOR_LOG</div>
                    <div class="content-text advisor-content">{advisor_clean}</div>
                </div>
        """
        
    # 3. 基金卡片列表
    html += '<div class="section-box" style="background:rgba(0,0,0,0.1);">'
    html += f'<div class="section-title">SYS.03 // TACTICAL_PROPOSALS</div>'
    
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
        ma_align = tech.get('ma_alignment', '-')
        vol_status = tech.get('volatility_status', '-')
        recent_gain = tech.get('recent_gain', 0)
        macd_trend = tech.get('macd', {}).get('trend', '-')
        
        mode = meta.get('mode', 'WAIT')
        rationale = _md_to_html(meta.get('rationale', 'No logic provided'))
        exec_note = _md_to_html(ai_full.get('execution_notes', ''))
        
        badge_cls, badge_txt = "bg-gray", "[DEFENSE] 观望"
        if decision == "EXECUTE" or "买入" in decision:
            badge_cls, badge_txt = "bg-red", f"[LONG] ¥{amount:,}"
        elif decision == "SELL" or "卖出" in decision:
            badge_cls, badge_txt = "bg-green", "[SHORT/EXIT]"
        elif decision == "HOLD_CASH" or "空仓" in decision:
            badge_cls, badge_txt = "bg-gray", "[NULL] 强制空仓"
            
        gain_cls = "pos" if recent_gain > 0 else "neg"
        
        html += f"""
        <div class="fund-card">
            <div class="card-head">
                <div>
                    <span class="mode-label">TRACK_{mode}</span>
                    <span class="fund-name">{name}</span>
                    <span class="fund-code">[{code}]</span>
                </div>
                <span class="badge {badge_cls}">{badge_txt}</span>
            </div>
            
            <div class="quant-grid">
                <div class="q-item">
                    <span class="q-label">QUANT_SCORE</span>
                    <span class="q-val cyber">{score}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">GAIN_5D</span>
                    <span class="q-val {gain_cls}">{recent_gain}%</span>
                </div>
                <div class="q-item">
                    <span class="q-label">RSI_14</span>
                    <span class="q-val">{rsi}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">MA_ALIGN</span>
                    <span class="q-val">{ma_align}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">VOLATILITY</span>
                    <span class="q-val">{vol_status}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">MACD</span>
                    <span class="q-val">{macd_trend}</span>
                </div>
            </div>
            
            <div class="logic-area">
                <div class="logic-head">>> ALGO_RATIONALE</div>
                <div class="logic-body">{rationale}</div>
        """
        
        if exec_note and len(exec_note) > 2:
            html += f"""
                <div class="tactical-note">
                    <div style="color:{COLOR_CYBER}; margin-bottom:5px;"><b>[EXECUTION_MATRIX & DEBATE_LOG]</b></div>
                    {exec_note}
                </div>
            """
            
        days = trend.get('days_to_event', 'NULL')
        if str(days) not in ['NULL', 'None', '']:
             html += f"""
                <div class="event-countdown">
                    [T-{days}] EVENTS APPROACHING...
                </div>
            """
            
        html += """
            </div>
        </div>
        """
        
    html += '</div>' # end section-box
    
    # 4. 底部新闻
    html += f"""
                <div class="section-box">
                    <div class="section-title">SYS.04 // MARKET_DATAFEED</div>
                    <ul style="padding-left:10px; margin:0; font-size:12px; color:{COLOR_TEXT_SUB}; list-style: none; font-family: monospace;">
    """
    for news in news_list[:5]:
        clean_news = _md_to_html(news)
        if len(clean_news) > 5:
            html += f"<li style='margin-bottom:8px; border-bottom:1px dashed {COLOR_BORDER}; padding-bottom:5px;'><span style='color:{COLOR_CYBER_SEC}; margin-right:8px;'>[RAW]</span>{clean_news[:100]}...</li>"
            
    html += f"""
                    </ul>
                </div>
                
                <div class="footer">
                    SYSTEM: FUND-AI-ADVISOR<br>
                    INITIATED BY: KKEN61291-ENG<br>
                    © 2026 VARIANCE: ZERO. ALL VARIANCES CONTROLLED.
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html
