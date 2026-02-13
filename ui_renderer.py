import datetime
import re
import os
import base64
from utils import logger

def _md_to_html(text):
    """
    [工具] 深度 Markdown 清洗器 (防止 H5 代码残留)
    """
    if not text: return ""
    text = str(text)
    
    # 1. 基础清理
    text = text.strip()
    
    # 2. 转义 HTML (防止 <script> 等注入，但保留基础格式)
    text = text.replace("```html", "").replace("```", "")
    
    # 3. 样式化 Markdown (适配深色主题配色)
    # **加粗** -> <b>
    text = re.sub(r'\*\*(.*?)\*\*', r'<b style="color:#fab005;">\1</b>', text)
    # *斜体* -> <i>
    text = re.sub(r'\*(.*?)\*', r'<i>\1</i>', text)
    # ## 标题 -> 强调用色 (金色)
    text = re.sub(r'^#+\s*(.*?)$', r'<div style="color:#fab005; font-weight:bold; margin-top:5px;">\1</div>', text, flags=re.MULTILINE)
    # - 列表 -> •
    text = re.sub(r'^\s*[\-\*]\s+', '• ', text, flags=re.MULTILINE)
    
    # 4. 换行处理 (把 \n 变成 HTML 换行)
    text = text.replace('\n', '<br>')
    
    return text

def render_html_report_v19(news_list, results, cio_review, advisor_review):
    """
    V19.0 UI 渲染器 - V19.3 结构 + V15.20 深色金融配色 + Logo 嵌入
    """
    
    # --- V15.20 配色方案 ---
    COLOR_GOLD = "#fab005" 
    COLOR_RED = "#fa5252"  
    COLOR_GREEN = "#51cf66" 
    COLOR_TEXT_MAIN = "#e9ecef"
    COLOR_TEXT_SUB = "#adb5bd"
    COLOR_BG_MAIN = "#0f1215" 
    COLOR_BG_CARD = "#16191d"
    COLOR_BORDER = "#2c3e50"
    
    css = f"""
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; 
               background-color: {COLOR_BG_MAIN}; margin: 0; padding: 0; color: {COLOR_TEXT_MAIN}; }}
        .wrapper {{ width: 100%; background-color: {COLOR_BG_MAIN}; padding: 20px 0; }}
        .container {{ max-width: 650px; margin: 0 auto; background-color: #0a0c0e; 
                      border: 1px solid {COLOR_BORDER}; border-radius: 8px; overflow: hidden; 
                      box-shadow: 0 4px 20px rgba(0,0,0,0.8); }}
        
        .header {{ background: linear-gradient(135deg, #16191d 0%, #0f1215 100%); 
                   padding: 25px; text-align: center; color: {COLOR_TEXT_MAIN}; 
                   border-bottom: 1px solid {COLOR_BORDER}; }}
        .header h1 {{ margin: 0; font-size: 22px; font-weight: 600; color: {COLOR_GOLD}; letter-spacing: 1px; }}
        .date-line {{ font-size: 13px; color: {COLOR_TEXT_SUB}; margin-top: 8px; opacity: 0.8; }}
        
        .section-box {{ padding: 20px; border-bottom: 1px solid #25282c; }}
        .section-title {{ font-size: 16px; font-weight: bold; color: {COLOR_GOLD}; 
                         border-left: 4px solid {COLOR_GOLD}; padding-left: 10px; margin-bottom: 15px; }}
        .content-text {{ font-size: 14px; line-height: 1.6; color: {COLOR_TEXT_MAIN}; }}
        
        /* 基金卡片 - 深色主题 */
        .fund-card {{ border: 1px solid #25282c; border-radius: 8px; margin-bottom: 20px; 
                      overflow: hidden; box-shadow: 0 4px 10px rgba(0,0,0,0.5); 
                      background: {COLOR_BG_CARD}; border-left: 3px solid {COLOR_GOLD}; }}
        .card-head {{ background: rgba(0,0,0,0.2); padding: 12px 15px; 
                      display: flex; justify-content: space-between; align-items: center; 
                      border-bottom: 1px solid #333; }}
        .fund-name {{ font-size: 16px; font-weight: bold; color: {COLOR_TEXT_MAIN}; }}
        .fund-code {{ font-size: 12px; color: {COLOR_TEXT_SUB}; margin-left: 5px; }}
        
        /* 徽章 - 适配深色 */
        .badge {{ padding: 3px 10px; border-radius: 4px; font-size: 12px; font-weight: bold; 
                  color: white; text-transform: uppercase; border: 1px solid transparent; }}
        .bg-red {{ background-color: rgba(250, 82, 82, 0.15); color: {COLOR_RED}; border-color: {COLOR_RED}; }}
        .bg-green {{ background-color: rgba(81, 207, 102, 0.15); color: {COLOR_GREEN}; border-color: {COLOR_GREEN}; }}
        .bg-gray {{ background-color: rgba(255, 255, 255, 0.05); color: {COLOR_TEXT_SUB}; border-color: #495057; }}
        .bg-orange {{ background-color: rgba(250, 176, 5, 0.15); color: {COLOR_GOLD}; border-color: {COLOR_GOLD}; }}
        
        /* 模式标签 */
        .mode-label {{ font-size: 10px; padding: 2px 6px; border-radius: 3px; 
                       border: 1px solid #444; margin-right: 5px; background: rgba(255,255,255,0.05); 
                       color: {COLOR_TEXT_SUB}; }}
        
        /* 量化仪表盘 (Grid Layout) */
        .quant-grid {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; padding: 15px; background: transparent; }}
        .q-item {{ display: flex; flex-direction: column; padding: 8px; 
                   background: rgba(0,0,0,0.2); border-radius: 4px; border: 1px solid #333; }}
        .q-label {{ font-size: 10px; color: {COLOR_TEXT_SUB}; text-transform: uppercase; margin-bottom: 4px; }}
        .q-val {{ font-size: 14px; font-weight: 600; color: {COLOR_TEXT_MAIN}; }}
        .q-val.pos {{ color: {COLOR_RED}; }}
        .q-val.neg {{ color: {COLOR_GREEN}; }}
        .q-val.gold {{ color: {COLOR_GOLD}; }}
        
        /* 逻辑区域 */
        .logic-area {{ padding: 15px; border-top: 1px dashed #333; background: rgba(0,0,0,0.2); }}
        .logic-head {{ font-size: 12px; font-weight: bold; color: {COLOR_GOLD}; margin-bottom: 8px; }}
        .logic-body {{ font-size: 13px; color: {COLOR_TEXT_SUB}; line-height: 1.5; }}
        
        /* 战术指令 */
        .tactical-note {{ margin-top: 10px; padding: 10px; background: rgba(52, 152, 219, 0.1); 
                          border-radius: 4px; font-size: 12px; color: #74c0fc; 
                          border-left: 3px solid #3498db; }}
        
        /* 事件倒计时 */
        .event-countdown {{ margin-top: 8px; font-size: 12px; color: {COLOR_GOLD}; font-weight: bold; }}
        
        /* 底部 */
        .footer {{ text-align: center; padding: 20px; font-size: 11px; color: #444; background: transparent; }}
        
        /* Logo 区域 */
        .logo-area {{ text-align: center; margin-bottom: 15px; }}
        .logo-area img {{ width: 200px; max-width: 80%; display: block; margin: 0 auto; filter: brightness(0.9); }}
        .tagline {{ font-size: 10px; color: {COLOR_GOLD}; letter-spacing: 2px; margin-top: 10px; 
                    text-transform: uppercase; opacity: 0.8; }}
        
        /* 手机适配 */
        @media only screen and (max-width: 600px) {{
            .container {{ width: 100% !important; border-radius: 0; border-left: none; border-right: none; }}
            .quant-grid {{ grid-template-columns: 1fr 1fr; }}
            .wrapper {{ padding: 0; }}
        }}
        
        /* 强制覆盖 AI 生成内容的背景色 */
        .cio-content, .advisor-content {{ line-height: 1.6; font-size: 14px; color: {COLOR_TEXT_MAIN} !important; }}
        .cio-content *, .advisor-content * {{ background: transparent !important; color: inherit !important; }}
    </style>
    """
    
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # --- Logo 智能处理 (Base64 嵌入) ---
    logo_path = "logo.png"
    alt_logo_path = "Gemini_Generated_Image_d7oeird7oeird7oe.jpg"
    logo_src = "https://raw.githubusercontent.com/kken61291-eng/Fund-AI-Advisor/main/logo.png"  # 兜底链接
    
    target_logo = logo_path if os.path.exists(logo_path) else (alt_logo_path if os.path.exists(alt_logo_path) else None)
    
    if target_logo:
        try:
            with open(target_logo, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
                mime = "image/png" if target_logo.endswith('png') else "image/jpeg"
                logo_src = f"data:{mime};base64,{b64}"
                logger.info(f"🎨 Logo 已通过 Base64 嵌入: {target_logo}")
        except Exception as e:
            logger.error(f"Logo 嵌入失败: {e}")
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Fund AI Report V15.20</title>
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
                    <div class="date-line">{current_date} | V20.20 洞察微澜，御风而行</div>
                    <div class="tagline">MAGPIE SENSES THE WIND</div>
                </div>
    """
    
    # 1. CIO 战略复盘
    if cio_review:
        cio_clean = _md_to_html(cio_review)
        html += f"""
                <div class="section-box">
                    <div class="section-title">🧠 CIO 战略研判</div>
                    <div class="content-text cio-content">{cio_clean}</div>
                </div>
        """
        
    # 2. Advisor 复盘 (V15.20 特色)
    if advisor_review:
        advisor_clean = _md_to_html(advisor_review)
        html += f"""
                <div class="section-box" style="border-left: 3px solid {COLOR_GOLD};">
                    <div class="section-title">🐦 鹊知风·实战复盘</div>
                    <div class="content-text advisor-content">{advisor_clean}</div>
                </div>
        """
        
    # 3. 基金卡片列表
    html += '<div class="section-box" style="background:rgba(0,0,0,0.2);">'
    
    for res in results:
        # 解包数据
        name = res['name']
        code = res['code']
        decision = res.get('decision', 'HOLD')
        amount = res.get('amount', 0)
        
        tech = res.get('tech', {})
        ai_full = res.get('ai_full', {})
        meta = ai_full.get('strategy_meta', {})
        trend = ai_full.get('trend_analysis', {})
        
        # 核心字段
        score = tech.get('quant_score', 0)
        rsi = tech.get('rsi', 0)
        ma_align = tech.get('ma_alignment', '-')
        vol_status = tech.get('volatility_status', '-')
        recent_gain = tech.get('recent_gain', 0)
        macd_trend = tech.get('macd', {}).get('trend', '-')
        
        # 模式与逻辑
        mode = meta.get('mode', 'WAIT')
        rationale = _md_to_html(meta.get('rationale', '无核心逻辑'))
        exec_note = _md_to_html(ai_full.get('execution_notes', ''))
        
        # 样式判定
        badge_cls, badge_txt = "bg-gray", "☕ 观望"
        if decision == "EXECUTE" or "买入" in decision:
            badge_cls, badge_txt = "bg-red", f"⚡ 买入 ¥{amount:,}"
        elif decision == "SELL" or "卖出" in decision:
            badge_cls, badge_txt = "bg-green", "💰 卖出"
        elif decision == "HOLD_CASH" or "空仓" in decision:
            badge_cls, badge_txt = "bg-gray", "☕ 空仓防御"
            
        gain_cls = "pos" if recent_gain > 0 else "neg"
        
        # --- 卡片 HTML 构造 ---
        html += f"""
        <div class="fund-card">
            <div class="card-head">
                <div>
                    <span class="mode-label">{mode}</span>
                    <span class="fund-name">{name}</span>
                    <span class="fund-code">{code}</span>
                </div>
                <span class="badge {badge_cls}">{badge_txt}</span>
            </div>
            
            <div class="quant-grid">
                <div class="q-item">
                    <span class="q-label">量化评分</span>
                    <span class="q-val gold">{score}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">5日涨幅</span>
                    <span class="q-val {gain_cls}">{recent_gain}%</span>
                </div>
                <div class="q-item">
                    <span class="q-label">RSI (14)</span>
                    <span class="q-val">{rsi}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">均线排列</span>
                    <span class="q-val">{ma_align}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">波动状态</span>
                    <span class="q-val">{vol_status}</span>
                </div>
                <div class="q-item">
                    <span class="q-label">MACD趋势</span>
                    <span class="q-val">{macd_trend}</span>
                </div>
            </div>
            
            <div class="logic-area">
                <div class="logic-head">💡 投委会逻辑</div>
                <div class="logic-body">{rationale}</div>
        """
        
        # 如果有战术指令 (Execution Notes)
        if exec_note and len(exec_note) > 2:
            html += f"""
                <div class="tactical-note">
                    <strong>🎯 执行战术：</strong>{exec_note}
                </div>
            """
            
        # 如果有事件倒计时
        days = trend.get('days_to_event', 'NULL')
        if str(days) not in ['NULL', 'None', '']:
             html += f"""
                <div class="event-countdown">
                    ⏳ 距离关键事件还有 {days} 天
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
                    <div class="section-title">📰 市场热点摘要</div>
                    <ul style="padding-left:15px; margin:0; font-size:13px; color:{COLOR_TEXT_SUB}; list-style: none;">
    """
    for news in news_list[:5]:
        clean_news = _md_to_html(news)
        if len(clean_news) > 5:
            html += f"<li style='margin-bottom:8px; border-bottom:1px solid #25282c; padding-bottom:5px;'><span style='color:{COLOR_GOLD}; margin-right:4px;'>●</span>{clean_news[:100]}...</li>"
            
    html += f"""
                    </ul>
                </div>
                
                <div class="footer">
                    EST. 2026 | POWERED BY AI | MAGPIE SENSES THE WIND
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html
