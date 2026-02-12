import datetime
import re

def _md_to_html(text):
    """
    [工具] 深度 Markdown 清洗器 (防止 H5 代码残留)
    """
    if not text: return ""
    text = str(text)
    
    # 1. 基础清理
    text = text.strip()
    
    # 2. 转义 HTML (防止 <script> 等注入，但保留基础格式)
    # 注意：我们稍后会自己生成 HTML 标签，所以这里先不完全转义，
    # 而是针对性处理 LLM 可能输出的乱码
    text = text.replace("```html", "").replace("```", "")
    
    # 3. 样式化 Markdown
    # **加粗** -> <b>
    text = re.sub(r'\*\*(.*?)\*\*', r'<b style="color:#333;">\1</b>', text)
    # *斜体* -> <i>
    text = re.sub(r'\*(.*?)\*', r'<i>\1</i>', text)
    # ## 标题 -> 强调用色
    text = re.sub(r'^#+\s*(.*?)$', r'<div style="color:#2c3e50; font-weight:bold; margin-top:5px;">\1</div>', text, flags=re.MULTILINE)
    # - 列表 -> •
    text = re.sub(r'^\s*[\-\*]\s+', '• ', text, flags=re.MULTILINE)
    
    # 4. 换行处理 (把 \n 变成 HTML 换行)
    text = text.replace('\n', '<br>')
    
    return text

def render_html_report_v19(news_list, results, cio_review, advisor_review):
    """
    V19.3 UI 渲染器 - 全指标展示版
    """
    
    css = """
    <style>
        body { font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f4f4f4; margin: 0; padding: 0; color: #333; }
        .wrapper { width: 100%; background-color: #f4f4f4; padding: 20px 0; }
        .container { max-width: 650px; margin: 0 auto; background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.05); }
        
        .header { background: #2c3e50; padding: 25px; text-align: center; color: white; }
        .header h1 { margin: 0; font-size: 22px; font-weight: 600; }
        .date-line { font-size: 13px; opacity: 0.8; margin-top: 5px; }
        
        .section-box { padding: 20px; border-bottom: 1px solid #eee; }
        .section-title { font-size: 16px; font-weight: bold; color: #2c3e50; border-left: 4px solid #3498db; padding-left: 10px; margin-bottom: 15px; }
        .content-text { font-size: 14px; line-height: 1.6; color: #444; }
        
        /* 基金卡片 */
        .fund-card { border: 1px solid #e0e0e0; border-radius: 8px; margin-bottom: 20px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.02); }
        .card-head { background: #f8f9fa; padding: 12px 15px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #eee; }
        .fund-name { font-size: 16px; font-weight: bold; color: #2c3e50; }
        .fund-code { font-size: 12px; color: #888; margin-left: 5px; }
        
        /* 徽章 */
        .badge { padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; color: white; text-transform: uppercase; }
        .bg-red { background-color: #e74c3c; }
        .bg-green { background-color: #27ae60; }
        .bg-gray { background-color: #95a5a6; }
        .bg-orange { background-color: #f39c12; }
        
        /* 模式标签 */
        .mode-label { font-size: 10px; padding: 2px 6px; border-radius: 3px; border: 1px solid #ddd; margin-right: 5px; background: #fff; color: #555; }
        
        /* 量化仪表盘 (Grid Layout) */
        .quant-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; padding: 15px; background: #fff; }
        .q-item { display: flex; flex-direction: column; }
        .q-label { font-size: 10px; color: #999; text-transform: uppercase; }
        .q-val { font-size: 13px; font-weight: 600; color: #333; }
        .q-val.pos { color: #e74c3c; }
        .q-val.neg { color: #27ae60; }
        
        /* 逻辑区域 */
        .logic-area { padding: 15px; border-top: 1px dashed #eee; background: #fafafa; }
        .logic-head { font-size: 12px; font-weight: bold; color: #666; margin-bottom: 5px; }
        .logic-body { font-size: 13px; color: #444; line-height: 1.5; }
        
        /* 战术指令 */
        .tactical-note { margin-top: 10px; padding: 8px; background: #e8f4fd; border-radius: 4px; font-size: 12px; color: #2980b9; border-left: 3px solid #3498db; }
        
        /* 底部 */
        .footer { text-align: center; padding: 20px; font-size: 11px; color: #aaa; background: #f4f4f4; }
        
        /* 手机适配 */
        @media only screen and (max-width: 600px) {
            .container { width: 100% !important; border-radius: 0; }
            .quant-grid { grid-template-columns: 1fr 1fr; } /* 手机上两列显示 */
        }
    </style>
    """
    
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Fund AI Report</title>
        {css}
    </head>
    <body>
        <div class="wrapper">
            <div class="container">
                <div class="header">
                    <h1>🦅 鹊知风全量化日报</h1>
                    <div class="date-line">{current_date} | v19.3 全指标透视</div>
                </div>
    """
    
    # 1. CIO 战略复盘
    if cio_review:
        html += f"""
                <div class="section-box">
                    <div class="section-title">🧠 CIO 战略研判</div>
                    <div class="content-text">{_md_to_html(cio_review)}</div>
                </div>
        """
        
    # 2. 基金卡片列表
    html += '<div class="section-box" style="background:#f9f9f9;">'
    
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
        badge_cls, badge_txt = "bg-gray", "观望"
        if decision == "EXECUTE" or "买入" in decision:
            badge_cls, badge_txt = "bg-red", f"买入 ¥{amount}"
        elif decision == "SELL" or "卖出" in decision:
            badge_cls, badge_txt = "bg-green", "卖出"
        elif decision == "HOLD_CASH" or "空仓" in decision:
            badge_cls, badge_txt = "bg-gray", "空仓防御"
            
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
                    <span class="q-val" style="color:#3498db">{score}</span>
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
        if str(days) != 'NULL' and str(days) != 'None':
             html += f"""
                <div style="margin-top:8px; font-size:12px; color:#8e44ad; font-weight:bold;">
                    ⏳ 距离关键事件还有 {days} 天
                </div>
            """
            
        html += """
            </div>
        </div>
        """
        
    html += '</div>' # end section-box
    
    # 3. 底部新闻
    html += """
                <div class="section-box">
                    <div class="section-title">📰 市场热点摘要</div>
                    <ul style="padding-left:15px; margin:0; font-size:13px; color:#555;">
    """
    for news in news_list[:5]:
        clean_news = _md_to_html(news)
        if len(clean_news) > 5:
            html += f"<li style='margin-bottom:8px;'>{clean_news[:100]}...</li>"
            
    html += """
                    </ul>
                </div>
                
                <div class="footer">
                    &copy; 2026 Fund AI Advisor | Generated by DeepSeek-V3.2 & R1
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html
