import os
import re
import base64

# --- 配色方案 (深邃极客风) ---
COLOR_GOLD = "#ffd700"       # 更亮的金色
COLOR_RED = "#ff4d4f"        # 警示红
COLOR_GREEN = "#52c41a"      # 极客绿
COLOR_TEXT_MAIN = "#e6f7ff"  # 冷白
COLOR_TEXT_SUB = "#8c8c8c"   # 灰色
COLOR_BG_MAIN = "#000000"    # 纯黑背景
COLOR_BG_CARD = "#141414"    # 卡片深灰
COLOR_BORDER = "#303030"     # 边框色

def clean_markdown(text):
    """
    深度清洗 Markdown 标记，确保显示纯文本
    """
    if not text: return "暂无内容"
    
    # 1. 移除代码块标记 (```html, ```)
    text = re.sub(r'```[a-zA-Z]*', '', text)
    
    # 2. 移除标题标记 (### Title -> Title)
    text = re.sub(r'#+\s+', '', text)
    
    # 3. 移除加粗/斜体 (**text**, *text*)
    text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)
    text = re.sub(r'\*(.*?)\*', r'\1', text)
    
    # 4. 移除列表符号 (- item -> item)
    text = re.sub(r'^\s*-\s+', '', text, flags=re.MULTILINE)
    
    # 5. 移除 HTML 标签 (<br>, <p>) 防止冲突
    text = re.sub(r'<[^>]+>', '', text)
    
    return text.strip()

def render_html_report_v17(all_news, results, cio_html, advisor_html):
    """
    [V17.0 UI 引擎] 生成全量化仪表盘 HTML
    """
    # 1. 深度清洗 Markdown
    cio_html = clean_markdown(cio_html)
    advisor_html = clean_markdown(advisor_html)
    
    # 2. 新闻列表 (终端风格)
    news_items = ""
    for n in all_news[:15]: # 只取前15条
        news_items += f'<div style="padding:4px 0;border-bottom:1px dashed #333;color:{COLOR_TEXT_SUB};"><span style="color:{COLOR_GOLD};margin-right:6px;">›</span>{n}</div>'
    
    # 3. 生成 ETF 卡片
    rows = ""
    for r in results:
        tech = r.get('tech', {})
        ai_data = r.get('ai_analysis', {})
        
        # --- A. AI 观点提取 & 清洗 ---
        bull_say = clean_markdown(ai_data.get('cgo_proposal', {}).get('catalyst', '无明显催化'))
        bear_say = clean_markdown(ai_data.get('cro_audit', {}).get('max_drawdown_scenario', '无'))
        chairman = clean_markdown(ai_data.get('chairman_conclusion', '无结论'))

        # --- B. 交易动作样式 ---
        if r['amount'] > 0:
            act_style = f"background:rgba(82,196,26,0.15);color:{COLOR_GREEN};border:1px solid {COLOR_GREEN};"
            act_text = f"⚡ 买入 ¥{r['amount']:,}"
        elif r['is_sell']:
            act_style = f"background:rgba(255,77,79,0.15);color:{COLOR_RED};border:1px solid {COLOR_RED};"
            act_text = f"🔻 卖出 ¥{int(r['sell_value']):,}"
        else:
            act_style = "background:rgba(255,255,255,0.08);color:#bfbfbf;border:1px solid #434343;"
            act_text = "☕ 观望"

        # 量化理由标签
        reasons = " ".join([f"<span style='border:1px solid #444;background:#1f1f1f;padding:2px 6px;font-size:10px;border-radius:2px;color:{COLOR_TEXT_SUB};margin-right:4px;'>{x}</span>" for x in tech.get('quant_reasons', [])])

        # --- C. 全量量化指标提取 ---
        adx_val = tech.get('trend_strength', {}).get('adx', 0)
        trend_type = tech.get('trend_strength', {}).get('trend_type', '-')
        ma_align = tech.get('ma_alignment', '-')
        rsi_val = tech.get('rsi', '-')
        atr_pct = tech.get('volatility', {}).get('atr_percent', 0)
        boll_pos = tech.get('bollinger', {}).get('pct_b', 0)
        vol_ratio = tech.get('volume_analysis', {}).get('vol_ratio', 1.0)
        vr_24 = tech.get('volume_analysis', {}).get('vr_24', 100)
        macd_hist = tech.get('macd', {}).get('hist', 0)
        
        # 动态配色
        trend_color = COLOR_RED if trend_type == 'BULL' else (COLOR_GREEN if trend_type == 'BEAR' else COLOR_TEXT_SUB)
        hist_color = COLOR_RED if macd_hist > 0 else COLOR_GREEN

        # --- 卡片 HTML 结构 ---
        rows += f"""
        <div class="card" style="background:{COLOR_BG_CARD}; margin-bottom:16px; border:1px solid {COLOR_BORDER}; border-radius:6px; overflow:hidden;">
            <div style="padding:12px 15px; background:#1f1f1f; display:flex; justify-content:space-between; align-items:center; border-bottom:1px solid {COLOR_BORDER};">
                <div>
                    <span style="font-size:16px; font-weight:bold; color:{COLOR_TEXT_MAIN};">{r['name']}</span>
                    <span style="font-size:12px; color:{COLOR_GOLD}; font-family:monospace; margin-left:6px;">[{r['code']}]</span>
                </div>
                <div style="padding:4px 12px; font-size:12px; font-weight:bold; border-radius:4px; {act_style}">{act_text}</div>
            </div>
            
            <div style="padding:15px;">
                <div style="display:flex; justify-content:space-between; margin-bottom:12px; align-items:baseline;">
                     <div>
                        <span style="font-size:24px; font-weight:bold; color:{COLOR_GOLD};">{tech.get('final_score', 0)}</span>
                        <span style="font-size:11px; color:{COLOR_TEXT_SUB};">分 (基准{tech.get('quant_score',0)} + AI修正{tech.get('ai_adjustment',0)})</span>
                     </div>
                     <div style="font-size:11px; color:{COLOR_TEXT_SUB};">
                        风控状态: <span style="color:{COLOR_RED}">{tech.get('tech_cro_comment','-')}</span>
                     </div>
                </div>

                <div style="background:#0a0a0a; padding:10px; border-radius:4px; border:1px solid #333; margin-bottom:10px;">
                    <div style="display:grid; grid-template-columns: repeat(4, 1fr); gap:8px; font-size:11px; color:{COLOR_TEXT_SUB}; font-family:monospace;">
                        <span>RSI:  <b style="color:{COLOR_TEXT_MAIN}">{rsi_val}</b></span>
                        <span>ADX:  <b style="color:{trend_color}">{adx_val}</b></span>
                        <span>MA:   <b style="color:{trend_color}">{ma_align}</b></span>
                        <span>MACD: <b style="color:{hist_color}">{macd_hist}</b></span>
                        
                        <span>ATR%: {atr_pct}%</span>
                        <span>布林: {boll_pos}</span>
                        <span>量比: {vol_ratio}</span>
                        <span>VR24: {vr_24}</span>
                    </div>
                </div>

                <div style="margin-bottom:12px;">{reasons}</div>
                
                <div style="border-top:1px solid #333; padding-top:10px; font-size:11px; line-height:1.5;">
                    <div style="margin-bottom:6px; display:flex;">
                        <span style="color:{COLOR_GREEN}; font-weight:bold; width:40px; flex-shrink:0;">CGO:</span>
                        <span style="color:#d9f7be;">{bull_say}</span>
                    </div>
                    <div style="margin-bottom:6px; display:flex;">
                        <span style="color:{COLOR_RED}; font-weight:bold; width:40px; flex-shrink:0;">CRO:</span>
                        <span style="color:#ffccc7;">{bear_say}</span>
                    </div>
                    <div style="background:rgba(255,215,0,0.05); padding:8px; border-left:2px solid {COLOR_GOLD}; margin-top:8px;">
                        <span style="color:{COLOR_GOLD}; font-weight:bold;">⚖️ CIO 终审:</span>
                        <span style="color:{COLOR_TEXT_MAIN};">{chairman}</span>
                    </div>
                </div>
            </div>
        </div>"""

    # 4. Logo 读取
    logo_src = "[https://raw.githubusercontent.com/kken61291-eng/Fund-AI-Advisor/main/logo.png](https://raw.githubusercontent.com/kken61291-eng/Fund-AI-Advisor/main/logo.png)"
    if os.path.exists("logo.png"):
        try:
            with open("logo.png", "rb") as f:
                logo_src = f"data:image/png;base64,{base64.b64encode(f.read()).decode()}"
        except: pass

    # 5. 组装最终 HTML
    return f"""<!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{ background-color: {COLOR_BG_MAIN}; color: {COLOR_TEXT_MAIN}; font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 20px; }}
            .container {{ max-width: 700px; margin: 0 auto; }}
            .header {{ text-align: center; margin-bottom: 30px; border-bottom: 2px solid {COLOR_BORDER}; padding-bottom: 20px; }}
            .logo-text {{ color: {COLOR_GOLD}; font-size: 10px; letter-spacing: 3px; margin-top: 10px; text-transform: uppercase; }}
            
            .section-box {{ background: {COLOR_BG_CARD}; border: 1px solid {COLOR_BORDER}; border-radius: 6px; margin-bottom: 20px; padding: 15px; }}
            .section-title {{ font-size: 13px; font-weight: bold; margin-bottom: 10px; padding-bottom: 8px; border-bottom: 1px solid #333; display: flex; align-items: center; }}
            .report-content {{ font-size: 13px; line-height: 1.6; color: #d9d9d9; white-space: pre-wrap; }}
            
            /* 移动端适配 */
            @media (max-width: 480px) {{ 
                body {{ padding: 10px; }} 
                .container {{ width: 100%; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <img src="{logo_src}" style="width:180px; max-width:60%; display:block; margin:0 auto;">
                <div class="logo-text">MAGPIE QUANT SYSTEM V17.0</div>
            </div>
            
            <div class="section-box">
                <div class="section-title" style="color:{COLOR_GOLD};">
                    <span style="margin-right:6px;">📡</span> 全球市场快讯
                </div>
                <div style="font-size:11px; line-height:1.4;">{news_items}</div>
            </div>
            
            <div class="section-box" style="border-left: 3px solid {COLOR_RED};">
                <div class="section-title" style="color:{COLOR_RED};">
                    <span style="margin-right:6px;">🛑</span> CIO 战略审计报告
                </div>
                <div class="report-content">{cio_html}</div>
            </div>
            
            <div class="section-box" style="border-left: 3px solid {COLOR_GOLD};">
                <div class="section-title" style="color:{COLOR_GOLD};">
                    <span style="margin-right:6px;">🐦</span> 趋势一致性审计
                </div>
                <div class="report-content">{advisor_html}</div>
            </div>
            
            {rows}
            
            <div style="text-align:center; color:#444; font-size:10px; margin-top:40px; padding-bottom:20px;">
                POWERED BY DEEPSEEK-V3.2 & GEMINI PRO | QUANT ENGINE V17.0
            </div>
        </div>
    </body>
    </html>"""
