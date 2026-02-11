import os
import re
import base64

# --- V17.1 极客深色主题配色 ---
COLOR_GOLD = "#ffd700"       # 核心金
COLOR_RED = "#ff4d4f"        # 警示红
COLOR_GREEN = "#52c41a"      # 极客绿
COLOR_TEXT_MAIN = "#f0f0f0"  # 主文本
COLOR_TEXT_SUB = "#8c8c8c"   # 副文本
COLOR_BG_PAGE = "#0a0a0a"    # 页面背景
COLOR_BG_CARD = "#141414"    # 卡片背景
COLOR_BORDER = "#303030"     # 边框线

def clean_ai_report_content(text):
    """
    [V17.1 核心修复] 智能清洗 AI 返回的 HTML
    1. 彻底移除 <style>...</style> 及其内部代码，防止 CSS 源码泄露。
    2. 移除 <html>, <body> 等外层包裹，只保留内容。
    3. *保留* 表格、字体、颜色等格式标签，确保样式不丢失。
    """
    if not text: return "<span style='color:#666'>暂无分析内容</span>"
    
    # 1. 移除 Markdown 代码块标记 (```html, ```)
    text = re.sub(r'```(?:html|json|xml|css)?', '', text)
    text = re.sub(r'```', '', text)

    # 2. 【关键修复】移除 <style> 代码块 (防止截图中的 body {...} 乱码出现)
    # 使用 DOTALL 模式，确保跨行匹配
    text = re.sub(r'<style.*?>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)

    # 3. 移除网页结构标签，但保留内部 HTML
    for tag in ['html', 'head', 'body', '!DOCTYPE html']:
        text = re.sub(r'<{}.*?>'.format(tag), '', text, flags=re.IGNORECASE)
        text = re.sub(r'</{}>'.format(tag), '', text, flags=re.IGNORECASE)

    # 4. 移除 Markdown 标题标记 (# Title) 转为 HTML 样式，或者直接保留由 AI 生成的 HTML
    # 这里简单处理一下常见的 Markdown 加粗，防止 AI 混用
    text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
    
    return text.strip()

def render_html_report_v17(all_news, results, cio_html, advisor_html):
    """
    [V17.1 UI 引擎] 生成双栏布局 + 样式修复的 HTML 报告
    """
    # 1. 清洗并提取 AI 报告的核心 HTML 内容
    cio_content = clean_ai_report_content(cio_html)
    advisor_content = clean_ai_report_content(advisor_html)
    
    # 2. 新闻列表 (极简终端风)
    news_items = ""
    for n in all_news[:12]: 
        news_items += f'<div class="news-item"><span class="bullet">›</span>{n}</div>'
    
    # 3. 生成 ETF 卡片流
    cards_html = ""
    for r in results:
        tech = r.get('tech', {})
        ai_data = r.get('ai_analysis', {})
        
        # 提取 AI 观点 (纯文本清洗)
        bull_say = re.sub(r'\*\*|`', '', ai_data.get('cgo_proposal', {}).get('catalyst', '无明显催化'))
        bear_say = re.sub(r'\*\*|`', '', ai_data.get('cro_audit', {}).get('max_drawdown_scenario', '无'))
        chairman = re.sub(r'\*\*|`', '', ai_data.get('chairman_conclusion', '无结论'))

        # 交易动作徽章
        if r['amount'] > 0:
            act_badge = f'<div class="badge buy">⚡ 买入 ¥{r["amount"]:,}</div>'
            card_border_color = COLOR_RED # 买入高亮红框
        elif r['is_sell']:
            act_badge = f'<div class="badge sell">🔻 卖出 ¥{int(r["sell_value"]):,}</div>'
            card_border_color = COLOR_GREEN
        else:
            act_badge = f'<div class="badge hold">☕ 观望</div>'
            card_border_color = COLOR_BORDER

        # 量化标签
        tags = "".join([f'<span class="tag">{x}</span>' for x in tech.get('quant_reasons', [])])

        # 指标提取
        idx_info = f"指数: {r.get('index_name', 'N/A')}" # 【新增】指数代码
        rsi = tech.get('rsi', '-')
        adx = tech.get('trend_strength', {}).get('adx', 0)
        ma_align = tech.get('ma_alignment', '-')
        macd_hist = tech.get('macd', {}).get('hist', 0)
        
        # 动态颜色
        trend_cls = 'text-red' if 'BULL' in str(tech.get('trend_strength', {}).get('trend_type')) else 'text-green'
        
        cards_html += f"""
        <div class="card" style="border-left: 3px solid {card_border_color};">
            <div class="card-header">
                <div>
                    <span class="stock-name">{r['name']}</span>
                    <span class="stock-code">{r['code']}</span>
                    <span class="index-code" title="跟踪指数代码">({idx_info})</span>
                </div>
                {act_badge}
            </div>
            
            <div class="card-body">
                <div class="score-row">
                    <div>
                        <span class="main-score">{tech.get('final_score', 0)}</span>
                        <span class="sub-text">分 (基准{tech.get('quant_score',0)} + AI{tech.get('ai_adjustment',0)})</span>
                    </div>
                    <div class="sub-text">风控: <span style="color:{COLOR_RED}">{tech.get('tech_cro_comment','-')}</span></div>
                </div>

                <div class="metrics-grid">
                    <div>RSI: <b class="text-white">{rsi}</b></div>
                    <div>ADX: <b class="{trend_cls}">{adx}</b></div>
                    <div>MA: <b class="{trend_cls}">{ma_align}</b></div>
                    <div>MACD: <b>{macd_hist}</b></div>
                    <div>ATR%: {tech.get('volatility', {}).get('atr_percent', 0)}%</div>
                    <div>量比: {tech.get('volume_analysis', {}).get('vol_ratio', 1)}</div>
                </div>

                <div style="margin: 8px 0;">{tags}</div>
                
                <div class="ai-box">
                    <div class="ai-row"><span class="role-label cgo">CGO</span> {bull_say}</div>
                    <div class="ai-row"><span class="role-label cro">CRO</span> {bear_say}</div>
                    <div class="ai-row cio-row"><span class="role-label cio">CIO</span> {chairman}</div>
                </div>
            </div>
        </div>"""

    # 4. Logo 处理
    logo_src = "https://raw.githubusercontent.com/kken61291-eng/Fund-AI-Advisor/main/logo.png"
    if os.path.exists("logo.png"):
        try:
            with open("logo.png", "rb") as f:
                logo_src = f"data:image/png;base64,{base64.b64encode(f.read()).decode()}"
        except: pass

    # 5. 组装最终 HTML (内嵌 CSS 确保邮件兼容性)
    return f"""<!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            /* 全局重置 */
            body {{ background-color: {COLOR_BG_PAGE}; color: {COLOR_TEXT_MAIN}; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 20px; font-size: 14px; }}
            .container {{ max-width: 800px; margin: 0 auto; }}
            a {{ color: {COLOR_GOLD}; text-decoration: none; }}
            
            /* 头部 */
            .header {{ text-align: center; margin-bottom: 25px; padding-bottom: 15px; border-bottom: 1px solid #333; }}
            .title {{ color: {COLOR_GOLD}; font-size: 12px; letter-spacing: 4px; margin-top: 8px; font-weight: bold; text-transform: uppercase; }}
            
            /* 布局网格 (关键修改: 双栏布局) */
            .dashboard-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 20px; }}
            @media (max-width: 600px) {{ .dashboard-grid {{ grid-template-columns: 1fr; }} }} /* 手机端自动堆叠 */
            
            /* 通用板块盒子 */
            .box {{ background: {COLOR_BG_CARD}; border: 1px solid {COLOR_BORDER}; border-radius: 8px; overflow: hidden; display: flex; flex-direction: column; }}
            .box-header {{ background: #1f1f1f; padding: 10px 15px; font-size: 13px; font-weight: bold; border-bottom: 1px solid {COLOR_BORDER}; display: flex; align-items: center; }}
            .box-body {{ padding: 15px; font-size: 13px; line-height: 1.6; color: #d9d9d9; overflow-x: auto; }}
            
            /* 修复 AI 报告内容的样式 (让 AI 生成的表格漂亮一点) */
            .box-body table {{ width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 12px; }}
            .box-body th, .box-body td {{ border: 1px solid #444; padding: 6px; text-align: left; }}
            .box-body th {{ background: #333; color: {COLOR_GOLD}; }}
            
            /* 新闻列表 */
            .news-item {{ padding: 5px 0; border-bottom: 1px dashed #333; color: {COLOR_TEXT_SUB}; font-size: 12px; }}
            .bullet {{ color: {COLOR_GOLD}; margin-right: 8px; font-weight: bold; }}
            
            /* ETF 卡片 */
            .card {{ background: {COLOR_BG_CARD}; border: 1px solid {COLOR_BORDER}; border-radius: 8px; margin-bottom: 15px; overflow: hidden; }}
            .card-header {{ padding: 10px 15px; background: rgba(255,255,255,0.03); border-bottom: 1px solid {COLOR_BORDER}; display: flex; justify-content: space-between; align-items: center; }}
            .card-body {{ padding: 15px; }}
            
            .stock-name {{ font-size: 15px; font-weight: bold; color: {COLOR_TEXT_MAIN}; }}
            .stock-code {{ font-size: 12px; color: {COLOR_TEXT_SUB}; margin-left: 5px; font-family: monospace; }}
            .index-code {{ font-size: 11px; color: #666; margin-left: 5px; }}
            
            /* 徽章与标签 */
            .badge {{ padding: 4px 10px; border-radius: 4px; font-size: 11px; font-weight: bold; }}
            .buy {{ background: rgba(82,196,26,0.15); color: {COLOR_GREEN}; border: 1px solid {COLOR_GREEN}; }}
            .sell {{ background: rgba(255,77,79,0.15); color: {COLOR_RED}; border: 1px solid {COLOR_RED}; }}
            .hold {{ background: rgba(255,255,255,0.1); color: #999; border: 1px solid #555; }}
            
            .tag {{ display: inline-block; background: #262626; border: 1px solid #444; color: #aaa; padding: 2px 6px; border-radius: 3px; font-size: 10px; margin-right: 4px; }}
            
            /* 指标 Grid */
            .metrics-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; background: #000; padding: 10px; border-radius: 4px; border: 1px solid #333; font-family: monospace; font-size: 11px; color: {COLOR_TEXT_SUB}; margin-top: 10px; }}
            .text-red {{ color: {COLOR_RED}; }} .text-green {{ color: {COLOR_GREEN}; }} .text-white {{ color: #fff; }}
            
            /* AI 角色行 */
            .ai-box {{ margin-top: 12px; font-size: 12px; }}
            .ai-row {{ margin-bottom: 6px; display: flex; align-items: flex-start; }}
            .role-label {{ font-size: 10px; padding: 1px 4px; border-radius: 3px; margin-right: 6px; width: 30px; text-align: center; flex-shrink: 0; display: inline-block; }}
            .cgo {{ background: rgba(82,196,26,0.2); color: {COLOR_GREEN}; }}
            .cro {{ background: rgba(255,77,79,0.2); color: {COLOR_RED}; }}
            .cio {{ background: rgba(255,215,0,0.2); color: {COLOR_GOLD}; }}
            .cio-row {{ background: rgba(255,215,0,0.05); padding: 8px; border-radius: 4px; margin-top: 8px; border-left: 2px solid {COLOR_GOLD}; }}
            
            /* 底部 */
            .footer {{ text-align: center; margin-top: 40px; color: #444; font-size: 10px; border-top: 1px solid #222; padding-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <img src="{logo_src}" style="width:160px; max-width:50%; display:block; margin:0 auto;">
                <div class="title">Magpie Quant System V17.1</div>
            </div>
            
            <div class="box" style="margin-bottom: 20px;">
                <div class="box-header" style="color:{COLOR_GOLD};">
                    <span style="margin-right:8px;">📡</span> 全球市场快讯
                </div>
                <div class="box-body" style="padding: 10px 15px;">
                    {news_items}
                </div>
            </div>
            
            <div class="dashboard-grid">
                <div class="box" style="border-top: 3px solid {COLOR_RED};">
                    <div class="box-header">
                        <span style="color:{COLOR_RED}; margin-right:6px;">🛑</span> CIO 战略审计
                    </div>
                    <div class="box-body">
                        {cio_content}
                    </div>
                </div>
                
                <div class="box" style="border-top: 3px solid {COLOR_GOLD};">
                    <div class="box-header">
                        <span style="color:{COLOR_GOLD}; margin-right:6px;">🐦</span> 趋势一致性审计
                    </div>
                    <div class="box-body">
                        {advisor_content}
                    </div>
                </div>
            </div>
            
            {cards_html}
            
            <div class="footer">
                POWERED BY DEEPSEEK-V3.2 & GEMINI PRO | DATA ENGINE V17.1
            </div>
        </div>
    </body>
    </html>"""
