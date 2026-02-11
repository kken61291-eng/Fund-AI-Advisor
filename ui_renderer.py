import os
import re
import base64

# --- V19.0 配色方案 (全暗色/高密度) ---
COLOR_GOLD = "#ffd700"        # 核心金 (高亮)
COLOR_RED = "#ff4d4f"         # 警示红
COLOR_GREEN = "#52c41a"       # 极客绿
COLOR_TEXT_MAIN = "#e6e6e6"   # 灰白 (避免纯白刺眼)
COLOR_TEXT_SUB = "#999999"    # 暗灰
COLOR_BG_PAGE = "#050505"     # 近乎纯黑的背景
COLOR_BG_CARD = "#111111"     # 极深灰卡片
COLOR_BORDER = "#222222"      # 隐形边框

def format_markdown_to_html(text):
    """
    [V19.1] 增强型 Markdown 渲染器 - 去除链接版
    """
    if not text: return "<span style='color:#666'>暂无内容</span>"
    
    # 0. 彻底移除链接和图片 (核心修改)
    # 移除 Markdown 图片 ![...](...)
    text = re.sub(r'!\[.*?\]\(.*?\)', '', text)
    # 移除 Markdown 链接 [...](...) 只保留文字
    text = re.sub(r'\[(.*?)\]\(.*?\)', r'\1', text)
    # 移除裸露的 http/https 链接
    text = re.sub(r'https?://\S+', '', text)
    
    # 1. 移除干扰代码
    text = re.sub(r'```(?:html|json|xml|css)?', '', text)
    text = re.sub(r'```', '', text)
    text = re.sub(r'<style.*?>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    
    # 2. 标题转换 (极度紧凑)
    # #### 小标题
    text = re.sub(r'^####\s+(.*?)$', r'<div style="color:#ffd700; font-weight:bold; margin:8px 0 4px 0; font-size:13px; border-left:2px solid #ffd700; padding-left:6px;">\1</div>', text, flags=re.MULTILINE)
    # ### 中标题
    text = re.sub(r'^###\s+(.*?)$', r'<h4 style="margin:12px 0 6px 0; color:#fff; border-bottom:1px solid #333; padding-bottom:2px; font-size:14px;">\1</h4>', text, flags=re.MULTILINE)
    # ## 大标题
    text = re.sub(r'^##\s+(.*?)$', r'<h3 style="margin:15px 0 8px 0; color:#fff; font-size:15px;">\1</h3>', text, flags=re.MULTILINE)
    
    # 3. 加粗与列表
    text = re.sub(r'\*\*(.*?)\*\*', r'<b style="color:#fff;">\1</b>', text)
    # 列表项 (支持 - 和 *) - 减少行间距
    text = re.sub(r'^\s*[-*]\s+(.*?)$', r'<div style="margin-bottom:2px; padding-left:12px; color:#ccc; position:relative; font-size:13px; line-height:1.4;"><span style="position:absolute; left:0; color:#444;">•</span>\1</div>', text, flags=re.MULTILINE)
    
    # 4. 换行优化 (极度紧凑，减少空行高度)
    text = text.replace('\n\n', '<div style="height:4px;"></div>').replace('\n', '<br>')
    
    # 5. 颜色强制修正
    text = re.sub(r'color:\s*#000000', 'color: #e6e6e6', text)
    text = re.sub(r'color:\s*black', 'color: #e6e6e6', text)

    return text.strip()

def render_html_report_v19(all_news, results, cio_html, advisor_html):
    """
    [V19.1 UI 引擎] 零留白沉浸式布局
    """
    cio_content = format_markdown_to_html(cio_html)
    advisor_content = format_markdown_to_html(advisor_html)
    
    # 新闻列表 (单行紧凑)
    news_items = ""
    for n in all_news[:12]: 
        # 移除 Markdown 链接格式，防止新闻里带有 URL
        n = re.sub(r'https?://\S+', '', n)
        
        # 移除时间戳前缀，只保留内容
        clean_n = re.sub(r'^\[.*?\]\s*', '', n) 
        # 提取时间
        time_match = re.match(r'^\[(.*?)\]', n)
        time_str = time_match.group(1) if time_match else ""
        
        news_items += f'''
        <div class="news-item">
            <span class="news-time">{time_str}</span>
            <span class="news-content">{clean_n}</span>
        </div>'''
    
    # 卡片流
    cards_html = ""
    for r in results:
        tech = r.get('tech', {})
        ai_data = r.get('ai_analysis', {})
        
        # 观点清洗
        bull_say = re.sub(r'\*\*|`', '', ai_data.get('cgo_proposal', {}).get('catalyst', '-'))
        bear_say = re.sub(r'\*\*|`', '', ai_data.get('cro_audit', {}).get('max_drawdown_scenario', '-'))
        chairman = re.sub(r'\*\*|`', '', ai_data.get('chairman_conclusion', '无结论'))

        if r['amount'] > 0:
            act_badge = f'<div class="badge buy">⚡ 买入 ¥{r["amount"]:,}</div>'
            border_style = f"border-left: 2px solid {COLOR_RED};"
        elif r['is_sell']:
            act_badge = f'<div class="badge sell">🔻 卖出 ¥{int(r["sell_value"]):,}</div>'
            border_style = f"border-left: 2px solid {COLOR_GREEN};"
        else:
            act_badge = f'<div class="badge hold">☕ 观望</div>'
            border_style = f"border-left: 2px solid {COLOR_BORDER};"

        idx_info = f"{r.get('index_name', 'N/A')}"
        tags = "".join([f'<span class="tag">{x}</span>' for x in tech.get('quant_reasons', [])])
        
        # 动态颜色
        trend_type = str(tech.get('trend_strength', {}).get('trend_type'))
        trend_color = COLOR_RED if 'BULL' in trend_type else (COLOR_GREEN if 'BEAR' in trend_type else COLOR_TEXT_SUB)
        
        cards_html += f"""
        <div class="card" style="{border_style}">
            <div class="card-header">
                <div style="display:flex; align-items:baseline;">
                    <span class="stock-name">{r['name']}</span>
                    <span class="stock-code">{r['code']}</span>
                </div>
                {act_badge}
            </div>
            
            <div class="card-body">
                <div class="score-row">
                    <div>
                        <span class="main-score" style="color:{COLOR_GOLD}">{tech.get('final_score', 0)}</span>
                        <span class="sub-text">分</span>
                    </div>
                    <div class="risk-info">
                        风控: <span style="color:{COLOR_RED}">{tech.get('tech_cro_comment','-')}</span>
                    </div>
                </div>

                <div class="metrics-grid">
                    <div>RSI: <b style="color:#fff">{tech.get('rsi','-')}</b></div>
                    <div>ADX: <b style="color:{trend_color}">{tech.get('trend_strength', {}).get('adx', 0)}</b></div>
                    <div>MA: <b style="color:{trend_color}">{tech.get('ma_alignment', '-')}</b></div>
                    <div>ATR%: {tech.get('volatility', {}).get('atr_percent', 0)}</div>
                    <div>量比: {tech.get('volume_analysis', {}).get('vol_ratio', 1)}</div>
                    <div>MACD: <b>{tech.get('macd', {}).get('hist', 0)}</b></div>
                </div>

                <div style="margin: 6px 0 4px 0; line-height:1.2;">{tags}</div>
                
                <div class="ai-box">
                    <div class="ai-row"><span class="role cgo">CGO</span> {bull_say}</div>
                    <div class="ai-row"><span class="role cro">CRO</span> {bear_say}</div>
                    <div class="ai-row cio-row"><span class="role cio">CIO</span> {chairman}</div>
                </div>
            </div>
        </div>"""

    # Logo 读取 (即使读取失败也不使用网络图片)
    logo_src = "" 
    if os.path.exists("logo.png"):
        try:
            with open("logo.png", "rb") as f:
                logo_src = f"data:image/png;base64,{base64.b64encode(f.read()).decode()}"
        except: pass
    
    # 如果没有本地Logo，显示文字标题代替，不显示破损图片图标
    logo_html = f'<img src="{logo_src}" style="width:120px; max-width:50%; display:block; margin:0 auto; filter: brightness(0.9);">' if logo_src else ""

    # HTML 组装
    return f"""<!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            /* --- V19.1 全局重置 (紧凑版) --- */
            body {{ 
                background-color: {COLOR_BG_PAGE}; 
                color: {COLOR_TEXT_MAIN}; 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; 
                margin: 0; 
                padding: 0;
                min-height: 100vh; /* 强制背景铺满全屏 */
                font-size: 13px; 
                line-height: 1.4; 
            }}
            
            .container {{ 
                max-width: 600px; 
                margin: 0 auto; 
                padding: 10px 10px 30px 10px; /* 顶部padding大幅减小 */
            }}
            
            /* 头部 Logo 区域 (压缩高度) */
            .header {{ 
                text-align: center; 
                margin-bottom: 12px; /* 减小 */
                padding-top: 5px;   /* 减小，贴顶 */
            }}
            .title {{ 
                color: {COLOR_GOLD}; 
                font-size: 10px; 
                letter-spacing: 2px; 
                margin-top: 4px; 
                text-transform: uppercase; 
                opacity: 0.8;
            }}
            
            /* 通用容器盒子 (压缩间距) */
            .box {{ 
                background-color: {COLOR_BG_CARD}; 
                border: 1px solid {COLOR_BORDER}; 
                border-radius: 6px; 
                margin-bottom: 8px; /* 减小模块间距 */
                overflow: hidden; 
            }}
            
            .box-header {{ 
                background-color: rgba(255,255,255,0.03); 
                padding: 6px 10px; /* 减小内边距 */
                font-size: 12px; 
                font-weight: bold; 
                border-bottom: 1px solid {COLOR_BORDER}; 
                color: {COLOR_GOLD};
                display: flex;
                align-items: center;
            }}
            
            .box-body {{ 
                padding: 10px; /* 内容紧凑 */
                color: #e6e6e6; 
                font-size: 13px;
            }}
            
            /* 新闻列表优化 */
            .news-item {{ 
                padding: 3px 0; /* 极致压缩 */
                border-bottom: 1px dashed #1a1a1a; 
                display: flex; 
                align-items: flex-start;
            }}
            .news-item:last-child {{ border-bottom: none; }}
            .news-time {{ 
                color: {COLOR_GOLD}; 
                font-family: monospace; 
                margin-right: 6px; 
                font-size: 11px; 
                flex-shrink: 0;
                opacity: 0.8;
            }}
            .news-content {{ color: #ccc; font-size: 12px; }}
            
            /* ETF 卡片 */
            .card {{ 
                background-color: {COLOR_BG_CARD}; 
                border: 1px solid {COLOR_BORDER}; 
                border-radius: 6px; 
                margin-bottom: 6px; /* 卡片间距更小 */
                overflow: hidden; 
            }}
            
            .card-header {{ 
                padding: 6px 10px; 
                background-color: rgba(255,255,255,0.02); 
                border-bottom: 1px solid {COLOR_BORDER}; 
                display: flex; 
                justify-content: space-between; 
                align-items: center; 
            }}
            
            .stock-name {{ font-size: 14px; font-weight: bold; color: #fff; margin-right: 6px; }}
            .stock-code {{ font-size: 12px; color: #666; font-family: monospace; }}
            
            .badge {{ padding: 1px 6px; border-radius: 3px; font-size: 10px; font-weight: bold; }}
            .buy {{ background: rgba(82,196,26,0.15); color: #73d13d; border: 1px solid rgba(82,196,26,0.3); }}
            .sell {{ background: rgba(255,77,79,0.15); color: #ff7875; border: 1px solid rgba(255,77,79,0.3); }}
            .hold {{ background: rgba(255,255,255,0.05); color: #777; border: 1px solid #333; }}
            
            .card-body {{ padding: 8px 10px; }}
            
            .score-row {{ display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 6px; }}
            .main-score {{ font-size: 18px; font-weight: bold; font-family: monospace; }}
            .sub-text {{ font-size: 11px; color: #666; }}
            .risk-info {{ font-size: 11px; color: #888; }}
            
            /* 指标网格 */
            .metrics-grid {{ 
                display: grid; 
                grid-template-columns: repeat(3, 1fr); 
                gap: 4px; 
                background: #080808; 
                padding: 6px; 
                border-radius: 4px; 
                border: 1px solid #222; 
                font-family: monospace; 
                font-size: 11px; 
                color: #888; 
            }}
            
            .tag {{ display: inline-block; background: #1a1a1a; border: 1px solid #333; color: #bbb; padding: 0px 4px; border-radius: 2px; font-size: 10px; margin-right: 3px; }}
            
            .ai-box {{ margin-top: 8px; padding-top: 6px; border-top: 1px solid #222; font-size: 12px; }}
            .ai-row {{ margin-bottom: 3px; display: flex; color: #aaa; line-height: 1.3; }}
            .role {{ font-size: 9px; padding: 1px 4px; border-radius: 2px; margin-right: 6px; width: 24px; text-align: center; flex-shrink: 0; height: 13px; line-height: 13px; }}
            .cgo {{ background: rgba(82,196,26,0.15); color: #52c41a; }}
            .cro {{ background: rgba(255,77,79,0.15); color: #ff4d4f; }}
            .cio {{ background: rgba(255,215,0,0.15); color: #ffd700; }}
            .cio-row {{ color: #fff; margin-top: 4px; }}
            
            .footer {{ text-align: center; margin-top: 20px; color: #333; font-size: 10px; border-top: 1px solid #111; padding-top: 10px; letter-spacing: 1px; }}
            
            /* 表格适配 */
            table {{ width: 100%; border-collapse: collapse; font-size: 12px; margin: 4px 0; }}
            th {{ text-align: left; color: #888; border-bottom: 1px solid #333; padding: 2px; font-weight: normal; }}
            td {{ padding: 2px; border-bottom: 1px solid #222; color: #ddd; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                {logo_html}
                <div class="title">Magpie Quant System V19.1</div>
            </div>
            
            <div class="box">
                <div class="box-header">
                    <span style="margin-right:6px;">📡</span> 全球市场快讯
                </div>
                <div class="box-body" style="padding: 6px 10px;">
                    {news_items}
                </div>
            </div>
            
            <div class="box" style="border-top: 2px solid {COLOR_RED};">
                <div class="box-header">
                    <span style="color:{COLOR_RED}; margin-right:6px;">🛑</span> CIO 战略审计
                </div>
                <div class="box-body">
                    {cio_content}
                </div>
            </div>
            
            <div class="box" style="border-top: 2px solid {COLOR_GOLD};">
                <div class="box-header">
                    <span style="color:{COLOR_GOLD}; margin-right:6px;">🐦</span> 趋势一致性审计
                </div>
                <div class="box-body">
                    {advisor_content}
                </div>
            </div>
            
            {cards_html}
            
            <div class="footer">
                POWERED BY DEEPSEEK-V3.2 & GEMINI PRO
            </div>
        </div>
    </body>
    </html>"""
