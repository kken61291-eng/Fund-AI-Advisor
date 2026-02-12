import datetime
import re

def _md_to_html(text):
    """
    [工具] Markdown 清洗器
    将 LLM 输出的原始 Markdown 转换为适合邮件展示的简易 HTML
    """
    if not text: return ""
    
    # 1. 转义 HTML 特殊字符 (防止注入)
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    
    # 2. **加粗** -> <strong>加粗</strong>
    text = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', text)
    
    # 3. 去除标题符 (#, ##)
    text = re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
    
    # 4. 处理列表 (- 或 *) -> •
    text = re.sub(r'^\s*[\-\*]\s+', '• ', text, flags=re.MULTILINE)
    
    # 5. 换行符 -> <br>
    text = text.replace('\n', '<br>')
    
    return text

def render_html_report_v19(news_list, results, cio_review, advisor_review):
    """
    V19.1 UI 渲染器
    - 适配 v3.5 四态架构
    - 增加 Markdown 清洗
    - 强化手机端阅读体验
    """
    
    # --- CSS 样式 (邮件兼容性优化) ---
    css = """
    <style>
        /* 基础重置 */
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: #f4f4f4; color: #333; line-height: 1.6; margin: 0; padding: 0; -webkit-text-size-adjust: 100%; }
        
        /* 容器 */
        .wrapper { width: 100%; table-layout: fixed; background-color: #f4f4f4; padding-bottom: 40px; }
        .webkit { max-width: 600px; margin: 0 auto; background-color: #ffffff; }
        .outer { margin: 0 auto; width: 100%; max-width: 600px; font-family: sans-serif; color: #333333; }
        
        /* 头部 */
        .header { background: #2c3e50; padding: 20px; text-align: center; color: #ffffff; }
        .header h1 { margin: 0; font-size: 20px; font-weight: 600; }
        .header .date { font-size: 12px; opacity: 0.8; margin-top: 5px; }
        
        /* 战略复盘区 (CIO) */
        .strategic-box { background-color: #f8f9fa; border-left: 4px solid #2c3e50; padding: 15px; margin: 20px 15px; font-size: 14px; color: #444; }
        .strategic-box h3 { margin-top: 0; color: #2c3e50; font-size: 15px; border-bottom: 1px solid #eee; padding-bottom: 8px; margin-bottom: 8px; }
        
        /* 基金卡片 */
        .fund-card { border: 1px solid #e1e4e8; border-radius: 8px; margin: 15px; overflow: hidden; background: #fff; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }
        
        /* 卡片头部 */
        .card-header { padding: 12px 15px; background: #fdfdfd; border-bottom: 1px solid #eee; display: flex; justify-content: space-between; align-items: center; }
        .fund-info { display: flex; align-items: center; }
        .fund-title { font-size: 16px; font-weight: bold; color: #333; }
        .fund-code { font-size: 12px; color: #999; margin-left: 6px; }
        
        /* 决策标签 */
        .badge { padding: 3px 8px; border-radius: 3px; font-size: 11px; font-weight: bold; color: #fff; text-transform: uppercase; white-space: nowrap; }
        .badge-buy { background-color: #d9534f; }
        .badge-sell { background-color: #5cb85c; }
        .badge-wait { background-color: #f0ad4e; }
        .badge-cash { background-color: #999; }
        
        /* 模式标签 */
        .mode-tag { font-size: 10px; padding: 2px 5px; border-radius: 3px; margin-right: 8px; border: 1px solid #eee; white-space: nowrap; display: inline-block; }
        .mode-trend { color: #d9534f; background: #fff5f5; border-color: #ffdce0; }
        .mode-event { color: #6f42c1; background: #f8f0fc; border-color: #eaddf5; }
        .mode-reversal { color: #0275d8; background: #f0f8ff; border-color: #cce5ff; }
        .mode-wait { color: #666; background: #eee; }
        
        /* 卡片内容 */
        .card-body { padding: 15px; }
        
        /* 数据表格 */
        .stats-table { width: 100%; border-collapse: collapse; margin-bottom: 12px; }
        .stats-table td { padding: 3px 0; font-size: 13px; vertical-align: top; }
        .stats-label { color: #888; width: 70px; }
        .stats-val { color: #333; font-weight: 500; text-align: right; }
        
        /* 逻辑区域 */
        .logic-box { border-top: 1px dashed #eee; padding-top: 12px; margin-top: 5px; }
        .logic-title { font-size: 12px; color: #999; font-weight: bold; margin-bottom: 5px; text-transform: uppercase; letter-spacing: 0.5px; }
        .logic-content { font-size: 14px; color: #333; line-height: 1.5; }
        
        /* 事件特殊样式 */
        .event-box { margin-top: 12px; background: #f8f0fc; padding: 10px; border-radius: 4px; border-left: 3px solid #6f42c1; font-size: 13px; }
        .event-days { color: #d9534f; font-weight: bold; font-size: 15px; }
        
        /* 底部 */
        .footer { text-align: center; font-size: 11px; color: #aaa; padding: 20px; }
        
        /* 移动端强适配 */
        @media only screen and (max-width: 600px) {
            .webkit { width: 100% !important; max-width: 100% !important; }
            .fund-title { font-size: 15px !important; }
            .logic-content { font-size: 14px !important; }
            .card-body { padding: 12px !important; }
            /* 强制单栏布局，防止表格撑开 */
            .stats-table, .stats-table tbody, .stats-table tr, .stats-table td { display: block; width: 100%; }
            .stats-table tr { display: flex; justify-content: space-between; margin-bottom: 4px; border-bottom: 1px solid #f9f9f9; }
            .stats-table tr:last-child { border-bottom: none; }
            .stats-val { text-align: right; }
        }
    </style>
    """
    
    # --- HTML 结构 ---
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    
    html = f"""
    <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
    <html xmlns="http://www.w3.org/1999/xhtml">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
        <title>鹊知风日报</title>
        {css}
    </head>
    <body>
        <div class="wrapper">
            <div class="webkit">
                <div class="header">
                    <h1>🦅 鹊知风 AI 量化日报 (v19.1)</h1>
                    <div class="date">{current_date} | 全天候四态架构</div>
                </div>
    """
    
    # --- 1. 战略复盘 (清洗 Markdown) ---
    if cio_review:
        clean_cio = _md_to_html(cio_review)
        html += f"""
                <div class="strategic-box">
                    <h3>🧠 CIO 战略复盘</h3>
                    <div>{clean_cio}</div>
                </div>
        """
        
    # --- 2. 基金卡片 ---
    for res in results:
        name = res['name']
        code = res['code']
        amount = res['amount']
        is_sell = res['is_sell']
        decision = res.get('decision', 'HOLD')
        
        # 提取数据
        tech = res.get('tech', {})
        ai_full = res.get('ai_full', {})
        meta = ai_full.get('strategy_meta', {})
        trend = ai_full.get('trend_analysis', {})
        
        mode = meta.get('mode', 'WAIT')
        rationale = _md_to_html(meta.get('rationale', '暂无逻辑')) # 清洗逻辑
        stage = trend.get('stage', '-')
        
        # 指标
        score = tech.get('quant_score', 0)
        rsi = tech.get('rsi', 50)
        recent_gain = tech.get('recent_gain', 0)
        vol_status = tech.get('volatility_status', '-')
        
        # 徽章逻辑
        badge_class = "badge-wait"
        badge_text = "观望"
        action_text = "保持关注"
        action_color = "#999"
        
        if decision == "EXECUTE" or amount > 0:
            badge_class = "badge-buy"
            badge_text = "买入"
            action_text = f"买入 ¥{amount}"
            action_color = "#d9534f"
        elif is_sell or decision == "SELL":
            badge_class = "badge-sell"
            badge_text = "卖出"
            action_text = "建议止盈/损"
            action_color = "#5cb85c"
        elif decision == "HOLD_CASH":
            badge_class = "badge-cash"
            badge_text = "空仓"
            action_text = "现金防御"
            action_color = "#777"

        # 模式标签
        mode_html = ""
        if "TREND" in mode: 
            mode_html = '<span class="mode-tag mode-trend">A轨·趋势</span>'
        elif "EVENT" in mode: 
            mode_html = '<span class="mode-tag mode-event">C轨·潜伏</span>'
        elif "REVERSAL" in mode: 
            mode_html = '<span class="mode-tag mode-reversal">B轨·反转</span>'
        else:
            mode_html = '<span class="mode-tag mode-wait">D轨·观望</span>'

        # 卡片 HTML
        html += f"""
                <div class="fund-card">
                    <div class="card-header">
                        <div class="fund-info">
                            {mode_html}
                            <span class="fund-title">{name}</span>
                            <span class="fund-code">{code}</span>
                        </div>
                        <span class="badge {badge_class}">{badge_text}</span>
                    </div>
                    
                    <div class="card-body">
                        <table class="stats-table">
                            <tr>
                                <td class="stats-label">操作建议</td>
                                <td class="stats-val" style="color:{action_color}; font-weight:bold;">{action_text}</td>
                            </tr>
                            <tr>
                                <td class="stats-label">量化评分</td>
                                <td class="stats-val">{score} <span style="font-size:10px; color:#999;">/100</span></td>
                            </tr>
                            <tr>
                                <td class="stats-label">5日涨幅</td>
                                <td class="stats-val">{recent_gain}%</td>
                            </tr>
                            <tr>
                                <td class="stats-label">RSI指标</td>
                                <td class="stats-val">{rsi}</td>
                            </tr>
                            <tr>
                                <td class="stats-label">波动状态</td>
                                <td class="stats-val">{vol_status}</td>
                            </tr>
                        </table>
                        
                        <div class="logic-box">
                            <div class="logic-title">AI 核心逻辑</div>
                            <div class="logic-content">{rationale}</div>
                        </div>
        """
        
        # 事件倒计时模块
        days = trend.get('days_to_event', 'NULL')
        if "EVENT" in mode and str(days) != "NULL" and str(days) != "None":
            exec_notes = _md_to_html(ai_full.get('execution_notes', ''))
            html += f"""
                        <div class="event-box">
                            ⏳ 距离关键事件还有 <span class="event-days">{days}</span> 天
                            <div style="margin-top:5px; color:#666; font-size:12px;">
                                ⚠️ {exec_notes}
                            </div>
                        </div>
            """

        # 风控拦截模块
        cro = ai_full.get('cro_risk_audit', {})
        if not cro: cro = ai_full.get('cro_arbitration', {})
        
        # 简单判断是否显示 CRO 信息（如果有拦截或警告）
        if decision == "REJECT" or decision == "HOLD_CASH":
             # 把字典转成字符串清洗后显示
             cro_str = _md_to_html(str(cro).replace('{','').replace('}','').replace("'", ""))
             html += f"""
                        <div style="margin-top:10px; padding:8px; background:#fff5f5; border-radius:4px; font-size:12px; color:#c0392b;">
                            🛡️ <strong>CRO 风控拦截:</strong><br/>{cro_str}
                        </div>
             """
             
        html += """
                    </div>
                </div>
        """

    # --- 3. 底部新闻列表 ---
    html += """
                <div class="strategic-box" style="border-left-color:#ddd; background:#fff;">
                    <h3>📰 市场热点摘要</h3>
                    <ul style="padding-left:18px; margin:0;">
    """
    for news in news_list[:5]:
        clean_news = _md_to_html(news)
        if len(clean_news) > 5:
            html += f"<li style='margin-bottom:6px;'>{clean_news[:80]}...</li>"
            
    html += """
                    </ul>
                </div>
                
                <div class="footer">
                    <p>Risk Warning: AI-generated content for quantitative research only.</p>
                    <p>&copy; 2026 Fund AI Advisor</p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html
