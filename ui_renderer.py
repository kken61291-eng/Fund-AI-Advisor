import datetime

def render_html_report_v19(news_list, results, cio_review, advisor_review):
    """
    V19.0 UI 渲染器 - 适配 v3.5 四态全天候架构
    特点：
    1. 增加 [模式] 徽章 (Trend/Reversal/Event/Wait)
    2. 增加 [事件] 倒计时展示
    3. 优化 [资金] 与 [技术] 的多维度展示
    """
    
    # --- CSS 样式定义 ---
    css = """
    <style>
        body { font-family: 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f0f2f5; color: #333; line-height: 1.6; margin: 0; padding: 20px; }
        .container { max-width: 800px; margin: 0 auto; background: #fff; padding: 30px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); }
        
        /* 头部 */
        .header { text-align: center; margin-bottom: 30px; border-bottom: 2px solid #eaebed; padding-bottom: 20px; }
        .header h1 { margin: 0; color: #1a1a1a; font-size: 24px; }
        .header .date { color: #666; font-size: 14px; margin-top: 5px; }
        
        /* 战略复盘区 (CIO) */
        .strategic-box { background: #f8f9fa; border-left: 5px solid #2c3e50; padding: 15px; margin-bottom: 30px; border-radius: 4px; }
        .strategic-box h3 { margin-top: 0; color: #2c3e50; font-size: 16px; display: flex; align-items: center; }
        .strategic-box .content { font-size: 14px; color: #444; white-space: pre-line; }
        
        /* 基金卡片 */
        .fund-card { border: 1px solid #e1e4e8; border-radius: 8px; margin-bottom: 25px; overflow: hidden; background: #fff; transition: transform 0.2s; }
        .fund-card:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        
        /* 卡片头部 */
        .card-header { padding: 12px 15px; display: flex; justify-content: space-between; align-items: center; background: #fdfdfd; border-bottom: 1px solid #eee; }
        .fund-title { font-size: 18px; font-weight: bold; color: #333; }
        .fund-code { font-size: 13px; color: #999; margin-left: 5px; font-weight: normal; }
        
        /* 决策标签 */
        .badge { padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; color: #fff; text-transform: uppercase; }
        .badge-buy { background-color: #d9534f; } /* 红: 买入 */
        .badge-sell { background-color: #5cb85c; } /* 绿: 卖出 */
        .badge-wait { background-color: #f0ad4e; } /* 黄: 观望 */
        .badge-cash { background-color: #777; }    /* 灰: 空仓 */
        
        /* 模式标签 (v3.5) */
        .mode-tag { font-size: 11px; padding: 2px 6px; border-radius: 3px; margin-right: 5px; border: 1px solid #ddd; }
        .mode-trend { color: #d9534f; border-color: #d9534f; background: #fff5f5; }
        .mode-event { color: #6f42c1; border-color: #6f42c1; background: #f8f0fc; }
        .mode-reversal { color: #0275d8; border-color: #0275d8; background: #f0f8ff; }
        
        /* 卡片内容区 */
        .card-body { padding: 15px; display: flex; flex-wrap: wrap; }
        
        /* 左侧：数据面板 */
        .stats-panel { flex: 1; min-width: 200px; border-right: 1px solid #eee; padding-right: 15px; margin-right: 15px; }
        .stat-row { display: flex; justify-content: space-between; margin-bottom: 8px; font-size: 13px; }
        .stat-label { color: #888; }
        .stat-value { font-weight: 600; color: #333; }
        
        /* 右侧：逻辑面板 */
        .logic-panel { flex: 2; min-width: 250px; }
        .logic-title { font-size: 12px; color: #999; font-weight: bold; margin-bottom: 5px; }
        .logic-content { font-size: 14px; color: #444; line-height: 1.5; }
        .highlight { background: #fffbe6; padding: 2px 5px; border-radius: 3px; }
        
        /* 事件特殊样式 */
        .event-box { margin-top: 10px; background: #f8f0fc; padding: 8px; border-radius: 4px; border-left: 3px solid #6f42c1; font-size: 13px; }
        .event-days { color: #d9534f; font-weight: bold; font-size: 16px; margin: 0 3px; }
        
        /* 底部 */
        .footer { margin-top: 40px; text-align: center; font-size: 12px; color: #aaa; border-top: 1px solid #eee; padding-top: 20px; }
        
        /* 移动端适配 */
        @media (max-width: 600px) {
            .card-body { flex-direction: column; }
            .stats-panel { border-right: none; border-bottom: 1px solid #eee; padding-right: 0; padding-bottom: 15px; margin-right: 0; margin-bottom: 15px; }
        }
    </style>
    """
    
    # --- HTML 头部 ---
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>鹊知风 AI 基金日报</title>
        {css}
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🦅 鹊知风 AI 量化日报 (v19.0)</h1>
                <div class="date">{current_date} | 全天候四态架构</div>
            </div>
    """
    
    # --- 1. 战略复盘区 ---
    if cio_review:
        html += f"""
            <div class="strategic-box">
                <h3>🧠 CIO 战略复盘</h3>
                <div class="content">{cio_review}</div>
            </div>
        """
        
    # --- 2. 基金卡片循环 ---
    for res in results:
        name = res['name']
        code = res['code']
        amount = res['amount']
        is_sell = res['is_sell']
        decision = res.get('decision', 'HOLD')
        
        # 提取 v3.5 数据
        tech = res.get('tech', {})
        ai_full = res.get('ai_full', {}) # 原始 AI 数据
        meta = ai_full.get('strategy_meta', {})
        trend = ai_full.get('trend_analysis', {})
        
        mode = meta.get('mode', 'UNKNOWN') # TREND, EVENT, REVERSAL
        rationale = meta.get('rationale', '暂无逻辑')
        stage = trend.get('stage', '-')
        
        # 技术指标
        score = tech.get('quant_score', 0)
        rsi = tech.get('rsi', 50)
        recent_gain = tech.get('recent_gain', 0)
        vol_status = tech.get('volatility_status', '-')
        
        # 决策徽章颜色与文本
        badge_class = "badge-wait"
        badge_text = "观望"
        action_desc = "保持关注"
        
        if decision == "EXECUTE" or amount > 0:
            badge_class = "badge-buy"
            badge_text = "买入"
            action_desc = f"建议买入 ¥{amount}"
        elif is_sell or decision == "SELL":
            badge_class = "badge-sell"
            badge_text = "卖出"
            action_desc = "建议止盈/止损"
        elif decision == "HOLD_CASH":
            badge_class = "badge-cash"
            badge_text = "空仓"
            action_desc = "现金为王 (垃圾时间)"
            
        # 模式标签样式
        mode_class = ""
        mode_cn = mode
        if "TREND" in mode: 
            mode_class = "mode-trend"
            mode_cn = "A轨 · 趋势跟随"
        elif "EVENT" in mode: 
            mode_class = "mode-event"
            mode_cn = "C轨 · 事件潜伏"
        elif "REVERSAL" in mode: 
            mode_class = "mode-reversal"
            mode_cn = "B轨 · 困境反转"
        elif "WAIT" in mode:
            mode_cn = "D轨 · 防御"
            
        # 构造卡片 HTML
        html += f"""
        <div class="fund-card">
            <div class="card-header">
                <div>
                    <span class="mode-tag {mode_class}">{mode_cn}</span>
                    <span class="fund-title">{name}<span class="fund-code">{code}</span></span>
                </div>
                <span class="badge {badge_class}">{badge_text}</span>
            </div>
            
            <div class="card-body">
                <div class="stats-panel">
                    <div class="stat-row">
                        <span class="stat-label">操作建议</span>
                        <span class="stat-value" style="color:#d9534f">{action_desc}</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">量化评分</span>
                        <span class="stat-value">{score} 分</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">RSI (14)</span>
                        <span class="stat-value">{rsi}</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">5日涨幅</span>
                        <span class="stat-value">{recent_gain}%</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">波动状态</span>
                        <span class="stat-value">{vol_status}</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">当前阶段</span>
                        <span class="stat-value">{stage}</span>
                    </div>
                </div>
                
                <div class="logic-panel">
                    <div class="logic-title">🧠 AI 核心逻辑</div>
                    <div class="logic-content">
                        {rationale}
                    </div>
        """
        
        # 如果是事件驱动模式，额外显示倒计时
        days_to_event = trend.get('days_to_event', 'NULL')
        if "EVENT" in mode and str(days_to_event) != "NULL":
            execution_notes = ai_full.get('execution_notes', '')
            html += f"""
                    <div class="event-box">
                        ⏳ 距离关键事件还有 <span class="event-days">{days_to_event}</span> 天
                        <br>
                        <span style="color:#666; font-size:12px;">⚠️ 纪律: {execution_notes}</span>
                    </div>
            """
            
        # 如果有风控否决
        cro_audit = ai_full.get('cro_risk_audit', {})
        if not cro_audit: cro_audit = ai_full.get('cro_arbitration', {}) # 兼容不同命名
        
        if decision == "REJECT" or "VETO" in str(ai_full):
            html += f"""
                    <div style="margin-top:10px; color:#d9534f; font-size:13px; background:#fff5f5; padding:5px; border-radius:4px;">
                        🛡️ <strong>CRO 拦截:</strong> {cro_audit}
                    </div>
            """

        html += """
                </div>
            </div>
        </div>
        """

    # --- 3. 底部与新闻 ---
    html += """
            <div class="strategic-box" style="background:#fff; border-left:4px solid #ddd;">
                <h3>📰 本地新闻摘要 (Top Headlines)</h3>
                <ul style="font-size:13px; color:#666; padding-left:20px;">
    """
    
    # 简单的列出前 5 条新闻
    for i, news in enumerate(news_list[:5]):
        if len(news) > 5:
            html += f"<li>{news[:100]}...</li>"
            
    html += """
                </ul>
            </div>
            
            <div class="footer">
                <p>⚠️ 风险提示：本报告由 AI 自动生成 (DeepSeek-V3.2/R1)，仅供量化策略研究参考，不构成任何投资建议。</p>
                <p>&copy; 2026 鹊知风 Fund AI Advisor</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html
