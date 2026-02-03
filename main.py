def render_html_report(market_ctx, funds_results, daily_total_cap):
    """
    ✨ V7.1 鎏金财富版 UI (Gilded Wealth Edition)
    全中文、富含财富元素、高级感
    """
    invested = sum(r['amount'] for r in funds_results if r['amount'] > 0)
    cash_display = f"{invested:,}"
    
    # 1. 预处理：将英文标签转换为高级中文术语，并分类
    buys = []
    sells = []
    waits = []
    
    for r in funds_results:
        # 翻译标签
        label = r['position_type']
        if "STRONG BUY" in label: cn_label = "🔥 强力增持 (重仓)"
        elif "BUY+" in label: cn_label = "🔥 强力增持 (重仓)" # V7.0可能出现的标签
        elif "BUY" in label: cn_label = "✅ 标准建仓"
        elif "ADD" in label: cn_label = "🧪 试探性买入"
        elif "SELL ALL" in label: cn_label = "🚫 清仓离场 (落袋)"
        elif "SELL" in label: cn_label = f"✂️ 减仓锁定 ({label.split(' ')[-1]})"
        elif "WAIT" in label: cn_label = "⏸️ 持币/持仓观望"
        else: cn_label = label
        r['cn_label'] = cn_label

        # 分类
        if r['amount'] > 0: buys.append(r)
        elif r.get('is_sell'): sells.append(r)
        else: waits.append(r)

    # 宏观颜色判断
    north_val = market_ctx.get('north_money', '0')
    macro_class = "macro-neu"
    if "+" in str(north_val) and "0.00" not in str(north_val): macro_class = "macro-up"
    elif "-" in str(north_val): macro_class = "macro-down"

    # --- HTML 开始 ---
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            /* 引入高级衬线字体 */
            @import url('https://fonts.googleapis.com/css2?family=Noto+Serif+SC:wght@500;700&family=Roboto+Mono&display=swap');
            
            body {{
                background-color: #0a0a0a; /* 深邃黑底 */
                color: #e0e0e0;
                font-family: "Noto Serif SC", "Microsoft YaHei", serif;
                margin: 0; padding: 20px;
                background-image: url('https://www.transparenttextures.com/patterns/cubes.png'); /* 隐约的财富纹理背景 */
            }}
            .container {{
                max-width: 680px; margin: 0 auto;
                background: #141414;
                border: 2px solid #D4AF37; /* 鎏金边框 */
                border-radius: 12px; box-shadow: 0 10px 30px rgba(212,175,55,0.15);
                overflow: hidden;
            }}
            /* 金色渐变文字效果 */
            .gold-text {{
                background: linear-gradient(to right, #D4AF37, #FCEabb, #D4AF37);
                -webkit-background-clip: text; color: transparent;
                font-weight: bold;
            }}
            
            /* --- 头部与仪表盘 --- */
            .header {{
                background: linear-gradient(180deg, #1f1f1f 0%, #141414 100%);
                padding: 30px; text-align: center;
                border-bottom: 2px solid #D4AF37;
            }}
            .title {{ font-size: 28px; margin: 0; letter-spacing: 2px; }}
            .subtitle {{ color: #888; font-size: 12px; margin-top: 10px; }}
            
            .dashboard {{
                display: flex; border-bottom: 1px solid #333;
                background: #1a1a1a;
            }}
            .dash-item {{
                flex: 1; padding: 20px; text-align: center;
                border-right: 1px solid #333;
            }}
            .dash-item:last-child {{ border-right: none; }}
            .dash-title {{ font-size: 12px; color: #aaa; margin-bottom: 8px; display: flex; align-items: center; justify-content: center; }}
            .dash-value {{ font-size: 22px; font-family: "Roboto Mono", monospace; }}
            .macro-up {{ color: #ff4d4f; }} .macro-down {{ color: #52c41a; }} .macro-neu {{ color: #D4AF37; }}

            /* --- 交易卡片 --- */
            .section-title {{
                padding: 20px 30px 10px; color: #D4AF37; font-size: 16px;
                display: flex; align-items: center; border-bottom: 1px solid #222;
            }}
            .card {{
                margin: 15px 30px; background: #1c1c1c;
                border: 1px solid #333; border-radius: 8px; overflow: hidden;
            }}
            /* 买入卡片风格 */
            .card-buy {{ border-left: 4px solid #ff4d4f; }}
            .buy-header {{ background: rgba(255, 77, 79, 0.1); color: #ff4d4f; }}
            /* 卖出卡片风格 */
            .card-sell {{ border-left: 4px solid #52c41a; }}
            .sell-header {{ background: rgba(82, 196, 26, 0.1); color: #52c41a; }}
            
            .card-top {{
                padding: 12px 20px; display: flex; justify-content: space-between; align-items: center;
                font-family: "Roboto Mono"; font-weight: bold;
            }}
            .card-body {{ padding: 15px 20px; }}
            .fund-title {{ font-size: 16px; font-weight: bold; color: #fff; }}
            .fund-code {{ font-size: 12px; color: #666; margin-left: 5px; }}
            .score-box {{ float: right; font-family: "Roboto Mono"; color: #D4AF37; }}
            
            .reason-tag {{
                display: inline-block; background: #252525; color: #aaa;
                padding: 4px 8px; border-radius: 4px; font-size: 11px;
                margin-right: 5px; margin-top: 8px; border: 1px solid #333;
            }}
            /* 强调风控理由 */
            .reason-risk {{ color: #FCEabb; border-color: #D4AF37; background: rgba(212,175,55,0.1); }}

            /* --- 观望列表 --- */
            summary {{ padding: 20px 30px; cursor: pointer; color: #666; font-size: 13px; user-select: none; }}
            summary:hover {{ color: #D4AF37; }}
            .wait-list {{ padding: 0 30px 20px; font-size: 12px; color: #555; line-height: 1.8; }}

            .footer {{
                padding: 25px; text-align: center; color: #444; font-size: 11px;
                background: #0f0f0f; border-top: 1px solid #222;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1 class="title"><span class="gold-text">💰 鎏金量化·财富内参</span></h1>
                <div class="subtitle">{datetime.now().strftime('%Y年%m月%d日')} | V7.1 实战风控版</div>
            </div>
            
            <div class="dashboard">
                <div class="dash-item">
                    <div class="dash-title">🌍 市场风向标</div>
                    <div class="dash-value {macro_class}">{market_ctx.get('north_label')} {market_ctx.get('north_money')}</div>
                </div>
                <div class="dash-item">
                    <div class="dash-title">💸 今日投入金 (CNY)</div>
                    <div class="dash-value gold-text">¥{cash_display}</div>
                </div>
            </div>
    """

    # --- 渲染买入卡片 ---
    if buys:
        html += '<div class="section-title">📈 财富增值机遇 (买入)</div>'
        for r in buys:
            score = r['tech']['quant_score']
            html += f"""
            <div class="card card-buy">
                <div class="card-top buy-header">
                    <span>{r['cn_label']}</span>
                    <span>+¥{r['amount']:,}</span>
                </div>
                <div class="card-body">
                    <div>
                        <span class="fund-title">{r['name']}</span>
                        <span class="fund-code">{r['code']}</span>
                        <span class="score-box">量化评分: {score}</span>
                    </div>
                    <div style="margin-top:10px;">
                        {''.join([f'<span class="reason-tag {"reason-risk" if "风控" in reason or "锁" in reason else ""}">{reason}</span>' for reason in r['tech']['quant_reasons']])}
                    </div>
                </div>
            </div>
            """

    # --- 渲染卖出卡片 ---
    if sells:
        html += '<div class="section-title">🛡️ 风险控制行动 (卖出)</div>'
        for r in sells:
            score = r['tech']['quant_score']
            val = int(r.get('sell_value', 0))
            val_display = f"¥{val:,}" if val > 0 else "全部份额"
            html += f"""
            <div class="card card-sell">
                <div class="card-top sell-header">
                    <span>{r['cn_label']}</span>
                    <span>卖出: {val_display}</span>
                </div>
                <div class="card-body">
                    <div>
                        <span class="fund-title">{r['name']}</span>
                        <span class="fund-code">{r['code']}</span>
                        <span class="score-box">量化评分: {score}</span>
                    </div>
                    <div style="margin-top:10px;">
                        {''.join([f'<span class="reason-tag {"reason-risk" if "风控" in reason or "锁" in reason else ""}">{reason}</span>' for reason in r['tech']['quant_reasons']])}
                    </div>
                </div>
            </div>
            """

    # --- 观望列表 ---
    if waits:
        html += f"""
        <details>
            <summary>⏸️ 查看 {len(waits)} 只观望标的 (未触发信号)</summary>
            <div class="wait-list">
                {' • '.join([f"{r['name']}({r['tech']['quant_score']}分)" for r in waits])}
            </div>
        </details>
        """
    else:
        html += '<div style="padding:30px; text-align:center; color:#666;">今日无观望标的，全线出击。</div>'

    html += """
            <div class="footer">
                注：评分低于60分或触发风控将执行卖出；持有不足7天强制触发「七日锁」保护。
                <br>SYSTEM GENERATED | 纪律执行是财富积累的前提
            </div>
        </div>
    </body>
    </html>
    """
    return html
