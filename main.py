import yaml
import os
import time
from datetime import datetime
from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from market_scanner import MarketScanner
from technical_analyzer import TechnicalAnalyzer
from utils import send_email, logger

# --- 基础配置加载函数 (补回) ---
def load_config():
    with open('config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

# --- 逻辑熔断校验 (V5.1 新增) ---
def logic_check(ai_result, tech_data):
    """
    🛡️ 逻辑熔断器：防止 AI 胡说八道
    """
    confidence = ai_result.get('confidence', 0)
    action = ai_result.get('action_advice', '观望')
    
    # 规则 1: 熊市不重仓
    if tech_data['trend_weekly'] == 'DOWN' and confidence > 6:
        logger.warning(f"⚠️ 逻辑修正: 周线DOWN，AI信心{confidence}过高 -> 强制降级为4")
        ai_result['confidence'] = 4
        ai_result['action_advice'] = "观望"
        ai_result['thesis'] += " [系统修正: 周线空头趋势下，AI原判断过于激进，已强制降级]"
        
    # 规则 2: RSI 中位不是底
    if 30 < tech_data['rsi'] < 50 and "买" in action and confidence > 5:
        if "背离" not in str(ai_result): # 除非AI明确识别出背离
            logger.warning(f"⚠️ 逻辑修正: RSI{tech_data['rsi']}无背离，不宜买入 -> 强制观望")
            ai_result['confidence'] = 3
            ai_result['action_advice'] = "观望"
            
    return ai_result

# --- 仓位计算算法 (V5.0 新增) ---
def calculate_position(ai_result, base_amount):
    """
    💰 仓位计算 (配合逻辑校验)
    """
    action = ai_result.get('action_advice', '观望')
    confidence = ai_result.get('confidence', 0)
    
    if "卖" in action or "清仓" in action: return 0, "卖出/止盈"
    if "观望" in action: return 0, "观望"

    # 只有经过逻辑校验的高分才买
    if "强力" in action or confidence >= 8:
        return int(base_amount * 2.5), "🔥 机会难得"
    elif "买" in action and confidence >= 6:
        return int(base_amount), "✅ 尝试建仓"
    else:
        return 0, "⚠️ 胜率不足"

# --- HTML 报告渲染 (补回) ---
def render_html_report(market_ctx, funds_results):
    COLOR_RED = "#d32f2f"     # 涨/买
    COLOR_GREEN = "#2e7d32"   # 跌/卖
    COLOR_BG = "#f5f7fa"      # 极简灰背景
    
    # 宏观颜色
    north_money = market_ctx.get('north_money', "0")
    try: 
        check_val = float(str(north_money).replace('%', ''))
    except: 
        check_val = 0
    north_color = COLOR_RED if check_val > 0 else COLOR_GREEN
    
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background-color: {COLOR_BG}; margin: 0; padding: 20px; color: #333; }}
            .container {{ max-width: 650px; margin: 0 auto; background: #fff; border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); overflow: hidden; }}
            .header {{ background: linear-gradient(135deg, #FFD700 0%, #FFA500 100%); color: #333; padding: 25px; text-align: center; }}
            .market-box {{ display: flex; padding: 15px; border-bottom: 1px solid #eee; gap: 10px; }}
            .card {{ padding: 20px; border-bottom: 1px solid #eee; transition: all 0.2s; }}
            .card:hover {{ background-color: #fafafa; }}
            .tag {{ padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }}
            .buy-tag {{ background: #ffebee; color: {COLOR_RED}; }}
            .sell-tag {{ background: #e8f5e9; color: {COLOR_GREEN}; }}
            .wait-tag {{ background: #f5f5f5; color: #999; }}
            .glossary {{ background: #f8f9fa; padding: 20px; font-size: 13px; color: #666; border-top: 1px solid #eee; }}
            .glossary h4 {{ margin: 0 0 10px 0; color: #333; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1 style="margin:0; font-size:22px;">💰 AI 绝对收益内参 (V5.1)</h1>
                <p style="margin:5px 0 0; font-size:13px; opacity:0.8;">{datetime.now().strftime('%Y-%m-%d')} | 逻辑严管版</p>
            </div>
            
            <div class="market-box">
                <div style="flex:1; background:#fff; border:1px solid #eee; border-radius:8px; padding:10px; text-align:center;">
                    <div style="font-size:12px; color:#999;">{market_ctx.get('north_label', '宏观')}</div>
                    <div style="font-size:18px; font-weight:bold; color:{north_color};">{north_money}</div>
                </div>
                <div style="flex:2; background:#fff; border:1px solid #eee; border-radius:8px; padding:10px;">
                    <div style="font-size:12px; color:#999;">🔥 领涨风口</div>
                    <div style="font-size:13px; color:#333; margin-top:3px;">
                        {' '.join(market_ctx.get('top_sectors', ['暂无'])[:3])}
                    </div>
                </div>
            </div>
    """

    all_glossary = {} 

    for res in funds_results:
        if 'glossary' in res['ai'] and res['ai']['glossary']:
            all_glossary.update(res['ai']['glossary'])

        action = res['action']
        amt_display = f"¥{res['amount']}" if res['amount'] > 0 else "0"
        
        if res['amount'] > 0:
            tag_class = "buy-tag"
            act_text = f"{res['position_type']} {amt_display}"
        elif "卖" in action:
            tag_class = "sell-tag"
            act_text = "🚫 建议卖出"
        else:
            tag_class = "wait-tag"
            act_text = "☕️ 观望等待"

        weekly_trend = res['tech'].get('trend_weekly', 'UNKNOWN')
        trend_icon = "📈" if weekly_trend == "UP" else "📉"
        trend_color = COLOR_RED if weekly_trend == "UP" else COLOR_GREEN

        html += f"""
            <div class="card">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                    <div>
                        <strong style="font-size:16px;">{res['name']}</strong>
                        <span style="font-size:12px; color:#999; margin-left:5px;">{res['code']}</span>
                    </div>
                    <div class="tag {tag_class}">{act_text}</div>
                </div>
                
                <div style="display:grid; grid-template-columns: 1fr 1fr; gap:10px; font-size:13px; color:#666; margin-bottom:12px;">
                    <div>RSI: <b style="color:#333">{res['tech']['rsi']}</b></div>
                    <div>大势: <span style="color:{trend_color}">{trend_icon} {weekly_trend}</span></div>
                    <div>AI信心: <b style="color:#FF9800">{res['ai'].get('confidence', 0)}/10</b></div>
                    <div>乖离: {res['tech']['bias_20']}%</div>
                </div>

                <div style="background:#fff8e1; padding:10px; border-radius:6px; font-size:14px; color:#5d4037; line-height:1.5;">
                    <b>💡 操盘逻辑:</b> {res['ai']['thesis']}
                </div>
                
                <div style="margin-top:8px; font-size:12px;">
                    <span style="color:{COLOR_RED}">[利多]</span> {res['ai'].get('pros', '-')} <br>
                    <span style="color:{COLOR_GREEN}">[风险]</span> {res['ai'].get('risk_warning', '-')}
                </div>
            </div>
        """
    
    if all_glossary:
        html += '<div class="glossary"><h4>📖 操盘手人话词典 (AI生成)</h4>'
        for term, explain in all_glossary.items():
            html += f'<p><b>【{term}】</b>: {explain}</p>'
        html += '</div>'

    html += "</div></body></html>"
    return html

# --- 主程序 ---
def main():
    config = load_config()
    fetcher = DataFetcher()
    scanner = MarketScanner()
    try: analyst = NewsAnalyst()
    except: analyst = None

    logger.info(">>> 启动 V5.1 重构版 (逻辑严管 + 联网补全)...")
    market_ctx = scanner.get_market_sentiment()
    funds_results = []
    
    BASE_AMT = config['global']['base_invest_amount']

    for fund in config['funds']:
        try:
            logger.info(f"=== 深度校验 {fund['name']} ===")
            
            # 1. 获取数据
            data_dict = fetcher.get_fund_history(fund['code'])
            
            # 2. 计算指标
            tech_indicators = TechnicalAnalyzer.calculate_indicators(data_dict)
            
            if not tech_indicators:
                logger.warning("数据不足，跳过")
                continue

            # 3. AI 分析
            ai_result = {
                "thesis": "AI 离线", "action_advice": "观望", 
                "confidence": 0, "pros": "", "cons": "", "glossary": {}
            }
            if analyst:
                news = analyst.fetch_news_titles(fund['sector_keyword'])
                ai_result = analyst.analyze_fund_v4(fund['name'], tech_indicators, market_ctx, news)

            # 4. 🛡️ 逻辑熔断校验 (新增步骤)
            ai_result = logic_check(ai_result, tech_indicators)

            # 5. 仓位计算
            final_amt, pos_type = calculate_position(ai_result, BASE_AMT)
            
            funds_results.append({
                "name": fund['name'],
                "code": fund['code'],
                "action": ai_result.get('action_advice', '观望'),
                "amount": final_amt,
                "position_type": pos_type,
                "tech": tech_indicators,
                "ai": ai_result
            })

            logger.info(f"最终决策: {pos_type} | 信心: {ai_result.get('confidence')}")
            time.sleep(1)

        except Exception as e:
            logger.error(f"分析失败: {e}")

    if funds_results:
        html_report = render_html_report(market_ctx, funds_results)
        send_email("💰 AI 绝对收益内参 (V5.1 重构版)", html_report)

if __name__ == "__main__":
    main()
