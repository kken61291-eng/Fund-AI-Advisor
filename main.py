import yaml
import os
import time
from datetime import datetime
from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from market_scanner import MarketScanner
from technical_analyzer import TechnicalAnalyzer
from utils import send_email, logger

def load_config():
    with open('config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def render_html_report(market_ctx, funds_results):
    COLOR_RED = "#d32f2f"
    COLOR_GREEN = "#2e7d32"
    COLOR_GRAY = "#616161"
    BG_COLOR = "#f5f5f5"
    
    north_val = market_ctx.get('north_money', 0)
    try:
        check_val = float(str(north_val).replace('%', ''))
    except:
        check_val = 0
    north_color = COLOR_RED if check_val > 0 else COLOR_GREEN
    north_bg = "#ffebee" if check_val > 0 else "#e8f5e9"
    
    html = f"""
    <html>
    <body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; background-color: {BG_COLOR}; margin: 0; padding: 20px;">
        <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
            <div style="background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); color: white; padding: 20px; text-align: center;">
                <h1 style="margin: 0; font-size: 24px;">🚀 AI 深度投顾日报 (Kimi极速版)</h1>
                <p style="margin: 5px 0 0; opacity: 0.8; font-size: 14px;">{datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
            </div>
            <div style="padding: 20px; border-bottom: 1px solid #eee;">
                <h3 style="margin-top: 0; color: #333;">🌍 市场风向标</h3>
                <div style="display: flex; gap: 10px;">
                    <div style="flex: 1; background-color: {north_bg}; padding: 10px; border-radius: 8px; text-align: center;">
                        <div style="font-size: 12px; color: #666;">{market_ctx.get('north_label', '宏观数据')}</div>
                        <div style="font-size: 20px; font-weight: bold; color: {north_color};">{north_val}</div>
                    </div>
                    <div style="flex: 1; background-color: #e3f2fd; padding: 10px; border-radius: 8px;">
                        <div style="font-size: 12px; color: #666; text-align: center;">领涨板块</div>
                        <div style="font-size: 12px; color: #1565c0; margin-top: 5px; line-height: 1.4; text-align: center;">
                            {'<br>'.join(market_ctx.get('top_sectors', ['暂无数据'])[:3])}
                        </div>
                    </div>
                </div>
            </div>
    """

    for res in funds_results:
        action = res['action']
        if "买" in action: card_color = COLOR_RED; btn_bg = "#ffebee"
        elif "卖" in action: card_color = COLOR_GREEN; btn_bg = "#e8f5e9"
        else: card_color = COLOR_GRAY; btn_bg = "#f5f5f5"

        weekly_tag = ""
        if res['tech'].get('trend_weekly') == "DOWN":
            weekly_tag = "<span style='color:green; font-size:10px; margin-left:5px;'>[周线向下]</span>"
        elif res['tech'].get('trend_weekly') == "UP":
            weekly_tag = "<span style='color:red; font-size:10px; margin-left:5px;'>[周线向上]</span>"

        html += f"""
            <div style="padding: 20px; border-bottom: 1px solid #eee;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <div>
                        <h3 style="margin: 0; color: #333; font-size: 18px;">{res['name']}</h3>
                        <span style="font-size: 12px; color: #999;">{res['code']}</span>
                    </div>
                    <div style="background-color: {card_color}; color: white; padding: 5px 12px; border-radius: 20px; font-weight: bold; font-size: 14px;">
                        {action}
                    </div>
                </div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 15px;">
                    <div style="background-color: {btn_bg}; padding: 8px; border-radius: 6px;">
                        <span style="display: block; font-size: 12px; color: #666;">建议金额</span>
                        <span style="font-weight: bold; color: {card_color}; font-size: 16px;">¥{int(res['amount'])}</span>
                    </div>
                    <div style="background-color: #f9f9f9; padding: 8px; border-radius: 6px;">
                        <span style="display: block; font-size: 12px; color: #666;">RSI / 趋势</span>
                        <span style="font-weight: bold; color: #333; font-size: 16px;">{res['tech']['rsi']} {weekly_tag}</span>
                        <span style="font-size: 12px; color: #999;">(偏离 {res['tech']['bias_20']}%)</span>
                    </div>
                </div>
                <div style="background-color: #fff8e1; border-left: 4px solid #ffc107; padding: 10px; border-radius: 4px; margin-bottom: 10px;">
                    <strong style="color: #f57f17; font-size: 12px;">🧠 AI 核心逻辑:</strong>
                    <p style="margin: 5px 0 0; font-size: 14px; color: #444; line-height: 1.5;">{res['ai']['thesis']}</p>
                </div>
                <div style="font-size: 12px; color: #666; line-height: 1.6;">
                    <div style="margin-bottom: 4px;">📈 <span style="color: {COLOR_RED};">利多:</span> {res['ai'].get('pros', 'N/A')}</div>
                    <div>📉 <span style="color: {COLOR_GREEN};">利空:</span> {res['ai'].get('cons', 'N/A')}</div>
                </div>
            </div>
        """
    
    html += "</div></body></html>"
    return html

def main():
    config = load_config()
    fetcher = DataFetcher()
    scanner = MarketScanner()
    analyst = None
    try: analyst = NewsAnalyst()
    except Exception as e: logger.error(f"AI初始化失败: {e}")

    logger.info(">>> 启动全市场扫描 (V4.1 极速版)...")
    market_ctx = scanner.get_market_sentiment()
    funds_results = []

    for fund in config['funds']:
        try:
            logger.info(f"=== 分析 {fund['name']} ===")
            
            # 1. 获取数据
            data_dict = fetcher.get_fund_history(fund['code'])
            
            # 2. Python 预计算指标
            tech_indicators = TechnicalAnalyzer.calculate_indicators(data_dict)
            
            if not tech_indicators:
                logger.warning(f"{fund['name']} 数据不足，跳过")
                continue

            # 3. AI 决策
            ai_result = {"thesis": "AI跳过", "action_advice": "观望"}
            if analyst:
                news = analyst.fetch_news_titles(fund['sector_keyword'])
                ai_result = analyst.analyze_fund_v4(fund['name'], tech_indicators, market_ctx, news)

            # 4. 简单策略映射
            action = ai_result.get('action_advice', '观望')
            base_amt = config['global']['base_invest_amount']
            final_amt = 0
            if "买" in action:
                final_amt = base_amt
                if "强力" in action: final_amt *= 1.2
            
            funds_results.append({
                "name": fund['name'],
                "code": fund['code'],
                "action": action,
                "amount": final_amt,
                "tech": tech_indicators,
                "ai": ai_result
            })

            # 【关键修改】 15s -> 1s
            # Kimi 支持 500 RPM，这里只留 1秒 防止 AkShare 接口请求过快被封 IP
            logger.info("⚡️ 闪电分析完成，短暂休整 1s...")
            time.sleep(1)

        except Exception as e:
            logger.error(f"分析 {fund['name']} 失败: {e}")

    if funds_results:
        try:
            html_report = render_html_report(market_ctx, funds_results)
            send_email("📊 AI 深度投顾日报 (Kimi版)", html_report)
        except Exception as e:
            logger.error(f"发送失败: {e}")

if __name__ == "__main__":
    main()
