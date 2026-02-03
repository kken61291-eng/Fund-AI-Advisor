import yaml
import os
import time
from datetime import datetime
from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from market_scanner import MarketScanner
from technical_analyzer import TechnicalAnalyzer
from utils import send_email, logger

# ... (load_config, render_html_report 保持不变，请复制之前的) ...
# 为了节省篇幅，这里重点展示修改后的 main 和 逻辑校验函数

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
        # 这里需要你把之前的 render_html_report 函数也放进来，为了代码完整性
        # (请直接复用 V5.0 的 render_html_report 代码，完全兼容)
        html_report = render_html_report(market_ctx, funds_results)
        send_email("💰 AI 绝对收益内参 (V5.1 重构版)", html_report)

if __name__ == "__main__":
    main()
