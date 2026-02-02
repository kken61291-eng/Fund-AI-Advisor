import yaml
import os
from data_fetcher import DataFetcher
from news_analyst import NewsAnalyst
from strategy import StrategyEngine
from utils import send_email, logger

def load_config():
    with open('config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def main():
    # 1. 初始化
    config = load_config()
    fetcher = DataFetcher()
    
    # AI 初始化 (带容错，防止AI服务挂了影响整体运行)
    analyst = None
    try:
        analyst = NewsAnalyst()
    except Exception as e:
        logger.error(f"AI 初始化失败 (可能是Key错误或网络问题): {e}")

    engine = StrategyEngine(config)
    
    report = "🚀 每日基金 AI 投顾报告 🚀\n\n"
    
    # 2. 遍历基金
    for fund in config['funds']:
        try:
            logger.info(f"=== 开始分析 {fund['name']} ===")
            
            # A. 获取技术数据
            tech_data = fetcher.get_fund_history(fund['code'])
            
            # B. 获取新闻与情绪
            s_score, s_summary = 5, "AI暂时无法连接"
            if analyst:
                try:
                    titles = analyst.fetch_news_titles(fund['sector_keyword'])
                    s_score, s_summary = analyst.analyze_sentiment(fund['sector_keyword'], titles)
                except Exception as ai_e:
                    logger.warning(f"AI分析步骤出错: {ai_e}")
                    s_summary = "新闻获取或分析失败"
            
            # C. 生成策略
            advice = engine.evaluate(fund, tech_data, s_score, s_summary)
            
            report += advice + "\n------------------\n"
            
        except Exception as e:
            logger.error(f"分析 {fund['name']} 时出错: {e}")
            report += f"⚠️ {fund['name']} 分析失败: {str(e)}\n\n"

    # 3. 输出并发送邮件
    print(report)
    
    try:
        send_email("今日基金操作建议", report)
    except Exception as e:
        logger.error(f"邮件发送失败: {e}")

if __name__ == "__main__":
    main()
