import requests
import xml.etree.ElementTree as ET
from utils import retry, logger

class MarketScanner:
    def __init__(self):
        pass

    @retry(retries=2)
    def get_macro_news(self):
        """
        V11.2 宏观雷达：获取5条带来源的核心宏观新闻
        """
        # 针对性搜索词：涵盖A股、央行、宏观经济
        url = "https://news.google.com/rss/search?q=A股+宏观经济+中国央行+美联储&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        
        news_list = []
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            
            # 遍历 RSS item
            for item in root.findall('.//item')[:5]:
                title_full = item.find('title').text
                # Google RSS 格式通常是 "标题 - 来源"
                if ' - ' in title_full:
                    title, source = title_full.rsplit(' - ', 1)
                else:
                    title = title_full
                    source = "市场快讯"
                    
                news_list.append({
                    "title": title,
                    "source": source
                })
                
            if not news_list:
                return [{"title": "市场宏观数据获取中...", "source": "System"}]
                
            return news_list
            
        except Exception as e:
            logger.error(f"宏观新闻获取失败: {e}")
            return [{"title": "宏观数据暂时不可用", "source": "System"}]

    def get_market_sentiment(self):
        # 保留原有接口兼容性，但在 V11.2 中主要使用 get_macro_news
        news = self.get_macro_news()
        return f"{news[0]['title']} - {news[0]['source']}"
