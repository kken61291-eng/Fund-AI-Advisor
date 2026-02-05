import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from utils import retry, logger

class MarketScanner:
    def __init__(self):
        pass

    def _parse_pub_date(self, pub_date_str):
        """
        解析时间并返回 datetime 对象（无时区）
        """
        try:
            if not pub_date_str: return None
            # 解析 RFC 822 格式
            pub_dt = parsedate_to_datetime(pub_date_str)
            # 移除时区以便比较
            if pub_dt.tzinfo:
                pub_dt = pub_dt.replace(tzinfo=None)
            return pub_dt
        except:
            return None

    def _format_source_label(self, source, pub_dt):
        """
        根据新闻的新旧程度，动态生成来源标签
        """
        now = datetime.now()
        delta = now - pub_dt
        days = delta.days

        if days < 1:
            # 24小时内：不加后缀，保持清爽
            return source
        elif days < 30:
            # 1个月内：显示天数
            return f"{source} · {days}天前"
        elif days < 365:
            # 1年内：显示月份
            return f"{source} · {pub_dt.strftime('%m-%d')}"
        else:
            # 远古新闻：显示年份
            return f"{source} · {pub_dt.strftime('%Y-%m')}"

    @retry(retries=2)
    def get_macro_news(self):
        """
        V11.11 无限溯源：
        获取所有可用新闻 -> 按时间倒序排列 -> 取前5条 -> 动态标记旧闻
        """
        # 移除所有时间限制参数，让 Google 返回库里有的所有相关新闻
        url = "https://news.google.com/rss/search?q=A股+中国央行+美联储+财政部+宏观经济&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        
        candidates = []
        
        try:
            response = requests.get(url, timeout=15) # 稍微增加超时时间
            root = ET.fromstring(response.content)
            
            # 1. 抓取所有条目
            for item in root.findall('.//item'):
                title_full = item.find('title').text
                pub_date_str = item.find('pubDate').text
                pub_dt = self._parse_pub_date(pub_date_str)
                
                if not pub_dt: continue

                # 处理标题和来源
                if ' - ' in title_full:
                    title, source = title_full.rsplit(' - ', 1)
                else:
                    title = title_full
                    source = "快讯"
                
                candidates.append({
                    "title": title,
                    "source": source,
                    "pub_dt": pub_dt
                })
                
        except Exception as e:
            logger.error(f"RSS获取失败: {e}")
            return [{"title": "宏观数据源连接中断", "source": "System Error"}]

        # 2. 核心逻辑：按时间倒序排列 (最新的在前)
        # 这样即使 Google 返回乱序，我们也能拿到相对最新的
        candidates.sort(key=lambda x: x['pub_dt'], reverse=True)

        # 3. 截取前 5 条 (如果没有新闻，candidates 为空)
        final_list = candidates[:5]

        # 4. 兜底逻辑：真的连一年前的新闻都没有吗？
        if not final_list:
            return [{"title": "全网静默 (数据源无任何历史记录)", "source": "System Watch"}]

        # 5. 格式化输出
        formatted_news = []
        for news in final_list:
            # 生成带时间戳的来源标签
            new_source = self._format_source_label(news['source'], news['pub_dt'])
            formatted_news.append({
                "title": news['title'],
                "source": new_source
            })
            
        return formatted_news

    def get_market_sentiment(self):
        news = self.get_macro_news()
        return f"{news[0]['title']} - {news[0]['source']}"
