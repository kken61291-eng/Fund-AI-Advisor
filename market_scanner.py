import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from utils import retry, logger

class MarketScanner:
    def __init__(self):
        pass

    def _is_recent(self, pub_date_str):
        """
        [V11.9 核心修复] 时间防火墙
        解析 RSS 的 pubDate (RFC 822格式)，过滤掉超过 48 小时的旧闻。
        例如: "Wed, 04 Feb 2026 09:30:00 GMT"
        """
        try:
            if not pub_date_str:
                return False
                
            # 使用 email.utils 解析标准 RSS 时间格式
            pub_dt = parsedate_to_datetime(pub_date_str)
            
            # 转换为不带时区的本地时间进行比较 (简化处理，防止时区报错)
            if pub_dt.tzinfo:
                pub_dt = pub_dt.replace(tzinfo=None)
            
            # 容差设为 48 小时，确保是“鲜活”的宏观消息
            # 如果新闻时间是 2025 年的，这里相减会非常大，直接返回 False
            return datetime.now() - pub_dt < timedelta(hours=48)
            
        except Exception as e:
            logger.warning(f"时间解析失败: {pub_date_str} -> {e}")
            return False # 解析失败视为旧闻，宁缺毋滥

    @retry(retries=2)
    def get_macro_news(self):
        """
        获取5条带来源的核心宏观新闻，并强制执行时间过滤
        """
        # Google 参数 when:2d 经常失效，必须靠代码二次清洗
        url = "https://news.google.com/rss/search?q=A股+宏观经济+中国央行+美联储&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        
        news_list = []
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            
            # 遍历 RSS item
            for item in root.findall('.//item'):
                # 只要前5条有效新闻
                if len(news_list) >= 5: 
                    break

                title_full = item.find('title').text
                pub_date = item.find('pubDate').text # 获取发布时间
                
                # --- 关键修复：强制时间检查 ---
                if not self._is_recent(pub_date):
                    # 如果是旧闻，直接跳过，看下一条
                    continue
                # -----------------------

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
                # 如果过滤完发现全是旧闻，宁愿返回空状态也不误导决策
                return [{"title": "暂无最新宏观数据 (48h内)", "source": "System Watch"}]
                
            return news_list
            
        except Exception as e:
            logger.error(f"宏观新闻获取失败: {e}")
            return [{"title": "宏观数据暂时不可用", "source": "System Error"}]

    def get_market_sentiment(self):
        """
        兼容旧接口，返回第一条新闻作为摘要
        """
        news = self.get_macro_news()
        return f"{news[0]['title']} - {news[0]['source']}"
