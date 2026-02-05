import akshare as ak
import requests
import re
from datetime import datetime
from utils import logger, retry

class MarketScanner:
    def __init__(self):
        pass

    @retry(retries=2, delay=2) 
    def get_macro_news(self):
        """
        获取宏观新闻 (使用东财要闻接口)
        """
        news_list = []
        try:
            # [修复] 改用最稳定的 stock_news_em
            df = ak.stock_news_em(symbol="要闻")
            
            keywords = ["央行", "加息", "降息", "GDP", "CPI", "美联储", "战争", "重磅", "国务院", "A股", "人民币", "PMI", "黄金"]
            
            count = 0
            for _, row in df.iterrows():
                title = str(row.get('title', ''))
                # 东财接口只有 public_time, title, content(可能为空)
                if not title: continue
                
                # 简单过滤
                if any(k in title for k in keywords):
                    news_list.append({
                        "title": title.strip(),
                        "source": "东方财富",
                        "time": row.get('public_time', '')
                    })
                    count += 1
                    if count >= 5: break
            
            if not news_list:
                # 兜底返回前3条，不管有没有关键词
                for _, row in df.head(3).iterrows():
                    news_list.append({"title": row['title'], "source": "东方财富", "time": row.get('public_time','')})
                
            return news_list
            
        except Exception as e:
            logger.warning(f"宏观新闻获取失败: {e}")
            return [{"title": "宏观数据源暂不可用", "source": "System"}]

    def get_sector_news(self, keyword):
        return []
