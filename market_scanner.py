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
        news_list = []
        try:
            df = ak.stock_news_em(symbol="要闻")
            
            title_col = 'title'
            if 'title' not in df.columns:
                if '新闻标题' in df.columns: title_col = '新闻标题'
                elif '文章标题' in df.columns: title_col = '文章标题'
            
            time_col = 'public_time'
            if 'public_time' not in df.columns:
                if '发布时间' in df.columns: time_col = '发布时间'
                elif 'time' in df.columns: time_col = 'time'

            keywords = ["央行", "加息", "降息", "GDP", "CPI", "美联储", "战争", "重磅", "国务院", "A股", "人民币", "PMI", "黄金", "财政"]
            
            count = 0
            for _, row in df.iterrows():
                title = str(row.get(title_col, ''))
                pub_time = str(row.get(time_col, ''))
                if not title or title == 'nan': continue
                
                if any(k in title for k in keywords):
                    news_list.append({
                        "title": title.strip(),
                        "source": "东方财富",
                        "time": pub_time
                    })
                    count += 1
                    if count >= 5: break
            
            if not news_list:
                for _, row in df.head(3).iterrows():
                    news_list.append({
                        "title": str(row.get(title_col, '无标题')), 
                        "source": "东方财富", 
                        "time": str(row.get(time_col, ''))
                    })
            return news_list
            
        except Exception as e:
            logger.warning(f"宏观新闻获取微瑕: {e}")
            return [{"title": "市场数据源波动，建议关注盘面资金流向。", "source": "系统提示"}]

    def get_sector_news(self, keyword):
        return []
