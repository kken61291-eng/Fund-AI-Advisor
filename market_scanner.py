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
        获取全市场重磅新闻 (剔除汇总类，保留原子化快讯)
        """
        news_list = []
        try:
            # 使用东财要闻接口
            df = ak.stock_news_em(symbol="要闻")
            
            # 列名自适应
            title_col = 'title'
            if 'title' not in df.columns:
                if '新闻标题' in df.columns: title_col = '新闻标题'
                elif '文章标题' in df.columns: title_col = '文章标题'
            
            time_col = 'public_time'
            if 'public_time' not in df.columns:
                if '发布时间' in df.columns: time_col = '发布时间'
                elif 'time' in df.columns: time_col = 'time'

            # 高权重关键词
            keywords = ["央行", "加息", "降息", "GDP", "CPI", "美联储", "突发", "重磅", "国务院", "立案", "违约", "停牌", "外资", "财政部", "印花税"]
            # 垃圾词过滤 (剔除汇总类)
            junk_words = ["汇总", "集锦", "回顾", "收评", "早报", "晚报", "盘前", "要闻精选"]

            for _, row in df.iterrows():
                title = str(row.get(title_col, ''))
                pub_time = str(row.get(time_col, ''))
                
                if not title or title == 'nan': continue
                
                # 过滤垃圾汇总
                if any(jw in title for jw in junk_words):
                    continue
                
                # 只要包含关键词，或者标题长度适中且不含垃圾词
                if any(k in title for k in keywords):
                    news_list.append({
                        "title": title.strip(),
                        "source": "全球快讯",
                        "time": pub_time
                    })
            
            # 如果筛选后太少，补充前几条非垃圾词新闻
            if len(news_list) < 5:
                for _, row in df.iterrows():
                    title = str(row.get(title_col, ''))
                    if any(jw in title for jw in junk_words): continue
                    
                    # 查重
                    if any(n['title'] == title for n in news_list): continue
                    
                    news_list.append({
                        "title": title.strip(), 
                        "source": "市场资讯", 
                        "time": str(row.get(time_col, ''))
                    })
                    if len(news_list) >= 10: break

            return news_list
            
        except Exception as e:
            logger.warning(f"宏观新闻获取微瑕: {e}")
            return [{"title": "市场数据源波动，建议关注盘面资金流向。", "source": "系统提示", "time": ""}]

    def get_sector_news(self, keyword):
        return []
