import akshare as ak
import requests
import re
from datetime import datetime
from utils import logger, retry

class MarketScanner:
    def __init__(self):
        pass

    def _format_time(self, time_str):
        """统一时间格式为 MM-DD HH:MM"""
        try:
            # 尝试解析完整时间 YYYY-MM-DD HH:MM:SS
            dt = datetime.strptime(str(time_str), "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%m-%d %H:%M")
        except:
            # 如果解析失败（比如已经是短时间），尝试保留关键部分
            s = str(time_str)
            if len(s) > 10: return s[5:16] # 截取 MM-DD HH:MM
            return s

    @retry(retries=2, delay=2) 
    def get_macro_news(self):
        """
        获取全市场重磅新闻 (剔除汇总类，保留原子化快讯)
        """
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

            keywords = ["央行", "加息", "降息", "GDP", "CPI", "美联储", "突发", "重磅", "国务院", "立案", "违约", "停牌", "外资", "财政部", "印花税", "A股"]
            junk_words = ["汇总", "集锦", "回顾", "收评", "早报", "晚报", "盘前", "要闻精选", "公告一览"]

            for _, row in df.iterrows():
                title = str(row.get(title_col, ''))
                raw_time = str(row.get(time_col, ''))
                
                if not title or title == 'nan': continue
                if any(jw in title for jw in junk_words): continue
                
                # [修复] 统一时间格式
                clean_time = self._format_time(raw_time)
                
                if any(k in title for k in keywords):
                    news_list.append({
                        "title": title.strip(),
                        "source": "全球快讯",
                        "time": clean_time
                    })
            
            if len(news_list) < 5:
                for _, row in df.iterrows():
                    title = str(row.get(title_col, ''))
                    raw_time = str(row.get(time_col, ''))
                    if any(jw in title for jw in junk_words): continue
                    
                    if any(n['title'] == title for n in news_list): continue
                    
                    news_list.append({
                        "title": title.strip(), 
                        "source": "市场资讯", 
                        "time": self._format_time(raw_time)
                    })
                    if len(news_list) >= 10: break

            return news_list
            
        except Exception as e:
            logger.warning(f"宏观新闻获取微瑕: {e}")
            return [{"title": "数据源波动，关注盘面资金。", "source": "系统", "time": datetime.now().strftime("%m-%d %H:%M")}]

    def get_sector_news(self, keyword):
        return []
