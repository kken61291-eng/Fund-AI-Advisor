import akshare as ak
import socket
from utils import retry, logger
from news_analyst import NewsAnalyst 

class MarketScanner:
    def __init__(self):
        socket.setdefaulttimeout(5.0)
        try: self.ai = NewsAnalyst()
        except: self.ai = None

    def get_market_sentiment(self):
        data = {"north_money": "0", "north_label": "数据中", "top_sectors": []}
        try:
            df = ak.stock_zh_index_spot_em(symbol="sh000001")
            if not df.empty:
                pct = float(df.iloc[0]['涨跌幅'])
                data['north_money'] = f"{pct:+.2f}%"
                data['north_label'] = "上证指数"
        except:
            if self.ai: 
                t = self.ai.fetch_news_titles("今日A股 上证指数")
                data['north_money'] = "见摘要"; data['north_label'] = t[0] if t else "未知"
        return data
