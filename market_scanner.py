import akshare as ak
import pandas as pd
import socket
from utils import retry, logger
from news_analyst import NewsAnalyst 

class MarketScanner:
    def __init__(self):
        socket.setdefaulttimeout(5.0)
        try: self.ai_backup = NewsAnalyst()
        except: self.ai_backup = None

    def _get_column_by_fuzzy(self, df, keywords):
        for col in df.columns:
            col_str = str(col).lower()
            for kw in keywords:
                if kw in col_str:
                    return col
        return None

    def _ai_search_market_status(self, missing_type):
        if not self.ai_backup: return "数据源故障且AI离线"
        query = ""
        if missing_type == "north": query = "今日A股 上证指数 涨跌幅"
        elif missing_type == "sector": query = "今日A股 领涨板块 涨幅榜"
        titles = self.ai_backup.fetch_news_titles(query)
        return " | ".join(titles[:3]) if titles else "搜索无结果"

    @retry(retries=1)
    def get_market_sentiment(self):
        market_data = {
            "north_money": "0",
            "north_label": "数据获取中",
            "top_sectors": [],
            "market_status": "未知"
        }
        # 1. 宏观 (上证)
        try:
            df = ak.stock_zh_index_spot_em(symbol="sh000001")
            if not df.empty:
                pct_col = self._get_column_by_fuzzy(df, ["涨跌幅", "pct", "change"])
                if pct_col:
                    pct_val = float(df.iloc[0][pct_col])
                    market_data['north_money'] = f"{pct_val:+.2f}%"
                    market_data['north_label'] = "上证指数"
        except Exception as e:
            web_info = self._ai_search_market_status("north")
            market_data['north_label'] = "AI补救"
            market_data['north_money'] = "见摘要"

        # 2. 板块
        try:
            df_sector = ak.stock_board_industry_name_em()
            if not df_sector.empty:
                name_col = self._get_column_by_fuzzy(df_sector, ["名称", "板块", "name"])
                pct_col = self._get_column_by_fuzzy(df_sector, ["涨跌幅", "涨跌", "pct", "change"])
                if name_col and pct_col:
                    df_top = df_sector.sort_values(by=pct_col, ascending=False).head(3)
                    sectors = []
                    for _, row in df_top.iterrows():
                        sectors.append(f"{row[name_col]}({row[pct_col]:+.2f}%)")
                    market_data['top_sectors'] = sectors
        except Exception as e:
             market_data['top_sectors'] = ["暂无"]

        return market_data
