import akshare as ak
import pandas as pd
from datetime import datetime
from utils import logger, retry

class MarketScanner:
    """
    市场扫描器 - V3.5 适配版
    新增：北向/主力资金流向获取
    """
    def __init__(self):
        pass

    def _format_time(self, time_str):
        """统一时间格式为 MM-DD HH:MM"""
        try:
            s = str(time_str)
            if len(s) > 10: 
                # 尝试解析标准格式
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%m-%d %H:%M")
            return s
        except:
            return str(time_str)[:16]

    @retry(retries=2, delay=2)
    def get_market_vitality(self):
        """
        [v3.5] 获取全市场生命力指标
        返回: 
        - net_flow: 北向/主力资金净流入 (亿元)
        - market_mood: 市场情绪状态
        """
        try:
            # 优先尝试获取北向资金 (作为全市场风向标)
            # 注意：接口可能随 akshare 版本变化，增加容错
            try:
                # 东方财富-北向资金实时流向
                flow_df = ak.stock_hsgt_north_net_flow_in_em(symbol="北上")
                # 获取最新一条数据
                raw_val = flow_df.iloc[-1]['value']
                net_flow = raw_val / 100000000 # 转为亿元
            except:
                logger.warning("北向资金接口调用失败，尝试主力资金接口...")
                # 备选：个股主力资金流向（这里简化为返回0，或可接入板块资金流）
                net_flow = 0.0
            
            mood = "Neutral"
            if net_flow > 20: mood = "Bullish"
            elif net_flow < -20: mood = "Bearish"
            
            return {
                "net_flow": round(net_flow, 2), # 亿元
                "market_mood": mood
            }
        except Exception as e:
            logger.warning(f"资金流获取失败: {e}")
            return {"net_flow": 0, "market_mood": "Neutral"}

    def get_leader_status(self, sector_keyword):
        """
        [v3.5] 获取板块龙头状态 (用于补涨逻辑校验)
        目前返回默认状态，后续可接入板块成分股涨幅排序
        """
        # Placeholder
        return "UNKNOWN" 

    @retry(retries=2, delay=2) 
    def get_macro_news(self):
        """
        获取全市场重磅新闻 (V14.19 智能兜底版)
        """
        news_list = []
        try:
            # 东方财富-新闻联播/要闻
            df = ak.stock_news_em(symbol="要闻")
            
            # 列名兼容性处理
            title_col = 'title'
            if 'title' not in df.columns:
                if '新闻标题' in df.columns: title_col = '新闻标题'
                elif '文章标题' in df.columns: title_col = '文章标题'
            
            time_col = 'public_time'
            if 'public_time' not in df.columns:
                if '发布时间' in df.columns: time_col = '发布时间'
                elif 'time' in df.columns: time_col = 'time'

            # 关键词库
            keywords = [
                "中共中央", "政治局", "国务院", "发改委", "财政部", "证监会", "央行", 
                "加息", "降息", "降准", "LPR", "社融", "GDP", "CPI", "PMI", 
                "印花税", "注册制", "北向", "外资", "增持", "回购", "汇金"
            ]
            
            junk_words = ["汇总", "集锦", "收评", "早报", "晚报", "公告一览"]

            # 1. 关键词筛选
            for _, row in df.iterrows():
                title = str(row.get(title_col, ''))
                raw_time = str(row.get(time_col, ''))
                
                if not title or title == 'nan': continue
                if any(jw in title for jw in junk_words): continue
                
                if any(k in title for k in keywords):
                    news_list.append({
                        "title": title.strip(),
                        "source": "全球快讯",
                        "time": self._format_time(raw_time)
                    })

            # 2. 兜底策略
            if len(news_list) == 0:
                for _, row in df.iterrows():
                    title = str(row.get(title_col, ''))
                    raw_time = str(row.get(time_col, ''))
                    if any(jw in title for jw in junk_words): continue
                    
                    news_list.append({
                        "title": title.strip(), 
                        "source": "市场资讯", 
                        "time": self._format_time(raw_time)
                    })
                    if len(news_list) >= 5: break

            return news_list
            
        except Exception as e:
            logger.warning(f"宏观新闻获取异常: {e}")
            return [{"title": "数据源波动，关注盘面资金。", "source": "系统", "time": datetime.now().strftime("%m-%d %H:%M")}]

    def get_sector_news(self, keyword):
        return []
