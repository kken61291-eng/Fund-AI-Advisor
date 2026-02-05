import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from utils import retry, logger

class MarketScanner:
    def __init__(self):
        pass

    def _parse_cls_time(self, time_str):
        """
        解析财联社时间 (格式通常为 HH:MM)
        需要补全为今天的日期
        """
        try:
            now = datetime.now()
            # 财联社只给 HH:MM，默认为当天
            t = datetime.strptime(time_str, "%H:%M")
            return now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
        except: return datetime.now()

    @retry(retries=1)
    def _get_cailian_press(self):
        """
        [源1] 财联社电报 - A股短线情绪的核心
        替代雪球/同花顺，这是它们的上游数据源
        """
        try:
            # 获取财联社电报
            df = ak.stock_telegraph_cls(symbol="全部")
            # 数据清洗
            news_list = []
            for _, row in df.iterrows():
                # 只要最近 24 小时内的
                pub_time = f"{row['日期']} {row['时间']}"
                pub_dt = datetime.strptime(pub_time, "%Y-%m-%d %H:%M")
                
                if datetime.now() - pub_dt > timedelta(hours=24):
                    continue

                content = row['内容']
                # 简化内容：如果是长文，只取第一句或标题
                title = row['标题'] if row['标题'] else content[:40] + "..."
                
                news_list.append({
                    "title": title,
                    "source": "财联社·电报",
                    "pub_dt": pub_dt
                })
            return news_list[:10] # 取前10条备选
        except Exception as e:
            logger.warning(f"财联社源获取失败: {e}")
            return []

    @retry(retries=1)
    def _get_eastmoney_news(self):
        """
        [源2] 东方财富 - 全球财经快讯
        用于补充宏观视角
        """
        try:
            # 东方财富-全球财经快讯
            df = ak.stock_info_global_cls(symbol="全部")
            news_list = []
            for _, row in df.iterrows():
                pub_time = f"{row['发布日期']} {row['发布时间']}"
                pub_dt = datetime.strptime(pub_time, "%Y-%m-%d %H:%M:%S")

                if datetime.now() - pub_dt > timedelta(hours=24):
                    continue
                
                news_list.append({
                    "title": row['标题'],
                    "source": "东财·快讯",
                    "pub_dt": pub_dt
                })
            return news_list[:10]
        except Exception as e:
            logger.warning(f"东方财富源获取失败: {e}")
            return []

    def get_macro_news(self):
        """
        V12.0 全视之眼：多源聚合 + 实时热点
        """
        logger.info("正在连接 财联社 & 东方财富 实时数据终端...")
        
        # 1. 并行获取多源数据
        cls_news = self._get_cailian_press()
        em_news = self._get_eastmoney_news()
        
        # 2. 合并数据池
        all_news = cls_news + em_news
        
        # 3. 按时间倒序排列 (最新的在最前)
        all_news.sort(key=lambda x: x['pub_dt'], reverse=True)
        
        # 4. 智能筛选 (去重 + 关键词加权)
        final_list = []
        seen_titles = set()
        
        # 关键词加权：优先展示包含这些词的新闻
        keywords = ["央行", "美联储", "降息", "GDP", "CPI", "牛市", "暴跌", "成交额", "北向"]
        
        # 先挑有关键词的
        for news in all_news:
            if len(final_list) >= 3: break
            if any(k in news['title'] for k in keywords) and news['title'] not in seen_titles:
                final_list.append(news)
                seen_titles.add(news['title'])
        
        # 再填补剩下的空位
        for news in all_news:
            if len(final_list) >= 5: break
            if news['title'] not in seen_titles:
                final_list.append(news)
                seen_titles.add(news['title'])
        
        # 5. 格式化输出 (计算 'x小时前')
        output = []
        if not final_list:
             return [{"title": "全网情报静默 (API连接异常或无重大消息)", "source": "System Watch"}]

        for news in final_list:
            delta = datetime.now() - news['pub_dt']
            
            if delta.seconds < 3600:
                time_label = f"{int(delta.seconds/60)}分钟前"
            elif delta.days < 1:
                time_label = f"{int(delta.seconds/3600)}小时前"
            else:
                time_label = "1天前"
            
            output.append({
                "title": news['title'],
                "source": f"{news['source']} [{time_label}]"
            })
            
        return output

    def get_market_sentiment(self):
        news = self.get_macro_news()
        return f"{news[0]['title']} - {news[0]['source']}"
