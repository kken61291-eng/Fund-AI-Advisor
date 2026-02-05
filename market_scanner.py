import akshare as ak
import requests
import pandas as pd
import time
import random
from datetime import datetime, timedelta
from utils import retry, logger

class MarketScanner:
    def __init__(self):
        # 伪装成浏览器，防止被反爬
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.cls.cn/"
        }

    def _parse_time(self, time_val):
        """
        通用时间解析器
        """
        try:
            now = datetime.now()
            # 1. 时间戳 (10位或13位)
            if isinstance(time_val, (int, float)):
                if time_val > 10000000000: # 13位毫秒
                    return datetime.fromtimestamp(time_val / 1000)
                return datetime.fromtimestamp(time_val)
            
            # 2. 字符串处理
            time_str = str(time_val).strip()
            
            # 格式: "2026-02-05 09:30:00"
            if "-" in time_str and ":" in time_str:
                return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            # 格式: "09:30" (补全为当天)
            elif ":" in time_str and len(time_str) <= 5:
                t = datetime.strptime(time_str, "%H:%M")
                return now.replace(hour=t.hour, minute=t.minute, second=0)
            
            return now
        except: return datetime.now()

    @retry(retries=1)
    def _get_eastmoney(self):
        """
        [源1] 东方财富 (EastMoney) - 稳健基石
        使用 AkShare 接口
        """
        news_list = []
        try:
            # 获取7x24小时全球财经直播
            df = ak.stock_news_em(symbol="全部")
            for _, row in df.iterrows():
                pub_dt = self._parse_time(f"{row['发布日期']} {row['发布时间']}")
                if datetime.now() - pub_dt > timedelta(hours=24): continue
                
                news_list.append({
                    "title": row['内容'][:80] + "..." if len(row['内容']) > 80 else row['内容'],
                    "source": "东方财富",
                    "pub_dt": pub_dt,
                    "weight": 1.0 # 基础权重
                })
        except Exception as e:
            logger.warning(f"东方财富源获取失败: {e}")
        return news_list

    @retry(retries=1)
    def _get_cailian_direct(self):
        """
        [源2] 财联社 (Cailian Press) - 速度之王
        直接调用官方 API，绕过 akshare 报错问题
        """
        news_list = []
        url = "https://www.cls.cn/nodeapi/telegraphList"
        params = {
            "rn": 20,           # 取20条
            "sv": "7.7.5"
        }
        try:
            # 财联社通常需要时间戳签名，但基础电报接口目前是公开的
            res = requests.get(url, params=params, headers=self.headers, timeout=5)
            if res.status_code == 200:
                data = res.json().get('data', {}).get('roll_data', [])
                for item in data:
                    title = item.get('title', '')
                    content = item.get('content', '')
                    # 财联社电报有时候没有标题，只有内容
                    final_title = title if title else (content[:80] + "..." if content else "快讯")
                    
                    # 财联社返回的是10位时间戳
                    pub_dt = self._parse_time(item.get('ctime', time.time()))
                    
                    if datetime.now() - pub_dt > timedelta(hours=24): continue
                    
                    news_list.append({
                        "title": final_title,
                        "source": "财联社·电报",
                        "pub_dt": pub_dt,
                        "weight": 1.5 # 财联社权重更高，因为它是短线风向标
                    })
        except Exception as e:
            logger.warning(f"财联社直连失败: {e}")
        return news_list

    @retry(retries=1)
    def _get_cninfo_direct(self):
        """
        [源3] 巨潮资讯 (Cninfo) - 官方喉舌
        直接调用官网'要闻'接口
        """
        news_list = []
        # 巨潮资讯-要闻 API
        url = "http://www.cninfo.com.cn/new/information/getFrontInfo"
        params = {"type": "1"} # type=1 通常是宏观/市场要闻
        
        try:
            res = requests.post(url, data=params, headers=self.headers, timeout=5)
            if res.status_code == 200:
                data = res.json()
                # 巨潮返回的数据结构通常比较深
                for item in data[:10]: # 取前10条
                    title = item.get('title', '')
                    if not title: continue
                    
                    # 巨潮时间格式通常是 "yyyy-MM-dd HH:mm:ss"
                    pub_dt = self._parse_time(item.get('publishTime', ''))
                    
                    if datetime.now() - pub_dt > timedelta(hours=48): continue # 巨潮更新慢，放宽到48h
                    
                    news_list.append({
                        "title": title,
                        "source": "巨潮资讯·要闻",
                        "pub_dt": pub_dt,
                        "weight": 1.2 # 官方性质，权重次高
                    })
        except Exception as e:
            logger.warning(f"巨潮资讯直连失败: {e}")
        return news_list

    def get_macro_news(self):
        """
        V12.5 三源合一：东财 + 财联社(直连) + 巨潮(直连)
        """
        logger.info("启动三源情报网 (EastMoney | Cailian | Cninfo)...")
        
        # 1. 并行获取三路数据
        news_pool = []
        news_pool.extend(self._get_eastmoney())
        news_pool.extend(self._get_cailian_direct())
        news_pool.extend(self._get_cninfo_direct())
        
        # 2. 灾难备份 (如果三路都挂了)
        if not news_pool:
            logger.error("API数据源全线异常，切换至 Google RSS 灾难备份...")
            return self._fallback_google_rss()
        
        # 3. 排序：按 (时间权重 * 0.7 + 来源权重 * 0.3) 排序？
        # 简单点：直接按时间倒序，最新的在最前
        news_pool.sort(key=lambda x: x['pub_dt'], reverse=True)
        
        # 4. 智能筛选与去重
        final_list = []
        seen_titles = set()
        
        # 关键词库 (Keywords)
        key_macro = ["央行", "美联储", "GDP", "CPI", "LPR", "印花税", "证监会"]
        key_market = ["成交额", "北向", "跳水", "拉升", "涨停", "违约"]
        
        # 策略：优先选带关键词的，且优先选财联社的
        for news in news_pool:
            if len(final_list) >= 6: break # 展示6条
            
            # 去重 (简单模糊匹配)
            is_duplicate = False
            for seen in seen_titles:
                # 如果标题相似度过高 (重合字符超过70%)
                if news['title'][:10] in seen or seen[:10] in news['title']:
                    is_duplicate = True
                    break
            if is_duplicate: continue
            
            # 标记为已选
            seen_titles.add(news['title'])
            
            # 格式化时间标签
            delta = datetime.now() - news['pub_dt']
            if delta.seconds < 3600:
                time_lbl = f"{int(delta.seconds/60)}分钟前"
            elif delta.days < 1:
                time_lbl = f"{int(delta.seconds/3600)}小时前"
            else:
                time_lbl = "1天前"
            
            final_list.append({
                "title": news['title'],
                "source": f"{news['source']} [{time_lbl}]"
            })
            
        return final_list

    def _fallback_google_rss(self):
        """
        RSS 备份
        """
        try:
            url = "https://news.google.com/rss/search?q=A股+宏观&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
            res = requests.get(url, timeout=10)
            root = ET.fromstring(res.content)
            ret = []
            for item in root.findall('.//item')[:5]:
                t = item.find('title').text.split(' - ')[0]
                ret.append({"title": t, "source": "RSS备份"})
            return ret
        except:
            return [{"title": "全网数据源连接中断", "source": "System Offline"}]

    def get_market_sentiment(self):
        news = self.get_macro_news()
        return f"{news[0]['title']} - {news[0]['source']}"
