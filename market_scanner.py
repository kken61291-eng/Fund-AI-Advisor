import akshare as ak
import requests
import re
from datetime import datetime
from utils import logger, retry

class MarketScanner:
    def __init__(self):
        pass

    def _format_time(self, time_str):
        """统一时间格式"""
        try:
            # 东财直播流的时间通常是 "2026-02-05 14:30:00"
            dt = datetime.strptime(str(time_str), "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%m-%d %H:%M")
        except:
            s = str(time_str)
            if len(s) > 10: return s[5:16]
            return s

    @retry(retries=2, delay=2) 
    def get_macro_news(self):
        """
        获取全球 7x24小时 直播快讯 (替代财联社电报)
        """
        news_list = []
        try:
            # [核心升级] 改用全球财经直播流 (最快讯源)
            df = ak.stock_info_global_ems()
            
            # 这里的列名通常是 '发布时间', '内容', '标题'
            # 内容往往比标题更丰富，我们优先看内容
            
            # 关键词库 (保持 V14.15 的天网配置)
            keywords = [
                "中共中央", "政治局", "国务院", "证监会", "央行", "新华社",
                "加息", "降息", "降准", "LPR", "社融", "M2", "信贷", "流动性",
                "GDP", "CPI", "PPI", "PMI", "非农", "汇率", "人民币",
                "印花税", "T+0", "注册制", "做空", "市值管理", "回购",
                "汇金", "证金", "社保", "大基金", "北向", "外资",
                "突发", "重磅", "立案", "违约", "战争", "地缘"
            ]
            
            # 垃圾词 (直播流里会有很多无用行情播报)
            junk_words = ["报", "美元", "现货", "期货", "日内", "新高", "新低", "行情"] 

            count = 0
            for _, row in df.iterrows():
                content = str(row.get('内容', ''))
                title = str(row.get('标题', ''))
                pub_time = str(row.get('发布时间', ''))
                
                # 如果标题为空，用内容的前50个字代替
                display_text = title if title and title != 'nan' else content
                if not display_text: continue
                
                # 清洗 HTML 标签
                display_text = re.sub(r'<[^>]+>', '', display_text).strip()
                
                # 过滤垃圾
                if any(jw in display_text for jw in junk_words): continue
                
                # 匹配关键词
                if any(k in display_text for k in keywords):
                    news_list.append({
                        "title": display_text[:60] + ("..." if len(display_text)>60 else ""), # 控制长度
                        "source": "全球7x24",
                        "time": self._format_time(pub_time)
                    })
                    count += 1
                    if count >= 10: break # 取前10条最热乎的

            return news_list
            
        except Exception as e:
            logger.warning(f"快讯获取微瑕: {e}")
            # 兜底返回一条系统消息
            return [{"title": "实时数据链路重连中，请关注静态盘面。", "source": "系统", "time": datetime.now().strftime("%m-%d %H:%M")}]

    def get_sector_news(self, keyword):
        return []
