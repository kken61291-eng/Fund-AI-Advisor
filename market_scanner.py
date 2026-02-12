import akshare as ak
import pandas as pd
import re
from datetime import datetime
from utils import logger, retry

class MarketScanner:
    """
    市场扫描器 - V19.2 (资金流修复版)
    
    变更说明：
    1. [移除] 已停更的北向资金接口 (stock_hsgt_north_net_flow_in_em)
    2. [新增] 全市场主力资金流向 (Main Force Flow) - 来源: 东方财富行业板块汇总
    3. [保留] 宏观新闻获取 (get_macro_news)
    """
    def __init__(self):
        pass

    def _format_time(self, time_str):
        """
        [工具] 统一时间格式为 MM-DD HH:MM
        """
        try:
            s = str(time_str)
            # 处理 "2024-02-12 10:00:00" 格式
            if len(s) > 10: 
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                return dt.strftime("%m-%d %H:%M")
            return s
        except:
            # 兜底返回前16位
            return str(time_str)[:16]

    def _parse_flow_value(self, val):
        """
        [工具] 解析带单位的资金数值
        例如: '-15.5亿' -> -15.5; '3000万' -> 0.3; '2.5万' -> 0.00025
        统一返回单位: 亿元 (float)
        """
        try:
            # 如果已经是数字，假设单位是元，转为亿元
            if isinstance(val, (int, float)):
                return float(val) / 100000000 
            
            s = str(val).strip()
            unit = 1.0 # 默认单位为亿
            
            if '亿' in s:
                unit = 1.0
                s = s.replace('亿', '')
            elif '万' in s:
                unit = 0.0001
                s = s.replace('万', '')
            else:
                # 纯数字字符串，默认为元，转为亿
                unit = 0.00000001
            
            return float(s) * unit
        except:
            return 0.0

    @retry(retries=2, delay=2)
    def get_market_vitality(self):
        """
        [v19.2] 获取全市场生命力指标 (资金流向)
        替代方案：汇总所有行业板块的“今日主力净流入”
        """
        try:
            # 1. 获取东方财富行业资金流向 (实时/盘后)
            # indicator="今日" 代表当日实时数据
            sector_flow_df = ak.stock_sector_fund_flow_rank(indicator="今日")
            
            if sector_flow_df is None or sector_flow_df.empty:
                logger.warning("主力资金接口返回为空，返回中性信号")
                return {"net_flow": 0, "market_mood": "Neutral"}

            # 2. 计算全市场净流入 (Sum of all sectors)
            # 自动寻找包含 "净流入" 和 "主力" 的列名
            total_flow = 0.0
            target_col = None
            
            for col in sector_flow_df.columns:
                # 排除 "占比" 列，只找金额列
                if "净流入" in col and "主力" in col and "占比" not in col:
                    target_col = col
                    break
            
            if target_col:
                # 累加所有板块的净流入
                for val in sector_flow_df[target_col]:
                    total_flow += self._parse_flow_value(val)
            else:
                logger.warning(f"未找到资金流列名: {sector_flow_df.columns}")

            # 3. 定性判断 (Market Mood)
            # 阈值：>100亿为强势，<-100亿为弱势
            mood = "Neutral"
            if total_flow > 100: mood = "Bullish"
            elif total_flow < -100: mood = "Bearish"
            
            logger.info(f"💰 全市场主力净流入: {round(total_flow, 2)}亿 ({mood})")
            
            return {
                "net_flow": round(total_flow, 2), # 单位：亿元
                "market_mood": mood
            }

        except Exception as e:
            logger.warning(f"资金流获取失败 (Plan B): {e}")
            return {"net_flow": 0, "market_mood": "Neutral"}

    def get_leader_status(self, sector_keyword):
        """
        [v3.5] 获取板块龙头状态 (Placeholder)
        目前返回默认状态，防止报错
        """
        return "UNKNOWN" 

    @retry(retries=2, delay=2) 
    def get_macro_news(self):
        """
        获取全市场重磅新闻 (V14.19 智能兜底版)
        逻辑：先用关键词过滤“要闻”，如果没结果，则启用兜底策略获取前5条
        """
        news_list = []
        try:
            # 东方财富-新闻联播/要闻
            df = ak.stock_news_em(symbol="要闻")
            
            # 列名兼容性处理 (防止接口列名变动)
            title_col = 'title'
            if 'title' not in df.columns:
                if '新闻标题' in df.columns: title_col = '新闻标题'
                elif '文章标题' in df.columns: title_col = '文章标题'
            
            time_col = 'public_time'
            if 'public_time' not in df.columns:
                if '发布时间' in df.columns: time_col = '发布时间'
                elif 'time' in df.columns: time_col = 'time'

            # 核心关键词库
            keywords = [
                "中共中央", "政治局", "国务院", "发改委", "财政部", "证监会", "央行", 
                "加息", "降息", "降准", "LPR", "社融", "GDP", "CPI", "PMI", 
                "印花税", "注册制", "北向", "外资", "增持", "回购", "汇金"
            ]
            
            # 垃圾词过滤
            junk_words = ["汇总", "集锦", "收评", "早报", "晚报", "公告一览"]

            # 1. 第一轮：关键词筛选
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

            # 2. 第二轮：兜底策略 (如果关键词没命中，取前5条)
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
        """
        板块新闻获取 (目前返回空列表，避免报错)
        """
        return []
