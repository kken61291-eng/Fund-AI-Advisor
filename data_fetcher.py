import akshare as ak
import pandas as pd
import time
import random
import os
import yaml
from datetime import datetime, time as dt_time
from utils import logger, retry, get_beijing_time

class DataFetcher:
    def __init__(self):
        # [V15.13] 本地数据仓库配置
        # 注意：这里保持您原有的 data_cache 目录名称
        self.DATA_DIR = "data_cache"
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)
            
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
        ]

    def _verify_data_freshness(self, df, fund_code, source_name):
        """数据新鲜度审计 (通用)"""
        if df is None or df.empty: return
        
        try:
            last_date = pd.to_datetime(df.index[-1]).date()
            now_bj = get_beijing_time()
            today_date = now_bj.date()
            is_trading_time = (dt_time(9, 30) <= now_bj.time() <= dt_time(15, 0))
            
            log_prefix = f"📅 [{source_name}] {fund_code} 最新日期: {last_date}"
            
            if last_date == today_date:
                logger.info(f"{log_prefix} | ✅ 数据已更新至今日")
            elif last_date < today_date:
                days_gap = (today_date - last_date).days
                # 如果是交易时间且数据滞后，才警告
                if is_trading_time and days_gap >= 1:
                    logger.warning(f"{log_prefix} | ⚠️ 数据滞后 {days_gap} 天 (请运行爬虫更新)")
                else:
                    logger.info(f"{log_prefix} | ⏸️ 历史数据就绪")
        except Exception as e:
            logger.warning(f"审计数据新鲜度失败: {e}")

    @retry(retries=3, delay=5)
    def _fetch_from_network(self, fund_code):
        """
        [私有方法] 纯联网获取数据 (东财 -> 新浪 -> 腾讯)
        供 update_cache 调用
        """
        # 1. 东财 (EastMoney) - 优先数据源
        try:
            # 模拟随机延时 (基础延时)
            time.sleep(random.uniform(1.0, 2.0)) 
            df = ak.fund_etf_hist_em(symbol=fund_code, period="daily", start_date="20200101", end_date="20500101", adjust="qfq")
            rename_map = {'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}
            df.rename(columns=rename_map, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            if not df.empty: return df, "东财"
        except: pass

        # 2. 新浪 (Sina)
        try:
            time.sleep(1)
            df = ak.fund_etf_hist_sina(symbol=fund_code)
            if df.index.name in ['date', '日期']: df = df.reset_index()
            # 简单的列对齐逻辑
            if len(df.columns) >= 6:
                df.columns = ['date', 'open', 'high', 'low', 'close', 'volume'] + list(df.columns[6:])
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                # 类型清洗
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
                return df, "新浪"
        except: pass

        # 3. 腾讯 (Tencent)
        try:
            time.sleep(1)
            prefix = 'sh' if fund_code.startswith('5') else ('sz' if fund_code.startswith('1') else '')
            if prefix:
                df = ak.stock_zh_a_hist_tx(symbol=f"{prefix}{fund_code}", start_date="20200101", adjust="qfq")
                rename_map = {'日期':'date', '开盘':'open', '收盘':'close', '最高':'high', '最低':'low', '成交量':'volume'}
                df.rename(columns=rename_map, inplace=True)
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                if not df.empty: return df, "腾讯"
        except: pass
        
        return None, None

    def update_cache(self, fund_code):
        """
        [爬虫专用] 联网下载数据并保存到本地 CSV
        """
        df, source = self._fetch_from_network(fund_code)
        if df is not None and not df.empty:
            file_path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
            df.to_csv(file_path)
            logger.info(f"💾 [{source}] {fund_code} 数据已保存至 {file_path}")
            
            # [新增优化] 如果是东财数据，强制等待 50 秒，防止接口封禁
            # 这样可以最大程度保证后续的基金也能用到东财数据
            if source == "东财":
                logger.info("⏳ [东财] 触发频率保护机制，等待 50 秒...")
                time.sleep(50)
                
            return True
        else:
            logger.error(f"❌ {fund_code} 所有数据源(东财/新浪/腾讯)均获取失败")
            return False

    def get_fund_history(self, fund_code, days=250):
        """
        [主程序专用] 只读模式：直接从本地 CSV 读取数据
        """
        file_path = os.path.join(self.DATA_DIR, f"{fund_code}.csv")
        
        if not os.path.exists(file_path):
            # 这里的提示引导用户去运行爬虫
            logger.warning(f"⚠️ 本地缓存缺失: {fund_code}，请等待 GitHub Action 爬虫运行")
            return None
            
        try:
            # 读取 CSV
            df = pd.read_csv(file_path)
            
            # 还原索引和数据类型
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            
            self._verify_data_freshness(df, fund_code, "本地缓存")
            return df
            
        except Exception as e:
            logger.error(f"❌ 读取本地缓存失败 {fund_code}: {e}")
            return None

# ==========================================
# [新增] 独立运行入口 (让此脚本变身爬虫)
# ==========================================
if __name__ == "__main__":
    print("🚀 [DataFetcher] 启动多源行情抓取 (V15.15 Full Mode)...")
    
    # 1. 简易加载 Config
    def load_config_local():
        try:
            with open('config.yaml', 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except:
            return {}

    cfg = load_config_local()
    funds = cfg.get('funds', [])
    
    if not funds:
        print("⚠️ 未找到基金列表，请检查 config.yaml")
        exit()

    # 2. 初始化
    fetcher = DataFetcher()
    success_count = 0
    
    # 3. 循环更新
    for fund in funds:
        code = fund.get('code')
        name = fund.get('name')
        print(f"🔄 更新: {name} ({code})...")
        
        try:
            # 调用 update_cache 进行联网下载
            # 注意：update_cache 内部现在包含了针对东财的 50s 等待逻辑
            if fetcher.update_cache(code):
                success_count += 1
            
            # 基础间隔，避免非东财源时请求过快
            # 如果刚刚触发了东财的50s等待，这里额外多睡1-2s也无妨
            time.sleep(random.uniform(1.0, 2.0))
            
        except Exception as e:
            print(f"❌ 更新异常 {name}: {e}")
            
    print(f"🏁 行情更新完成: {success_count}/{len(funds)}")
