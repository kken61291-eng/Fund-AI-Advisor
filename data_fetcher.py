import akshare as ak
import pandas as pd
import ta
from utils import retry, logger
from datetime import datetime, timedelta

class DataFetcher:
    def __init__(self):
        pass

    @retry(retries=3)
    def get_fund_history(self, code, start_date=None):
        """获取基金历史净值并计算技术指标"""
        if not start_date:
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
        
        logger.info(f"正在获取基金 {code} 的数据...")
        
        # 场外基金通用接口
        try:
            # 【修复点】这里原来是 fund=code，现在必须改名为 symbol=code
            df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
        except TypeError:
             # 防御性编程：如果 akshare 版本又变了，尝试位置参数
            df = ak.fund_open_fund_info_em(code, "单位净值走势")
        except Exception as e:
            logger.warning(f"接口1失败 ({e})，尝试备用接口...")
            # 备用：ETF接口
            df = ak.fund_etf_hist_sina(symbol=f"sz{code}")

        if df is None or df.empty:
            raise ValueError(f"未能获取到基金 {code} 的数据")

        # 数据清洗
        df = df.rename(columns={"净值日期": "date", "单位净值": "close", "日增长率": "change"})
        df['date'] = pd.to_datetime(df['date'])
        df['close'] = pd.to_numeric(df['close'])
        df = df.sort_values('date')

        # --- 技术指标计算 ---
        # 1. RSI
        df['rsi'] = ta.momentum.rsi(df['close'], window=14)
        
        # 2. MACD
        macd = ta.trend.MACD(df['close'])
        df['macd'] = macd.macd()
        df['macd_signal'] = macd.macd_signal()
        df['macd_diff'] = macd.macd_diff()
        
        # 3. MA20
        df['ma20'] = ta.trend.sma_indicator(df['close'], window=20)
        
        # 返回最近的一条数据
        latest_data = df.iloc[-1].to_dict()
        latest_data['price_position'] = 'bull' if latest_data['close'] > latest_data['ma20'] else 'bear'
        
        return latest_data

if __name__ == "__main__":
    fetcher = DataFetcher()
    # 本地测试时，确保你安装了依赖
    # print(fetcher.get_fund_history("001552"))
