import akshare as ak
import tushare as ts
import pandas as pd
import os
import datetime
from utils import retry, logger

class DataFetcher:
    def __init__(self):
        self.ts_token = os.getenv("TUSHARE_TOKEN")
        self.pro = None
        
        if self.ts_token:
            try:
                ts.set_token(self.ts_token)
                self.pro = ts.pro_api()
                logger.info("✅ Tushare 初始化成功")
            except Exception as e:
                logger.warning(f"Tushare 初始化失败: {e}")
        else:
            logger.info("ℹ️ 未检测到 TUSHARE_TOKEN，将使用纯 Akshare 模式")

    @retry(retries=3)
    def get_fund_history(self, code):
        """
        获取日线数据，并自动生成周线数据 (V4.1 修复版)
        """
        try:
            # 【关键修复】参数名从 fund 改为 symbol
            # 适配 AkShare 新版接口
            df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
            
            if df is None or df.empty:
                logger.warning(f"{code} 获取到的数据为空")
                return {"daily": pd.DataFrame(), "weekly": pd.DataFrame()}

            # 清洗数据
            df = df.rename(columns={"净值日期": "date", "单位净值": "close"})
            df['date'] = pd.to_datetime(df['date'])
            df['close'] = pd.to_numeric(df['close'])
            df = df.sort_values('date').set_index('date')
            
            # 生成周线 (Resample)
            weekly_df = df.resample('W').agg({'close': 'last'}).dropna()

            return {
                "daily": df,
                "weekly": weekly_df
            }
        except Exception as e:
            logger.error(f"数据获取失败 {code}: {e}")
            return {"daily": pd.DataFrame(), "weekly": pd.DataFrame()}
