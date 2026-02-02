import akshare as ak
import pandas as pd
from utils import retry, logger

class MarketScanner:
    def __init__(self):
        pass

    @retry(retries=2)
    def get_market_sentiment(self):
        logger.info("📡 正在扫描全市场 (V3.0 稳定版)...")
        market_data = {
            "north_money": 0,
            "north_label": "无数据",
            "top_sectors": [],
            "market_status": "震荡"
        }

        # --- 1. 获取北向资金 (策略：沪股通 + 深股通 合并计算) ---
        try:
            # 分别获取，防止“北向”总接口报错
            sh_df = ak.stock_hsgt_hist_em(symbol="沪股通")
            sz_df = ak.stock_hsgt_hist_em(symbol="深股通")
            
            total_inflow = 0
            success_count = 0

            # 处理沪股通
            if not sh_df.empty:
                # 找数值列
                for col in sh_df.columns:
                    if "净流入" in str(col):
                        val = float(sh_df.iloc[-1][col])
                        # 单位修正：如果是亿元单位(<1000)，直接用；如果是百万元，转亿
                        # 历史经验：akshare历史接口通常返回 亿元
                        # 我们假设它返回的是 亿元 (例如 12.5)
                        # 如果数值巨大(>10000)，说明是万元
                        if abs(val) > 10000: val /= 10000
                        total_inflow += val
                        success_count += 1
                        break
            
            # 处理深股通
            if not sz_df.empty:
                for col in sz_df.columns:
                    if "净流入" in str(col):
                        val = float(sz_df.iloc[-1][col])
                        if abs(val) > 10000: val /= 10000
                        total_inflow += val
                        success_count += 1
                        break

            if success_count > 0:
                net_inflow = round(total_inflow, 2)
                market_data['north_money'] = net_inflow
                
                if net_inflow > 20: market_data['north_label'] = "大幅流入"
                elif net_inflow > 0: market_data['north_label'] = "小幅流入"
                elif net_inflow > -20: market_data['north_label'] = "小幅流出"
                else: market_data['north_label'] = "大幅流出"
                
                logger.info(f"✅ 北向资金(沪+深)锁定: {net_inflow}亿")
            else:
                logger.warning("❌ 沪深数据均获取失败")

        except Exception as e:
            logger.error(f"北向资金计算异常: {e}")

        # --- 2. 获取领涨板块 (策略：直接用行情接口找涨幅榜) ---
        try:
            # 这个接口在你的日志里证明是通的，返回了 ['板块名称', '涨跌幅'...]
            df_sector = ak.stock_board_industry_name_em()
            
            if not df_sector.empty:
                # 1. 找名字列
                name_col = None
                for col in ["板块名称", "名称", "板块"]:
                    if col in df_sector.columns:
                        name_col = col
                        break
                
                # 2. 找涨跌幅列
                change_col = None
                for col in ["涨跌幅", "涨跌"]:
                    if col in df_sector.columns:
                        change_col = col
                        break

                if name_col and change_col:
                    # 按涨跌幅倒序
                    df_top = df_sector.sort_values(by=change_col, ascending=False).head(5)
                    
                    sectors = []
                    for _, row in df_top.iterrows():
                        s_name = row[name_col]
                        s_val = float(row[change_col])
                        # 格式：半导体(+3.5%)
                        sectors.append(f"{s_name}({s_val:+.2f}%)")
                    
                    market_data['top_sectors'] = sectors
                    logger.info(f"✅ 领涨板块锁定: {sectors}")
                else:
                    logger.warning(f"❌ 板块列名匹配失败: {df_sector.columns}")
        except Exception as e:
            logger.error(f"板块数据获取异常: {e}")

        return market_data
