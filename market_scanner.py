import akshare as ak
import pandas as pd
from utils import retry, logger
from datetime import datetime
import difflib # 引入模糊匹配库

class MarketScanner:
    def __init__(self):
        pass

    def _find_function_dynamically(self, keywords):
        """
        【黑科技】在 akshare 库中动态搜索包含所有关键词的函数名
        """
        # 获取 akshare 所有属性/函数列表
        all_attrs = dir(ak)
        
        # 筛选同时包含所有 keywords 的函数
        candidates = [
            attr for attr in all_attrs 
            if all(k in attr for k in keywords) 
            and not attr.startswith('_') # 排除私有方法
        ]
        
        # 按长度排序，通常越短的越可能是主入口，或者按相似度排序
        if candidates:
            # 优先找完全匹配或最短的
            candidates.sort(key=len)
            logger.info(f"🔍 动态搜索关键词 {keywords}，找到候选: {candidates[:3]}...")
            return getattr(ak, candidates[0]) # 返回第一个函数对象
        return None

    @retry(retries=2)
    def get_market_sentiment(self):
        logger.info("📡 启动自适应全市场扫描...")
        market_data = {
            "north_money": 0,
            "north_label": "无数据",
            "top_sectors": [],
            "market_status": "震荡"
        }

        # --- 1. 获取北向资金 (自适应模式) ---
        try:
            # A计划: 尝试已知最稳定的接口名
            func = None
            try:
                # 尝试直接调用（假定它存在）
                if hasattr(ak, 'stock_hsgt_north_net_flow_in_em'):
                    func = ak.stock_hsgt_north_net_flow_in_em
                elif hasattr(ak, 'stock_hsgt_hist_em'):
                    func = ak.stock_hsgt_hist_em
            except:
                pass

            # B计划: 如果A计划都没找到，启动动态搜索
            if func is None:
                logger.warning("⚠️ 标准接口未找到，启动动态搜索 'hsgt' + 'north'...")
                func = self._find_function_dynamically(['hsgt', 'north', 'flow'])
            
            # 执行函数
            if func:
                # 注意：不同接口参数可能不同，这里尝试通用参数
                try:
                    df_north = func(symbol="北上")
                except TypeError:
                    df_north = func() # 尝试无参调用

                if not df_north.empty:
                    # 智能解析：不管列名叫什么，找数值列
                    # 通常北向资金接口会有一列是 'value' 或 'net_flow'
                    # 我们取最后一列（通常是数值）或者通过类型判断
                    latest_row = df_north.iloc[-1]
                    
                    # 暴力查找法：在最后一行里找最大的那个数字（假设净流入是核心数据）
                    # 或者找包含 "当日"、"净流入" 字眼的列
                    target_col = None
                    for col in df_north.columns:
                        if "净流入" in str(col) or "value" in str(col).lower():
                            target_col = col
                            break
                    
                    if target_col:
                        val = float(latest_row[target_col])
                        # 单位修正：如果是很大的数(>1亿)，说明是元；如果很小，可能是亿元
                        if abs(val) > 100000000: 
                            val = val / 100000000 # 转亿
                        elif abs(val) > 10000:
                            val = val / 10000 # 万转亿 (不太可能，通常是元)
                        
                        market_data['north_money'] = round(val, 2)
                        
                        # 打标签
                        if val > 20: market_data['north_label'] = "大幅流入 (利好)"
                        elif val > 0: market_data['north_label'] = "小幅流入 (温和)"
                        elif val < -20: market_data['north_label'] = "大幅流出 (利空)"
                        else: market_data['north_label'] = "小幅流出 (承压)"
                        
                        logger.info(f"✅ 北向资金获取成功 ({func.__name__}): {val}亿")
                    else:
                        logger.warning(f"获取数据成功但无法识别列名: {df_north.columns}")
            else:
                logger.error("❌ 无法找到北向资金相关接口")

        except Exception as e:
            logger.error(f"北向资金模块异常: {e}")

        # --- 2. 获取板块资金 (自适应模式) ---
        try:
            # 搜索包含 "board", "industry" 的接口
            func_sector = getattr(ak, 'stock_board_industry_name_em', None)
            if not func_sector:
                func_sector = self._find_function_dynamically(['board', 'industry', 'name'])

            if func_sector:
                # 尝试调用，通常需要 indicator="资金流向"
                try:
                    df_sector = func_sector(indicator="资金流向")
                except:
                    df_sector = func_sector() # 盲试

                if not df_sector.empty:
                    # 智能找列名：找包含 "净流入" 或 "主力" 的列
                    sort_col = None
                    for col in df_sector.columns:
                        if "主力" in str(col) and "流入" in str(col):
                            sort_col = col
                            break
                    
                    if sort_col:
                        df_top = df_sector.sort_values(by=sort_col, ascending=False).head(5)
                        sectors = []
                        for _, row in df_top.iterrows():
                            # 假设第一列是板块名
                            name = row.iloc[0] if isinstance(row.iloc[0], str) else row.iloc[1]
                            val = float(row[sort_col]) / 100000000 # 转亿
                            sectors.append(f"{name}({val:.1f}亿)")
                        market_data['top_sectors'] = sectors
                        logger.info(f"✅ 主力热点获取成功: {sectors}")
        except Exception as e:
            logger.error(f"板块资金模块异常: {e}")

        return market_data
