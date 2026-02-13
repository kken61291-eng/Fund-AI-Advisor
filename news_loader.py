import requests
import json
import time
import random
import pandas as pd
from datetime import datetime
import pytz

# ==========================================
# 实时行情抓取模块 (Anti-Ban & Real-Time)
# ==========================================

class RealTimeDataFetcher:
    def __init__(self):
        # 预设随机 User-Agent 池
        self.ua_list = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        self.data_dir = "data_cache"
        import os
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def get_beijing_time(self):
        """获取当前北京时间字符串"""
        return datetime.now(pytz.timezone('Asia/Shanghai')).strftime("%Y-%m-%d %H:%M:%S")

    def get_headers(self, referer="https://quote.eastmoney.com/"):
        """生成随机请求头，强制关闭长连接以防被踢"""
        return {
            "User-Agent": random.choice(self.ua_list),
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": referer,
            "Connection": "close"  # 核心修复：防止 RemoteDisconnected 错误
        }

    def _format_symbol(self, symbol):
        """内部工具：识别市场前缀 (东财格式)"""
        # 上海证券交易所 (6开头股票, 5开头ETF)
        if symbol.startswith("6") or symbol.startswith("5"):
            return f"1.{symbol}"
        # 深圳证券交易所 (0、3开头股票, 1开头ETF)
        else:
            return f"0.{symbol}"

    def fetch_from_eastmoney(self, symbol):
        """
        方案一：东方财富 Push2 实时接口
        说明：这是目前最实时的公开接口，数据同步频率极高。
        """
        secid = self._format_symbol(symbol)
        # f43:最新价, f44:最高, f45:最低, f46:开盘, f47:成交量, f48:成交额, f170:涨跌幅
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fltt": "2",
            "invt": "2",
            "fields": "f43,f44,f45,f46,f47,f48,f169,f170,f60,f107",
            "secid": secid,
            "_": int(time.time() * 1000)
        }

        try:
            resp = requests.get(url, params=params, headers=self.get_headers(), timeout=5)
            if resp.status_code == 200:
                data = resp.json().get("data")
                if data and data['f43'] != '-':
                    return {
                        "symbol": symbol,
                        "name": data.get("f58", "N/A"),
                        "price": data["f43"],
                        "open": data["f46"],
                        "high": data["f44"],
                        "low": data["f45"],
                        "change_percent": data["f170"],
                        "volume": data["f47"],
                        "amount": data["f48"],
                        "time": self.get_beijing_time(),
                        "source": "EastMoney"
                    }
        except Exception as e:
            print(f"   ❌ 东财接口报错: {e}")
        return None

    def fetch_from_sina(self, symbol):
        """
        方案二：新浪财经实时接口 (备选)
        说明：新浪接口非常稳定，且支持批量获取，对高频请求友好。
        """
        market = "sh" if (symbol.startswith("6") or symbol.startswith("5")) else "sz"
        url = f"http://hq.sinajs.cn/list={market}{symbol}"
        headers = self.get_headers(referer="https://finance.sina.com.cn/")
        
        try:
            resp = requests.get(url, headers=headers, timeout=5)
            # 新浪返回的是 GBK 编码
            text = resp.content.decode('GBK')
            if '="' in text:
                data_str = text.split('="')[1].split(',')
                if len(data_str) > 30:
                    return {
                        "symbol": symbol,
                        "price": float(data_str[3]),
                        "open": float(data_str[1]),
                        "high": float(content[4]) if 'content' in locals() else float(data_str[4]),
                        "low": float(data_str[5]),
                        "time": f"{data_str[30]} {data_str[31]}",
                        "source": "Sina"
                    }
        except Exception as e:
            print(f"   ❌ 新浪接口报错: {e}")
        return None

    def get_realtime_quote(self, symbol):
        """
        统一调用入口：东财优先，新浪备份
        """
        # 1. 尝试东财
        result = self.fetch_from_eastmoney(symbol)
        if result:
            return result
        
        # 2. 东财失败，尝试新浪
        print(f"   ⚠️ {symbol} 东财源失效，正在切换新浪实时源...")
        time.sleep(0.5) # 短暂休眠规避
        result = self.fetch_from_sina(symbol)
        if result:
            return result
            
        return None

# ==========================================
# 主程序示例
# ==========================================

def main():
    fetcher = RealTimeDataFetcher()
    
    # 你日志中关注的代码列表
    target_symbols = ["510050", "510300", "510500", "159915"]
    
    print(f"🚀 [DataFetcher] 启动实时行情监测 - {fetcher.get_beijing_time()}")
    print("-" * 50)
    
    results = []
    for sym in target_symbols:
        # 增加随机延迟，防止频率过快触发防火墙
        time.sleep(random.uniform(0.5, 1.5))
        
        quote = fetcher.get_realtime_quote(sym)
        if quote:
            print(f"✅ [{quote['source']}] {sym} | 价格: {quote['price']} | 幅度: {quote.get('change_percent', 'N/A')}%")
            results.append(quote)
        else:
            print(f"❌ {sym} 所有实时源抓取失败")

    # 保存最新结果
    if results:
        df = pd.DataFrame(results)
        save_path = f"data_cache/realtime_quotes.csv"
        df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print("-" * 50)
        print(f"💾 数据已同步至: {save_path}")

if __name__ == "__main__":
    main()
