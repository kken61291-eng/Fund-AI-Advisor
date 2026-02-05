import akshare as ak
import requests
import time
from datetime import datetime, timedelta
from utils import retry, logger

class MarketScanner:
    def __init__(self):
        # 伪装头，用于直连财联社/巨潮
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def _parse_time(self, time_val):
        """通用时间解析"""
        try:
            now = datetime.now()
            # 1. 时间戳
            if isinstance(time_val, (int, float)):
                if time_val > 10000000000: return datetime.fromtimestamp(time_val / 1000)
                return datetime.fromtimestamp(time_val)
            # 2. 字符串
            s = str(time_val).strip()
            if "-" in s and ":" in s: return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            if ":" in s and len(s)<=5: 
                t = datetime.strptime(s, "%H:%M")
                return now.replace(hour=t.hour, minute=t.minute, second=0)
            return now
        except: return datetime.now()

    @retry(retries=1)
    def _fetch_source_eastmoney(self):
        """[源1] 东方财富 (通过AkShare)"""
        res = []
        try:
            df = ak.stock_news_em(symbol="全部")
            for _, row in df.iterrows():
                dt = self._parse_time(f"{row['发布日期']} {row['发布时间']}")
                if datetime.now() - dt > timedelta(hours=24): continue
                res.append({"title": row['内容'][:60], "source": "东财", "dt": dt})
        except: pass
        return res

    @retry(retries=1)
    def _fetch_source_cailian(self):
        """[源2] 财联社 (直连API，解决akshare报错)"""
        res = []
        try:
            url = "https://www.cls.cn/nodeapi/telegraphList"
            r = requests.get(url, params={"rn": 20}, headers=self.headers, timeout=5)
            if r.status_code == 200:
                for item in r.json().get('data', {}).get('roll_data', []):
                    title = item.get('title') or item.get('content', '')[:60]
                    dt = self._parse_time(item.get('ctime'))
                    if datetime.now() - dt > timedelta(hours=24): continue
                    res.append({"title": title, "source": "财联社", "dt": dt})
        except Exception as e:
            logger.warning(f"财联社直连失败: {e}")
        return res

    @retry(retries=1)
    def _fetch_source_cninfo(self):
        """[源3] 巨潮资讯 (直连API)"""
        res = []
        try:
            url = "http://www.cninfo.com.cn/new/information/getFrontInfo"
            r = requests.post(url, data={"type": "1"}, headers=self.headers, timeout=5)
            if r.status_code == 200:
                for item in r.json()[:10]:
                    dt = self._parse_time(item.get('publishTime'))
                    if datetime.now() - dt > timedelta(hours=48): continue
                    res.append({"title": item.get('title'), "source": "巨潮", "dt": dt})
        except: pass
        return res

    def get_macro_news(self):
        """三源合一：统一获取逻辑"""
        logger.info("启动三源情报网 (EastMoney | Cailian | Cninfo)...")
        
        # 1. 统一获取
        pool = []
        pool.extend(self._fetch_source_eastmoney())
        pool.extend(self._fetch_source_cailian())
        pool.extend(self._fetch_source_cninfo())

        # 2. 灾难备份
        if not pool:
            return [{"title": "API源连接超时，启用离线模式", "source": "System"}]

        # 3. 排序 (最新在前)
        pool.sort(key=lambda x: x['dt'], reverse=True)

        # 4. 筛选 (去重 + 关键词)
        final = []
        seen = set()
        keywords = ["央行", "美联储", "GDP", "CPI", "成交额", "北向", "违约", "印花税"]

        # 优先选带关键词的
        for n in pool:
            if len(final) >= 4: break
            if n['title'] in seen: continue
            if any(k in n['title'] for k in keywords):
                final.append(n)
                seen.add(n['title'])
        
        # 补齐
        for n in pool:
            if len(final) >= 6: break
            if n['title'] not in seen:
                final.append(n)
                seen.add(n['title'])

        # 5. 格式化输出
        output = []
        for n in final:
            delta = datetime.now() - n['dt']
            lbl = f"{int(delta.seconds/60)}分前" if delta.seconds<3600 else f"{int(delta.seconds/3600)}小时前"
            output.append({"title": n['title'], "source": f"{n['source']} [{lbl}]"})
            
        return output

    def get_market_sentiment(self):
        n = self.get_macro_news()
        return f"{n[0]['title']}"
