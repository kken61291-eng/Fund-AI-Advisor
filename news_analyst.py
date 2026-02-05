import os
import json
import requests
import xml.etree.ElementTree as ET
from openai import OpenAI
from utils import retry, logger

class NewsAnalyst:
    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL", "https://api.siliconflow.cn/v1") 
        self.model_name = os.getenv("LLM_MODEL", "Pro/moonshotai/Kimi-K2.5") 
        self.client = OpenAI(api_key=self.api_key, base_url=self.base_url) if self.api_key else None

        # --- [V12.2 核心] 板块逻辑矩阵 ---
        # 这里的逻辑是风控官的“底牌”，AI 必须基于这些硬逻辑来判卷
        self.SECTOR_LOGIC_MAP = {
            "红利": "【债性思维】核心看点是'10年期国债收益率'和'股息率差'。如果国债利率上行，红利吸引力下降（利空）。如果市场风险偏好极低，红利是避风港（利好）。风险点：拥挤交易导致股息率下降。",
            "煤炭": "【商品思维】核心看点是'焦煤/动力煤期货价格'、'电厂库存'和'旺季预期'。煤炭是高股息+周期。如果期货大跌但股价硬撑，是诱多。如果进入夏季/冬季用煤旺季，是强支撑。",
            "黄金": "【宏观思维】核心看点是'美债实际利率'（负相关）和'地缘政治'。美元走强通常利空黄金。如果是避险情绪推动（打仗），则忽略美元影响。风险点：流动性危机时黄金会被抛售换现金。",
            "半导体": "【成长思维】核心看点是'费城半导体指数(SOX)'共振、'国产替代率'和'大厂资本开支'。对利率敏感，降息利好。如果纳指大跌，A股半导体很难独善其身。",
            "AI通信": "【映射思维】核心看点是'美股英伟达/光模块龙头'的表现。这是典型的影子股逻辑。如果美股AI龙头破位，A股必跌。警惕'小作文'吹票但业绩无法落地的伪逻辑。",
            "证券": "【牛市旗手】核心看点是'两市成交额'（量在价先）和'政策风向'。券商是高Beta载体。如果成交额萎缩（<8000亿），券商的上涨都是耍流氓（诱多）。只有放量突破才是真启动。",
            "沪深300": "【国运思维】核心看点是'人民币汇率'（外资流向）和'社融数据'。汇率升值->北向流入->核心资产涨。如果汇率贬值且北向流出，大盘反弹多为一日游。",
            "新能源": "【产能思维】核心看点是'产能出清'和'价格战'。当前处于去库周期，任何上涨都先视为超跌反弹，直到看到行业龙头不再打价格战。",
            "医药": "【政策思维】核心看点是'集采政策(VBP)'和'反腐'。创新药看美债利率（融资成本）。避险属性较弱，受政策扰动极大。",
            "日经": "【汇率思维】核心看点是'日元汇率'。日元贬值->日股涨；日元升值（加息）->日股跌。警惕日本央行货币政策转向。",
            "纳指": "【流动性思维】核心看点是'美联储降息预期'和'七巨头财报'。只要美债利率不飙升，科技股泡沫就能维持。风险点：通胀反弹导致降息落空。"
        }

    @retry(retries=2)
    def fetch_news_titles(self, keyword):
        # 保持 V12.0 逻辑
        search_q = keyword + " 行业分析"
        if "红利" in keyword: search_q = "A股 红利指数 股息率"
        elif "美股" in keyword: search_q = "美联储 降息 纳斯达克 宏观"
        elif "半导体" in keyword: search_q = "半导体 周期 涨价"
        elif "黄金" in keyword: search_q = "黄金 避险 美元指数"
        elif "证券" in keyword: search_q = "A股 成交额 券商"
        
        url = f"https://news.google.com/rss/search?q={search_q}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            return [item.find('title').text for item in root.findall('.//item')[:5]]
        except: return []

    def _get_logic_chain(self, fund_name):
        """
        根据基金名称模糊匹配逻辑链
        """
        for key, logic in self.SECTOR_LOGIC_MAP.items():
            if key in fund_name:
                return logic
        return "【通用思维】关注量价配合，缩量上涨为诱多，放量滞涨为出货。顺势而为。"

    def analyze_fund_v4(self, fund_name, tech, market_ctx, news):
        """
        V12.2 微观审计：注入板块专属逻辑链
        """
        if not self.client: return {"comment": "AI Offline", "risk_alert": "", "adjustment": 0}

        risk = tech.get('risk_factors', {'bollinger_pct_b': 0.5, 'vol_ratio': 1.0, 'divergence': '无'})
        
        # 获取专属逻辑
        sector_logic = self._get_logic_chain(fund_name)

        tech_context = f"""
        [技术面侦测]
        - 基准分: {tech['quant_score']} (0-100)
        - 趋势: 周线{tech['trend_weekly']}, MACD{tech['macd']['trend']}
        - 资金: OBV斜率 {tech['flow']['obv_slope']}
        - 量比: {risk['vol_ratio']} (0.8以下缩量)
        - 背离: {risk['divergence']}
        """

        prompt = f"""
        # Role: 资深行业分析师 (Sector Specialist)
        # Task: 基于【专属逻辑链】对标的进行深度测谎。
        
        # Context
        - 标的: {fund_name}
        - 宏观新闻: {str(market_ctx)}
        - 个股舆情: {str(news)}
        - 技术数据: {tech_context}

        # 🧬 专属逻辑链 (Sector Logic Chain)
        请必须依据此逻辑进行判断，不要使用通用话术：
        >>> {sector_logic} <<<

        # 判决法则
        1. **逻辑验证**: 新闻/盘面是否符合上述逻辑链？(例如：券商涨了但没放量 -> 违反'牛市旗手'逻辑 -> 判定为假突破)。
        2. **技术共振**: 如果逻辑链利好 + OBV流入，给予加分 (+10~30)。
        3. **逻辑背离**: 如果逻辑链利空 (如煤价跌) 但股价涨，视为资金强拉，警惕补跌，给予重罚 (-30~50)。

        # Output JSON
        {{
            "comment": "80字深度分析。必须引用专属逻辑链中的关键词（如'成交额'、'美债利率'、'集采'等）。",
            "risk_alert": "20字致命风险点。",
            "adjustment": (整数 -100 到 +50) 
        }}
        """

        try:
            res = self.client.chat.completions.create(model=self.model_name, messages=[{"role":"user","content":prompt}], response_format={"type":"json_object"}, temperature=0.4)
            data = json.loads(res.choices[0].message.content)
            if 'adjustment' not in data: data['adjustment'] = 0
            return data
        except Exception as e:
            logger.error(f"AI 分析错误: {e}")
            return {"comment": "AI服务异常", "risk_alert": "无", "adjustment": 0}

    def review_report(self, summary):
        # 保持 V11.12 CIO 逻辑
        if not self.client: return "<p>CIO Offline</p>"
        prompt = f"""
        # Role: CIO (首席投资官)
        # Strategy: Core + Satellite
        # Plan: {summary}
        # Task: 宏观一致性 + 仓位评估 + 最终裁决
        # Output HTML: <div class='cio-seal'>CIO APPROVED</div><h3>CIO 战略审计</h3><p><strong>宏观定调：</strong>...</p><p><strong>双轨评估：</strong>...</p><p class='warning'><strong>最终裁决：</strong>...</p>
        """
        try:
            res = self.client.chat.completions.create(model=self.model_name, messages=[{"role":"user","content":prompt}], temperature=0.6)
            return res.choices[0].message.content.strip().replace('```html', '').replace('```', '')
        except: return "CIO Audit Failed."

    def advisor_review(self, summary, market_ctx):
        # 保持 V11.12 顾问逻辑
        if not self.client: return ""
        prompt = f"""
        # Role: 玄铁先生 (资产配置专家)
        # Context: {market_ctx} | Plan: {summary}
        # Task: 为场外基民提供独立验证。
        # Output HTML: <div class='advisor-title'>🗡️ 玄铁先生·场外实战复盘</div><p><strong>【势·验证】：</strong>...</p><p><strong>【术·底仓】：</strong>...</p><p><strong>【断·进攻】：</strong>...</p>
        """
        try:
            res = self.client.chat.completions.create(model=self.model_name, messages=[{"role":"user","content":prompt}], temperature=0.5)
            return res.choices[0].message.content.strip().replace('```html', '').replace('```', '')
        except: return "Advisor Offline."
