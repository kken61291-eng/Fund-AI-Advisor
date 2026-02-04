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

    @retry(retries=2)
    def fetch_news_titles(self, keyword):
        search_q = keyword + " 行业分析"
        if "红利" in keyword: search_q = "A股 红利指数 股息率"
        elif "美股" in keyword: search_q = "美联储 降息 纳斯达克 宏观"
        elif "半导体" in keyword: search_q = "半导体 周期 涨价"
        elif "黄金" in keyword: search_q = "黄金 避险 美元指数"
        
        url = f"https://news.google.com/rss/search?q={search_q} when:2d&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
        try:
            response = requests.get(url, timeout=10)
            root = ET.fromstring(response.content)
            return [item.find('title').text for item in root.findall('.//item')[:5]]
        except: return []

    def analyze_fund_v4(self, fund_name, tech, market_ctx, news):
        """
        V11.6 微观审计: 赋予 AI 自由裁量权
        不再规定具体的扣分数值，而是要求基于"逻辑闭环"进行判断。
        """
        if not self.client: return {"comment": "AI Offline", "risk_alert": "", "adjustment": 0}

        tech_context = f"""
        - 量化基准分: {tech['quant_score']} (0-100)
        - 趋势信号: 周线{tech['trend_weekly']}, MACD{tech['macd']['trend']}
        - 资金信号: OBV斜率 {tech['flow']['obv_slope']} (正流进/负流出)
        - 情绪信号: RSI {tech['rsi']}
        """

        prompt = f"""
        # Role: 资深风控官 (Risk Officer)
        你是一个多疑的、经验丰富的交易员。你不需要遵守死板的教条，而是要寻找**"故事"中的漏洞**。

        # Context
        - 标的: {fund_name}
        - 市场环境: {str(market_ctx)}
        - 机器打分: {tech_context}
        - 实时舆情: {str(news)}

        # Your Mission
        请像一个侦探一样审视上述数据。机器模型只看数字大小，容易被骗。你需要回答：
        **“当前的上涨（或下跌）逻辑是真实的，还是主力画出来的？”**

        # Thinking Framework (不要机械执行，要思考)
        1. **量价配合度**：价格涨了，但OBV（真金白银）跟了吗？如果是“无量空涨”，这是危险信号。
        2. **叙事与现实**：新闻里吹的天花乱坠，但技术面在破位吗？或者反之？
        3. **宏观共振**：这个标的的走势，符合当前的宏观大背景吗？（例如：降息利好黄金，若黄金跌，则是错杀机会）。

        # Output JSON
        {{
            "comment": "80字以内的深度洞察。不要陈述数据，要给出你的**定性判断**（如：诱多、洗盘、抢筹）。",
            "risk_alert": "20字以内最需要警惕的风险点。",
            "adjustment": (整数 -100 到 +50) 
            // 自由裁量权：
            // 如果你觉得是陷阱，可以重罚 (-40甚至更多)。
            // 如果你觉得是错杀，可以给予补偿分 (+20)。
            // 如果机器判断准确，填 0。
        }}
        """

        try:
            res = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}, 
                temperature=0.4 # 提高一点温度，增加灵活性
            )
            data = json.loads(res.choices[0].message.content)
            if 'adjustment' not in data: data['adjustment'] = 0
            return data
        except Exception as e:
            logger.error(f"AI 分析错误: {e}")
            return {"comment": "AI服务异常", "risk_alert": "无", "adjustment": 0}

    def review_report(self, summary):
        """
        V11.6 CIO: 辩证思维与战略定力
        不再做简单的“一致性检查”，而是进行“动态平衡评估”。
        """
        if not self.client: return "<p>CIO Offline</p>"
        
        prompt = f"""
        # Role: 首席投资官 (CIO)
        你掌管着【鎏金量化基金】的几十亿头寸。你深知市场是非线性的，**并不存在绝对的对错**，只有盈亏比（Risk/Reward Ratio）。
        
        # Our Strategy (双轨制)
        - **核心底仓 (Core)**: 黄金/红利/大盘。任务是**活着**。除非发生系统性崩盘，否则保持在场，不要因为短期波动轻易下车。
        - **卫星进攻 (Satellite)**: 科技/券商。任务是**掠夺**。必须精准打击，形势不对立即撤退。

        # Today's Plan from Quant Team
        {summary}

        # Your Audit Mission
        请运用你的直觉和经验，对这份计划进行**辩证评估**：

        1. **审视“模糊地带”**：
           - 比如：宏观在收紧，但某些板块在逆势走强（抱团）。这可能不是错误，而是**结构性机会**。请指出这种机会是否值得冒险。
        
        2. **评估“仓位舒适度”**：
           - 这份计划执行后，我们的账户是过于激进（睡不着觉）还是过于保守（踏空焦虑）？
           - 对照我们的双轨制，核心仓位是否够稳？卫星仓位是否够锐？

        3. **最终裁决**：
           - 不要只会说“批准”或“驳回”。请给出**方向性的微调建议**（例如：“科技仓位可以更激进一点，但要把止损线收紧”）。

        # Output Requirements (HTML Fragment)
        使用极具穿透力的金融语言。
        结构：
        <div class='cio-seal'>CIO APPROVED</div>
        <h3>CIO 战略审计</h3>
        <p><strong>宏观辩证：</strong>[分析市场的主要矛盾与次要矛盾]</p>
        <p><strong>双轨评估：</strong>[评价Core与Satellite的配合效率]</p>
        <p class='warning'><strong>最终裁决：</strong>[给出带有战术细节的指令]</p>
        """
        try:
            res = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.6 # CIO 需要更高的创造性和大局观
            )
            return res.choices[0].message.content.strip().replace('```html', '').replace('```', '')
        except: return "CIO Audit Failed."

    def advisor_review(self, summary, market_ctx):
        """
        V11.6 Sage: 博弈论视角与人性洞察
        """
        if not self.client: return ""

        prompt = f"""
        # Role: 50年经验的传奇顾问 (The Sage)
        你看透了市场的本质是**互为对手盘**。
        你对场外基金持有者充满同情，因为他们总是因为**T+1的时间差**和**追涨杀跌的人性弱点**而亏损。

        # Context
        宏观背景: {market_ctx}
        今日ETF盘面:
        {summary}

        # Task
        请给场外基民写一段**“私房话”**。不要打官腔，要像在茶馆里聊天一样透彻。
        
        # 思考角度
        1. **区分“真涨”和“假涨”**：
           - 如果ETF是缩量上涨，告诉基民：“这可能是主力在画图骗你们进场接盘，别动。”
           - 如果ETF是放量突破，告诉基民：“这趋势稳了，拿住别下车。”
        
        2. **利用“双轨制”心理按摩**：
           - 告诉持有核心资产（红利/黄金）的人：你们拿着的是金饭碗，别因为一天两天的波动就换成泥饭碗。
           - 告诉持有进攻资产（科技）的人：这是在刀口舔血，赚了就跑是最高美德。

        # Output HTML (无markdown)
        结构:
        <div class='advisor-title'>🎓 传奇顾问独立意见 (50-Year Sage)</div>
        <p><strong>给场外基民的私房话：</strong>[通俗、透彻、直击人心]</p>
        <p><strong>实战锦囊：</strong>[针对不同持有者的具体操作建议]</p>
        """
        try:
            res = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7 # 顾问说话要更有“人味”
            )
            return res.choices[0].message.content.strip().replace('```html', '').replace('```', '')
        except: return "Advisor Offline."
