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
        # 保持 V11.0 的逻辑修正层（单标的微观分析）
        if not self.client: return {"comment": "AI Offline", "risk_alert": "", "adjustment": 0}

        tech_context = f"""
        - 量化基准分: {tech['quant_score']} (0-100)
        - 周线趋势: {tech['trend_weekly']}
        - MACD形态: {tech['macd']['trend']}
        - 资金流向(OBV): {tech['flow']['obv_slope']}
        - RSI: {tech['rsi']}
        """

        prompt = f"""
        # Role: 20年经验首席风控官 (Risk Officer)
        # Data
        - 标的: {fund_name}
        - 宏观: {str(market_ctx)}
        - 技术: {tech_context}
        - 舆情: {str(news)}
        
        # Task: 逻辑审计与评分修正
        你的核心任务是**纠错**。量化模型容易被“缩量诱多”或“技术骗线”愚弄，你需要用经验识别陷阱。
        
        # Rules (严厉扣分制)
        - 严重背离 (价涨量缩 / 价格新高但OBV流出) -> 扣分 (-30 ~ -50)
        - 宏观冲突 (流动性收紧但高估值资产评分高) -> 扣分 (-20)
        - 完美形态 (逻辑通顺且共振) -> 不扣分或微调 (+5)
        
        # Output JSON
        {{
            "comment": "80字以内的犀利点评，指出是否背离，不要废话。",
            "risk_alert": "20字以内的致命风险点。",
            "adjustment": (整数, 负数代表扣分)
        }}
        """

        try:
            res = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}, 
                temperature=0.3
            )
            data = json.loads(res.choices[0].message.content)
            if 'adjustment' not in data: data['adjustment'] = 0
            return data
        except Exception as e:
            logger.error(f"AI 分析错误: {e}")
            return {"comment": "AI服务异常", "risk_alert": "无", "adjustment": 0}

    def review_report(self, summary):
        """
        V11.4: CIO 深度战略审计
        提示词重构：强化上帝视角、逻辑自洽性检查和金融专业度。
        """
        if not self.client: return "<p>CIO Offline</p>"
        
        prompt = f"""
        # Role Definition
        你是【鎏金量化基金】的 **首席投资官 (CIO)**。你拥有华尔街20年的宏观对冲经验。
        你的性格：**冷酷、反人性、精英主义**。你厌恶模棱两可的废话，只做基于逻辑的最终裁决。

        # Context (Market & Plan)
        {summary}

        # Your Audit Mission (上帝视角审计)
        请对这份由量化模型（Quant）和初级分析师提交的交易计划进行**严厉的战略审计**：

        1. **宏观一致性检查 (Macro Consistency)**:
           - 我们的多空方向是否与当前的宏观环境（如流动性收紧/宽松、北向资金流向）冲突？
           - *如果宏观差但计划激进，必须严厉警告。*

        2. **风格与轮动审计 (Style & Rotation)**:
           - 资金是在流向防御资产（红利/黄金/现金）还是进攻资产（科技/成长）？
           - 这种配置是否符合当前的风险偏好（Risk-on/Risk-off）？

        3. **逻辑自洽性验证 (Logic Check)**:
           - 检查是否存在“精神分裂”的交易（例如：一边做空纳指防风险，一边梭哈垃圾股）。

        # Output Requirements (HTML Fragment)
        请输出一段HTML代码（不含markdown标记），使用**极度专业、金融术语密集**的语言（如：流动性溢价、均值回归、风险敞口、Beta衰减）。

        结构如下：
        <div class='cio-seal'>CIO APPROVED</div>
        <h3>CIO 战略审计</h3>
        <p><strong>宏观定调：</strong>[用一句话定义当前市场阶段，如“流动性陷阱”或“技术性牛市”]</p>
        <p><strong>板块逻辑：</strong>[点评具体的板块配置逻辑，指出哪些是Alpha，哪些是Beta]</p>
        <p class='warning'><strong>最终裁决：</strong>[给出最终的战术指令，如“防御优先，现金为王”或“全面进攻”]</p>
        """
        try:
            res = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5
            )
            return res.choices[0].message.content.strip().replace('```html', '').replace('```', '')
        except: return "CIO Audit Failed."

    def advisor_review(self, summary, market_ctx):
        """
        V11.3: 50年经验传奇顾问 (The Sage)
        专注绝对收益和场外基金建议
        """
        if not self.client: return ""

        prompt = f"""
        # Role
        你是一位在市场生存了50年的**传奇民间投资顾问**。你不仅懂ETF，更深知场外基金（Mutual Funds）的坑（如赎回费、T+1确认、偷吃净值）。
        你的宗旨：**绝对收益，落袋为安**。你说话通俗易懂，像个老大哥。

        # Context
        宏观: {market_ctx}
        今日ETF策略:
        {summary}

        # Task
        请以“老法师”的口吻，给**场外基金持有者**写一段建议。
        重点关注：
        1. **时间差风险**：如果ETF大涨，提醒场外现在买进是追高接盘。
        2. **止盈提醒**：市场不好时，强调现金为王。
        3. **具体映射**：看到纳指涨，提醒定投QDII的拿住；看到红利涨，提醒债基和红利基的持有者。

        # Output HTML (无markdown)
        结构:
        <div class='advisor-title'>🎓 传奇顾问独立意见 (50-Year Sage)</div>
        <p><strong>给场外基民的话：</strong>...</p>
        <p><strong>绝对收益锦囊：</strong>...</p>
        """
        try:
            res = self.client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.6 
            )
            return res.choices[0].message.content.strip().replace('```html', '').replace('```', '')
        except: return "Advisor Offline."
