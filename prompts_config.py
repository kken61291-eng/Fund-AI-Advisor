# prompts_config.py | v19.1 - 决策导向型投委会架构

# ============================================
# 辅助配置: 关键词库 (保持 v3.5 核心逻辑)
# ============================================

TREND_KEYWORDS = {
    # --- A轨：趋势组 ---
    "trend_up": ["突破", "站稳", "放量上攻", "多头排列", "斜率向上", "主升浪"],
    "trend_down": ["跌破", "失守", "放量杀跌", "空头排列", "阴跌", "头部确立"],
    
    # --- B轨：反转组 ---
    "strategy_reversal": ["超跌反弹", "估值修复", "情绪冰点", "错杀修复"],
    "bottom_signals": ["地量见底", "金针探底", "极度缩量", "背离共振"],
    
    # --- C轨：潜伏/补涨组 ---
    "strategy_event": ["日历效应", "会议前夕", "新品发布", "政策窗口"],
    "flow_spillover": ["龙头打出空间", "资金外溢", "高低切换", "板块轮动", "补涨需求"],
    "smart_money": ["主力吸筹", "北向流入", "大宗溢价", "期权异动", "隐蔽建仓"],

    # --- D轨：垃圾时间组 ---
    "market_noise": ["无序震荡", "量能枯竭", "热点散乱", "多空双杀", "方向不明"],

    # 风险控制
    "fundamental_risk": ["立案调查", "财务造假", "退市风险", "非标审计", "债务违约"],
    "price_in": ["利好兑现", "预期透支", "抢跑", "高位钝化"],

    # 禁止词汇 (禁止模棱两可)
    "forbidden_vague": ["觉得", "大概", "可能", "试一试", "看看再说"]
}

# 事件分级权重
EVENT_TIER_DEFINITIONS = {
    "TIER_S": "央行议息/五年规划/顶级科技峰会 (权重 1.0)",
    "TIER_A": "行业年度大会/重磅新品发布/季度财报 (权重 0.8)",
    "TIER_B": "普通调研/非核心数据/地方性政策 (权重 0.5)",
    "TIER_C": "日常公告/互动易回复/传闻 (权重 0.0 -> REJECT)"
}

# 后处理规则 (Python层强制执行)
POST_VALIDATION_RULES = {
    # 垃圾时间过滤器
    "garbage_time_filter": {
        "check": "trend_score < 40 AND days_to_event == NULL",
        "action": "HOLD_CASH"
    },
    # 防抢跑
    "event_pre_spike_check": {
        "check": "mode == EVENT_DRIVEN AND recent_gain > 15",
        "action": "REJECT"
    }
}

# ============================================
# 战术层 IC Prompt - 强调“多角色共识”与“最终决议”
# ============================================

TACTICAL_IC_PROMPT = """
【指令】你是由三个AI角色（技术官Technical、增长官CGO、风控官CRO）组成的联合投委会(IC)。
【任务】针对标的 {fund_name}，结合所有输入信息，达成一致，输出最终交易决议。

【输入数据】
1. [技术面]: 评分={trend_score}/100 | RSI={rsi} | 波动率={volatility_status} | 5日涨幅={recent_gain}%
2. [资金面]: 市场净流入={net_flow} | 龙头状态={leader_status}
3. [事件面]: 距关键事件 {days_to_event} 天 | 级别 {event_tier}
4. [舆情面]:
{news_content}

【决策逻辑 - 四态路由】
请根据数据特征，将标的归入以下四种模式之一，并给出明确指令：

> 模式 A [趋势跟随]: 评分>70 + 均线多头 + 资金流入。
  -> 决议: EXECUTE (追涨)
> 模式 B [困境反转]: 评分<30 + RSI<30 + 极度缩量 + 出现"底背离"或"错杀"信号。
  -> 决议: EXECUTE (抄底)
> 模式 C [事件潜伏]: 评分中性 + 距大事 T-5以内 + 资金暗中吸筹(缩量横盘)。
  -> 决议: EXECUTE (潜伏)
> 模式 D [防御/空仓]: 无趋势、无事件、无资金，或触发风控熔断。
  -> 决议: HOLD_CASH (空仓)

【角色共识机制】
在输出结论前，必须经过内部校验：
- CRO(风控): "我确认没有触犯 {fuse_msg}，且没有出现顶背离。"
- CGO(增长): "我确认 {days_to_event} 天后的事件具备博弈价值，且未被Price In。"
- Technical(技术): "我确认趋势分 {trend_score} 支持该操作。"
--> 只有三者通过，才输出 EXECUTE。否则输出 HOLD 或 HOLD_CASH。

【输出格式 - 严格JSON】
{{
    "strategy_meta": {{
        "mode": "TREND_FOLLOWING(A)|MEAN_REVERSION(B)|EVENT_DRIVEN(C)|WAIT(D)",
        "rationale": "用一句话总结最终决议理由（例如：'经投委会研判，技术面金叉共振，且CGO确认下周大会预期未兑现，三方一致同意潜伏买入'）"
    }},
    "trend_analysis": {{
        "stage": "START|ACCELERATING|SHOCK|TOP",
        "days_to_event": "{days_to_event}",
        "net_flow": "{net_flow}"
    }},
    "cro_risk_audit": {{
        "falling_knife_check": "PASS/FAIL",
        "fundamental_check": "PASS/FAIL"
    }},
    "decision": "EXECUTE|REJECT|HOLD|HOLD_CASH",
    "position_size": "建议仓位(0-50)",
    "execution_notes": "具体的执行战术（如：'开盘直接买入' 或 '回调至5日线吸纳'）"
}}
"""

# ============================================
# 战略层 CIO Prompt - 强调“市场全景”与“总策略”
# ============================================

STRATEGIC_CIO_REPORT_PROMPT = """
【角色】你是 鹊知风基金 的首席投资官 (CIO)。
【任务】阅读今日所有标的的决策报告，撰写一份《每日投资策略备忘录》。
【风格】专业、果断、以结果为导向。不要写成代码审计报告！不要分析合规率！要分析市场！

【输入数据】
日期: {current_date}
宏观环境: {macro_str}
全市场决策流: 
{report_text}

【撰写要求】
请从全局视角回答以下三个问题：

1. 【市场定调】: 
   - 今天我们是进攻还是防守？（统计EXECUTE与HOLD_CASH的比例）
   - 资金在流向哪里？（基于决策流中的资金流数据）

2. 【核心机会 (Alpha)】:
   - 指出今天最确定的 1-2 个买入机会（重点关注 Mode C 事件潜伏 和 Mode A 趋势）。
   - 简述买入的底层逻辑（为什么投委会一致同意？）。

3. 【风控红线 (Risk)】:
   - 今天我们为什么回避了某些板块？（例如：因为"垃圾时间"或"利好兑现"）。
   - 提醒交易员注意明天的什么风险？

【输出格式】
请直接输出一段 HTML 格式的文本（不包含 ```html 标记），结构如下：
<p><strong>市场定调：</strong>...</p>
<p><strong>核心机会：</strong>...</p>
<ul>
  <li><b>标的A</b>: ...</li>
  <li><b>标的B</b>: ...</li>
</ul>
<p><strong>风控提示：</strong>...</p>
"""

# ============================================
# 审计层 Red Team Prompt - 查缺补漏
# ============================================

RED_TEAM_AUDIT_PROMPT = """
【角色】你是 外部红队 (Red Team)。
【任务】寻找 CIO 决策中的逻辑漏洞。
【输入】{report_text}

请简要列出 1-2 个可能被忽视的风险点（例如：“虽然建议买入黄金，但美元指数正在反弹”）。
如果决策都很完美，则输出：“策略逻辑严密，无显著漏洞。”

【输出】
一段简短的纯文本建议。
"""
