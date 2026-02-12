# prompts_config.py | v3.5 - 四态全天候架构：趋势/反转/潜伏/空仓

# ============================================
# 辅助配置: 关键词库 & 事件分级
# ============================================

TREND_KEYWORDS = {
    # --- A轨：趋势组 ---
    "trend_up": ["突破", "站稳", "放量上攻", "多头排列", "斜率向上", "主升浪"],
    "trend_down": ["跌破", "失守", "放量杀跌", "空头排列", "阴跌", "头部确立"],
    
    # --- B轨：反转组 ---
    "strategy_reversal": ["超跌反弹", "估值修复", "情绪冰点", "错杀修复"],
    "bottom_signals": ["地量见底", "金针探底", "极度缩量", "背离共振"],
    
    # --- C轨：潜伏/补涨组 (增强) ---
    "strategy_event": ["日历效应", "会议前夕", "新品发布", "政策窗口"],
    "flow_spillover": ["龙头打出空间", "资金外溢", "高低切换", "板块轮动", "补涨需求"], # New
    "smart_money": ["主力吸筹", "北向流入", "大宗溢价", "期权异动", "隐蔽建仓"],

    # --- D轨：垃圾时间组 (New) ---
    "market_noise": ["无序震荡", "量能枯竭", "热点散乱", "多空双杀", "方向不明", "流动性丧失"],

    # 风险控制
    "fundamental_risk": ["立案调查", "财务造假", "退市风险", "非标审计", "债务违约"],
    "price_in": ["利好兑现", "预期透支", "抢跑", "高位钝化"],

    # 禁止词汇
    "forbidden_vague": ["觉得", "大概", "可能", "试一试", "看看再说"]
}

# 事件分级权重 (用于 Mode C 决策)
EVENT_TIER_DEFINITIONS = {
    "TIER_S": "央行议息/五年规划/顶级科技峰会 (权重 1.0)",
    "TIER_A": "行业年度大会/重磅新品发布/季度财报 (权重 0.8)",
    "TIER_B": "普通调研/非核心数据/地方性政策 (权重 0.5)",
    "TIER_C": "日常公告/互动易回复/传闻 (权重 0.0 -> REJECT)"
}

# 量化阈值配置 (新增防抢跑阈值)
QUANT_THRESHOLDS = {
    "ma_alignment_bullish": {"5>20>60": True, "price>5ma": True},
    
    # C轨硬约束
    "event_constraints": {
        "pre_event_max_runup": 0.15,  # 事件前累计涨幅 >15% 则视为 Price In
        "min_days_to_event": 2,       # 至少提前2天
        "max_days_to_event": 20       # 最长潜伏20天
    },
    
    # 补涨硬约束
    "laggard_constraints": {
        "leader_gain": ">0.20",       # 龙头必须涨超20%
        "volume_ratio": ">0.8"        # 补涨股成交量不能太低(必须有量)
    },

    "divergence_trigger": {"rsi_divergence": 3, "volume_divergence": 2}
}

# ============================================
# 战术层投委会 (IC) Prompt 模板 - v3.5 四态生产版
# ============================================

TACTICAL_IC_PROMPT = """
【系统架构】鹊知风投委会 (IC) | 四态全天候策略 v3.5

【标的信息】
标的: {fund_name} | 距事件天数: {days_to_event} | 事件级别: {event_tier}
趋势强度: {trend_score} | 波动率状态: {volatility_status}
近期涨幅(5日): {recent_gain}% | 相对强弱(RS): {relative_strength}
资金信号: 主力净流入={net_flow} | 龙头股状态={leader_status}

【实时舆情 (权重预筛选)】
{news_content}

【前置过滤器：Mode D (Cash/Wait) 判定】
若满足以下任意一组条件，直接进入 [Mode D]:
1. [垃圾时间]: 趋势强度 < 40 AND 无明确事件(Tier B以下) AND 成交量 < 20日均量*0.6。
2. [风险释放]: 市场处于 "放量杀跌" (Mode B REJECT) 状态。
3. [预期透支]: 虽有事件，但近期涨幅 > 15% (Price In)。
>>> 输出: {{"decision": "HOLD_CASH", "rationale": "垃圾时间/风险释放/利好透支，现金为王"}}

【核心逻辑路由 (若非 Mode D)】
> Mode A (趋势): 均线多头 + 量价齐升。
> Mode B (反转): 极度缩量 + 底背离 + 错杀。
> Mode C (潜伏): 震荡/横盘 + 事件倒计时(Tier A/S) + 资金暗流。

【算法模块: Mode C (事件/补涨) 深度校验】

若申请进入 [Mode C]:
1. **级别校验 (Tier Check)**: 
   - 仅接受 TIER_S (重仓) 和 TIER_A (中仓)。
   - TIER_B 需配合强技术面。TIER_C 直接 REJECT。

2. **补涨逻辑校验 (Laggard Check)**:
   - 必须证明 "Leader Created Space" (龙头已打出空间)。
   - 必须证明 "Flow Spillover" (资金正在外溢，而非该股被遗弃)。
   - 警告：若龙头下跌，严禁做补涨 (覆巢之下无完卵)。

3. **防抢跑校验 (Anti-Chase)**:
   - 若 {recent_gain} > 15%，判定为 "利好出尽"，建议卖出而非买入。

【角色纪律 (Strict IC Protocols v3.5)】

1. 🐻 CRO (首席风控官) - 裁决者:
    【死锁裁决】当 CGO 喊 "潜伏" vs CRO 喊 "阴跌" 时：
    - 判据：看成交量。
    - 若放量下跌 (Vol > 1.2x) -> 判定为出货 -> **REJECT**。
    - 若缩量横盘 (Vol < 0.8x) -> 判定为吸筹 -> **ALLOW**。
    - 若 {days_to_event} < 1 -> 判定为博彩 -> **REJECT**。

2. 🦊 CGO (首席增长官) - 机会雷达:
    【时间止损协议】
    - 针对 Mode C，必须输出明确的 "Time Stop"。
    - 规则：事件落地日(T-0) 开盘即卖出，无论盈亏。

3. ⚖️ CIO (决策中枢) - 组合管理者:
    【决策公式】
    - Mode A: 仓位 40% (顺势)
    - Mode B: 仓位 20% (左侧)
    - Mode C: 仓位 30% (潜伏，Tier S可达40%)
    - Mode D: 仓位 0% (空仓)

【输出格式 - 严格JSON v3.5】
(注意：本回复严禁包含任何URL链接；)

{{
    "strategy_meta": {{
        "mode": "TREND_FOLLOWING|MEAN_REVERSION|EVENT_DRIVEN|WAIT(CASH)",
        "tier": "S|A|B|C (仅Mode C需要)",
        "rationale": "核心理由"
    }},
    "trend_analysis": {{
        "direction": "UP|DOWN|RANGE|BOTTOMING",
        "stage": "START|ACCELERATING|PRE_EVENT|NOISE",
        "confidence": "HIGH|MEDIUM|LOW",
        "key_levels": {{
            "stop_loss": "止损价",
            "time_stop": "YYYY-MM-DD (事件落地日/强制离场日)"
        }}
    }},
    "cro_arbitration": {{
        "volume_check": "PASS(缩量)|FAIL(放量)",
        "price_in_check": "PASS(未涨)|FAIL(已涨 >15%)",
        "laggard_validity": "PASS(资金外溢)|FAIL(弱者恒弱)|N/A"
    }},
    "decision": "EXECUTE|REJECT|HOLD|HOLD_CASH",
    "position_size": "建议仓位%",
    "execution_notes": "必须包含：'若事件落地前涨幅达15%止盈' 及 'T-0日强制离场' 指令"
}}
"""

# ============================================
# 战略层 CIO 复盘 Prompt - v3.5 策略一致性审计
# ============================================

STRATEGIC_CIO_REPORT_PROMPT = """
【系统角色】鹊知风 CIO | 策略一致性审计 v3.5
日期: {current_date}

【审计重点 - "不做什么" (The Art of Doing Nothing)】

1. Mode D 执行率审计:
    - 市场在"垃圾时间"时，系统是否管住了手？
    - 有无强行在无趋势、无事件时开仓？

2. Mode C 陷阱审计:
    - [接盘审计]: 是否在事件前涨幅已巨大的标的上建议追高？
    - [弱势审计]: 是否买入了没有任何资金流入迹象的"伪补涨"股？

3. 风险事件审计:
    - 检查是否在 news_content 包含 "fundamental_risk" 时仍建议持有？
"""

# ============================================
# 审计层 Red Team Prompt - v3.5 逻辑黑客
# ============================================

RED_TEAM_AUDIT_PROMPT = """
【系统角色】鹊知风 Red Team | 逻辑黑客 v3.5

【攻击测试 - 寻找系统的贪婪与恐惧】

Q1: 伪潜伏测试 (The 'Dead Money' Trap)
    - 标的横盘是因为"主力吸筹"还是"无人问津"？
    - 攻击点：若成交量持续萎缩且无 Event Tier A+ 支撑，判定为"死钱"，强制转 Mode D。

Q2: 抢跑测试 (The 'FOMO' Trap)
    - 距离事件还有 5 天，但股价已经两连阳。
    - 攻击点：此时买入赔率极低。系统是否发出了 "WAIT" 或 "SELL" 信号？若建议 BUY，视为严重漏洞。

Q3: 补涨妄想 (The 'Weakness' Trap)
    - 龙头涨了，龙二没动。
    - 攻击点：除非有 "flow_spillover" (板块成交量激增) 证据，否则龙二没动是因为它就是垃圾。系统是否识别了这一点？

【输出】HTML格式审计报告。
"""

# ============================================
# 后处理校验配置 (v3.5 - 强制裁决逻辑)
# ============================================

POST_VALIDATION_RULES = {
    # 1. 强制空仓过滤器
    "garbage_time_filter": {
        "check": "若 trend_score < 40 AND days_to_event > 20 AND mode != 'MEAN_REVERSION'",
        "action": "强制 decision='HOLD_CASH', rationale='垃圾时间，拒绝强行交易'"
    },

    # 2. Mode C 抢跑熔断
    "event_pre_spike_check": {
        "check": "若 mode == 'EVENT_DRIVEN' 且 recent_gain > 15",
        "action": "强制 decision='REJECT' 或 'SELL', rationale='预期透支(Price In)，盈亏比不佳'"
    },

    # 3. 补涨逻辑强校验
    "laggard_logic_check": {
        "check": "若 rationale 包含 '补涨'",
        "must_have": ["资金外溢", "板块轮动", "高低切换"], # 必须包含至少一个
        "action": "若缺乏资金证据，强制降级为 'HOLD' (不做弱者的弱者)"
    },

    # 4. Mode C 死锁裁决 (CRO vs CGO)
    "mode_c_arbitration": {
        "check": "若 mode == 'EVENT_DRIVEN' 且 decision == 'EXECUTE'",
        "arbitration_logic": """
            IF price < ma60 AND volume_ratio > 1.2:
                -> REJECT (放量下跌是出货)
            ELIF price_range < 0.05 AND volume_ratio < 0.8:
                -> ALLOW (缩量横盘是吸筹)
            ELSE:
                -> REDUCE_SIZE (降仓观察)
        """,
        "action": "根据逻辑覆写 decision"
    },
    
    # 5. 时间止损强制输出
    "time_stop_enforcement": {
        "check": "若 mode == 'EVENT_DRIVEN'，execution_notes 必须包含日期",
        "action": "若缺失，追加 '警告：需在事件落地日强制平仓'"
    }
}
