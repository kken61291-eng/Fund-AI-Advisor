# prompts_config.py | v19.6.5 - 认知对抗型架构 (A/B/C/D 四轨层级强制版)
# 核心逻辑：CIO预裁定 → Technical/CGO/CRO三方博弈(带强制边界) → 量化风控压力测试 → CIO战略否决
# 关键修复：HIGH风险水位下强制禁用A/C轨，杜绝"高风险环境下重仓追涨"

# ============================================
# 0. 战略层前置拦截器 (CIO预裁定模块)
# ============================================

CIO_PRE_FILTER = {
    # 市场水位判定算法 (基于多因子，非人工输入)
    "risk_level_algorithm": {
        "factors": {
            "vix_or_equivalent": "波动率指数",
            "credit_spread": "信用利差",
            "net_flow_5d": "5日主力资金流向",
            "margin_balance": "融资融券余额变化",
            "geopolitical_index": "地缘风险指数(基于新闻情绪)"
        },
        "thresholds": {
            "HIGH": "VIX>30 或 5日净流出>500亿 或 地缘指数>80",
            "MEDIUM": "VIX 20-30 或 净流出200-500亿",
            "LOW": "VIX<20 且 净流入 且 地缘指数<40"
        }
    },
    
    # 风险水位→可用轨道的强制映射 (不可违逆)
    "track_availability": {
        "HIGH": {
            "allowed_modes": ["B", "D"],
            "forbidden_modes": ["A", "C"],
            "max_position_per_etf": "5%",
            "cash_ratio_min": "70%",
            "total_position_max": "30%",
            "note": "只允许左侧反转(B)或空仓防御(D)，严禁追涨"
        },
        "MEDIUM": {
            "allowed_modes": ["A", "B", "C"],
            "forbidden_modes": [],
            "max_position_per_etf": "15%",
            "cash_ratio_min": "30%",
            "total_position_max": "70%",
            "note": "全轨道可用，但需通过压力测试"
        },
        "LOW": {
            "allowed_modes": ["A", "B", "C"],
            "forbidden_modes": [],
            "max_position_per_etf": "25%",
            "cash_ratio_min": "10%",
            "total_position_max": "90%",
            "note": "积极进攻，但仍需分散"
        }
    },
    
    # 特殊情景强制干预
    "emergency_override": {
        "circuit_breaker_korea": "韩国熔断→所有亚太相关ETF强制D轨",
        "war_risk_tier_s": "TIER_S地缘事件→黄金/油气单标的上限降至3%",
        "liquidity_crisis": "全市场缩量30%→所有A轨提案降仓50%"
    }
}

# ============================================
# 1. 基础配置: 核心关键词库 (量化阈值增强版)
# ============================================

TREND_KEYWORDS = {
    # --- A轨：趋势组 (带量化触发条件) ---
    "trend_up": {
        "keywords": ["突破", "站稳", "放量上攻", "多头排列", "斜率向上", "主升浪"],
        "quant_trigger": {
            "trend_score": ">80",
            "rsi_range": "40-70",
            "volatility": "MEDIUM",
            "volume_vs_ma20": ">1.2",
            "mandatory": "市场水位非HIGH"
        },
        "forbidden_if": ["rsi>75", "recent_gain>20%", "sector_breadth<50%"]
    },
    "trend_down": {
        "keywords": ["跌破", "失守", "放量杀跌", "空头排列", "阴跌", "头部确立"],
        "quant_trigger": {
            "trend_score": "<30",
            "rsi": "<40",
            "mandatory": "无"
        }
    },
    
    # --- B轨：反转组 (带赔率计算) ---
    "strategy_reversal": {
        "keywords": ["超跌反弹", "估值修复", "情绪冰点", "错杀修复"],
        "quant_trigger": {
            "rsi": "<30",
            "drawdown_20d": ">15%",
            "fundamental_intact": True,
            "upside_space": ">downside_risk*2",
            "mandatory": "市场水位任意"
        }
    },
    "bottom_signals": {
        "keywords": ["地量见底", "金针探底", "极度缩量", "背离共振"],
        "quant_trigger": {
            "volume_percentile": "<10",
            "rsi_divergence": True,
            "mandatory": "无"
        }
    },
    
    # --- C轨：潜伏/事件组 (带时间衰减) ---
    "strategy_event": {
        "keywords": ["日历效应", "会议前夕", "新品发布", "政策窗口"],
        "quant_trigger": {
            "days_to_event": "1-7",
            "recent_gain": "<15%",
            "time_decay_weight": ">0.3",
            "mandatory": "市场水位非HIGH"
        },
        "decay_function": "lambda d: exp(-0.3 * d) if d<=7 else 0",
        "forbidden_if": ["recent_gain>20%", "rsi>70", "crowdedness>60"]
    },
    "flow_spillover": {
        "keywords": ["龙头打出空间", "资金外溢", "高低切换", "板块轮动", "补涨需求"],
        "quant_trigger": {
            "leader_gain_5d": ">20%",
            "sector_beta": "0.6-0.9",
            "lag_days": "<3",
            "mandatory": "无"
        }
    },
    
    # --- D轨：垃圾时间组 (强制触发) ---
    "market_noise": {
        "keywords": ["无序震荡", "量能枯竭", "热点散乱", "多空双杀", "方向不明"],
        "quant_trigger": {
            "trend_score": "40-60",
            "days_to_event": "NULL|>14",
            "volatility": "HIGH",
            "force_d_track": True
        },
        "override_authority": "强制归入D轨，无视Technical/CGO反对"
    },

    # 风险控制 (自动否决)
    "fundamental_risk": {
        "keywords": ["立案调查", "财务造假", "退市风险", "非标审计", "债务违约"],
        "auto_veto": True,
        "veto_authority": "CRO_PREEMPTIVE",
        "action": "REJECT_ALL_MODES"
    },
    "price_in": {
        "keywords": ["利好兑现", "预期透支", "抢跑", "高位钝化"],
        "quant_trigger": {
            "recent_gain": ">20%",
            "rsi": ">75"
        },
        "auto_reject_c_track": True,
        "demote_to": "B_track_if_rsi<35_else_REJECT"
    },

    # 禁止词汇 (扩展至逻辑层)
    "forbidden_vague": [
        "觉得", "大概", "可能", "试一试", "看看再说", 
        "或", "也许", "应该", "似乎", "听说"
    ]
}

# ============================================
# 2. 二级风控：量化压力测试矩阵
# ============================================

RISK_VETO_KEYWORDS = {
    "fake_breakout": {
        "keywords": ["缩量上涨", "背离", "上影线过长", "板块分化", "孤军奋战"],
        "quant_test": {
            "volume_vs_ma20": "<0.8",
            "sector_breadth": "<50%",
            "rejection_count_5d": ">=2",
            "action_if_true": "DEMOTE_TO_B_OR_REJECT"
        }
    },
    "poor_r_r": {
        "keywords": ["压力位临近", "获利盘涌出", "空间有限", "均线压制"],
        "quant_test": {
            "distance_to_resistance": "<8%",
            "profit_ratio_20d": ">60%",
            "upside_downside_ratio": "<1:2",
            "action_if_true": "REJECT"
        }
    },
    "market_trap": {
        "keywords": ["诱多", "骗线", "流动性枯竭", "杀估值"],
        "quant_test": {
            "bid_ask_spread": ">0.5%",
            "depth_imbalance": ">2:1",
            "vix_spike_1d": ">30%",
            "action_if_true": "FORCE_D_TRACK"
        }
    },
    "crowdedness_risk": {
        "keywords": ["一致性预期", "全民热议", "爆款", "秒光"],
        "quant_test": {
            "rsi": ">75",
            "media_sentiment": ">0.8",
            "fund_flow_3d": ">+15%",
            "action_if_true": "VETO_IF_C_TRACK"
        }
    }
}

# ============================================
# 3. 事件层级与衰减函数 (精确量化)
# ============================================

EVENT_TIER_DEFINITIONS = {
    "TIER_S": {
        "desc": "央行议息/战争爆发/顶级峰会/重大政策转向",
        "base_weight": 1.0,
        "valid_window_days": 4,
        "decay_func": "step",
        "decay_params": {
            "T-3_to_T-1": 1.0,
            "T-0": 1.0,
            "T+1": 0.5,
            "T+2": 0
        },
        "special_rule": "HIGH水位下仓位上限降至3%"
    },
    "TIER_A": {
        "desc": "行业大会/重磅新品/季度财报/重要数据",
        "base_weight": 0.8,
        "valid_window_days": 10,
        "decay_func": "linear",
        "decay_params": {
            "T-7": 0.8,
            "T-0": 1.0,
            "T+3": 0.3
        }
    },
    "TIER_B": {
        "desc": "普通调研/非核心数据/地方政策",
        "base_weight": 0.5,
        "valid_window_days": 4,
        "decay_func": "early_decay",
        "decay_params": {
            "T-3": 0.5,
            "T-2": 0.3,
            "T-1": 0.1,
            "T-0": 0.1
        },
        "note": "T-3后权重<0.3，自动失效"
    },
    "TIER_C": {
        "desc": "日常公告/传闻/互动易回复",
        "base_weight": 0.0,
        "valid_window_days": 0,
        "decay_func": "null",
        "action": "REJECT_C_TRACK",
        "escape_clause": "仅当RSI<25且跌幅>20%时，可转为B轨候选"
    }
}

POST_VALIDATION_RULES = {
    "garbage_time_filter": {
        "check": "trend_score >= 40 AND trend_score <= 60 AND (days_to_event == NULL OR days_to_event > 14) AND volatility == HIGH",
        "action": "FORCE_D_TRACK",
        "override": "无视Technical/CGO反对",
        "priority": 1
    },
    "risk_level_conflict": {
        "check": "market_risk_level == HIGH AND proposed_mode in ['A', 'C']",
        "action": "VETO_AND_DEMOTE",
        "fallback": "若CGO坚持，则仓位上限3%且硬止损-5%",
        "priority": 2
    },
    "event_pre_spike_check": {
        "check": "mode == EVENT_DRIVEN AND recent_gain > 15",
        "action": "DEMOTE_TO_B_OR_REJECT",
        "demote_logic": "若RSI<35且drawdown>10%则转B轨，否则REJECT",
        "priority": 3
    },
    "factor_concentration": {
        "check": "Top 3标的共享同一宏观因子(如地缘冲突)",
        "action": "FORCE_DIVERSIFICATION",
        "rule": "必须替换至少1个为负相关资产，或总仓位降至15%",
        "priority": 4
    },
    "liquidity_discount": {
        "check": "volatility == HIGH AND mode in ['A', 'C']",
        "action": "POSITION_DOWN_30%",
        "note": "高波动下假设成交价较信号价劣化3%",
        "priority": 5
    }
}

# ============================================
# 4. 战术层IC提案 (v19.6.5 强制约束版)
# ============================================

TACTICAL_IC_PROMPT = """
【系统状态】
市场水位: {market_risk_level} | 可用轨道: {allowed_modes} | 严禁轨道: {forbidden_modes}
单标的上限: {max_position} | 现金比例≥{cash_ratio} | 总仓位上限: {total_position_max}

【指令】你是由Technical、CGO、CRO组成的战术投委会(IC)，以及拥有强制裁决权的IC主席。
【核心约束】CIO预裁定已设定上述边界，任何超出边界的提案将被系统自动拦截。

【标的】{fund_name} ({fund_code})
【输入数据】
- 技术面: 评分={trend_score}/100 | RSI={rsi} | 波动率={volatility_status} | 5日涨幅={recent_gain}% | 20日回撤={drawdown_20d}% | 成交量分位={volume_percentile}%
- 资金面: 净流入={net_flow}亿 | 龙头状态={leader_status} | 板块内上涨家数比={sector_breadth}%
- 事件面: 距关键事件{days_to_event}天 | 级别={event_tier} | 衰减后权重={decayed_weight:.2f}
- 舆情面: {news_content}

【三方博弈 (带强制边界)】

📊 Technical (概率守门员):
- HIGH水位禁令: 严禁提议Mode A，即使trend_score=100。
- Mode A条件: trend_score>80 AND rsi 40-70 AND volume_vs_ma20>1.2 AND 水位非HIGH
- Mode B条件: rsi<30 AND drawdown_20d>15% AND volume_percentile<20
- 默认立场: 若不符合任何模式，支持D轨防御

🚀 CGO (赔率狙击手):
- HIGH水位禁令: Mode C仅允许TIER_S事件+days_to_event<=3+recent_gain<10%，否则转向B或放弃。
- 赔率公式: upside_space = |历史压力位-当前价|, downside_risk = |当前价-支撑位|
- 有效赔率: upside_space / downside_risk > 2.0
- 时间衰减: 使用{decay_func}计算，weight<0.3时事件失效
- 事件抢跑: recent_gain>15%时，C轨自动降级

🛡️ CRO (先制风控):
- 先制否决权: 检测到{fundamental_risk}关键词→立即REJECT，终止讨论。
- HIGH水位职责: 对所有非D轨提案，必须量化:
  - 假设打破情景(如地缘缓和)下的最大回撤
  - 流动性折扣(HIGH波动率下成交价劣化估计)
  - 硬止损位(技术位或-5%，取更严格者)

【主席裁决流程】
1. 约束检查: 若proposed_mode in {forbidden_modes}→直接否决
2. 可用轨道择优: 在{allowed_modes}中选confidence最高者
3. 仓位预分配: A轨15%→{max_position}, B轨10%→{max_position}, C轨12%→{max_position}, D轨0%
4. 关键假设声明: 必须明确"该提案成立的前提条件"
5. 假设打破应对: 前提打破时的无条件清仓逻辑

【输出格式 - 严格JSON】
{{
    "system_constraints": {{
        "market_risk_level": "{market_risk_level}",
        "allowed_modes": {allowed_modes},
        "forbidden_modes": {forbidden_modes},
        "max_position": "{max_position}",
        "cash_ratio": "{cash_ratio}"
    }},
    "constraint_violation_check": {{
        "violated": "TRUE|FALSE",
        "violation_details": "若TRUE，列出冲突"
    }},
    "debate_transcript": {{
        "Technical": {{
            "stance": "支持/反对某模式",
            "analysis": "基于概率的分析...",
            "quant_data": "使用的具体数值"
        }},
        "CGO": {{
            "stance": "支持/反对某模式",
            "odds_calculation": "upside=X, downside=Y, ratio=Z",
            "time_decay": "原始权重W，衰减后={decayed_weight:.2f}",
            "event_quality": "事件质量评估"
        }},
        "CRO": {{
            "stance": "支持/反对某模式",
            "tail_risk_scenario": "假设打破情景",
            "max_drawdown_estimate": "X%",
            "liquidity_discount": "Y%",
            "hard_stop": "具体止损位"
        }}
    }},
    "chairman_verdict": {{
        "mode_selected": "A|B|C|D",
        "confidence": "0-100",
        "position_preliminary": "X%",
        "key_assumption": "提案成立的核心前提(如'中东冲突持续至T+3')",
        "assumption_break_trigger": "前提打破时的无条件清仓条件",
        "hard_stop_loss": "具体价位或跌幅",
        "time_stop": "T+N日无条件离场"
    }},
    "mode_justification": "为何在可用轨道中选择此模式，而非其他"
}}
"""

# ============================================
# 5. 风控层终审 (v19.6.5 量化压力测试版)
# ============================================

RISK_CONTROL_VETO_PROMPT = """
【系统状态】
市场水位: {market_risk_level} | 审核标的数: {candidate_count}
特殊干预: {emergency_override_status}

【角色】风控委员会主席，拥有降级、否决、强化限制权。
【输入】IC提案列表 (含约束检查结果、假设前提、初步仓位):
{candidates_context}

【压力测试矩阵 (必须输出数值结果)】

### 测试1: 板块协同度 (Sector Synergy Score, 0-100)
计算公式: 
synergy = (sector_breadth × 0.4) + (correlation_with_leader × 30 × 0.3) + (fund_flow_consistency × 0.3)

阈值: >70通过, 40-70观察(仓位降30%), <40否决

### 测试2: 空间损耗 (Upside Room, 盈亏比)
计算:
upside% = min(历史压力位, 事件目标价) - 当前价
downside% = 当前价 - max(技术支撑位, 硬止损位)
r_r_ratio = upside% / |downside%|

阈值: >2.0通过, 1.5-2.0降级(仓位降50%), <1.5否决

### 测试3: 拥挤度 (Crowdedness Index, 0-100)
计算:
crowdedness = (rsi_normalized × 0.3) + (recent_gain_percentile × 0.3) + (media_sentiment_score × 0.4)

阈值: <30宽松, 30-60适中(正常), >60拥挤:
- C轨否决(事件已被定价)
- B轨需RSI<25且跌幅>20%才通过

### 测试4: 尾部风险压力 (Tail Risk Stress Test)
基于IC提供的{assumption_break_scenario}:
冲击幅度 = 历史类似事件平均跌幅 × 波动率调整系数
流动性折扣 = HIGH波动率? 3% : 1%
最大损失 = 仓位% × (冲击幅度 + 流动性折扣)

阈值: <2%通过, 2-5%降级(仓位降至3%), >5%否决

### 测试5: 因子集中度 (Factor Concentration)
检查Top 3标的是否共享:
- 同一宏观因子(地缘/利率/政策)
- 同一板块(资源/科技/消费)
- 同一事件驱动源

若共享→强制替换至少1个为负相关资产，或总仓位降至15%

【输出格式 - 严格JSON】
{{
    "market_context": {{
        "risk_level": "{market_risk_level}",
        "emergency_override": "{emergency_override_status}",
        "liquidity_environment": "NORMAL|STRESSED|CRISIS"
    }},
    "stress_test_results": [
        {{
            "code": "{fund_code}",
            "name": "{fund_name}",
            "ic_proposal": {{
                "mode": "A|B|C",
                "position": "X%",
                "confidence": "Y",
                "key_assumption": "..."
            }},
            "test_scores": {{
                "synergy": "X/100",
                "upside_downside_ratio": "X:Y",
                "crowdedness": "Z/100",
                "tail_risk_max_loss": "A%",
                "factor_overlap": "LOW|MEDIUM|HIGH"
            }},
            "veto_decision": "APPROVE|DEMOTE|VETO",
            "position_final": "X% (调整后)",
            "adjustment_reason": "若降级/否决，说明原因",
            "execution_conditions": {{
                "hard_stop_loss": "具体价位",
                "time_stop": "T+N日",
                "assumption_monitor": "需监控的假设前提"
            }}
        }}
    ],
    "portfolio_level_constraints": {{
        "total_position_limit": "X%",
        "cash_ratio_final": "Y%",
        "sector_concentration": "单一主题≤Z%",
        "factor_diversification": "强制负相关资产≥W%"
    }},
    "risk_summary": "组合风险敞口分析: 多头暴露X%, 地缘因子集中度Y%, 尾部风险损失上限Z%"
}}
"""

# ============================================
# 6. 战略层CIO定调 (v19.6.5 增强否决权版)
# ============================================

STRATEGIC_CIO_REPORT_PROMPT = """
【系统状态】
日期: {current_date} | 市场水位: {market_risk_level} | 判定依据: {risk_level_rationale}

【角色】首席投资官(CIO)，拥有对风控报告的再审核权与战略否决权。
【输入】
1. 风控委员会终审报告: {risk_committee_json}
2. 历史回测: 类似{market_risk_level}水位下，各模式胜率/赔率数据
3. 组合当前状态: 现有持仓暴露分析

【CIO专属战略审核清单】

1. 组合风险敞口集中度审查
   - Top 3标的是否共享同一宏观因子?
   - 地缘冲突暴露是否>30%?
   - 若集中→强制要求替换1个为负相关资产(如国债/反向ETF)

2. 时间维度一致性审查
   - 事件窗口是否冲突?(如T+1与T+7混配导致资金效率低下)
   - 建议: 统一至T-3到T+5窗口，或明确分层(短线/波段)

3. 止损可执行性审查 (HIGH水位关键)
   - -5%硬止损在HIGH波动率下能否成交?
   - 流动性折扣是否已计入?
   - 若不可执行→降级为"模拟盘观察"

4. 风险收益比再平衡
   - 组合整体盈亏比是否>1.5:1?
   - 若不足→否决 weakest link

5. 战略否决权行使
   - 识别"风控过审但CIO判断风险收益比不佳"的标的
   - 常见场景: 过拟合同一因子、事件窗口过长、止损不可执行

【输出结构】
1. 市场水位与战略约束声明
2. 核心确信列表 (Approved with Constraints): 含模式、仓位、核心假设、假设打破清仓条件、硬止损
3. CIO战略否决列表 (Strategic Veto): 风控通过但被CIO否决的标的及原因
4. 防御性配置方案: 现金管理、对冲工具、期权保护
5. 交易台执行指令: 入场条件、仓位上限、硬止损、时间止损、监控频率
6. 左侧监控池: 已进入超卖区但未达反转标准的标的及触发条件
7. 假设前提监控清单与紧急预案

【输出格式 - 严格JSON】
{{
    "cio_strategic_review": {{
        "market_risk_level": "{market_risk_level}",
        "risk_rationale": "{risk_level_rationale}",
        "strategic_stance": "防御性反转优先|趋势跟随|事件驱动|绝对防御",
        "constraints": {{
            "allowed_modes": {allowed_modes},
            "cash_ratio_min": "{cash_ratio}",
            "max_single_position": "{max_position}"
        }}
    }},
    "approved_list": [
        {{
            "code": "...",
            "name": "...",
            "mode": "A|B|C",
            "position": "X%",
            "key_assumption": "...",
            "assumption_break_trigger": "...",
            "hard_stop": "...",
            "time_stop": "T+N日",
            "monitoring": "监控指标"
        }}
    ],
    "strategic_veto_list": [
        {{
            "code": "...",
            "risk_committee_reason": "风控通过理由",
            "cio_veto_reason": "CIO否决理由",
            "suggested_alternative": "替代方案或监控条件"
        }}
    ],
    "defensive_allocation": {{
        "cash_management": "...",
        "hedge_instruments": "...",
        "options_protection": "..."
    }},
    "execution_directives": [
        {{
            "code": "...",
            "entry_condition": "非市价追，等待回踩X%",
            "position_cap": "X%",
            "hard_stop": "价位或跌幅",
            "time_stop": "T+N日无条件离场",
            "monitoring_frequency": "日度|事件触发"
        }}
    ],
    "watchlist": {{
        "criteria": "RSI<20且跌幅>25%",
        "candidates": ["...", "..."],
        "upgrade_trigger": "地缘风险消退信号"
    }},
    "assumption_monitoring": {{
        "premises": ["中东冲突持续", "两会政策超预期", "亚太市场稳定"],
        "indicators": {{"oil_price": "...", "gold_price": "...", "kospi": "..."}},
        "emergency_plan": "假设打破时的减仓/清仓流程"
    }}
}}
"""

# ============================================
# 7. 兼容性补丁
# ============================================

RED_TEAM_AUDIT_PROMPT = """
【红队审计】 (可选模块，用于策略回测时)
【任务】以对手盘视角，寻找策略漏洞。

【审计清单】
1. 若所有标的共享同一因子，策略是否过度拟合?
2. HIGH水位下是否有A/C轨提案漏网?
3. 止损位是否过于宽松(如-8%在HIGH波动率下几乎必然触发)?
4. 事件衰减计算是否正确(检查T-7事件权重)?

【输出】漏洞列表及严重等级
"""

# 版本信息
__version__ = "19.6.5"
__changelog__ = """
v19.6.5: 
- 新增CIO_PRE_FILTER战略前置拦截器
- HIGH水位强制禁用A/C轨，只允许B/D轨
- 全部关键词库增加量化触发条件
- 事件衰减函数精确化(指数/线性/阶梯)
- 风控压力测试矩阵全面量化
- CIO增加战略否决权与组合层面审查
"""
