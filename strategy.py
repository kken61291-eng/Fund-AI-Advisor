from utils import logger
# 如果需要引用 POST_VALIDATION_RULES 常量，可取消注释，这里直接将逻辑内嵌以减少依赖问题
# from prompts_config import POST_VALIDATION_RULES 

class StrategyEngine:
    """
    策略执行引擎 - V3.5 适配版
    核心职能：执行后处理规则 (Post-Validation)，生成最终决策报告
    """
    def __init__(self, config):
        self.cfg = config
        self.base_amt = config['global'].get('base_invest_amount', 10000)
    
    def apply_post_validation(self, ai_result, tech_data, days_to_event):
        """
        [硬核风控] 执行 Python 侧的强制校验
        对应 prompts_config 中的 POST_VALIDATION_RULES
        """
        # 提取关键字段
        decision = ai_result.get('decision', 'HOLD')
        meta = ai_result.get('strategy_meta', {})
        mode = meta.get('mode', 'WAIT')
        rationale = meta.get('rationale', '')
        pos_size = ai_result.get('position_size', 0)
        
        trend_score = tech_data.get('quant_score', 0)
        recent_gain = tech_data.get('recent_gain', 0)

        # --- Rule 1: 垃圾时间过滤器 (Garbage Time Filter) ---
        # 条件：趋势分低 + 无事件 + 非反转模式
        is_garbage_time = (trend_score < 40) and (str(days_to_event) == "NULL") and (mode != 'MEAN_REVERSION')
        
        if is_garbage_time:
            if decision == 'EXECUTE':
                logger.warning(f"🛡️ [系统拦截] 垃圾时间过滤器触发: Trend={trend_score}, Mode={mode}")
                decision = "HOLD_CASH"
                rationale = "[系统强制] 垃圾时间，拒绝强行交易。 " + rationale
                pos_size = 0
                mode = "WAIT(CASH)"

        # --- Rule 2: 防抢跑检查 (Anti-Chase) ---
        # 条件：事件驱动模式 + 5日涨幅 > 15%
        if mode == 'EVENT_DRIVEN' and recent_gain > 15:
            logger.warning(f"🛡️ [系统拦截] 防抢跑熔断: 5日涨幅 {recent_gain}% > 15%")
            decision = "REJECT"
            rationale = "[系统强制] 预期透支(Price In)，盈亏比不佳。 " + rationale
            pos_size = 0

        # --- Rule 3: 补涨逻辑强校验 (Laggard Check) ---
        if "补涨" in rationale and mode == 'EVENT_DRIVEN':
            # 简化的资金外溢检查：如果成交量没有显著放大(Vol Ratio < 1.0)，视为弱势
            vol_ratio = tech_data.get('volume_analysis', {}).get('vol_ratio', 1.0)
            if vol_ratio < 0.8:
                logger.warning(f"🛡️ [系统拦截] 伪补涨逻辑: 成交量低迷 ({vol_ratio})")
                decision = "HOLD"
                rationale = "[系统强制] 缺乏资金外溢证据，视为弱者恒弱。 " + rationale
                pos_size = 0

        # 回写结果
        ai_result['decision'] = decision
        ai_result['position_size'] = pos_size
        if 'strategy_meta' not in ai_result: ai_result['strategy_meta'] = {}
        ai_result['strategy_meta']['mode'] = mode
        ai_result['strategy_meta']['rationale'] = rationale
        
        return ai_result

    def calculate_final_decision(self, fund_info, tech_data, ai_result, market_ctx):
        """
        生成最终的可读报告
        """
        # 1. 获取事件天数 (优先从AI结果拿，若无则NULL)
        days_to_event = ai_result.get('trend_analysis', {}).get('days_to_event', "NULL")
        
        # 2. 执行后处理校验
        ai_result = self.apply_post_validation(ai_result, tech_data, days_to_event)
        
        # 3. 提取最终状态
        decision = ai_result['decision']
        mode = ai_result.get('strategy_meta', {}).get('mode', 'UNKNOWN')
        reason = ai_result.get('strategy_meta', {}).get('rationale', 'No reason')
        size_pct = ai_result.get('position_size', 0)
        
        # 4. 计算金额
        final_amt = int(self.base_amt * (size_pct / 100)) if decision == "EXECUTE" else 0
        
        # 5. 格式化报告
        fund_name = fund_info.get('name', 'Unknown')
        fund_code = fund_info.get('code', '000000')
        
        report = f"**{fund_name} ({fund_code})**\n"
        report += f"🚦 **模式**: [{mode}] -> {decision}\n"
        
        if decision == "EXECUTE":
            report += f"💰 **建议**: 买入 {size_pct}% 仓位 (¥{final_amt})\n"
        elif decision == "HOLD_CASH":
            report += f"🛑 **建议**: 空仓观望 (Cash is King)\n"
        else:
            report += f"👀 **建议**: {decision}\n"
            
        report += f"🧠 **逻辑**: {reason}\n"
        
        # 补充事件信息
        if mode == 'EVENT_DRIVEN':
            exec_notes = ai_result.get('execution_notes', '无')
            report += f"⏳ **潜伏**: 距事件 {days_to_event} 天 | {exec_notes}\n"
            
        # 补充数据看板
        score = tech_data.get('quant_score', 0)
        gain = tech_data.get('recent_gain', 0)
        rsi = tech_data.get('rsi', 0)
        
        report += f"📊 **看板**: Score={score} | 5日涨幅={gain}% | RSI={rsi}\n"
        
        # 风险提示
        if 'cro_arbitration' in ai_result:
            cro = ai_result['cro_arbitration']
            if isinstance(cro, dict): # 确保是字典
                vol_check = cro.get('volume_check', '-')
                report += f"🛡️ **CRO审计**: 量能{vol_check} | {ai_result.get('cro_risk_audit', {}).get('fundamental_check', '')}\n"

        return report
