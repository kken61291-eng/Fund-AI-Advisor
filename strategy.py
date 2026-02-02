class StrategyEngine:
    def __init__(self, config):
        self.cfg = config
        self.base_amt = config['global']['base_invest_amount']
    
    def calculate_final_decision(self, fund_info, tech_data, ai_result, market_ctx):
        """
        结合 AI 智慧与量化规则的最终决策
        """
        action = ai_result.get('action_advice', '观望')
        thesis = ai_result.get('thesis', '无逻辑')
        
        # --- 宏观红绿灯机制 (Macro Traffic Light) ---
        # 如果北向资金流出超过 50亿，视为系统性风险，强制减仓或暂停
        is_system_risk = market_ctx['north_money'] < -50
        
        # --- 风口捕捉 (Opportunity Hunter) ---
        # 检查该基金所属板块，是否在今日全市场主力流入 Top5 中
        sector_hot = False
        for top_sec in market_ctx['top_sectors']:
            # 简单的关键词匹配，比如 "白酒" in "食品饮料"
            if fund_info['sector_keyword'] in top_sec:
                sector_hot = True
                break
        
        # --- 资金计算逻辑 ---
        final_amt = 0
        
        if "买入" in action:
            final_amt = self.base_amt
            
            # 1. 顺势加仓：如果是热点板块，加倍
            if sector_hot:
                final_amt *= 1.5
                thesis += " [🔥命中今日主力风口]"
            
            # 2. 强力买入信号
            if "强力" in action:
                final_amt *= 1.2
            
            # 3. 抄底信号：RSI < 30
            if tech_data['rsi'] < 30:
                thesis += " [超卖反弹博弈]"

        # --- 风险熔断 ---
        if is_system_risk and final_amt > 0:
            final_amt *= 0.5 # 减半
            thesis += " [⚠️外资大幅流出，仓位折半]"

        # 生成人类可读报告
        report = f"**{fund_info['name']} ({fund_info['code']})**\n"
        report += f"🎯 **决策**: {action} | 💰 **建议金额**: ¥{int(final_amt)}\n"
        report += f"🧠 **核心逻辑**: {thesis}\n"
        report += f"📈 **利多**: {ai_result.get('pros', 'N/A')}\n"
        report += f"📉 **利空**: {ai_result.get('cons', 'N/A')}\n"
        report += f"🛡️ **风险**: {ai_result.get('risk_warning', 'N/A')}\n"
        report += f"📊 **技术**: RSI={tech_data['rsi']:.1f} | 趋势={tech_data['price_position']}\n"
        
        return report
