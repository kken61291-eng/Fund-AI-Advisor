import json
import os
from datetime import datetime
from utils import logger

class PortfolioTracker:
    def __init__(self, file_path='portfolio.json'):
        self.file_path = file_path
        self.data = self._load_data()

    def _load_data(self):
        """
        V10.5: 健壮的数据加载
        自动检测并修复缺失的 JSON 结构，防止 KeyError
        """
        # 标准结构定义
        default_structure = {
            "positions": {},       # 持仓详情
            "cash": 0,             # 现金余额
            "history": [],         # 交易历史
            "signal_record": {}    # 信号记录 (V10新增)
        }

        # 1. 文件不存在，返回默认
        if not os.path.exists(self.file_path):
            return default_structure

        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content: # 文件为空
                    return default_structure
                data = json.loads(content)

            # 2. 结构自检与修复 (Schema Migration)
            # 遍历标准结构，如果 data 缺什么 key，就补什么
            has_changed = False
            for key, default_val in default_structure.items():
                if key not in data:
                    logger.warning(f"⚠️ 账本修复: 补全缺失字段 '{key}'")
                    data[key] = default_val
                    has_changed = True
            
            # 如果修复过，立即回写文件
            if has_changed:
                self._save_data_internal(data)

            return data

        except Exception as e:
            logger.error(f"账本加载失败，重置为默认: {e}")
            return default_structure

    def _save_data(self):
        """保存当前内存数据"""
        self._save_data_internal(self.data)

    def _save_data_internal(self, data_to_save):
        """内部保存逻辑"""
        try:
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"账本保存失败: {e}")

    def get_position(self, code):
        """获取持仓详情"""
        # 双重保险：防止 positions 字段意外丢失
        if "positions" not in self.data:
            self.data["positions"] = {}
            
        if code not in self.data['positions']:
            return {"cost": 0, "shares": 0, "first_buy_date": None, "held_days": 0}
        
        pos = self.data['positions'][code]
        
        # 计算持仓天数
        held_days = 0
        if pos.get('first_buy_date'):
            try:
                first_date = datetime.strptime(pos['first_buy_date'], "%Y-%m-%d")
                held_days = (datetime.now() - first_date).days
            except: pass
        
        return {
            "cost": pos.get('avg_cost', 0),
            "shares": pos.get('shares', 0),
            "first_buy_date": pos.get('first_buy_date'),
            "held_days": held_days
        }

    def add_trade(self, code, name, amount, price, is_sell=False):
        """记录交易"""
        if "positions" not in self.data: self.data["positions"] = {}
        
        if code not in self.data['positions']:
            self.data['positions'][code] = {"shares": 0, "avg_cost": 0, "first_buy_date": None}
        
        pos = self.data['positions'][code]
        
        if not is_sell: # 买入
            cost_total = pos['shares'] * pos['avg_cost'] + amount
            new_shares = amount / price
            pos['shares'] += new_shares
            pos['avg_cost'] = cost_total / pos['shares'] if pos['shares'] > 0 else 0
            if pos['first_buy_date'] is None:
                pos['first_buy_date'] = datetime.now().strftime("%Y-%m-%d")
            
        else: # 卖出
            sell_shares = amount / price
            pos['shares'] = max(0, pos['shares'] - sell_shares)
            if pos['shares'] < 10: # 碎股清零
                pos['shares'] = 0
                pos['avg_cost'] = 0
                pos['first_buy_date'] = None

        # 记录到历史列表
        if "history" not in self.data: self.data["history"] = []
        self.data['history'].append({
            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "code": code, "name": name, "side": "SELL" if is_sell else "BUY",
            "amount": amount, "price": round(price, 3)
        })
        
        self._save_data()

    def confirm_trades(self):
        """T+1 确认 (占位，暂不需复杂逻辑)"""
        pass

    def record_signal(self, code, strategy_label):
        """记录信号历史"""
        if "signal_record" not in self.data: self.data["signal_record"] = {}
        
        record_list = self.data['signal_record'].get(code, [])
        today_str = datetime.now().strftime("%m-%d")
        
        short_status = "W"
        if "买" in strategy_label or "增持" in strategy_label: short_status = "B"
        elif "清仓" in strategy_label: short_status = "C"
        elif "卖" in strategy_label or "减仓" in strategy_label: short_status = "S"
        
        if not record_list or record_list[-1]['date'] != today_str:
            record_list.append({"date": today_str, "s": short_status})
        
        if len(record_list) > 10:
            record_list = record_list[-10:]
            
        self.data['signal_record'][code] = record_list
        self._save_data()

    def get_signal_history(self, code):
        if "signal_record" not in self.data: return []
        return self.data['signal_record'].get(code, [])
