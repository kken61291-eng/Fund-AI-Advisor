import json
import os
import fcntl
import tempfile
import shutil
import pandas as pd
from datetime import datetime
from utils import logger

class PortfolioTracker:
    def __init__(self, file_path="portfolio.json"):
        self.file_path = file_path
        self.data = self._load_atomic()

    def _load_atomic(self):
        """
        🛡️ 原子读取 + 结构自检 + 异常熔断
        """
        default_structure = {"holdings": {}, "pending": []}
        
        if not os.path.exists(self.file_path):
            return default_structure
            
        try:
            data = default_structure
            with open(self.file_path, 'r', encoding='utf-8') as f:
                # 尝试获取共享锁
                try:
                    fcntl.flock(f, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    data = json.load(f)
                    fcntl.flock(f, fcntl.LOCK_UN)
                except IOError:
                    data = json.load(f)
            
            # 结构完整性校验
            if not isinstance(data, dict):
                logger.warning("⚠️ 账本损坏，重置为空账本")
                return default_structure

            if "holdings" not in data: data["holdings"] = {}
            if "pending" not in data: data["pending"] = []

            # 清理脏数据
            dirty_keys = []
            for code, pos in data["holdings"].items():
                if not isinstance(pos, dict):
                    dirty_keys.append(code)
                    continue
                cost = pos.get("cost", 0)
                shares = pos.get("shares", 0)
                if cost < 0 or shares < 0:
                    dirty_keys.append(code)

            if dirty_keys:
                for k in dirty_keys:
                    del data["holdings"][k]
                self.save_atomic_data(data)

            return data

        except Exception as e:
            logger.error(f"🔥 账本致命错误: {e}, 重置为安全模式")
            return default_structure

    def save_atomic_data(self, data_to_save=None):
        target_data = data_to_save if data_to_save else self.data
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, dir='.', encoding='utf-8') as tmp:
                json.dump(target_data, tmp, indent=4, ensure_ascii=False)
                tmp.flush()
                os.fsync(tmp.fileno())
                tmp_name = tmp.name
            
            with open(self.file_path, 'a') as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                shutil.move(tmp_name, self.file_path)
                fcntl.flock(f, fcntl.LOCK_UN)
        except Exception as e:
            logger.error(f"账本保存失败: {e}")

    def save_atomic(self):
        self.save_atomic_data()

    def confirm_trades(self):
        """
        🛡️ T+1 交易确认机制
        """
        today = datetime.now().strftime("%Y-%m-%d")
        new_pending = []
        confirmed_count = 0

        if not isinstance(self.data.get("pending"), list):
            self.data["pending"] = []

        for trade in self.data["pending"]:
            if not isinstance(trade, dict) or "confirm_date" not in trade:
                continue 
                
            if trade["confirm_date"] <= today:
                self._execute_confirmed_trade(trade)
                confirmed_count += 1
            else:
                new_pending.append(trade)
        
        if confirmed_count > 0:
            self.data["pending"] = new_pending
            self.save_atomic()
            logger.info(f"✅ 已确认 {confirmed_count} 笔 T+1 交易")

    def _execute_confirmed_trade(self, trade):
        code = trade.get("code")
        trade_amt = trade.get("amount", 0)
        price = trade.get("price", 1.0)
        
        if not code: return

        if code not in self.data["holdings"]:
            self.data["holdings"][code] = {"cost": 0.0, "amount": 0.0, "shares": 0.0}
        
        pos = self.data["holdings"][code]
        
        # 买入 (金额)
        if trade_amt > 0:
            new_shares = trade_amt / price
            total_shares = pos["shares"] + new_shares
            total_cost_amt = (pos["shares"] * pos["cost"]) + trade_amt
            
            pos["cost"] = total_cost_amt / total_shares if total_shares > 0 else 0
            pos["shares"] = round(total_shares, 2)
            pos["amount"] = round(total_shares * pos["cost"], 2)
            
        # 卖出 (份额)
        else:
            sell_shares = abs(trade_amt)
            pos["shares"] = max(0, round(pos["shares"] - sell_shares, 2))
            pos["amount"] = round(pos["shares"] * pos["cost"], 2)
            
            if pos["shares"] < 10: 
                del self.data["holdings"][code]

    def add_trade(self, code, name, amount, price, is_sell=False):
        """
        🛡️ 区分金额与份额
        """
        confirm_date = (datetime.now() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        final_val = 0
        
        if not is_sell:
            final_val = amount
            logger.info(f"📝 [T+1待确认] 买入 {name} ¥{amount}")
        else:
            pos = self.data["holdings"].get(code)
            if not pos: 
                logger.warning(f"⚠️ 空仓无法卖出: {name}")
                return
            
            shares_to_sell = amount / price
            current_shares = pos["shares"]
            if shares_to_sell > current_shares: shares_to_sell = current_shares
            
            final_val = -shares_to_sell # 负数 = 份额
            logger.info(f"📝 [T+1待确认] 卖出 {name} {shares_to_sell:.2f}份 (约¥{amount})")

        self.data["pending"].append({
            "code": code,
            "name": name,
            "amount": final_val,
            "price": price,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "confirm_date": confirm_date
        })
        self.save_atomic()

    def get_position(self, code):
        h = self.data["holdings"].get(code, {})
        return {
            "cost": h.get("cost", 0.0),
            "shares": h.get("shares", 0.0),
            "amount": h.get("amount", 0.0)
        }
