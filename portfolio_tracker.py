import json
import os
import threading
from datetime import datetime
from utils import logger

class PortfolioTracker:
    def __init__(self, filepath='portfolio.json'):
        self.filepath = filepath
        self.lock = threading.Lock()
        self._load_portfolio()

    def _load_portfolio(self):
        if not os.path.exists(self.filepath):
            self.portfolio = {}
            self._save_portfolio()
        else:
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    self.portfolio = json.load(f)
            except Exception:
                self.portfolio = {}

    def _save_portfolio(self):
        with open(self.filepath, 'w', encoding='utf-8') as f:
            json.dump(self.portfolio, f, indent=2, ensure_ascii=False)

    def get_position(self, code):
        if code not in self.portfolio:
            return {'shares': 0, 'cost': 0.0, 'held_days': 0}
        pos = self.portfolio[code]
        if 'cost' not in pos: pos['cost'] = 0.0
        if 'shares' not in pos: pos['shares'] = 0
        return pos

    def add_trade(self, code, name, amount_or_value, price, is_sell=False):
        if price <= 0: return

        if code not in self.portfolio:
            self.portfolio[code] = {
                "name": name,
                "shares": 0,
                "cost": 0.0,
                "held_days": 0,
                "history": []
            }
        
        pos = self.portfolio[code]
        shares_change = amount_or_value / price
        
        record = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "price": round(price, 3),
            "s": "S" if is_sell else "B"
        }

        if is_sell:
            real_sell_shares = min(pos['shares'], shares_change)
            pos['shares'] = max(0, pos['shares'] - real_sell_shares)
            record['amt'] = -int(real_sell_shares * price)
            
            if pos['shares'] == 0:
                pos['cost'] = 0.0 
                pos['held_days'] = 0
        else:
            old_value = pos['shares'] * pos['cost']
            new_invest = shares_change * price
            total_shares = pos['shares'] + shares_change
            
            if total_shares > 0:
                new_cost = (old_value + new_invest) / total_shares
                pos['cost'] = round(new_cost, 4)
            
            pos['shares'] = total_shares
            record['amt'] = int(amount_or_value)
            if pos['held_days'] == 0:
                pos['held_days'] = 1

        pos['history'].append(record)
        if len(pos['history']) > 10:
            pos['history'] = pos['history'][-10:]

        self._save_portfolio()
        logger.info(f"⚖️ 账本更新 {name}: {'卖出' if is_sell else '买入'} | 最新成本: {pos.get('cost',0):.3f}")

    def record_signal(self, code, signal):
        pass 

    def get_signal_history(self, code):
        if code in self.portfolio:
            return self.portfolio[code].get('history', [])
        return []
        
    def confirm_trades(self):
        today = datetime.now().strftime("%Y-%m-%d")
        for code, pos in self.portfolio.items():
            if pos['shares'] > 0:
                pos['held_days'] = pos.get('held_days', 0) + 1
        self._save_portfolio()
