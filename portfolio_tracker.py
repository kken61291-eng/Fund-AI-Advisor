import json
import os
from datetime import datetime
from utils import logger

class PortfolioTracker:
    def __init__(self, file_path='portfolio.json'):
        self.file_path = file_path
        self.data = self._load_data()

    def _load_data(self):
        default = {"positions": {}, "cash": 0, "history": [], "signal_record": {}}
        if not os.path.exists(self.file_path): return default
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content: return default
                data = json.loads(content)
            # 自动修复缺失字段
            for k, v in default.items():
                if k not in data: data[k] = v
            return data
        except: return default

    def _save_data(self):
        with open(self.file_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def get_position(self, code):
        if code not in self.data['positions']:
            return {"cost": 0, "shares": 0, "first_buy_date": None, "held_days": 0}
        pos = self.data['positions'][code]
        held_days = 0
        if pos.get('first_buy_date'):
            try: held_days = (datetime.now() - datetime.strptime(pos['first_buy_date'], "%Y-%m-%d")).days
            except: pass
        return {"cost": pos.get('avg_cost', 0), "shares": pos.get('shares', 0), "first_buy_date": pos.get('first_buy_date'), "held_days": held_days}

    def add_trade(self, code, name, amount, price, is_sell=False):
        if code not in self.data['positions']: self.data['positions'][code] = {"shares": 0, "avg_cost": 0, "first_buy_date": None}
        pos = self.data['positions'][code]
        if not is_sell:
            cost_total = pos['shares'] * pos['avg_cost'] + amount
            pos['shares'] += amount / price
            pos['avg_cost'] = cost_total / pos['shares'] if pos['shares'] > 0 else 0
            if not pos['first_buy_date']: pos['first_buy_date'] = datetime.now().strftime("%Y-%m-%d")
        else:
            pos['shares'] = max(0, pos['shares'] - amount / price)
            if pos['shares'] < 10: pos['shares'] = 0; pos['avg_cost'] = 0; pos['first_buy_date'] = None
        self.data['history'].append({"date": datetime.now().strftime("%Y-%m-%d %H:%M"), "code": code, "name": name, "side": "SELL" if is_sell else "BUY", "amount": amount, "price": round(price, 3)})
        self._save_data()

    def confirm_trades(self): pass

    def record_signal(self, code, label):
        rec = self.data['signal_record'].get(code, [])
        status = "B" if "买" in label else ("S" if "卖" in label else "W")
        today = datetime.now().strftime("%m-%d")
        if not rec or rec[-1]['date'] != today: rec.append({"date": today, "s": status})
        self.data['signal_record'][code] = rec[-10:]
        self._save_data()

    def get_signal_history(self, code): return self.data['signal_record'].get(code, [])
