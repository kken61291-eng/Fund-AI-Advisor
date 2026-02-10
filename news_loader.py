import akshare as ak
import json
import os
import time
import pandas as pd
from datetime import datetime
import hashlib

# --- 配置 ---
DATA_DIR = "data_news"
if not os.path.exists(DATA_DIR):
    os.makedirs(self.DATA_DIR)

def get_today_str():
    return datetime.now().strftime("%Y-%m-%d")

def generate_news_id(item):
    """生成新闻唯一指纹，防止重复"""
    # 组合 时间+标题 作为唯一标识
    raw = f"{item.get('time','')}{item.get('title','')}"
    return hashlib.md5(raw.encode('utf-8')).hexdigest()

def clean_time_str(t_str):
    """标准化时间格式为 YYYY-MM-DD HH:MM:SS"""
    if not t_str: return ""
    try:
        # 尝试解析常见格式
        if len(str(t_str)) == 10: # 可能是时间戳 1700000000
             return datetime.fromtimestamp(int(t_str)).strftime("%Y-%m-%d %H:%M:%S")
        if len(str(t_str)) > 19:
            return str(t_str)[:19]
        return str(t_str)
    except:
        return str(t_str)

def fetch_and_save_news():
    print(f"📡 [NewsLoader] 启动双源抓取 (EastMoney + CLS) - {get_today_str()}...")
    
    all_news_items = []

    # ----------------------------------------------------
    # 1. 抓取 东方财富 (EastMoney) 7x24
    # ----------------------------------------------------
    try:
        print("   - 正在抓取: 东方财富 (EastMoney)...")
        df_em = ak.stock_telegraph_em()
        if df_em is not None and not df_em.empty:
            for _, row in df_em.iterrows():
                title = str(row.get('title', '')).strip()
                content = str(row.get('content', '')).strip()
                public_time = clean_time_str(row.get('public_time', ''))
                
                if not title or len(title) < 2: continue
                
                all_news_items.append({
                    "time": public_time,
                    "title": title,
                    "content": content,
                    "source": "EastMoney"
                })
    except Exception as e:
        print(f"   ❌ 东财抓取失败: {e}")

    # ----------------------------------------------------
    # 2. 抓取 财联社 (CLS) 电报
    # ----------------------------------------------------
    try:
        print("   - 正在抓取: 财联社 (CLS)...")
        # 财联社接口返回字段通常为: title, content, ctime
        df_cls = ak.stock_telegraph_cls()
        if df_cls is not None and not df_cls.empty:
            for _, row in df_cls.iterrows():
                title = str(row.get('title', '')).strip()
                content = str(row.get('content', '')).strip()
                # 财联社的时间字段可能叫 ctime 或 publish_time
                raw_time = row.get('ctime', row.get('publish_time', ''))
                public_time = clean_time_str(raw_time)
                
                # 财联社有些只有content没有title，或者title就是content
                if not title and content:
                    title = content[:30] + "..."
                
                if not title: continue

                all_news_items.append({
                    "time": public_time,
                    "title": title,
                    "content": content,
                    "source": "CLS"
                })
    except Exception as e:
        print(f"   ❌ 财联社抓取失败: {e}")

    # ----------------------------------------------------
    # 3. 合并入库 & 去重
    # ----------------------------------------------------
    if not all_news_items:
        print("⚠️ 未获取到任何新闻数据")
        return

    today_file = os.path.join(DATA_DIR, f"news_{get_today_str()}.jsonl")
    
    # 读取已存 ID
    existing_ids = set()
    if os.path.exists(today_file):
        with open(today_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    saved_item = json.loads(line)
                    if 'id' in saved_item:
                        existing_ids.add(saved_item['id'])
                except: pass

    # 写入新数据
    new_count = 0
    # 按时间倒序排列（最新的在前），但写入时我们追加，所以顺序不严格影响逻辑，关键是ID去重
    # 这里简单按时间排序一下，方便查看
    all_news_items.sort(key=lambda x: x['time'], reverse=True)

    with open(today_file, 'a', encoding='utf-8') as f:
        for item in all_news_items:
            item_id = generate_news_id(item)
            item['id'] = item_id
            
            if item_id not in existing_ids:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                existing_ids.add(item_id)
                new_count += 1
    
    print(f"✅ 入库完成: 新增 {new_count} 条 | 总存量 {len(existing_ids)} 条 | 来源: EastMoney & CLS")

if __name__ == "__main__":
    fetch_and_save_news()
