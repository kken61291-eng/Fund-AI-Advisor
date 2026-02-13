import akshare as ak
import json
import os
import time
import requests
import pandas as pd
from datetime import datetime
import hashlib
import pytz
import re
from bs4 import BeautifulSoup

# --- Selenium 模块 ---
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
except ImportError:
    print("❌ 缺少 Selenium 依赖，请在 requirements.txt 中添加: selenium, webdriver-manager, beautifulsoup4")

# --- 配置 ---
DATA_DIR = "data_news"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def get_beijing_time():
    return datetime.now(pytz.timezone('Asia/Shanghai'))

def get_today_str():
    return get_beijing_time().strftime("%Y-%m-%d")

def generate_news_id(item):
    raw = f"{item.get('time','')}{item.get('title','')}"
    return hashlib.md5(raw.encode('utf-8')).hexdigest()

def clean_time_str(t_str):
    if not t_str: return ""
    try:
        if len(str(t_str)) == 10: 
             return datetime.fromtimestamp(int(t_str)).strftime("%Y-%m-%d %H:%M:%S")
        if len(str(t_str)) > 19:
            return str(t_str)[:19]
        return str(t_str)
    except:
        return str(t_str)

# ==========================================
# 1. 东财抓取 (双保险模式)
# ==========================================
def fetch_eastmoney_direct():
    items = []
    try:
        print("   - [Plan B] 启动东财直连模式 (Direct API)...")
        url = "https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_50_1_.html"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://kuaixun.eastmoney.com/"
        }
        resp = requests.get(url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            text = resp.text
            try:
                start_idx = text.find('{')
                end_idx = text.rfind('}')
                if start_idx != -1 and end_idx != -1:
                    json_str = text[start_idx : end_idx + 1]
                    data = json.loads(json_str)
                    news_list = data.get('LivesList', [])
                    for news in news_list:
                        title = news.get('title', '').strip()
                        digest = news.get('digest', '').strip()
                        show_time = news.get('showtime', '') 
                        content = digest if len(digest) > len(title) else title
                        if not title: continue
                        items.append({
                            "time": show_time, "title": title, "content": content, "source": "EastMoney"
                        })
                    print(f"   - [Plan B] 成功解析并获取 {len(items)} 条数据")
            except Exception as parse_e:
                print(f"   - [Plan B] JSON 解析异常: {parse_e}")
    except Exception as e:
        print(f"   ❌ [Plan B] 东财直连失败: {e}")
    return items

def fetch_eastmoney():
    items = []
    try:
        print("   - [Plan A] 正在抓取: 东方财富 (Akshare)...")
        df_em = ak.stock_telegraph_em()
        if df_em is not None and not df_em.empty:
            for _, row in df_em.iterrows():
                title = str(row.get('title', '')).strip()
                content = str(row.get('content', '')).strip()
                public_time = clean_time_str(row.get('public_time', ''))
                if not title or len(title) < 2: continue
                items.append({
                    "time": public_time, "title": title, "content": content, "source": "EastMoney"
                })
            print(f"   - [Plan A] 成功获取 {len(items)} 条数据")
            return items
    except Exception as e:
        print(f"   ⚠️ Akshare 调用出错，切换至 Plan B...")
    return fetch_eastmoney_direct()

# ==========================================
# 2. 财联社抓取 (Selenium 增强版)
# ==========================================
def fetch_cls_selenium():
    items = []
    driver = None
    try:
        print("   - [Browser] 正在启动 Chrome 抓取: 财联社 (CLS)...")
        
        chrome_options = Options()
        # 🟢 必须开启无头模式，否则服务器无法运行
        chrome_options.add_argument("--headless") 
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        # 伪装 User-Agent
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        
        url = "https://www.cls.cn/telegraph"
        driver.get(url)
        
        # 等待加载
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "telegraph-list"))
            )
        except:
            print("   ⚠️ 等待网页加载超时，尝试直接解析...")

        # 模拟滚动加载更多
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(2) 

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        nodes = soup.find_all("div", class_="telegraph-list-item")
        if not nodes:
            nodes = soup.select("div.telegraph-content-box")

        print(f"   - 捕获到 {len(nodes)} 个网页节点")

        current_date_prefix = get_beijing_time().strftime("%Y-%m-%d")

        for node in nodes:
            try:
                time_span = node.find("span", class_="telegraph-time")
                time_str = time_span.get_text().strip() if time_span else ""
                
                # 时间补全逻辑
                if len(time_str) < 10 and ":" in time_str:
                    if len(time_str) <= 5:
                        full_time = f"{current_date_prefix} {time_str}:00"
                    else:
                        full_time = f"{current_date_prefix} {time_str}"
                else:
                    full_time = time_str

                content_div = node.find("div", class_="telegraph-content")
                if not content_div: content_div = node.find("div", class_="telegraph-detail")
                
                content_text = content_div.get_text().strip() if content_div else ""
                
                if content_text:
                    title = content_text[:40] + "..." if len(content_text) > 40 else content_text
                    
                    items.append({
                        "time": full_time, "title": title, "content": content_text, "source": "CLS"
                    })
            except: continue

    except Exception as e:
        print(f"   ❌ 财联社(Selenium)抓取失败: {e}")
        print("   (提示: 请确保服务器已安装 Chrome 和 ChromeDriver)")
    finally:
        if driver:
            try: driver.quit()
            except: pass
    
    return items

# ==========================================
# 主程序
# ==========================================
def fetch_and_save_news():
    today_date = get_today_str()
    print(f"📡 [NewsLoader] 启动混合抓取 (Smart Mode) - {today_date}...")
    
    all_news_items = []

    # 1. 东财
    em_items = fetch_eastmoney()
    all_news_items.extend(em_items)

    print(f"⏳ 正在启动浏览器抓取财联社...")
    # 2. 财联社 (Selenium)
    cls_items = fetch_cls_selenium()
    all_news_items.extend(cls_items)

    # 3. 入库
    if not all_news_items:
        print("⚠️ 未获取到任何新闻数据")
        return

    today_file = os.path.join(DATA_DIR, f"news_{today_date}.jsonl")
    existing_ids = set()
    
    if os.path.exists(today_file):
        with open(today_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    saved_item = json.loads(line)
                    if 'id' in saved_item:
                        existing_ids.add(saved_item['id'])
                except: pass

    new_count = 0
    all_news_items.sort(key=lambda x: x['time'], reverse=True)

    with open(today_file, 'a', encoding='utf-8') as f:
        for item in all_news_items:
            item_id = generate_news_id(item)
            item['id'] = item_id
            if item_id not in existing_ids:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                existing_ids.add(item_id)
                new_count += 1
    
    print(f"✅ 入库完成: 新增 {new_count} 条 (EM:{len(em_items)} | CLS:{len(cls_items)})")

if __name__ == "__main__":
    fetch_and_save_news()
