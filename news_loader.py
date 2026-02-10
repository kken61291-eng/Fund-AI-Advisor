import akshare as ak
import json
import os
import time
import pandas as pd
from datetime import datetime
import hashlib
import pytz
from bs4 import BeautifulSoup

# --- Selenium 模块 (模拟浏览器) ---
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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
        # 尝试解析常见格式
        if len(str(t_str)) == 10: 
             return datetime.fromtimestamp(int(t_str)).strftime("%Y-%m-%d %H:%M:%S")
        if len(str(t_str)) > 19:
            return str(t_str)[:19]
        return str(t_str)
    except:
        return str(t_str)

# ==========================================
# 1. 东财抓取 (使用 Akshare API)
# ==========================================
def fetch_eastmoney():
    items = []
    try:
        print("   - [API] 正在抓取: 东方财富 (EastMoney)...")
        # 强制更新一下接口，防止报错
        df_em = ak.stock_telegraph_em()
        if df_em is not None and not df_em.empty:
            for _, row in df_em.iterrows():
                title = str(row.get('title', '')).strip()
                content = str(row.get('content', '')).strip()
                public_time = clean_time_str(row.get('public_time', ''))
                
                if not title or len(title) < 2: continue
                items.append({
                    "time": public_time,
                    "title": title,
                    "content": content,
                    "source": "EastMoney"
                })
    except Exception as e:
        print(f"   ❌ 东财抓取失败: {e}")
    return items

# ==========================================
# 2. 财联社抓取 (使用 Selenium 模拟浏览器)
# ==========================================
def fetch_cls_selenium():
    items = []
    driver = None
    try:
        print("   - [Browser] 正在启动 Chrome 抓取: 财联社 (CLS)...")
        
        # 配置无头浏览器 (Headless Chrome)
        chrome_options = Options()
        chrome_options.add_argument("--headless") # 无界面模式
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        # 伪装 User-Agent
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

        # 自动安装并启动 Driver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 设置超时
        driver.set_page_load_timeout(30)
        
        # 访问财联社电报页面
        url = "https://www.cls.cn/telegraph"
        driver.get(url)
        
        # 等待内容加载 (等待列表出现)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "telegraph-list"))
            )
        except:
            print("   ⚠️ 等待网页加载超时，尝试直接解析...")

        # 滚动一下屏幕触发懒加载
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(2) 

        # 获取页面 HTML
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # 解析数据 (根据财联社网页结构)
        # 通常是一个 class="telegraph-list" 的列表
        # 每一项可能有 class="telegraph-content-box" 等
        
        # 寻找所有的时间线节点 (这需要根据 cls 实际 html 结构调整，以下是通用抓取逻辑)
        # 目前 CLS 结构通常是: div.telegraph-list -> div.telegraph-list-item
        nodes = soup.find_all("div", class_="telegraph-list-item")
        
        if not nodes:
            # 备用方案：尝试找所有带时间戳样式的文本
            nodes = soup.select("div.telegraph-content-box")

        print(f"   - 捕获到 {len(nodes)} 个网页节点")

        current_date_prefix = get_beijing_time().strftime("%Y-%m-%d")

        for node in nodes:
            try:
                # 提取时间 (通常在 span 中)
                time_span = node.find("span", class_="telegraph-time")
                time_str = time_span.get_text().strip() if time_span else ""
                
                # 补全日期 (网页通常只显示 14:30)
                if len(time_str) <= 5 and ":" in time_str:
                    full_time = f"{current_date_prefix} {time_str}:00"
                else:
                    full_time = time_str

                # 提取内容
                content_div = node.find("div", class_="telegraph-content")
                if not content_div:
                    # 尝试备用结构
                    content_div = node.find("div", class_="telegraph-detail")
                
                content_text = content_div.get_text().strip() if content_div else ""
                
                # 财联社电报通常没有独立标题，内容第一句即标题
                if content_text:
                    title = content_text[:40] + "..." if len(content_text) > 40 else content_text
                    
                    items.append({
                        "time": full_time,
                        "title": title,
                        "content": content_text,
                        "source": "CLS"
                    })
            except: continue

    except Exception as e:
        print(f"   ❌ 财联社(Selenium)抓取失败: {e}")
    finally:
        if driver:
            driver.quit()
    
    return items

# ==========================================
# 主程序
# ==========================================
def fetch_and_save_news():
    today_date = get_today_str()
    print(f"📡 [NewsLoader] 启动混合抓取 (Akshare + Selenium) - {today_date}...")
    
    all_news_items = []

    # 1. 东财
    em_items = fetch_eastmoney()
    all_news_items.extend(em_items)

    # 2. 财联社 (浏览器模式)
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
    # 简单按时间字符串倒序
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
