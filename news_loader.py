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
import difflib  # 🟢 新增：用于计算文本相似度
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
# 2. 财联社抓取 (API + Selenium 双保险增强版)
# ==========================================
def fetch_cls_api():
    """ 🟢 新增：尝试通过公开 API 直接获取财联社电报，速度极快 """
    items = []
    try:
        print("   - [Plan A] 正在尝试通过 API 抓取: 财联社 (CLS)...")
        url = "https://www.cls.cn/nodeapi/telegraphList?rn=50"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if "data" in data and "roll_data" in data["data"]:
                for news in data["data"]["roll_data"]:
                    title = news.get("title", "")
                    content = news.get("content", "")
                    ctime = news.get("ctime")
                    
                    if not title and not content: continue
                    final_title = title if title else (content[:40] + "..." if len(content) > 40 else content)
                    final_content = content if content else title
                    full_time = datetime.fromtimestamp(ctime).strftime("%Y-%m-%d %H:%M:%S") if ctime else ""
                    
                    items.append({
                        "time": full_time, "title": final_title, "content": final_content, "source": "CLS"
                    })
                print(f"   - [Plan A] 成功通过 API 获取 {len(items)} 条财联社数据")
                return items
    except Exception as e:
        print(f"   ⚠️ API 抓取异常或拦截: {e}，将自动切换至浏览器模式...")
    return items

def fetch_cls_selenium():
    items = []
    driver = None
    try:
        print("   - [Plan B] 正在启动 Chrome 抓取: 财联社 (CLS)...")
        
        chrome_options = Options()
        chrome_options.add_argument("--headless") 
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        
        url = "https://www.cls.cn/telegraph"
        driver.get(url)
        
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "telegraph-list"))
            )
        except:
            print("   ⚠️ 等待网页加载超时，尝试直接解析...")

        print("   - 正在自动向下滚动网页以加载更多历史电报...")
        for _ in range(4):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5) 

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        nodes = soup.find_all("div", class_="telegraph-list-item")
        if not nodes:
            nodes = soup.select("div.telegraph-content-box")

        print(f"   - [Plan B] 捕获到 {len(nodes)} 个网页节点")

        current_date_prefix = get_beijing_time().strftime("%Y-%m-%d")

        for node in nodes:
            try:
                time_span = node.find("span", class_="telegraph-time")
                time_str = time_span.get_text().strip() if time_span else ""
                
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

def fetch_cls():
    """ 综合控制台，混合调度财联社数据获取 """
    items = fetch_cls_api()
    if not items or len(items) < 15:
        print("   ⚠️ API 获取数据偏少，启动 Selenium 进行深度抓取补充...")
        sel_items = fetch_cls_selenium()
        
        seen_titles = set([i['title'] for i in items])
        for si in sel_items:
            if si['title'] not in seen_titles:
                items.append(si)
                seen_titles.add(si['title'])
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

    # 🟢 核心优化：构建东财新闻纯净文本池与纯净标题池
    em_clean_texts = set()
    em_clean_titles = set()
    for item in em_items:
        clean_text = re.sub(r'[^\w\u4e00-\u9fa5]', '', item.get('content', '') + item.get('title', ''))
        clean_title = re.sub(r'[^\w\u4e00-\u9fa5]', '', item.get('title', ''))
        
        if len(clean_text) >= 10: 
            em_clean_texts.add(clean_text)
        if len(clean_title) >= 5:
            em_clean_titles.add(clean_title)

    print(f"⏳ 正在启动财联社抓取任务...")
    
    # 2. 财联社
    cls_items_raw = fetch_cls()

    # 🟢 核心优化：多维度拦截已存在于东财中的重复新闻
    cls_items_filtered = []
    filtered_count = 0
    
    for item in cls_items_raw:
        cls_clean_text = re.sub(r'[^\w\u4e00-\u9fa5]', '', item.get('content', '') + item.get('title', ''))
        cls_clean_title = re.sub(r'[^\w\u4e00-\u9fa5]', '', item.get('title', ''))
        is_duplicate = False
        
        # 拦截层级 1：纯净标题双向包含匹配（过滤绝大多数情况）
        if len(cls_clean_title) >= 5:
            for em_title in em_clean_titles:
                if cls_clean_title in em_title or em_title in cls_clean_title:
                    is_duplicate = True
                    break

        # 拦截层级 2：纯净正文高精度比对（针对标题失效、但正文高度相似的情况）
        if not is_duplicate and len(cls_clean_text) >= 10:
            for em_text in em_clean_texts:
                # 模糊包含匹配
                if cls_clean_text in em_text or em_text in cls_clean_text:
                    is_duplicate = True
                    break
                
                # 文本相似度匹配：取前 100 个纯净字符进行相似度计算，克服前缀干扰
                # 如果前100个字的相似度达到 80% 以上，判定为同一条新闻
                sim_ratio = difflib.SequenceMatcher(None, cls_clean_text[:100], em_text[:100]).quick_ratio()
                if sim_ratio > 0.8:
                    is_duplicate = True
                    break
                    
        if is_duplicate:
            filtered_count += 1
        else:
            cls_items_filtered.append(item)

    if filtered_count > 0:
        print(f"   🛡️ [去重拦截] 发现 {filtered_count} 条财联社新闻已在东财中播报过，自动拦截过滤。")

    all_news_items.extend(cls_items_filtered)

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
    
    print(f"✅ 入库完成: 新增 {new_count} 条 (EM:{len(em_items)} | CLS:{len(cls_items_filtered)})")

if __name__ == "__main__":
    fetch_and_save_news()
