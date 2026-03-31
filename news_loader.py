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
import difflib  # 🟢 用于计算文本相似度
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

# 🟢 深度丰富的：新闻价值评估词库
IMPORTANT_KEYWORDS = [
    # 宏观与核心机构
    "央行", "证监会", "发改委", "国务院", "财政部", "外汇局", "金融监管总局", "工信部", "商务部", "统计局", "美联储", "欧央行", "政治局", "国常会",
    # 货币与财政政策
    "降息", "降准", "LPR", "MLF", "逆回购", "减税", "免税", "万亿", "千亿", "特别国债", "专项债",
    # 市场情绪与极值
    "重磅", "突破", "新规", "反垄断", "暴涨", "暴跌", "涨停", "跌停", "地天板", "天地板", "历史新高", "历史新低", "熔断", "爆仓", "黑天鹅", "灰犀牛",
    # 重大企业与行业事件
    "停牌", "退市", "立案调查", "借壳", "资产重组", "举牌", "大额增持", "大额回购", "业绩大增", "扭亏为盈", "制裁", "关税",
    # 关键经济数据
    "CPI", "PPI", "PMI", "GDP", "非农", "社融", "外汇储备"
]

TRASH_KEYWORDS = [
    # 互动与水文
    "互动平台表示", "投资者提问", "董秘", "感谢您的关注", "感谢关注", "投资者关系活动", "调研纪要", "暂无计划", "不涉及相关业务", "传闻不实", "注意投资风险",
    # 例行公告与流程
    "例行", "正常波动", "无重大未披露", "异动公告", "交易异常波动", "补充质押", "解质押", "质押延期", "减持计划", "集中竞价减持",
    "届董事会", "届监事会", "换届选举", "辞职报告", "聘任", "股东大会", "决议公告", "进展公告", "例行维护", "正常开展", "提示性公告",
    "工商变更", "变更注册地址", "修改公司章程", "完成注销", "核准"
]

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
# 🟢 核心优化 - 新闻分级与清洗模块
# ==========================================
def evaluate_and_clean_news(title, content):
    """
    根据关键词对新闻进行分级处理
    返回: (处理后的内容, 是否保留该条新闻)
    """
    full_text = f"{title} {content}"
    
    # 1. 判断是否为“垃圾/不重要”新闻（命中无用词且未命中重要词）
    is_trash = any(kw in full_text for kw in TRASH_KEYWORDS)
    is_important = any(kw in full_text for kw in IMPORTANT_KEYWORDS)
    
    if is_trash and not is_important:
        return "", False # 直接丢弃
        
    # 2. 判断是否为“重要”新闻
    if is_important:
        return content, True # 原样全量保留
        
    # 3. “一般”新闻进行精简（保留第一句话或截断）
    # 尝试按句号/感叹号切分，提取核心的首句
    sentences = re.split(r'([。！？!?])', content)
    if len(sentences) > 1:
        # 拼接第一句话和它的标点符号
        simplified_content = sentences[0] + sentences[1]
    else:
        # 如果没有明显标点，最多保留 60 个字符
        simplified_content = content[:60] + "..." if len(content) > 60 else content
        
    return simplified_content, True

def calculate_jaccard_similarity(text1, text2):
    """ 计算字符集合相似度，弥补 difflib 对语序敏感的缺陷 """
    set1, set2 = set(text1), set(text2)
    if not set1 or not set2: return 0.0
    return len(set1.intersection(set2)) / len(set1.union(set2))

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
                        # 🟢 移除 source 字段
                        items.append({
                            "time": show_time, "title": title, "content": content
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
                # 🟢 移除 source 字段
                items.append({
                    "time": public_time, "title": title, "content": content
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
                    
                    # 🟢 移除 source 字段
                    items.append({
                        "time": full_time, "title": final_title, "content": final_content
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
                    
                    # 🟢 移除 source 字段
                    items.append({
                        "time": full_time, "title": title, "content": content_text
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

    # 1. 东财 (抓取 + 评估清洗)
    raw_em_items = fetch_eastmoney()
    em_items = []
    discarded_em_count = 0
    for item in raw_em_items:
        new_content, keep = evaluate_and_clean_news(item['title'], item['content'])
        if keep:
            item['content'] = new_content
            em_items.append(item)
        else:
            discarded_em_count += 1
            
    if discarded_em_count > 0:
        print(f"   🗑️ [清洗] 剔除了 {discarded_em_count} 条东财低价值新闻。")

    all_news_items.extend(em_items)

    # 🟢 构建东财新闻纯净文本池与纯净标题池 (仅使用清洗后的高质量数据)
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
    
    # 2. 财联社 (抓取)
    cls_items_raw = fetch_cls()

    # 🟢 核心优化：清洗财联社数据 + 多维度拦截重复新闻
    cls_items_filtered = []
    filtered_duplicate_count = 0
    discarded_cls_count = 0
    
    for item in cls_items_raw:
        # 首先进行价值评估清洗
        new_content, keep = evaluate_and_clean_news(item['title'], item['content'])
        if not keep:
            discarded_cls_count += 1
            continue
            
        item['content'] = new_content
        
        cls_clean_text = re.sub(r'[^\w\u4e00-\u9fa5]', '', item.get('content', '') + item.get('title', ''))
        cls_clean_title = re.sub(r'[^\w\u4e00-\u9fa5]', '', item.get('title', ''))
        is_duplicate = False
        
        # 拦截层级 1：纯净标题双向包含匹配
        if len(cls_clean_title) >= 5:
            for em_title in em_clean_titles:
                if cls_clean_title in em_title or em_title in cls_clean_title:
                    is_duplicate = True
                    break

        # 拦截层级 2：纯净正文高精度比对 (结合 difflib 和 Jaccard)
        if not is_duplicate and len(cls_clean_text) >= 10:
            for em_text in em_clean_texts:
                if cls_clean_text in em_text or em_text in cls_clean_text:
                    is_duplicate = True
                    break
                
                # 双维度相似度计算：弥补单一算法的缺陷
                cls_prefix, em_prefix = cls_clean_text[:100], em_text[:100]
                sim_ratio = difflib.SequenceMatcher(None, cls_prefix, em_prefix).quick_ratio()
                jaccard_ratio = calculate_jaccard_similarity(cls_prefix, em_prefix)
                
                # 只要任意一种相似度达到阈值，就判定为重复
                if sim_ratio > 0.75 or jaccard_ratio > 0.7:
                    is_duplicate = True
                    break
                    
        if is_duplicate:
            filtered_duplicate_count += 1
        else:
            cls_items_filtered.append(item)

    if discarded_cls_count > 0:
        print(f"   🗑️ [清洗] 剔除了 {discarded_cls_count} 条财联社低价值新闻。")
    if filtered_duplicate_count > 0:
        print(f"   🛡️ [去重] 发现 {filtered_duplicate_count} 条财联社新闻已在东财中播报过，自动拦截过滤。")

    all_news_items.extend(cls_items_filtered)

    # 3. 入库
    if not all_news_items:
        print("⚠️ 未获取到任何有效新闻数据")
        return

    today_file = os.path.join(DATA_DIR, f"news_{today_date}.jsonl")
    existing_ids = set()
    
    # 🟢 由于本地不再存 id 字段，读取时动态生成历史数据的 id 以用于去重
    if os.path.exists(today_file):
        with open(today_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    saved_item = json.loads(line)
                    existing_ids.add(generate_news_id(saved_item))
                except: pass

    new_count = 0
    all_news_items.sort(key=lambda x: x['time'], reverse=True)

    with open(today_file, 'a', encoding='utf-8') as f:
        for item in all_news_items:
            # 🟢 在内存中动态计算 id 进行比对，不再将 id 写入 item 字典
            item_id = generate_news_id(item)
            if item_id not in existing_ids:
                # 确保字典内干净，直接序列化
                item.pop('source', None)
                item.pop('id', None)
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                existing_ids.add(item_id)
                new_count += 1
    
    print(f"✅ 入库完成: 新增 {new_count} 条 (EM:{len(em_items)} | CLS:{len(cls_items_filtered)})")

if __name__ == "__main__":
    fetch_and_save_news()
