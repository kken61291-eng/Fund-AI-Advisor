# ... (前面的 import 保持不变) ...

class NewsAnalyst:
    # ... (__init__ 保持不变) ...

    def get_market_context(self, max_length=15000):
        """
        [核心修改] 获取全天候市场舆情
        策略: 本地积攒的全量新闻 + 实时抓取的最新突发
        """
        # 1. 读取本地积攒的“旧闻” (Base Context)
        today_str = datetime.now().strftime("%Y-%m-%d") # 注意时区
        file_path = f"data_news/news_{today_str}.jsonl"
        
        news_list = []
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        news_list.append(json.loads(line))
                    except: pass
        
        # 2. [补丁] 现场抓一次最新的，防止漏掉最近1小时
        try:
            # 复用之前的实时抓取逻辑，只取最新的5条
            live_news = self._fetch_eastmoney_news() # 返回的是字符串列表
            # 格式化一下
            for n in live_news:
                news_list.append({"time": "刚刚", "title": n, "source": "Live"})
        except: pass

        # 3. 格式化为大文本
        # 倒序排列，优先展示最新的
        # news_list.sort(key=lambda x: x['time'], reverse=True) 
        
        full_text = ""
        for n in news_list:
            # 格式: [14:30] 标题...
            line = f"[{str(n['time'])[5:16]}] {n['title']}\n"
            full_text += line
        
        # 4. 截断保护 (虽然V3支持长文本，但为了省钱和速度，还是要做个上限)
        # 15000字符大约对应 7k-10k tokens，足够R1推理了
        if len(full_text) > max_length:
            return full_text[:max_length] + "\n...(历史消息已截断)"
        
        return full_text if full_text else "今日暂无重大新闻。"

    # ... (analyze_fund_v5 保持不变，但调用方式变了) ...
