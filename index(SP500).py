import urllib.request
import re
import json
import csv
from datetime import datetime, timedelta

def crawl_multpl_data(metric_name, url):
    """
    通用的爬蟲函式，負責抓取 multpl.com 的歷史數據並輸出 CSV
    """
    # 建立請求，加入 User-Agent 模擬一般瀏覽器
    req = urllib.request.Request(
        url, 
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    )
    
    print(f"正在擷取 [{metric_name}] 的網頁資料...")
    try:
        with urllib.request.urlopen(req) as response:
            html = response.read().decode('utf-8')
    except Exception as e:
        print(f"[{metric_name}] 網頁請求失敗: {e}")
        return

    # 使用正規表達式尋找 JavaScript 中的變數 'let pi = [...]'
    match = re.search(r'let pi = (\[\[.*?\], \[.*?\].*?\]);', html)
    if not match:
        print(f"[{metric_name}] 無法在原始碼中找到目標資料結構。")
        return
        
    data_str = match.group(1)
    
    # 轉為 Python List
    data = json.loads(data_str)
    
    timestamps_in_days = data[0]
    values = data[1]
    
    epoch = datetime(1970, 1, 1)
    results = []
    
    # 資料配對與時間過濾
    for days, val in zip(timestamps_in_days, values):
        current_date = epoch + timedelta(days=days)
        
        # 篩選條件：2020年(含)到現在
        if current_date.year >= 2000:
            results.append([current_date.strftime('%Y-%m-%d'), round(val, 2)])
            
    # 寫入 CSV 檔案，檔名動態帶入指標名稱
    filename = f'sp500_{metric_name}_2020_to_present.csv'
    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'Value'])
        writer.writerows(results)
        
    print(f"完成！[{metric_name}] 共篩選出 {len(results)} 筆資料，已儲存至 '{filename}'。\n")


if __name__ == '__main__':
    # 將所有要爬取的目標網址整理成字典 (Dictionary)，方便未來隨時新增或維護
    targets = {
        "pe_ratio": "https://www.multpl.com/s-p-500-pe-ratio",
        "price_to_book": "https://www.multpl.com/s-p-500-price-to-book",
        "dividend_yield": "https://www.multpl.com/s-p-500-dividend-yield"
    }
    
    # 透過迴圈依序執行爬蟲任務
    for name, url in targets.items():
        crawl_multpl_data(name, url)