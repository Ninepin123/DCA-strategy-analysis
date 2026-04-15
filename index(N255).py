import urllib.request
import urllib.error
import re
import csv
import time
from datetime import datetime

def fetch_nikkei_data():
    # 定義要爬取的指標和時間範圍
    metrics = ['dividend', 'per', 'pbr']
    start_year = 2000
    current_year = datetime.now().year
    years = range(start_year, current_year + 1)
    months = range(1, 13)

    # 建立一個字典來統整所有資料，格式：{ "YYYY-MM-DD": {"dividend": [val1, val2], "per": [...], "pbr": [...]} }
    master_data = {}

    # 設定 User-Agent，避免被伺服器阻擋
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    print("開始執行爬蟲，這可能需要幾分鐘的時間以避免頻繁請求...")

    for metric in metrics:
        print(f"\n--- 正在擷取指標: {metric.upper()} ---")
        for year in years:
            for month in months:
                # 組合隱藏的 API URL
                url = f"https://indexes.nikkei.co.jp/en/nkave/statistics/dataload?list={metric}&year={year}&month={month}"
                req = urllib.request.Request(url, headers=headers)
                
                try:
                    with urllib.request.urlopen(req) as response:
                        html = response.read().decode('utf-8')

                        # 使用正則表達式找出 <tbody> 裡面的所有 <tr> (過濾掉表頭)
                        tbody_match = re.search(r'<tbody>(.*?)</tbody>', html, re.DOTALL)
                        if not tbody_match:
                            continue # 如果這個月沒有資料(例如未來月份)，就跳過

                        # 抓出每一列 <tr>
                        rows = re.findall(r'<tr>(.*?)</tr>', tbody_match.group(1), re.DOTALL)
                        
                        for row in rows:
                            # 抓出每一個儲存格 <td>
                            tds = re.findall(r'<td>(.*?)</td>', row, re.DOTALL)
                            if tds:
                                # 清除可能的空白字元
                                date_str = tds[0].strip()
                                
                                # 將 "Apr/01/2026" 轉換為標準的 "2026-04-01" 格式，方便排序
                                try:
                                    date_obj = datetime.strptime(date_str, '%b/%d/%Y')
                                    iso_date = date_obj.strftime('%Y-%m-%d')
                                except ValueError:
                                    iso_date = date_str

                                # 將該日期的資料存入字典
                                if iso_date not in master_data:
                                    master_data[iso_date] = {}
                                
                                # 儲存除了日期以外的數值 (通常會有兩欄：Simple Average, Index Weight Basis)
                                master_data[iso_date][metric] = [td.strip() for td in tds[1:]]

                    # 禮貌性延遲，避免對伺服器造成壓力被 Ban IP (0.5秒)
                    time.sleep(0.5)

                except urllib.error.HTTPError as e:
                    print(f"無法取得 {year} 年 {month} 月資料: HTTP {e.code}")
                except Exception as e:
                    print(f"發生錯誤 {url}: {e}")

    # 準備寫入 CSV
    output_filename = "nikkei_historical_data.csv"
    print(f"\n資料擷取完畢，準備寫入 {output_filename}...")

    with open(output_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # 寫入標題列 (假設每個指標都有兩個數據欄位)
        header = [
            'Date', 
            'Dividend_Simple_Average(%)', 'Dividend_Index_Weight_Basis(%)',
            'PER_Simple_Average', 'PER_Index_Weight_Basis',
            'PBR_Simple_Average', 'PBR_Index_Weight_Basis'
        ]
        writer.writerow(header)

        # 根據日期進行排序，確保輸出的時間軸是連續的
        for date in sorted(master_data.keys()):
            row = [date]
            for metric in metrics:
                # 如果該日期某個指標剛好沒資料，就填入空字串
                vals = master_data[date].get(metric, ['', ''])
                
                # 確保長度一致 (補足2個空位)
                while len(vals) < 2:
                    vals.append('')
                
                row.extend(vals[:2])
            
            writer.writerow(row)

    print("✅ CSV 檔案輸出成功！")

# 執行主程式
if __name__ == "__main__":
    fetch_nikkei_data()