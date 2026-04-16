# DCA 策略回測與跨市場估值比較工具

比較三大股指（S&P 500、日經 225、台灣加權指數）的四種定期定額策略績效，並提供跨市場 PE / PB / 殖利率估值視覺化分析。

---

## 專案架構

```
fintech/
│
├── DCA.py                          # 核心：四種 DCA 策略回測引擎（881 行）
├── taiex_history.py                # 爬蟲：Yahoo Finance 台股指數歷史資料（Selenium）
├── index(N255).py                  # 爬蟲：日經官網 PER / PBR / 殖利率
├── index(SP500).py                 # 爬蟲：multpl.com S&P 500 PE / PB / 殖利率
├── macromicro_pe_spider.py         # 爬蟲：MacroMicro 台股估值指標（Playwright）
│
├── data/                           # 回測用每日股價資料
│   ├── SP500_daily_raw.csv         #   14,191 筆，1970–2026
│   ├── Nikkei225_daily_raw.csv     #   13,838 筆，1970–2026
│   └── TAIEX_history.csv           #    7,043 筆，1997–2026
│
├── image_指標/                      # 估值指標資料與圖表
│   ├── compare_charts.py           #   三合一 PE / PB / 殖利率比較圖
│   ├── compare_charts_separate.py  #   分開繪製＋重大事件標註
│   ├── TAIEX_fundamental_raw.csv   #   台股月度 PE / PB / 殖利率
│   ├── sp500_combined_metrics.csv  #   S&P 500 半月頻率估值
│   ├── nikkei_historical_data.csv  #   日經日頻估值
│   ├── valuation_summary.csv       #   統計摘要與百分位排名
│   └── comparison_*.png            #   產出圖表
│
└── dca_output/                     # 回測產出（67 檔）
    ├── all_strategy_summary.csv    #   12 組回測總覽
    ├── comparison/                 #   策略 × 資產矩陣（報酬率 / XIRR / 最終價值）
    ├── {指數名稱}/                  #   各指數的逐月明細、時間序列、月報酬
    │   ├── Traditional_DCA/
    │   ├── Fixed_Share/
    │   ├── Value_Averaging/
    │   └── Variable_DCA/
    └── same_strategy_comparison/   #   同策略跨資產比較圖
```

### 三階段處理管線

```
階段一：資料收集（獨立爬蟲，互不依賴）
  taiex_history.py   ──→  data/TAIEX_history.csv
  index(N255).py     ──→  image_指標/nikkei_historical_data.csv
  index(SP500).py    ──→  S&P 500 估值 CSV
  macromicro_pe_spider.py ──→  台股 PE / PB / 殖利率

階段二：策略回測
  DCA.py（讀取 data/*.csv）──→ dca_output/（67 檔 CSV ＋ 圖表）

階段三：估值視覺化
  compare_charts*.py（讀取 image_指標/*.csv）──→ 比較圖表
```

---

## 四種 DCA 策略說明

| 策略 | 邏輯 | 特性 |
|------|------|------|
| **Traditional DCA** | 每月固定金額買入 | 最簡單的基準策略；投入金額固定，買入股數隨價格浮動 |
| **Fixed Share** | 每月固定股數買入 | 以首月價格計算基準股數；投入金額隨價格浮動，低買高買金額不同 |
| **Value Averaging** | 目標組合價值每月固定成長 | 低檔加碼、高檔減碼（預設禁止賣出），具自動擇時效果 |
| **Variable DCA** | 價格跌 >10% 投 1.5 倍、漲 >10% 投 0.5 倍 | 簡單的逆向操作規則，下跌加碼、上漲減碼 |

---

## 程式碼重點介紹

### `DCA.py` — 回測引擎

**資料結構：**
- `StrategySummary`（dataclass）：封裝單次回測的所有統計量，包含總投入、持有股數、最終價值、損益、總報酬率、CAGR、XIRR、平均成本。

**核心函式：**

| 函式 | 用途 |
|------|------|
| `validate_and_prepare_dataframe()` | 清洗原始 CSV、統一欄位名稱、自動選取 Adj_Close 或 Close |
| `get_monthly_first_trading_days()` | 將日頻資料重取樣為月頻（每月第一個交易日） |
| `compute_xirr()` | 以二分法求解 XNPV = 0 的折現率，計算內部報酬率（XIRR） |
| `build_dca_timeseries()` | 根據每月買入點重建每日組合淨值時間序列 |
| `run_all_strategies_for_one_csv()` | 對單一指數檔案執行全部四種策略 |
| `save_global_strategy_comparison()` | 產生策略 × 資產的報酬率 / XIRR / 最終價值矩陣 |
| `save_same_strategy_cross_asset_comparison()` | 同策略跨三個市場的比較圖（四類圖表） |

**回測參數：**
- 基礎月投金額：10,000
- 回測期間：2005-09-02 ～ 2026-04-13
- Value Averaging 預設 `allow_sell=False`（不賣出）

### `taiex_history.py` — Yahoo Finance 爬蟲

- 使用 **Selenium + Headless Chrome** 模擬瀏覽器操作
- 反偵測措施：自訂 User-Agent、關閉 automation flag
- 自動點擊「Show more」展開完整歷史資料
- 處理 Cookie 彈窗
- 資料驗證：檢查日期順序、重複項、OHLC 一致性
- 最多重試 3 次，指數退避間隔

### `index(N255).py` — 日經估值爬蟲

- 純 `urllib.request`，不需瀏覽器自動化
- 存取日經隱藏 API：`/nkave/statistics/dataload?list={指標}&year={年}&month={月}`
- 逐月遍歷，輸出「簡單平均」與「指數加權」兩種版本的 PER / PBR / 殖利率
- 每次請求間隔 0.5 秒（禮貌延遲）

### `index(SP500).py` — S&P 500 估值爬蟲

- 同樣使用 `urllib.request`
- 從 multpl.com 頁面 JavaScript 變數 `let pi = [...]` 提取時間戳＋數值
- 過濾 2000 年以後資料，分別輸出 PE / PB / 殖利率 CSV

### `macromicro_pe_spider.py` — MacroMicro 爬蟲

- 使用 **Playwright + Microsoft Edge**
- MacroMicro 的 API 需要 JWT 驗證，此腳本透過渲染真實頁面、從 Highcharts 圖表物件提取資料來繞過
- 反偵測：遮蔽 `navigator.webdriver`、自訂 User-Agent
- 依關鍵字或數值範圍比對辨識正確的資料序列
- 日頻價格資料重取樣為月頻

### `compare_charts_separate.py` — 估值比較圖（含事件標註）

- 三張獨立高解析度圖表（PE / PB / 殖利率各一）
- 標註重大市場事件：
  - 2007 次貸危機、2008 雷曼兄弟、2011 日本 311 地震
  - 2015 中國股災、2020 COVID-19、2022 升息／烏俄戰爭、2024 AI 熱潮
- 所有資料對齊為月頻後繪製

---

## 產出說明

`dca_output/` 包含 67 個檔案：

| 類型 | 數量 | 說明 |
|------|------|------|
| 逐月明細 CSV | 12 | 3 指數 × 4 策略，每月買入股數、金額、成本 |
| 每日時間序列 CSV | 12 | 完整每日組合淨值追蹤 |
| 月報酬率 CSV | 12 | 逐月報酬百分比 |
| 跨資產比較 CSV | 4 | 每種策略跨三市場的摘要 |
| 總覽 CSV | 1 | `all_strategy_summary.csv`，12 組回測結果總表 |
| 策略矩陣 CSV | 3 | 報酬率、XIRR、最終價值的策略 × 資產矩陣 |
| 比較圖 PNG | 23 | 各指數策略比較圖、同策略跨資產比較圖 |

---

## 依賴套件

```
pandas
numpy
matplotlib
selenium
playwright
beautifulsoup4
lxml
python-dateutil
```

瀏覽器驅動：
- Selenium 需安裝 ChromeDriver
- Playwright 需執行 `playwright install`

---

## 使用方式

**執行回測：**
```bash
python DCA.py
```

**更新台股歷史資料：**
```bash
python taiex_history.py
```

**更新估值指標：**
```bash
python index\(N255\).py      # 日經
python index\(SP500\).py     # S&P 500
python macromicro_pe_spider.py  # 台股（MacroMicro）
```

**產出估值比較圖：**
```bash
cd image_指標
python compare_charts.py           # 三合一圖
python compare_charts_separate.py  # 分開圖＋事件標註
```
