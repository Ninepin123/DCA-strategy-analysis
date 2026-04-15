import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.font_manager import FontProperties
import numpy as np
import os

# ── 中文字型設定 ──
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False

DATA_DIR = r"C:\Users\User\Desktop\fintech\image_指標"

# ============================================================
# 1. 讀取 & 清理資料
# ============================================================

# --- TAIEX (月資料) ---
taiex = pd.read_csv(os.path.join(DATA_DIR, "TAIEX_fundamental_raw.csv"))
taiex.columns = taiex.columns.str.strip()
taiex["date"] = pd.to_datetime(taiex["date"])
taiex = taiex.rename(columns={
    "PE_RATIO": "PE", "PB_RATIO": "PB", "DIVIDEND_YIELD": "DY"
})
taiex["Market"] = "台股 TAIEX"
taiex = taiex.set_index("date")[["PE", "PB", "DY", "Market"]].copy()

# --- S&P 500 (半月資料，PE / PB / DY 分散在不同列) ---
sp = pd.read_csv(os.path.join(DATA_DIR, "sp500_combined_metrics.csv"))
sp.columns = sp.columns.str.strip()
sp["Date"] = pd.to_datetime(sp["Date"])
sp = sp.rename(columns={
    "PE Ratio": "PE", "Price-to-Book": "PB", "Dividend Yield": "DY"
})
sp = sp.set_index("Date")
# 合併同一月份的 PE / PB / DY (同月內取最後一筆非空值)
sp_monthly = sp.resample("MS").agg({"PE": "last", "PB": "last", "DY": "last"})
sp_monthly["Market"] = "S&P 500"

# --- Nikkei (日資料，降頻為月) ---
nk = pd.read_csv(os.path.join(DATA_DIR, "nikkei_historical_data.csv"))
nk.columns = nk.columns.str.strip()
nk["Date"] = pd.to_datetime(nk["Date"])
nk = nk.rename(columns={
    "PER_Index_Weight_Basis": "PE",
    "PBR_Index_Weight_Basis": "PB",
    "Dividend_Index_Weight_Basis(%)": "DY",
})
nk["PE"] = pd.to_numeric(nk["PE"], errors="coerce")
nk["PB"] = pd.to_numeric(nk["PB"], errors="coerce")
nk["DY"] = pd.to_numeric(nk["DY"], errors="coerce")
nk = nk.set_index("Date")
nk_monthly = nk.resample("MS").agg({"PE": "last", "PB": "last", "DY": "last"})
nk_monthly["Market"] = "日經 Nikkei"

# ============================================================
# 2. 對齊共同期間 & 合併
# ============================================================
start = pd.Timestamp("2004-09-01")
end   = pd.Timestamp("2026-03-01")

taiex_m = taiex[taiex.index.to_period("M").isin(pd.period_range(start, end, freq="M"))]
sp_m    = sp_monthly[sp_monthly.index.to_period("M").isin(pd.period_range(start, end, freq="M"))]
nk_m    = nk_monthly[nk_monthly.index.to_period("M").isin(pd.period_range(start, end, freq="M"))]

# 統一 index 為月份第一天，方便對齊
taiex_m.index = taiex_m.index.to_period("M").to_timestamp()
sp_m.index    = sp_m.index.to_period("M").to_timestamp()
nk_m.index    = nk_m.index.to_period("M").to_timestamp()

combined = pd.concat([taiex_m, sp_m, nk_m])

# 樞紐表
pe_pivot = combined.pivot_table(columns="Market", values="PE", index=combined.index)
pb_pivot = combined.pivot_table(columns="Market", values="PB", index=combined.index)
dy_pivot = combined.pivot_table(columns="Market", values="DY", index=combined.index)

# 排序欄位順序
col_order = ["台股 TAIEX", "日經 Nikkei", "S&P 500"]
pe_pivot = pe_pivot.reindex(columns=col_order)
pb_pivot = pb_pivot.reindex(columns=col_order)
dy_pivot = dy_pivot.reindex(columns=col_order)

# ============================================================
# 3. 輔助統計（歷史百分位 & 當前值）
# ============================================================
stats = {}
for name, pivot in [("PE", pe_pivot), ("PB", pb_pivot), ("DY", dy_pivot)]:
    stats[name] = pd.DataFrame({
        "平均": pivot.mean(),
        "中位數": pivot.median(),
        "最小值": pivot.min(),
        "最大值": pivot.max(),
        "標準差": pivot.std(),
    })

# 取最新一筆非空值作為「當前值」
latest_vals = {}
for name, pivot in [("PE", pe_pivot), ("PB", pb_pivot), ("DY", dy_pivot)]:
    latest_vals[name] = {}
    for col in pivot.columns:
        series = pivot[col].dropna()
        if len(series) > 0:
            latest_vals[name][col] = series.iloc[-1]

# ============================================================
# 4. 繪圖
# ============================================================
colors = {
    "台股 TAIEX": "#E63946",
    "日經 Nikkei": "#457B9D",
    "S&P 500":    "#2A9D8F",
}

fig, axes = plt.subplots(3, 1, figsize=(16, 18), dpi=130)
fig.suptitle("台股 / 日經 / S&P 500  基本面指標比較\n(2004/09 – 2026/03)",
             fontsize=18, fontweight="bold", y=0.97)

metrics = [
    ("PE", "本益比 (Price-to-Earnings)", pe_pivot),
    ("PB", "股價淨值比 (Price-to-Book)", pb_pivot),
    ("DY", "現金殖利率 % (Dividend Yield)", dy_pivot),
]

for ax, (key, title, pivot) in zip(axes, metrics):
    for col in col_order:
        ax.plot(pivot.index, pivot[col], label=col, color=colors[col],
                linewidth=1.3, alpha=0.9)

    ax.set_title(title, fontsize=14, fontweight="bold", pad=10)
    ax.set_ylabel(title.split("(")[0].strip(), fontsize=11)
    ax.legend(loc="upper left", fontsize=10, framealpha=0.85)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.tick_params(axis="x", rotation=0)

    # X 軸：每 2 年顯示一次
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=1, interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    # 標注最新值
    for col in col_order:
        series = pivot[col].dropna()
        if len(series) > 0:
            last_date = series.index[-1]
            last_val  = series.iloc[-1]
            ax.annotate(f"{last_val:.2f}",
                        xy=(last_date, last_val),
                        xytext=(10, 0), textcoords="offset points",
                        fontsize=9, fontweight="bold", color=colors[col],
                        va="center")

plt.tight_layout(rect=[0, 0, 1, 0.95])
out_path = os.path.join(DATA_DIR, "comparison_pe_pb_dy.png")
fig.savefig(out_path, bbox_inches="tight")
print(f"[OK] 比較圖已儲存: {out_path}")
plt.close()

# ============================================================
# 5. 歷史百分位長條圖（當前值 vs 歷史百分位）
# ============================================================
fig2, axes2 = plt.subplots(1, 3, figsize=(18, 6), dpi=130)
fig2.suptitle("當前估值指標的歷史百分位排名\n(相對於 2004/09 – 2026/03 自身歷史)",
              fontsize=16, fontweight="bold", y=1.03)

for ax, (key, title, pivot) in zip(axes2, metrics):
    pcts = {}
    for col in col_order:
        series = pivot[col].dropna()
        if len(series) > 0:
            val = series.iloc[-1]
            pct = (series < val).sum() / len(series) * 100
            pcts[col] = pct

    bars = ax.bar(pcts.keys(), pcts.values(),
                  color=[colors[c] for c in pcts.keys()],
                  width=0.55, edgecolor="white", linewidth=1.5)

    # 在長條上方標注百分位數值 & 當前值
    for bar, col in zip(bars, pcts.keys()):
        val = latest_vals[key][col]
        pct = pcts[col]
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1.5,
                f"{pct:.0f}%\n({val:.2f})",
                ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("歷史百分位 (%)", fontsize=11)
    ax.set_ylim(0, 110)
    ax.axhline(y=50, color="gray", linestyle="--", alpha=0.5, linewidth=1)
    ax.text(0.98, 0.52, "中位數", transform=ax.transAxes,
            ha="right", fontsize=9, color="gray", alpha=0.7)
    ax.grid(axis="y", alpha=0.2, linestyle="--")

plt.tight_layout()
out_path2 = os.path.join(DATA_DIR, "percentile_ranking.png")
fig2.savefig(out_path2, bbox_inches="tight")
print(f"[OK] 百分位圖已儲存: {out_path2}")
plt.close()

# ============================================================
# 6. 匯出統計摘要 CSV
# ============================================================
summary_rows = []
for key, title, pivot in metrics:
    for col in col_order:
        series = pivot[col].dropna()
        if len(series) > 0:
            val = series.iloc[-1]
            pct = (series < val).sum() / len(series) * 100
            summary_rows.append({
                "指標": key,
                "市場": col,
                "當前值": round(val, 2),
                "歷史平均": round(series.mean(), 2),
                "歷史中位數": round(series.median(), 2),
                "最小值": round(series.min(), 2),
                "最大值": round(series.max(), 2),
                "標準差": round(series.std(), 2),
                "歷史百分位(%)": round(pct, 1),
            })

summary_df = pd.DataFrame(summary_rows)
summary_path = os.path.join(DATA_DIR, "valuation_summary.csv")
summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
print(f"[OK] 統計摘要已儲存: {summary_path}")

print("\n" + "="*60)
print("統計摘要")
print("="*60)
print(summary_df.to_string(index=False))
