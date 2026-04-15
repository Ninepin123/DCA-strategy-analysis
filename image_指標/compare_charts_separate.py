import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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
taiex = taiex.rename(columns={"PE_RATIO": "PE", "PB_RATIO": "PB", "DIVIDEND_YIELD": "DY"})
taiex["Market"] = "台股 TAIEX"
taiex = taiex.set_index("date")[["PE", "PB", "DY", "Market"]].copy()

# --- S&P 500 (半月資料) ---
sp = pd.read_csv(os.path.join(DATA_DIR, "sp500_combined_metrics.csv"))
sp.columns = sp.columns.str.strip()
sp["Date"] = pd.to_datetime(sp["Date"])
sp = sp.rename(columns={"PE Ratio": "PE", "Price-to-Book": "PB", "Dividend Yield": "DY"})
sp = sp.set_index("Date")
sp_monthly = sp.resample("MS").agg({"PE": "last", "PB": "last", "DY": "last"})
sp_monthly["Market"] = "S&P 500"

# --- Nikkei (日資料降頻為月) ---
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
# 2. 對齊共同期間
# ============================================================
start = pd.Timestamp("2004-09-01")
end   = pd.Timestamp("2026-03-01")
prange = pd.period_range(start, end, freq="M")

taiex_m = taiex[taiex.index.to_period("M").isin(prange)].copy()
sp_m    = sp_monthly[sp_monthly.index.to_period("M").isin(prange)].copy()
nk_m    = nk_monthly[nk_monthly.index.to_period("M").isin(prange)].copy()

taiex_m.index = taiex_m.index.to_period("M").to_timestamp()
sp_m.index    = sp_m.index.to_period("M").to_timestamp()
nk_m.index    = nk_m.index.to_period("M").to_timestamp()

combined = pd.concat([taiex_m, sp_m, nk_m])

col_order = ["台股 TAIEX", "日經 Nikkei", "S&P 500"]
pe_pivot = combined.pivot_table(columns="Market", values="PE", index=combined.index).reindex(columns=col_order)
pb_pivot = combined.pivot_table(columns="Market", values="PB", index=combined.index).reindex(columns=col_order)
dy_pivot = combined.pivot_table(columns="Market", values="DY", index=combined.index).reindex(columns=col_order)

# ============================================================
# 3. 重大事件標記
# ============================================================
events = [
    (pd.Timestamp("2007-10-01"), "2007/10\n次貸危機\n爆發"),
    (pd.Timestamp("2008-09-01"), "2008/09\n雷曼兄弟\n倒閉"),
    (pd.Timestamp("2011-03-01"), "2011/03\n311大地震\n（日經大跌）"),
    (pd.Timestamp("2015-06-01"), "2015/06\n中國股災"),
    (pd.Timestamp("2020-03-01"), "2020/03\nCOVID-19\n疫情崩盤"),
    (pd.Timestamp("2022-01-01"), "2022/01\nFed 升息\n俄烏戰爭"),
    (pd.Timestamp("2024-03-01"), "2024\nAI 熱潮\n推升估值"),
]

colors = {
    "台股 TAIEX": "#E63946",
    "日經 Nikkei": "#457B9D",
    "S&P 500":    "#2A9D8F",
}

metrics = [
    ("PE", "本益比 (Price-to-Earnings)", pe_pivot, "comparison_pe.png"),
    ("PB", "股價淨值比 (Price-to-Book)", pb_pivot, "comparison_pb.png"),
    ("DY", "現金殖利率 % (Dividend Yield)", dy_pivot, "comparison_dy.png"),
]

# ============================================================
# 4. 分別繪製三張圖
# ============================================================
for key, title, pivot, filename in metrics:
    fig, ax = plt.subplots(figsize=(18, 9), dpi=140)

    # 繪製三條線
    for col in col_order:
        ax.plot(pivot.index, pivot[col], label=col, color=colors[col],
                linewidth=1.6, alpha=0.9)

    ax.set_title(f"台股 / 日經 / S&P 500  —  {title}\n(2004/09 – 2026/03)",
                 fontsize=17, fontweight="bold", pad=14)

    ylabel_map = {"PE": "本益比", "PB": "股價淨值比", "DY": "殖利率 (%)"}
    ax.set_ylabel(ylabel_map[key], fontsize=13)
    ax.legend(loc="upper left", fontsize=12, framealpha=0.85)
    ax.grid(True, alpha=0.3, linestyle="--")

    # X 軸格式
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=1, interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.tick_params(axis="x", labelsize=10)

    # 標注最新值
    for col in col_order:
        series = pivot[col].dropna()
        if len(series) > 0:
            last_date = series.index[-1]
            last_val  = series.iloc[-1]
            ax.annotate(f"{last_val:.2f}",
                        xy=(last_date, last_val),
                        xytext=(12, 0), textcoords="offset points",
                        fontsize=11, fontweight="bold", color=colors[col],
                        va="center",
                        bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=colors[col], alpha=0.8))

    # 事件標記（只在主圖區畫淡色垂直線，文字放在下方 annotation 區）
    for ev_date, ev_label in events:
        ax.axvline(x=ev_date, color="gray", alpha=0.25, linewidth=1, linestyle=":")

    # ── 在圖表底部放事件標籤 ──
    # 把事件文字交錯放在 y 軸底部，避免重疊
    ymin, ymax = ax.get_ylim()
    text_y_offsets = [0.01, 0.06, 0.01, 0.06, 0.01, 0.06, 0.01]  # 交錯高度

    for i, (ev_date, ev_label) in enumerate(events):
        # 只在日期範圍內的事件才標
        if ev_date < pivot.index.min() or ev_date > pivot.index.max():
            continue
        ax.annotate(
            ev_label,
            xy=(ev_date, ymin),
            xytext=(0, 12 + (i % 2) * 22),
            textcoords="offset points",
            fontsize=7.5,
            color="#555555",
            ha="center", va="bottom",
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", fc="#FFFDE7", ec="#BDBDBD", alpha=0.85),
            arrowprops=dict(arrowstyle="-", color="#BDBDBD", lw=0.8),
        )

    # 加一點底部留白讓事件標籤不會被切掉
    ax.set_ylim(bottom=ax.get_ylim()[0])

    plt.tight_layout()
    out_path = os.path.join(DATA_DIR, filename)
    fig.savefig(out_path, bbox_inches="tight")
    print(f"[OK] 已儲存: {out_path}")
    plt.close()

print("\n Done! 三張圖已分別儲存。")
