import os
import glob
import math
from dataclasses import dataclass, asdict
from typing import Optional, List, Tuple, Dict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# =========================
# Matplotlib 基本設定
# =========================
plt.rcParams["figure.dpi"] = 140
plt.rcParams["savefig.dpi"] = 140
plt.rcParams["axes.unicode_minus"] = False
# 若你電腦顯示中文亂碼，可嘗試取消下面註解並改成你系統有的字型
# plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei"]
# plt.rcParams["font.family"] = "sans-serif"


# =========================
# 參數設定
# =========================
DEFAULT_MONTHLY_INVESTMENT = 10000.0
USE_ADJ_CLOSE_FIRST = True
ROUND_SHARES = False
SHARE_DECIMALS = 6
EXPORT_RESULTS = True
OUTPUT_DIR = "dca_output"

# 共同回測期間
BACKTEST_START_DATE = "2005-09-02"
BACKTEST_END_DATE = "2024-12-31"   # 不想設截止日可改成 None


# =========================
# 策略名稱
# =========================
STRATEGY_TRADITIONAL = "Traditional_DCA"
STRATEGY_FIXED_SHARE = "Fixed_Share"
STRATEGY_VALUE_AVERAGING = "Value_Averaging"
STRATEGY_VARIABLE_DCA = "Variable_DCA"

ALL_STRATEGIES = [
    STRATEGY_TRADITIONAL,
    STRATEGY_FIXED_SHARE,
    STRATEGY_VALUE_AVERAGING,
    STRATEGY_VARIABLE_DCA,
]


# =========================
# 資料結構
# =========================
@dataclass
class StrategySummary:
    file_name: str
    symbol_name: str
    strategy_name: str
    start_date: str
    end_date: str
    first_invest_date: str
    last_invest_date: str
    total_months_invested: int
    base_monthly_investment: float
    total_invested: float
    total_units: float
    final_price: float
    final_value: float
    profit_loss: float
    total_return_pct: float
    annualized_return_cagr_pct: Optional[float]
    xirr_pct: Optional[float]
    avg_cost: float
    price_column_used: str


# =========================
# 基本工具
# =========================
def ensure_output_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def to_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce"
    )


def validate_and_prepare_dataframe(
    df: pd.DataFrame,
    prefer_adj_close: bool = True
) -> Tuple[pd.DataFrame, str]:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "Date" not in df.columns:
        raise ValueError("缺少必要欄位：Date")

    # 同時支援 Adj Close 與 Adj_Close
    df = df.rename(columns={"Adj Close": "Adj_Close"})

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])

    numeric_cols = ["Open", "High", "Low", "Close", "Adj_Close", "Volume"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = to_numeric_series(df[col])

    price_col = None
    if prefer_adj_close and "Adj_Close" in df.columns and df["Adj_Close"].notna().sum() > 0:
        price_col = "Adj_Close"
    elif "Close" in df.columns and df["Close"].notna().sum() > 0:
        price_col = "Close"

    if price_col is None:
        raise ValueError("找不到可用價格欄位：Adj_Close 或 Close")

    df = df.dropna(subset=[price_col])
    df = df[df[price_col] > 0]
    df = df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)

    if df.empty:
        raise ValueError("清理後資料為空")

    return df, price_col


def get_monthly_first_trading_days(df: pd.DataFrame) -> pd.DataFrame:
    temp = df.copy()
    temp["YearMonth"] = temp["Date"].dt.to_period("M")
    out = temp.groupby("YearMonth", as_index=False).first()
    out = out.drop(columns=["YearMonth"])
    return out


def year_fraction(start_date: pd.Timestamp, end_date: pd.Timestamp) -> float:
    return (end_date - start_date).days / 365.2425


def compute_cagr(
    total_invested: float,
    final_value: float,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp
) -> Optional[float]:
    if total_invested <= 0 or final_value <= 0:
        return None
    yf = year_fraction(start_date, end_date)
    if yf <= 0:
        return None
    return (final_value / total_invested) ** (1 / yf) - 1


def xnpv(rate: float, cashflows: List[Tuple[pd.Timestamp, float]]) -> float:
    if rate <= -0.999999999:
        return np.inf
    t0 = cashflows[0][0]
    total = 0.0
    for dt, cf in cashflows:
        years = (dt - t0).days / 365.2425
        total += cf / ((1 + rate) ** years)
    return total


def compute_xirr(cashflows: List[Tuple[pd.Timestamp, float]]) -> Optional[float]:
    if len(cashflows) < 2:
        return None

    vals = [cf for _, cf in cashflows]
    if not (any(v < 0 for v in vals) and any(v > 0 for v in vals)):
        return None

    low = -0.9999
    high = 10.0

    f_low = xnpv(low, cashflows)
    f_high = xnpv(high, cashflows)

    expand_count = 0
    while f_low * f_high > 0 and expand_count < 50:
        high *= 2
        f_high = xnpv(high, cashflows)
        expand_count += 1
        if high > 1e6:
            break

    if f_low * f_high > 0:
        return None

    for _ in range(200):
        mid = (low + high) / 2
        f_mid = xnpv(mid, cashflows)

        if abs(f_mid) < 1e-10:
            return mid

        if f_low * f_mid <= 0:
            high = mid
        else:
            low = mid
            f_low = f_mid

    return (low + high) / 2


# =========================
# 建立策略明細
# =========================
def build_traditional_dca_plan(
    monthly_points: pd.DataFrame,
    monthly_investment: float,
    round_shares: bool,
    share_decimals: int
) -> pd.DataFrame:
    out = monthly_points.copy()
    out["InvestAmount"] = float(monthly_investment)
    out["BuyPrice"] = out["Price"].astype(float)

    if round_shares:
        out["UnitsBought"] = np.floor(out["InvestAmount"] / out["BuyPrice"])
        out["CashUsed"] = out["UnitsBought"] * out["BuyPrice"]
        out["CashRemainder"] = out["InvestAmount"] - out["CashUsed"]
    else:
        out["UnitsBought"] = (out["InvestAmount"] / out["BuyPrice"]).round(share_decimals)
        out["CashUsed"] = out["InvestAmount"]
        out["CashRemainder"] = 0.0

    return out


def build_fixed_share_plan(
    monthly_points: pd.DataFrame,
    monthly_investment: float,
    round_shares: bool,
    share_decimals: int
) -> pd.DataFrame:
    out = monthly_points.copy()
    out["BuyPrice"] = out["Price"].astype(float)

    first_price = float(out.iloc[0]["BuyPrice"])
    fixed_units = monthly_investment / first_price

    if round_shares:
        fixed_units = math.floor(fixed_units)
    else:
        fixed_units = round(fixed_units, share_decimals)

    out["UnitsBought"] = fixed_units
    out["CashUsed"] = out["UnitsBought"] * out["BuyPrice"]
    out["InvestAmount"] = out["CashUsed"]
    out["CashRemainder"] = 0.0
    return out


def build_value_averaging_plan(
    monthly_points: pd.DataFrame,
    monthly_investment: float,
    round_shares: bool,
    share_decimals: int,
    allow_sell: bool = False
) -> pd.DataFrame:
    out = monthly_points.copy()
    out["BuyPrice"] = out["Price"].astype(float)

    records = []
    cum_units = 0.0

    for idx, row in out.iterrows():
        month_idx = idx + 1
        price = float(row["BuyPrice"])
        target_value = month_idx * monthly_investment
        current_value = cum_units * price
        invest_amount = target_value - current_value

        if not allow_sell:
            invest_amount = max(0.0, invest_amount)

        if round_shares:
            if invest_amount >= 0:
                units_bought = np.floor(invest_amount / price)
            else:
                units_bought = -np.floor(abs(invest_amount) / price)
            cash_used = units_bought * price
        else:
            units_bought = round(invest_amount / price, share_decimals) if price > 0 else 0.0
            cash_used = units_bought * price

        cum_units += units_bought

        records.append({
            "Date": row["Date"],
            "BuyPrice": price,
            "TargetValue": target_value,
            "CurrentValueBeforeTrade": current_value,
            "InvestAmount": invest_amount,
            "UnitsBought": units_bought,
            "CashUsed": cash_used,
            "CashRemainder": invest_amount - cash_used
        })

    return pd.DataFrame(records)


def build_variable_dca_plan(
    monthly_points: pd.DataFrame,
    monthly_investment: float,
    round_shares: bool,
    share_decimals: int
) -> pd.DataFrame:
    out = monthly_points.copy()
    out["BuyPrice"] = out["Price"].astype(float)

    invest_amounts = []
    prev_price = None

    for _, row in out.iterrows():
        price = float(row["BuyPrice"])

        if prev_price is None:
            invest_amount = monthly_investment
        else:
            change_pct = (price / prev_price - 1) * 100
            if change_pct <= -10:
                invest_amount = monthly_investment * 1.5
            elif change_pct >= 10:
                invest_amount = monthly_investment * 0.5
            else:
                invest_amount = monthly_investment

        invest_amounts.append(invest_amount)
        prev_price = price

    out["InvestAmount"] = invest_amounts

    if round_shares:
        out["UnitsBought"] = np.floor(out["InvestAmount"] / out["BuyPrice"])
        out["CashUsed"] = out["UnitsBought"] * out["BuyPrice"]
        out["CashRemainder"] = out["InvestAmount"] - out["CashUsed"]
    else:
        out["UnitsBought"] = (out["InvestAmount"] / out["BuyPrice"]).round(share_decimals)
        out["CashUsed"] = out["InvestAmount"]
        out["CashRemainder"] = 0.0

    return out


# =========================
# 建立 time series 與 summary
# =========================
def finalize_monthly_points(monthly_points: pd.DataFrame) -> pd.DataFrame:
    monthly_points = monthly_points.copy()
    monthly_points["CumUnits"] = monthly_points["UnitsBought"].cumsum()
    monthly_points["CumInvested"] = monthly_points["CashUsed"].cumsum()
    monthly_points["AvgCost"] = np.where(
        monthly_points["CumUnits"] > 0,
        monthly_points["CumInvested"] / monthly_points["CumUnits"],
        np.nan
    )
    return monthly_points


def build_dca_timeseries(
    df: pd.DataFrame,
    monthly_points: pd.DataFrame,
    price_col: str
) -> pd.DataFrame:
    ts = df[["Date", price_col]].copy().rename(columns={price_col: "MarketPrice"})
    invest_table = monthly_points[["Date", "UnitsBought", "CashUsed"]].copy()

    ts = ts.merge(invest_table, on="Date", how="left")
    ts["UnitsBought"] = ts["UnitsBought"].fillna(0.0)
    ts["CashUsed"] = ts["CashUsed"].fillna(0.0)

    ts["CumUnits"] = ts["UnitsBought"].cumsum()
    ts["CumInvested"] = ts["CashUsed"].cumsum()
    ts["PortfolioValue"] = ts["CumUnits"] * ts["MarketPrice"]
    ts["AvgCost"] = np.where(ts["CumUnits"] > 0, ts["CumInvested"] / ts["CumUnits"], np.nan)
    ts["ProfitLoss"] = ts["PortfolioValue"] - ts["CumInvested"]
    ts["ReturnPct"] = np.where(
        ts["CumInvested"] > 0,
        (ts["PortfolioValue"] / ts["CumInvested"] - 1) * 100,
        np.nan
    )
    return ts


def build_monthly_return_series(ts: pd.DataFrame) -> pd.DataFrame:
    temp = ts.copy()
    temp["Date"] = pd.to_datetime(temp["Date"])
    temp["YearMonth"] = temp["Date"].dt.to_period("M")
    monthly = temp.groupby("YearMonth", as_index=False).last()
    monthly = monthly[["Date", "ReturnPct"]].copy()
    monthly["Date"] = pd.to_datetime(monthly["Date"])
    return monthly


def build_summary(
    csv_path: str,
    symbol_name: str,
    strategy_name: str,
    df: pd.DataFrame,
    monthly_points: pd.DataFrame,
    ts: pd.DataFrame,
    price_col: str,
    monthly_investment: float
) -> StrategySummary:
    last_row = ts.iloc[-1]
    final_date = pd.to_datetime(last_row["Date"])
    final_price = float(last_row["MarketPrice"])
    total_units = float(monthly_points["UnitsBought"].sum())
    total_invested = float(monthly_points["CashUsed"].sum())
    final_value = float(last_row["PortfolioValue"])
    profit_loss = final_value - total_invested
    total_return_pct = (final_value / total_invested - 1) * 100 if total_invested > 0 else np.nan

    first_invest_date = pd.to_datetime(monthly_points.iloc[0]["Date"])
    cashflows = [(pd.to_datetime(row["Date"]), -float(row["CashUsed"])) for _, row in monthly_points.iterrows()]
    cashflows.append((final_date, final_value))

    xirr = compute_xirr(cashflows)
    cagr = compute_cagr(total_invested, final_value, first_invest_date, final_date)

    return StrategySummary(
        file_name=os.path.basename(csv_path),
        symbol_name=symbol_name,
        strategy_name=strategy_name,
        start_date=pd.to_datetime(df["Date"].iloc[0]).strftime("%Y-%m-%d"),
        end_date=pd.to_datetime(df["Date"].iloc[-1]).strftime("%Y-%m-%d"),
        first_invest_date=first_invest_date.strftime("%Y-%m-%d"),
        last_invest_date=pd.to_datetime(monthly_points.iloc[-1]["Date"]).strftime("%Y-%m-%d"),
        total_months_invested=int(len(monthly_points)),
        base_monthly_investment=float(monthly_investment),
        total_invested=float(round(total_invested, 6)),
        total_units=float(round(total_units, 6)),
        final_price=float(round(final_price, 6)),
        final_value=float(round(final_value, 6)),
        profit_loss=float(round(profit_loss, 6)),
        total_return_pct=float(round(total_return_pct, 6)),
        annualized_return_cagr_pct=None if cagr is None else float(round(cagr * 100, 6)),
        xirr_pct=None if xirr is None else float(round(xirr * 100, 6)),
        avg_cost=float(round(total_invested / total_units, 6)) if total_units > 0 else np.nan,
        price_column_used=price_col
    )


# =========================
# 單一 CSV 跑四種策略
# =========================
def run_all_strategies_for_one_csv(
    csv_path: str,
    monthly_investment: float = DEFAULT_MONTHLY_INVESTMENT,
    prefer_adj_close: bool = USE_ADJ_CLOSE_FIRST,
    round_shares: bool = ROUND_SHARES,
    share_decimals: int = SHARE_DECIMALS,
    start_date: str = BACKTEST_START_DATE,
    end_date: Optional[str] = BACKTEST_END_DATE
) -> Dict[str, Dict[str, object]]:
    raw_df = pd.read_csv(csv_path)
    df, price_col = validate_and_prepare_dataframe(raw_df, prefer_adj_close=prefer_adj_close)

    start_dt = pd.to_datetime(start_date)
    df = df[df["Date"] >= start_dt].copy()

    if end_date is not None:
        end_dt = pd.to_datetime(end_date)
        df = df[df["Date"] <= end_dt].copy()

    df = df.reset_index(drop=True)

    if df.empty:
        raise ValueError(f"在 {start_date} 到 {end_date} 之間無可用資料")

    monthly_base = get_monthly_first_trading_days(df).copy()
    if monthly_base.empty:
        raise ValueError("找不到每月第一個交易日")

    monthly_base["Price"] = monthly_base[price_col].astype(float)
    symbol_name = os.path.splitext(os.path.basename(csv_path))[0]

    results = {}

    strategy_builders = {
        STRATEGY_TRADITIONAL: lambda mp: build_traditional_dca_plan(
            mp, monthly_investment, round_shares, share_decimals
        ),
        STRATEGY_FIXED_SHARE: lambda mp: build_fixed_share_plan(
            mp, monthly_investment, round_shares, share_decimals
        ),
        STRATEGY_VALUE_AVERAGING: lambda mp: build_value_averaging_plan(
            mp, monthly_investment, round_shares, share_decimals, allow_sell=False
        ),
        STRATEGY_VARIABLE_DCA: lambda mp: build_variable_dca_plan(
            mp, monthly_investment, round_shares, share_decimals
        ),
    }

    for strategy_name, builder in strategy_builders.items():
        monthly_points = builder(monthly_base.copy())
        monthly_points = finalize_monthly_points(monthly_points)
        ts = build_dca_timeseries(df, monthly_points, price_col)
        summary = build_summary(
            csv_path=csv_path,
            symbol_name=symbol_name,
            strategy_name=strategy_name,
            df=df,
            monthly_points=monthly_points,
            ts=ts,
            price_col=price_col,
            monthly_investment=monthly_investment
        )
        monthly_return_df = build_monthly_return_series(ts)

        results[strategy_name] = {
            "detail_df": monthly_points,
            "ts": ts,
            "summary": summary,
            "monthly_return_df": monthly_return_df,
        }

    return results


# =========================
# 視覺化
# =========================
def save_strategy_comparison_for_one_symbol(
    symbol_name: str,
    strategy_results: Dict[str, Dict[str, object]],
    output_dir: str
) -> None:
    symbol_dir = os.path.join(output_dir, symbol_name)
    ensure_output_dir(symbol_dir)

    # 單一標的，四策略歷月回報比較
    plt.figure(figsize=(13, 7))
    for strategy_name, result in strategy_results.items():
        mdf = result["monthly_return_df"].copy()
        mdf["Date"] = pd.to_datetime(mdf["Date"])
        plt.plot(mdf["Date"], mdf["ReturnPct"], label=strategy_name, marker="o", linewidth=1.6, markersize=3)

    plt.axhline(0, linewidth=1)
    plt.title(f"{symbol_name} - Strategy Monthly Return Comparison")
    plt.xlabel("Date")
    plt.ylabel("Return (%)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(symbol_dir, f"{symbol_name}_strategy_monthly_return_comparison.png"))
    plt.close()

    # 單一標的，四策略總報酬比較
    srows = [asdict(v["summary"]) for v in strategy_results.values()]
    sdf = pd.DataFrame(srows).sort_values("total_return_pct", ascending=False)

    plt.figure(figsize=(10, 6))
    plt.bar(sdf["strategy_name"], sdf["total_return_pct"])
    plt.title(f"{symbol_name} - Strategy Total Return Comparison")
    plt.xlabel("Strategy")
    plt.ylabel("Total Return (%)")
    plt.xticks(rotation=20)
    plt.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()
    plt.savefig(os.path.join(symbol_dir, f"{symbol_name}_strategy_total_return_comparison.png"))
    plt.close()


def save_global_strategy_comparison(summary_df: pd.DataFrame, output_dir: str) -> None:
    compare_dir = os.path.join(output_dir, "comparison")
    ensure_output_dir(compare_dir)

    # 各策略跨標的平均總報酬
    avg_df = (
        summary_df.groupby("strategy_name", as_index=False)
        .agg(
            avg_total_return_pct=("total_return_pct", "mean"),
            avg_xirr_pct=("xirr_pct", "mean"),
            avg_final_value=("final_value", "mean"),
        )
        .sort_values("avg_total_return_pct", ascending=False)
    )

    plt.figure(figsize=(10, 6))
    plt.bar(avg_df["strategy_name"], avg_df["avg_total_return_pct"])
    plt.title("Average Total Return by Strategy")
    plt.xlabel("Strategy")
    plt.ylabel("Average Total Return (%)")
    plt.xticks(rotation=20)
    plt.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()
    plt.savefig(os.path.join(compare_dir, "strategy_avg_total_return.png"))
    plt.close()

    # 各標的 x 各策略 矩陣
    pivot_return = summary_df.pivot(index="symbol_name", columns="strategy_name", values="total_return_pct")
    pivot_xirr = summary_df.pivot(index="symbol_name", columns="strategy_name", values="xirr_pct")
    pivot_final_value = summary_df.pivot(index="symbol_name", columns="strategy_name", values="final_value")

    pivot_return.to_csv(os.path.join(compare_dir, "strategy_total_return_matrix.csv"), encoding="utf-8-sig")
    pivot_xirr.to_csv(os.path.join(compare_dir, "strategy_xirr_matrix.csv"), encoding="utf-8-sig")
    pivot_final_value.to_csv(os.path.join(compare_dir, "strategy_final_value_matrix.csv"), encoding="utf-8-sig")

def save_same_strategy_cross_asset_comparison(
    summary_df: pd.DataFrame,
    all_monthly_returns_by_strategy: Dict[str, Dict[str, pd.DataFrame]],
    output_dir: str
) -> None:
    """
    針對同一個策略，比較多個 CSV / 標的

    all_monthly_returns_by_strategy 格式:
    {
        "Traditional_DCA": {
            "0050": monthly_return_df,
            "0056": monthly_return_df,
            ...
        },
        "Fixed_Share": {
            "0050": monthly_return_df,
            ...
        },
        ...
    }
    """
    compare_dir = os.path.join(output_dir, "same_strategy_comparison")
    ensure_output_dir(compare_dir)

    for strategy_name in ALL_STRATEGIES:
        strategy_dir = os.path.join(compare_dir, strategy_name)
        ensure_output_dir(strategy_dir)

        sdf = summary_df[summary_df["strategy_name"] == strategy_name].copy()
        if sdf.empty:
            continue

        # 1. 同一策略下，不同標的的總報酬比較
        plot_df = sdf.sort_values("total_return_pct", ascending=False)

        plt.figure(figsize=(12, 6))
        plt.bar(plot_df["symbol_name"], plot_df["total_return_pct"])
        plt.title(f"{strategy_name} - Total Return Comparison Across Assets")
        plt.xlabel("Symbol")
        plt.ylabel("Total Return (%)")
        plt.xticks(rotation=45, ha="right")
        plt.grid(True, alpha=0.3, axis="y")
        plt.tight_layout()
        plt.savefig(os.path.join(strategy_dir, f"{strategy_name}_01_total_return_across_assets.png"))
        plt.close()

        # 2. 同一策略下，不同標的的 XIRR 比較
        plot_df = sdf.dropna(subset=["xirr_pct"]).sort_values("xirr_pct", ascending=False)
        if not plot_df.empty:
            plt.figure(figsize=(12, 6))
            plt.bar(plot_df["symbol_name"], plot_df["xirr_pct"])
            plt.title(f"{strategy_name} - XIRR Comparison Across Assets")
            plt.xlabel("Symbol")
            plt.ylabel("XIRR (%)")
            plt.xticks(rotation=45, ha="right")
            plt.grid(True, alpha=0.3, axis="y")
            plt.tight_layout()
            plt.savefig(os.path.join(strategy_dir, f"{strategy_name}_02_xirr_across_assets.png"))
            plt.close()

        # 3. 同一策略下，不同標的的最終市值比較
        plot_df = sdf.sort_values("final_value", ascending=False)
        plt.figure(figsize=(12, 6))
        plt.bar(plot_df["symbol_name"], plot_df["final_value"])
        plt.title(f"{strategy_name} - Final Value Comparison Across Assets")
        plt.xlabel("Symbol")
        plt.ylabel("Final Value")
        plt.xticks(rotation=45, ha="right")
        plt.grid(True, alpha=0.3, axis="y")
        plt.tight_layout()
        plt.savefig(os.path.join(strategy_dir, f"{strategy_name}_03_final_value_across_assets.png"))
        plt.close()

        # 4. 同一策略下，不同標的的歷月回報折線圖
        monthly_map = all_monthly_returns_by_strategy.get(strategy_name, {})
        if monthly_map:
            plt.figure(figsize=(13, 7))

            for symbol_name, mdf in monthly_map.items():
                temp = mdf.copy()
                temp["Date"] = pd.to_datetime(temp["Date"])
                plt.plot(
                    temp["Date"],
                    temp["ReturnPct"],
                    label=symbol_name,
                    marker="o",
                    linewidth=1.5,
                    markersize=3
                )

            plt.axhline(0, linewidth=1)
            plt.title(f"{strategy_name} - Monthly Return Comparison Across Assets")
            plt.xlabel("Date")
            plt.ylabel("Return (%)")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(strategy_dir, f"{strategy_name}_04_monthly_return_across_assets.png"))
            plt.close()

        # 5. 同一策略摘要表
        sdf.sort_values("total_return_pct", ascending=False).to_csv(
            os.path.join(strategy_dir, f"{strategy_name}_summary.csv"),
            index=False,
            encoding="utf-8-sig"
        )

# =========================
# 匯出
# =========================
def export_strategy_result(
    symbol_name: str,
    strategy_name: str,
    detail_df: pd.DataFrame,
    ts: pd.DataFrame,
    monthly_return_df: pd.DataFrame,
    output_dir: str
) -> None:
    symbol_dir = os.path.join(output_dir, symbol_name, strategy_name)
    ensure_output_dir(symbol_dir)

    detail_out = detail_df.copy()
    detail_out["Date"] = pd.to_datetime(detail_out["Date"]).dt.strftime("%Y-%m-%d")
    detail_out.to_csv(
        os.path.join(symbol_dir, f"{symbol_name}_{strategy_name}_detail.csv"),
        index=False,
        encoding="utf-8-sig"
    )

    ts_out = ts.copy()
    ts_out["Date"] = pd.to_datetime(ts_out["Date"]).dt.strftime("%Y-%m-%d")
    ts_out.to_csv(
        os.path.join(symbol_dir, f"{symbol_name}_{strategy_name}_timeseries.csv"),
        index=False,
        encoding="utf-8-sig"
    )

    mr_out = monthly_return_df.copy()
    mr_out["Date"] = pd.to_datetime(mr_out["Date"]).dt.strftime("%Y-%m-%d")
    mr_out.to_csv(
        os.path.join(symbol_dir, f"{symbol_name}_{strategy_name}_monthly_return.csv"),
        index=False,
        encoding="utf-8-sig"
    )


# =========================
# 批次執行
# =========================
def run_all_strategies_for_multiple_csv(
    input_path_or_pattern: str,
    monthly_investment: float = DEFAULT_MONTHLY_INVESTMENT,
    prefer_adj_close: bool = USE_ADJ_CLOSE_FIRST,
    round_shares: bool = ROUND_SHARES,
    share_decimals: int = SHARE_DECIMALS,
    export_results: bool = EXPORT_RESULTS,
    output_dir: str = OUTPUT_DIR,
    start_date: str = BACKTEST_START_DATE,
    end_date: Optional[str] = BACKTEST_END_DATE
) -> pd.DataFrame:
    if os.path.isdir(input_path_or_pattern):
        csv_files = sorted(glob.glob(os.path.join(input_path_or_pattern, "*.csv")))
    elif any(ch in input_path_or_pattern for ch in ["*", "?", "[", "]"]):
        csv_files = sorted(glob.glob(input_path_or_pattern))
    else:
        csv_files = [input_path_or_pattern]

    csv_files = [f for f in csv_files if f.lower().endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError("找不到任何 CSV 檔")

    if export_results:
        ensure_output_dir(output_dir)

    all_summaries = []
    all_monthly_returns_by_strategy = {strategy: {} for strategy in ALL_STRATEGIES}

    for csv_file in csv_files:
        try:
            strategy_results = run_all_strategies_for_one_csv(
                csv_path=csv_file,
                monthly_investment=monthly_investment,
                prefer_adj_close=prefer_adj_close,
                round_shares=round_shares,
                share_decimals=share_decimals,
                start_date=start_date,
                end_date=end_date
            )

            symbol_name = os.path.splitext(os.path.basename(csv_file))[0]

            for strategy_name, result in strategy_results.items():
                summary = result["summary"]
                detail_df = result["detail_df"]
                ts = result["ts"]
                monthly_return_df = result["monthly_return_df"]

                all_summaries.append(asdict(summary))

                # 新增：收集「同一策略跨標的」的月報酬資料
                all_monthly_returns_by_strategy[strategy_name][symbol_name] = monthly_return_df.copy()

                if export_results:
                    export_strategy_result(
                        symbol_name=symbol_name,
                        strategy_name=strategy_name,
                        detail_df=detail_df,
                        ts=ts,
                        monthly_return_df=monthly_return_df,
                        output_dir=output_dir
                    )

            if export_results:
                save_strategy_comparison_for_one_symbol(symbol_name, strategy_results, output_dir)

            print(f"[完成] {csv_file}")

        except Exception as e:
            print(f"[失敗] {csv_file} -> {e}")

    if not all_summaries:
        raise RuntimeError("所有檔案都處理失敗")

    summary_df = pd.DataFrame(all_summaries).sort_values(
        by=["symbol_name", "total_return_pct"],
        ascending=[True, False]
    ).reset_index(drop=True)

    if export_results:
        summary_df.to_csv(
            os.path.join(output_dir, "all_strategy_summary.csv"),
            index=False,
            encoding="utf-8-sig"
        )
        save_global_strategy_comparison(summary_df, output_dir)
        save_same_strategy_cross_asset_comparison(
            summary_df=summary_df,
            all_monthly_returns_by_strategy=all_monthly_returns_by_strategy,
            output_dir=output_dir
        )

    return summary_df


def print_summary(summary_df: pd.DataFrame) -> None:
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", lambda x: f"{x:,.6f}")

    print("\n=== All Strategy Backtest Summary ===")
    print(summary_df)


# =========================
# 主程式
# =========================
if __name__ == "__main__":
    input_target = "data/*.csv"

    summary_df = run_all_strategies_for_multiple_csv(
        input_path_or_pattern=input_target,
        monthly_investment=10000,
        prefer_adj_close=True,
        round_shares=False,
        share_decimals=6,
        export_results=True,
        output_dir="dca_output",
        start_date="2005-09-02",
        end_date="2026-04-13"
    )

    print_summary(summary_df)