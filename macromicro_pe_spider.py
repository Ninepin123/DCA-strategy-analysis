"""
MacroMicro Chart Spider - Scrapes historical valuation data from MacroMicro.

Targets:
  - Chart 13940: TAIEX P/E ratio (台股本益比)
  - Chart 9295:  TAIEX P/B ratio (台股PB比)

Method: Playwright (Edge) → render Highcharts → extract data from JS object

MacroMicro's /charts/data/ API requires JWT authentication (login).
However, the chart renders publicly. This spider uses Playwright with
real Edge browser to bypass Cloudflare, renders the Highcharts chart,
and extracts data directly from the JavaScript chart object.
"""

import os
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_RAW_DIR = "data/raw"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

CHART_CONFIGS = {
    "PE": {
        "chart_id": 13940,
        "url": "https://www.macromicro.me/charts/13940/tai-wan-tai-gu-ben-yi-bi-yu-tai-gu-qu-shi",
        "output_file": "TAIEX_pe_ratio_raw.csv",
        "column": "PE_RATIO",
        "keywords": ["p/e", "pe ratio", "本益比"],
        "fallback_range": (5, 50),
    },
    "PB": {
        "chart_id": 9295,
        "url": "https://www.macromicro.me/charts/9295/tai-gu-PB-bi-VS-tai-gu-da-pan",
        "output_file": "TAIEX_pb_ratio_raw.csv",
        "column": "PB_RATIO",
        "keywords": ["p/b", "pb", "pb ratio", "淨值比", "股價淨值比"],
        "fallback_range": (0.5, 5),
    },
    "DIV_YIELD": {
        "chart_id": 76331,
        "url": "https://www.macromicro.me/charts/76331/Dividend-Yield",
        "output_file": "TAIEX_dividend_yield_raw.csv",
        "column": "DIVIDEND_YIELD",
        "keywords": ["dividend yield", "殖利率", "現金殖利率"],
        "fallback_range": (1, 10),
    },
}


def _parse_ts(ts) -> datetime:
    """Parse a Highcharts millisecond timestamp to datetime."""
    try:
        if ts > 1e11:
            return EPOCH + timedelta(milliseconds=int(ts))
        else:
            return EPOCH + timedelta(seconds=int(ts))
    except (OSError, ValueError, OverflowError):
        return None


def _extract_chart_from_macromicro(url: str) -> list:
    """
    Generic function: open a MacroMicro chart page with Playwright (Edge),
    expand to full range, and extract all Highcharts series data.

    Returns:
        list of dicts: [{name, points: [[ts, val], ...]}, ...]
    """
    from playwright.sync_api import sync_playwright

    logger.info("Launching Playwright (Edge)...")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            channel="msedge",
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="zh-TW",
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        page = context.new_page()

        logger.info(f"Navigating to {url}...")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(8000)

        # Wait for chart
        try:
            page.wait_for_selector(".highcharts-container", timeout=30000)
            logger.info("Chart container found.")
        except Exception:
            logger.warning("Chart container not found, waiting longer...")
            page.wait_for_timeout(15000)

        # Expand to full range
        logger.info("Expanding chart to full range...")
        try:
            for sel in [".highcharts-range-selector-buttons text"]:
                for el in page.query_selector_all(sel):
                    if el.text_content().strip().lower() in ("all", "全部", "max"):
                        el.click()
                        page.wait_for_timeout(2000)
                        break
        except Exception:
            pass

        page.evaluate("""() => {
            if (window.Highcharts && window.Highcharts.charts)
                for (const c of window.Highcharts.charts)
                    if (c.xAxis[0]) {
                        const e = c.xAxis[0].getExtremes();
                        c.xAxis[0].setExtremes(e.dataMin, e.dataMax);
                    }
        }""")
        page.wait_for_timeout(3000)

        # Extract series data
        logger.info("Extracting chart data...")
        full_data = page.evaluate("""() => {
            const results = [];
            if (!window.Highcharts || !window.Highcharts.charts) return results;
            for (const chart of window.Highcharts.charts) {
                if (!chart || !chart.series) continue;
                for (const series of chart.series) {
                    const points = [];
                    if (series.options && series.options.data) {
                        for (const pt of series.options.data) {
                            if (Array.isArray(pt) && pt.length >= 2)
                                points.push([pt[0], pt[1]]);
                            else if (typeof pt === 'object' && pt.x !== undefined)
                                points.push([pt.x, pt.y]);
                        }
                    }
                    if (points.length > 0)
                        results.push({ name: series.name || 'Unknown', points });
                }
            }
            return results;
        }""")

        browser.close()

    return full_data


def _identify_series(full_data: list, keywords: list, fallback_range: tuple) -> list:
    """Identify target series by keyword match, falling back to value range."""
    # Keyword match
    for kw in keywords:
        for s in full_data:
            if kw in s["name"].lower():
                return s["points"]

    # Fallback: value range
    lo, hi = fallback_range
    for s in full_data:
        vals = [p[1] for p in s["points"] if p[1] is not None]
        if len(vals) > 50:
            avg = sum(vals) / len(vals)
            if lo <= avg <= hi:
                return s["points"]

    return full_data[0]["points"] if full_data else []


def _build_dataframe(points: list, column: str) -> pd.DataFrame:
    """Convert raw [ts, val] points to a clean DataFrame."""
    rows = []
    for ts, val in points:
        dt = _parse_ts(ts)
        if dt and val is not None:
            rows.append({"date": dt.strftime("%Y-%m-%d"), column: round(val, 2)})

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def fetch_generic(metric: str) -> pd.DataFrame:
    """
    Fetch a valuation metric from MacroMicro.

    Args:
        metric: One of "PE", "PB", "DIV_YIELD" (must exist in CHART_CONFIGS)

    Returns:
        DataFrame with columns: [date, {column}] (monthly)
    """
    cfg = CHART_CONFIGS[metric]
    full_data = _extract_chart_from_macromicro(cfg["url"])

    if not full_data:
        logger.error("No chart data extracted.")
        return pd.DataFrame()

    for s in full_data:
        first = _parse_ts(s["points"][0][0])
        last = _parse_ts(s["points"][-1][0])
        logger.info(f"  '{s['name']}': {len(s['points'])} pts, "
                     f"{first.strftime('%Y-%m') if first else '?'} ~ "
                     f"{last.strftime('%Y-%m') if last else '?'}")

    points = _identify_series(full_data, cfg["keywords"], cfg["fallback_range"])
    df = _build_dataframe(points, cfg["column"])

    logger.info(f"Extracted {len(df)} {metric} records: "
                f"{df['date'].min().strftime('%Y-%m-%d')} ~ {df['date'].max().strftime('%Y-%m-%d')}, "
                f"range: {df[cfg['column']].min():.2f} ~ {df[cfg['column']].max():.2f}")
    return df


def fetch_with_price(metric: str) -> tuple:
    """
    Fetch a valuation metric and the accompanying price/index series.
    The price series is identified as the one with the most data points
    (typically daily data vs monthly metric data).

    Returns:
        (metric_df, price_df) tuple.
        price_df is monthly with columns [date, adj_price].
    """
    cfg = CHART_CONFIGS[metric]
    full_data = _extract_chart_from_macromicro(cfg["url"])

    if not full_data:
        return pd.DataFrame(), pd.DataFrame()

    for s in full_data:
        first = _parse_ts(s["points"][0][0])
        last = _parse_ts(s["points"][-1][0])
        logger.info(f"  '{s['name']}': {len(s['points'])} pts, "
                     f"{first.strftime('%Y-%m') if first else '?'} ~ "
                     f"{last.strftime('%Y-%m') if last else '?'}")

    # Extract metric series
    metric_points = _identify_series(full_data, cfg["keywords"], cfg["fallback_range"])
    metric_df = _build_dataframe(metric_points, cfg["column"])

    logger.info(f"Extracted {len(metric_df)} {metric} records: "
                f"{metric_df['date'].min().strftime('%Y-%m-%d')} ~ {metric_df['date'].max().strftime('%Y-%m-%d')}, "
                f"range: {metric_df[cfg['column']].min():.2f} ~ {metric_df[cfg['column']].max():.2f}")

    # Extract price series (the one with the most data points = daily price)
    price_df = pd.DataFrame()
    price_series = max(full_data, key=lambda s: len(s["points"]))

    if len(price_series["points"]) > len(metric_points) * 2:
        logger.info(f"Extracting price series: '{price_series['name']}' ({len(price_series['points'])} pts)")
        rows = []
        for ts, val in price_series["points"]:
            dt = _parse_ts(ts)
            if dt and val is not None:
                rows.append({"date": dt.strftime("%Y-%m-%d"), "adj_price": round(val, 2)})

        if rows:
            price_df = pd.DataFrame(rows)
            price_df["date"] = pd.to_datetime(price_df["date"])
            price_df = price_df.sort_values("date").reset_index(drop=True)
            # Resample daily to monthly (month-end)
            price_df = price_df.set_index("date").resample("ME").last().reset_index()

    return metric_df, price_df


# Convenience aliases
fetch_pe_ratio_from_macromicro = lambda: fetch_generic("PE")
fetch_pb_ratio_from_macromicro = lambda: fetch_generic("PB")
fetch_dividend_yield_from_macromicro = lambda: fetch_generic("DIV_YIELD")


def save_data(df: pd.DataFrame, filename: str):
    """Save DataFrame to CSV."""
    os.makedirs(DATA_RAW_DIR, exist_ok=True)
    filepath = os.path.join(DATA_RAW_DIR, filename)
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    logger.info(f"Saved to {filepath}")
    return filepath


if __name__ == "__main__":
    # Fetch Dividend Yield
    print("=" * 50)
    print("Fetching TAIEX Dividend Yield...")
    print("=" * 50)
    df = fetch_generic("DIV_YIELD")
    if not df.empty:
        print(f"\n{len(df)} records")
        print(df.head(10))
        print("...")
        print(df.tail(10))
        save_data(df, CHART_CONFIGS["DIV_YIELD"]["output_file"])
    else:
        print("No data extracted.")
