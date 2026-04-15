import os
import re
import time
import random
from datetime import datetime
from typing import Optional

import pandas as pd
from dateutil import parser as dtparser
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    NoSuchElementException,
    WebDriverException,
)

# ============================================================
# 設定區
# ============================================================

SYMBOL = "%5ETWII"
OUTPUT_CSV = "TAIEX_history.csv"

PERIOD1 = 868924800
PERIOD2 = 1776218131

BASE_URL = (
    f"https://finance.yahoo.com/quote/{SYMBOL}/history/"
    f"?period1={PERIOD1}&period2={PERIOD2}&filter=history&frequency=1d"
)

REQUIRED_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
NUMERIC_COLUMNS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

MAX_FULL_RETRIES = 3
PAGE_LOAD_TIMEOUT = 40
MIN_EXPECTED_ROWS = 6500  # ^TWII 自 1997 起，日資料應明顯大於這個數量


# ============================================================
# 基本工具
# ============================================================

def log(msg: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


def random_sleep(a: float = 0.08, b: float = 0.20) -> None:
    time.sleep(random.uniform(a, b))


def make_driver(headless: bool = True) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")

    options.page_load_strategy = "eager"
    options.add_argument("--window-size=1800,2200")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--lang=en-US")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    options.add_experimental_option("prefs", prefs)

    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def safe_click(driver, element) -> None:
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    random_sleep()
    try:
        element.click()
    except Exception:
        driver.execute_script("arguments[0].click();", element)


def open_with_retry(driver, url: str, tries: int = 2) -> None:
    last_error = None
    for i in range(tries):
        try:
            driver.get(url)
            return
        except TimeoutException as e:
            last_error = e
            log(f"driver.get timeout, retry {i + 1}/{tries}")
            time.sleep(1.5)
    raise last_error


def try_accept_cookie(driver) -> None:
    xpaths = [
        "//button[contains(., 'Accept')]",
        "//button[contains(., 'I accept')]",
        "//button[contains(., 'Agree')]",
        "//button[contains(., 'Got it')]",
        "//button[contains(., 'Continue')]",
    ]
    for xp in xpaths:
        try:
            btn = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, xp))
            )
            safe_click(driver, btn)
            log("Cookie/consent button clicked.")
            random_sleep()
            return
        except Exception:
            pass


def wait_table_ready(driver) -> None:
    WebDriverWait(driver, 18).until(
        EC.presence_of_element_located((By.TAG_NAME, "table"))
    )
    WebDriverWait(driver, 18).until(
        EC.presence_of_element_located((By.XPATH, "//table//tbody/tr"))
    )


def get_current_row_count(driver) -> int:
    return len(driver.find_elements(By.XPATH, "//table//tbody/tr"))


def click_show_more_until_done(driver) -> None:
    current_count = get_current_row_count(driver)
    log(f"Current visible rows: {current_count}")

    if current_count >= MIN_EXPECTED_ROWS:
        log("Rows already look complete. Skip Show more.")
        return

    prev_count = -1
    stable_rounds = 0

    while True:
        current_count = get_current_row_count(driver)

        if current_count == prev_count:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= 1:
            log("Row count no longer increases. Assume all rows loaded.")
            break

        prev_count = current_count

        show_more = None
        xpaths = [
            "//span[text()='Show more']/ancestor::button",
            "//button[.//span[text()='Show more']]",
            "//button[contains(., 'Show more')]",
        ]
        for xp in xpaths:
            try:
                candidate = driver.find_element(By.XPATH, xp)
                if candidate.is_displayed():
                    show_more = candidate
                    break
            except Exception:
                pass

        if show_more is None:
            log("No 'Show more' button found. Possibly already fully expanded.")
            break

        try:
            safe_click(driver, show_more)
            log("'Show more' clicked.")
            time.sleep(0.2)
        except Exception:
            log("Click 'Show more' failed. Stop expanding.")
            break


# ============================================================
# 資料正規化
# ============================================================

def normalize_number(value: Optional[str], is_volume: bool = False):
    if value is None:
        return None

    v = str(value).strip().replace(",", "")
    if v in {"", "-", "null", "None", "N/A"}:
        return None

    try:
        num = float(v)
        if is_volume:
            return int(num)
        return num
    except ValueError:
        return None


def normalize_date(value: str) -> str:
    dt = dtparser.parse(value)
    return dt.strftime("%Y-%m-%d")


def canonicalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df[REQUIRED_COLUMNS]

    df["Date"] = df["Date"].astype(str).map(normalize_date)

    for col in NUMERIC_COLUMNS:
        is_volume = (col == "Volume")
        df[col] = df[col].map(lambda x: normalize_number(x, is_volume=is_volume))

    df = df.dropna(subset=["Date", "Close"]).copy()
    df = df.drop_duplicates(subset=["Date"], keep="first").copy()

    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date", ascending=True).reset_index(drop=True)
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

    return df


# ============================================================
# 表格擷取：一次抓 HTML，再解析
# ============================================================

def parse_table_from_html(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table")
    if table is None:
        raise ValueError("No table found in page source.")

    tbody = table.find("tbody")
    if tbody is None:
        raise ValueError("No tbody found in table.")

    rows_data = []
    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        texts = [c.get_text(" ", strip=True) for c in cells]

        # 標準歷史資料列：7 欄
        if len(texts) == 7 and re.match(r"^[A-Za-z]{3}\s+\d{1,2},\s+\d{4}$", texts[0]):
            rows_data.append({
                "Date": texts[0],
                "Open": texts[1],
                "High": texts[2],
                "Low": texts[3],
                "Close": texts[4],
                "Adj Close": texts[5],
                "Volume": texts[6],
            })

    df = pd.DataFrame(rows_data, columns=REQUIRED_COLUMNS)
    if df.empty:
        raise ValueError("No valid table rows parsed.")

    return canonicalize_dataframe(df)


# ============================================================
# 驗證
# ============================================================

def validate_dataframe(df: pd.DataFrame, strict: bool = True) -> None:
    if df.empty:
        raise ValueError("DataFrame is empty.")

    if list(df.columns) != REQUIRED_COLUMNS:
        raise ValueError(f"Columns mismatch: {list(df.columns)}")

    dt_series = pd.to_datetime(df["Date"], errors="coerce")
    if dt_series.isna().any():
        bad = df.loc[dt_series.isna(), "Date"].tolist()[:10]
        raise ValueError(f"Invalid dates found: {bad}")

    if df["Date"].duplicated().any():
        dupes = df.loc[df["Date"].duplicated(), "Date"].tolist()[:10]
        raise ValueError(f"Duplicate dates found: {dupes}")

    if not dt_series.is_monotonic_increasing:
        raise ValueError("Dates are not sorted ascending.")

    if len(df) < MIN_EXPECTED_ROWS:
        raise ValueError(f"Too few rows: {len(df)}")

    for col in ["Open", "High", "Low", "Close", "Adj Close"]:
        if strict and df[col].isna().any():
            bad_dates = df.loc[df[col].isna(), "Date"].tolist()[:10]
            raise ValueError(f"NaN in {col}: {bad_dates}")

        if (df[col] < 0).any():
            raise ValueError(f"Negative value found in {col}")

    invalid_hl = df[df["Low"] > df["High"]]
    if not invalid_hl.empty:
        raise ValueError(f"Found Low > High rows: {invalid_hl.head(5).to_dict('records')}")

    invalid_open = df[(df["Open"] < df["Low"]) | (df["Open"] > df["High"])]
    if not invalid_open.empty:
        raise ValueError(f"Found Open outside [Low, High]: {invalid_open.head(5).to_dict('records')}")

    invalid_close = df[(df["Close"] < df["Low"]) | (df["Close"] > df["High"])]
    if not invalid_close.empty:
        raise ValueError(f"Found Close outside [Low, High]: {invalid_close.head(5).to_dict('records')}")


def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> None:
    a = canonicalize_dataframe(df1)
    b = canonicalize_dataframe(df2)

    if len(a) != len(b):
        raise ValueError(f"Row count mismatch: first={len(a)}, second={len(b)}")

    merged = a.merge(
        b,
        on="Date",
        suffixes=("_1", "_2"),
        how="outer",
        indicator=True
    )

    if (merged["_merge"] != "both").any():
        diff = merged[merged["_merge"] != "both"][["Date", "_merge"]].head(20)
        raise ValueError(f"Date set mismatch:\n{diff}")

    for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
        c1 = f"{col}_1"
        c2 = f"{col}_2"

        def is_equal(x, y):
            if pd.isna(x) and pd.isna(y):
                return True
            if pd.isna(x) != pd.isna(y):
                return False
            return abs(float(x) - float(y)) < 1e-9

        bad = merged[~merged.apply(lambda r: is_equal(r[c1], r[c2]), axis=1)]
        if not bad.empty:
            sample = bad[["Date", c1, c2]].head(10).to_dict("records")
            raise ValueError(f"Mismatch in {col}: {sample}")


# ============================================================
# 單次抓取
# ============================================================

def scrape_table_once(url: str, headless: bool = True, save_debug_html: bool = False) -> pd.DataFrame:
    driver = None
    try:
        driver = make_driver(headless=headless)
        log(f"Opening: {url}")

        open_with_retry(driver, url, tries=2)

        try_accept_cookie(driver)
        wait_table_ready(driver)
        click_show_more_until_done(driver)

        html = driver.page_source

        if save_debug_html:
            with open("debug_yahoo_page.html", "w", encoding="utf-8") as f:
                f.write(html)
            log("Saved debug_yahoo_page.html")

        df = parse_table_from_html(html)
        validate_dataframe(df, strict=True)

        log(f"Parsed rows from table HTML: {len(df)}")
        return df

    finally:
        if driver:
            driver.quit()


# ============================================================
# 驗證策略：單次抓取 + 嚴格驗證
# ============================================================

def scrape_verified(url: str) -> pd.DataFrame:
    df = scrape_table_once(url, headless=True)
    validate_dataframe(df, strict=True)
    return df


# ============================================================
# CSV
# ============================================================

def save_csv(df: pd.DataFrame, path: str) -> None:
    out = df.copy()
    out["Volume"] = pd.Series(out["Volume"], dtype="Int64")
    out.to_csv(path, index=False, encoding="utf-8-sig")
    log(f"CSV saved: {path}")


def verify_saved_csv(path: str, expected_df: pd.DataFrame) -> None:
    if not os.path.exists(path):
        raise ValueError(f"CSV not found: {path}")

    reloaded = pd.read_csv(path)
    reloaded = canonicalize_dataframe(reloaded)
    validate_dataframe(reloaded, strict=True)
    compare_dataframes(reloaded, expected_df)

    log("Saved CSV verification passed.")


# ============================================================
# 對外主流程
# ============================================================

def scrape_with_retries(url: str, output_csv: str, max_retries: int = MAX_FULL_RETRIES) -> pd.DataFrame:
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            log("=" * 72)
            log(f"Full scrape attempt {attempt}/{max_retries}")

            df = scrape_verified(url)
            validate_dataframe(df, strict=True)

            save_csv(df, output_csv)
            verify_saved_csv(output_csv, df)

            log(f"SUCCESS: {len(df)} rows written to {output_csv}")
            return df

        except Exception as e:
            last_error = e
            log(f"Attempt {attempt} failed: {repr(e)}")

            sleep_sec = min(4 * attempt, 12) + random.uniform(0.3, 0.8)
            log(f"Retrying after {sleep_sec:.1f}s ...")
            time.sleep(sleep_sec)

    raise RuntimeError(f"All retries failed. Last error: {repr(last_error)}")


if __name__ == "__main__":
    df = scrape_with_retries(BASE_URL, OUTPUT_CSV)
    print()
    print("Done.")
    print(f"Rows: {len(df)}")
    print(f"Date range: {df['Date'].iloc[0]} ~ {df['Date'].iloc[-1]}")
    print(f"Output: {OUTPUT_CSV}")