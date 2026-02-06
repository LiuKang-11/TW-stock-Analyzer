#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TPEx prices-only minimal pipeline (DAILY_CLOSE_quotes CSV)

Source:
  https://www.tpex.org.tw/web/stock/aftertrading/DAILY_CLOSE_quotes/stk_quote_result.php
Params:
  l=zh-tw
  o=data (CSV)
  d=ROC/YYYY/MM/DD   e.g. 114/01/02

Goal:
  Verify a single stock's close varies across days (e.g., 8390) for a given date range.

Optional:
  Upsert into SQLite daily_facts(date, stock_id, open, high, low, close, volume, avg_price, updated_at)
"""

from __future__ import annotations

import argparse
import datetime as dt
import sqlite3
import time
from typing import Optional, Tuple, Dict

import pandas as pd
import requests


TPEx_DAILY_CLOSE_URL = (
    "https://www.tpex.org.tw/web/stock/aftertrading/DAILY_CLOSE_quotes/stk_quote_result.php"
)

# CSV headers (zh-tw):
# 資料日期,代號,名稱,收盤,漲跌,開盤,最高,最低,均價,成交股數,成交金額,成交筆數,最後買價,最後賣價,發行股數,次日參考價,次日漲停價,次日跌停價


def ymd_iter(start: str, end: str):
    s = dt.datetime.strptime(start, "%Y-%m-%d").date()
    e = dt.datetime.strptime(end, "%Y-%m-%d").date()
    cur = s
    while cur <= e:
        yield cur
        cur += dt.timedelta(days=1)


def to_roc_slash(d: dt.date) -> str:
    roc_y = d.year - 1911
    return f"{roc_y:03d}/{d.month:02d}/{d.day:02d}"


def to_num(x) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if s in ("", "--", "nan", "NaN", "None"):
        return None
    try:
        return float(s.replace(",", ""))
    except Exception:
        return None


def fetch_daily_close_csv(session: requests.Session, date_iso: str, timeout: int = 30) -> pd.DataFrame:
    d = dt.datetime.strptime(date_iso, "%Y-%m-%d").date()
    roc = to_roc_slash(d)

    params = {"l": "zh-tw", "o": "data", "d": roc}
    r = session.get(TPEx_DAILY_CLOSE_URL, params=params, timeout=timeout)
    r.raise_for_status()

    ct = (r.headers.get("content-type") or "").lower()
    text = r.text or ""

    # Quick sanity check: CSV should contain header row with 資料日期,代號,名稱,...
    if "資料日期" not in text[:200]:
        # Some days may return HTML or a short message; treat as empty
        return pd.DataFrame()

    # Parse CSV
    # Note: TPEx CSV is usually comma-separated with quoted fields
    df = pd.read_csv(pd.io.common.StringIO(text))
    if df is None or df.empty:
        return pd.DataFrame()

    # Normalize headers
    df = df.rename(columns=lambda c: str(c).strip())

    # Attach ISO date (use requested date as truth)
    df["date"] = date_iso
    return df


def extract_one_stock_row(df: pd.DataFrame, stock_id: str) -> Optional[Dict]:
    if df is None or df.empty:
        return None

    # Some files might have '代號' as int; normalize to string
    if "代號" not in df.columns:
        return None

    sid = str(stock_id).strip()
    df2 = df.copy()
    df2["stock_id"] = df2["代號"].astype(str).str.strip()

    hit = df2[df2["stock_id"] == sid]
    if hit.empty:
        return None

    r = hit.iloc[0]

    open_ = to_num(r.get("開盤"))
    high = to_num(r.get("最高"))
    low = to_num(r.get("最低"))
    close = to_num(r.get("收盤"))
    avgp = to_num(r.get("均價"))
    vol = to_num(r.get("成交股數"))

    return {
        "date": str(r.get("date")),
        "stock_id": sid,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": vol,
        "avg_price": avgp,
    }


def db_init(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS daily_facts (
          date TEXT NOT NULL,
          stock_id TEXT NOT NULL,
          open REAL,
          high REAL,
          low REAL,
          close REAL,
          volume REAL,
          avg_price REAL,
          updated_at TEXT,
          PRIMARY KEY (date, stock_id)
        );

        CREATE INDEX IF NOT EXISTS idx_daily_facts_stock_date ON daily_facts(stock_id, date);
        """
    )
    conn.commit()


def upsert_one(conn: sqlite3.Connection, row: Dict) -> None:
    conn.execute(
        """
        INSERT INTO daily_facts(date, stock_id, open, high, low, close, volume, avg_price, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, stock_id) DO UPDATE SET
          open=excluded.open,
          high=excluded.high,
          low=excluded.low,
          close=excluded.close,
          volume=excluded.volume,
          avg_price=excluded.avg_price,
          updated_at=excluded.updated_at
        """,
        (
            row["date"],
            row["stock_id"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            row["avg_price"],
            dt.datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--stock", required=True, help="e.g., 8390")
    ap.add_argument("--db", default="", help="optional sqlite path; if empty, no DB write")
    ap.add_argument("--sleep", type=float, default=0.3, help="sleep between days")
    ap.add_argument("--timeout", type=int, default=30)
    args = ap.parse_args()

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv,application/csv,text/plain,*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.tpex.org.tw/web/stock/aftertrading/DAILY_CLOSE_quotes/",
        }
    )

    conn: Optional[sqlite3.Connection] = None
    if args.db:
        conn = sqlite3.connect(args.db)
        db_init(conn)

    print("=== TPEx prices-only (DAILY_CLOSE_quotes) ===")
    print(f"range={args.start}~{args.end} stock={args.stock} db={'(none)' if not args.db else args.db}")

    found_any = False
    for d in ymd_iter(args.start, args.end):
        date_iso = d.isoformat()
        try:
            df = fetch_daily_close_csv(session, date_iso, timeout=args.timeout)
            row = extract_one_stock_row(df, args.stock)

            if row is None:
                print(f"[SKIP] {date_iso} (no row; holiday/weekend or stock not found)")
            else:
                found_any = True
                print(
                    f"[OK] {date_iso} {args.stock} "
                    f"close={row['close']} open={row['open']} high={row['high']} low={row['low']} "
                    f"vol={row['volume']} avg={row['avg_price']}"
                )
                if conn is not None:
                    upsert_one(conn, row)

        except Exception as e:
            print(f"[ERR] {date_iso} {type(e).__name__}: {e}")

        time.sleep(args.sleep)

    if conn is not None:
        conn.close()

    if not found_any:
        raise SystemExit("[FAIL] No valid trading-day rows found in the given range.")


if __name__ == "__main__":
    main()
