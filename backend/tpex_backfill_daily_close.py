#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TPEx daily close quotes pipeline (route B: per-day, market-wide CSV)

Source:
  https://www.tpex.org.tw/web/stock/aftertrading/DAILY_CLOSE_quotes/stk_quote_result.php
Params:
  l=zh-tw
  o=data (CSV)
  d=ROC/YYYY/MM/DD   e.g. 114/01/02

Goal:
  Backfill TPEx OHLCV/avg_price into SQLite daily_facts using stock_list.market='TPEx'

Notes:
  - This script does NOT create/alter daily_facts schema. It assumes your existing schema.
  - It upserts ONLY these columns:
      open, high, low, close, volume, avg_price, updated_at
  - If you pass --stock, it will only upsert that single stock (debug mode).
"""

from __future__ import annotations

import argparse
import datetime as dt
import random
import sqlite3
import time
from io import StringIO
from typing import Optional, Dict, Iterable, Set, List, Tuple

import pandas as pd
import requests


TPEx_DAILY_CLOSE_URL = (
    "https://www.tpex.org.tw/web/stock/aftertrading/DAILY_CLOSE_quotes/stk_quote_result.php"
)

# CSV headers (zh-tw) commonly:
# 資料日期,代號,名稱,收盤,漲跌,開盤,最高,最低,均價,成交股數,成交金額,成交筆數,...

NEEDED_COLS = ["資料日期", "代號", "開盤", "最高", "最低", "收盤", "均價", "成交股數"]


def ymd_iter(start: str, end: str) -> Iterable[dt.date]:
    s = dt.datetime.strptime(start, "%Y-%m-%d").date()
    e = dt.datetime.strptime(end, "%Y-%m-%d").date()
    cur = s
    while cur <= e:
        yield cur
        cur += dt.timedelta(days=1)


def to_roc_slash(d: dt.date) -> str:
    roc_y = d.year - 1911
    return f"{roc_y:03d}/{d.month:02d}/{d.day:02d}"


def roc_to_iso(roc: str) -> str:
    # '114/01/02' -> '2025-01-02'
    yy, mm, dd = roc.split("/")
    y = int(yy) + 1911
    return f"{y:04d}-{int(mm):02d}-{int(dd):02d}"


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


def _strip_bom(text: str) -> str:
    # Handle UTF-8 BOM that can break header matching, e.g. '\ufeff資料日期'
    return text.lstrip("\ufeff")


def fetch_daily_close_csv(
    session: requests.Session,
    date_iso: str,
    timeout: int = 30,
    max_retries: int = 6,
) -> pd.DataFrame:
    d = dt.datetime.strptime(date_iso, "%Y-%m-%d").date()
    roc_req = to_roc_slash(d)

    params = {"l": "zh-tw", "o": "data", "d": roc_req}

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(TPEx_DAILY_CLOSE_URL, params=params, timeout=timeout)
            r.raise_for_status()

            ct = (r.headers.get("content-type") or "").lower()
            text = _strip_bom(r.text or "")

            # If HTML, likely blocked or error page
            if "text/html" in ct or text.lstrip().startswith("<!DOCTYPE html"):
                last_err = f"HTML response (status={r.status_code})"
                raise RuntimeError(last_err)

            # Quick sanity: must contain header-ish content
            if "資料日期" not in text[:300] or "代號" not in text[:300]:
                # Some days return short message or unexpected payload
                last_err = "Unexpected payload (missing header markers)"
                raise RuntimeError(last_err)

            # Parse CSV
            df = pd.read_csv(StringIO(text))
            if df is None or df.empty:
                return pd.DataFrame()

            # Normalize headers (strip spaces + BOM just in case)
            df = df.rename(columns=lambda c: str(c).strip().lstrip("\ufeff"))

            # Require columns
            missing = [c for c in NEEDED_COLS if c not in df.columns]
            if missing:
                last_err = f"Missing columns: {missing}"
                raise RuntimeError(last_err)

            # Verify returned date matches requested date (important!)
            # CSV column '資料日期' is ROC date like '114/01/02'
            first_roc = str(df["資料日期"].iloc[0]).strip().lstrip("\ufeff")
            first_iso = roc_to_iso(first_roc) if "/" in first_roc else None
            if first_iso != date_iso:
                # Treat mismatch as invalid (cached/previous trading day/blocked)
                return pd.DataFrame()

            # Attach ISO date for DB key
            df["date"] = date_iso
            return df

        except Exception as e:
            last_err = str(e)
            if attempt == max_retries:
                raise
            # backoff + jitter
            sleep_s = (2 ** (attempt - 1)) * 0.6 + random.random() * 0.4
            time.sleep(sleep_s)

    # Unreachable
    raise RuntimeError(last_err or "fetch failed")


def load_tpex_stock_ids(conn: sqlite3.Connection) -> Set[str]:
    rows = conn.execute(
        "SELECT stock_id FROM stock_list WHERE market='TPEx'"
    ).fetchall()
    return {str(r[0]).strip() for r in rows if r and r[0] is not None}


def extract_rows(
    df: pd.DataFrame,
    allowed_ids: Set[str],
    stock_only: Optional[str] = None,
) -> List[Tuple]:
    """
    Returns rows ready for DB upsert:
      (date, stock_id, open, high, low, close, volume, avg_price, updated_at)
    """
    if df is None or df.empty:
        return []

    df2 = df.copy()

    # Normalize stock id column to str
    df2["stock_id"] = df2["代號"].astype(str).str.strip()

    if stock_only:
        sid = str(stock_only).strip()
        df2 = df2[df2["stock_id"] == sid]
    else:
        df2 = df2[df2["stock_id"].isin(allowed_ids)]

    if df2.empty:
        return []

    now = dt.datetime.now().isoformat(timespec="seconds")

    out: List[Tuple] = []
    for _, r in df2.iterrows():
        sid = str(r.get("stock_id")).strip()
        date_iso = str(r.get("date")).strip()

        open_ = to_num(r.get("開盤"))
        high = to_num(r.get("最高"))
        low = to_num(r.get("最低"))
        close = to_num(r.get("收盤"))
        avgp = to_num(r.get("均價"))
        vol = to_num(r.get("成交股數"))

        out.append((date_iso, sid, open_, high, low, close, vol, avgp, now))

    return out


def upsert_many(conn: sqlite3.Connection, rows: List[Tuple]) -> int:
    if not rows:
        return 0
    conn.executemany(
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
        rows,
    )
    return len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--db", required=True, help="sqlite path (required for TPEx backfill)")
    ap.add_argument("--stock", default="", help="optional: only upsert this stock_id (debug)")
    ap.add_argument("--sleep", type=float, default=0.35, help="sleep between days (seconds)")
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--retries", type=int, default=6)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    tpex_ids = load_tpex_stock_ids(conn)
    if not tpex_ids:
        raise SystemExit("[FAIL] No TPEx stocks found in stock_list (market='TPEx').")

    stock_only = args.stock.strip() or None
    if stock_only and stock_only not in tpex_ids:
        print(f"[WARN] --stock={stock_only} is not in TPEx list; will still attempt to find it in CSV.")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv,application/csv,text/plain,*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.tpex.org.tw/web/stock/aftertrading/DAILY_CLOSE_quotes/",
        }
    )

    print("=== TPEx backfill (DAILY_CLOSE_quotes / per-day CSV) ===")
    print(f"db={args.db} range={args.start}~{args.end} mode={'single' if stock_only else 'all_TPEx'}")
    if not stock_only:
        print(f"TPEx universe size={len(tpex_ids)}")

    total_rows = 0
    ok_days = 0
    skip_days = 0
    err_days = 0

    for d in ymd_iter(args.start, args.end):
        date_iso = d.isoformat()
        try:
            df = fetch_daily_close_csv(
                session,
                date_iso,
                timeout=args.timeout,
                max_retries=args.retries,
            )
            if df is None or df.empty:
                print(f"[SKIP] {date_iso} (no valid CSV for this date; holiday/weekend/mismatch)")
                skip_days += 1
            else:
                rows = extract_rows(df, tpex_ids, stock_only=stock_only)
                if not rows:
                    print(f"[SKIP] {date_iso} (no matching rows after filtering)")
                    skip_days += 1
                else:
                    n = upsert_many(conn, rows)
                    conn.commit()
                    total_rows += n
                    ok_days += 1

                    if stock_only:
                        # print single row summary
                        r0 = rows[0]
                        _, sid, open_, high, low, close, vol, avgp, _ = r0
                        print(
                            f"[OK] {date_iso} {sid} close={close} open={open_} high={high} low={low} vol={vol} avg={avgp}"
                        )
                    else:
                        print(f"[OK] {date_iso} upsert_rows={n}")

        except Exception as e:
            conn.rollback()
            print(f"[ERR] {date_iso} {type(e).__name__}: {e}")
            err_days += 1

        time.sleep(args.sleep + random.random() * 0.15)

    conn.close()

    print("=== Summary ===")
    print(f"ok_days={ok_days} skip_days={skip_days} err_days={err_days} total_upserts={total_rows}")

    if ok_days == 0 and total_rows == 0:
        raise SystemExit("[FAIL] No valid trading-day data upserted in the given range.")


if __name__ == "__main__":
    main()
