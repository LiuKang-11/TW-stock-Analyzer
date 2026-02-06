#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
update_stock_list.py

Fetch full Taiwan stock list (TWSE + TPEx) and upsert into SQLite stock_list table.
"""

import argparse
import sqlite3
import requests
import datetime as dt
import pandas as pd


TWSE_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"


def fetch_json(url: str):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def debug_df(tag: str, df: pd.DataFrame, max_rows: int = 2):
    print(f"[DEBUG] {tag}: rows={len(df)} cols={df.columns.tolist()}")
    if len(df) > 0:
        print(df.head(max_rows).to_string(index=False))


def normalize_columns_fuzzy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fuzzy normalize columns between TWSE/TPEx variations.
    Tries common Chinese keys first, then falls back to heuristic matching.
    """
    if df.empty:
        return df

    # direct known mappings
    candidates = {
        "stock_id": ["公司代號", "股票代號", "證券代號", "代號", "Code"],
        "stock_name": ["公司名稱", "公司簡稱", "股票名稱", "證券名稱", "名稱", "Name"],
        "industry": ["產業別", "產業類別", "產業", "Industry"],
    }

    # If already normalized, keep.
    for tgt, src_list in candidates.items():
        if tgt in df.columns:
            continue
        for src in src_list:
            if src in df.columns:
                df = df.rename(columns={src: tgt})
                break

    # Heuristic: if still missing stock_id/stock_name, try "contains" matching
    if "stock_id" not in df.columns:
        for c in df.columns:
            if "代號" in c or "code" in str(c).lower():
                df = df.rename(columns={c: "stock_id"})
                break

    if "stock_name" not in df.columns:
        for c in df.columns:
            if "名稱" in c or "name" in str(c).lower() or "簡稱" in c:
                df = df.rename(columns={c: "stock_name"})
                break

    if "industry" not in df.columns:
        for c in df.columns:
            if "產業" in c or "industry" in str(c).lower():
                df = df.rename(columns={c: "industry"})
                break

    return df


def load_twse() -> pd.DataFrame:
    data = fetch_json(TWSE_URL)
    df = pd.DataFrame(data)
    debug_df("TWSE raw", df)

    if df.empty:
        return df

    df = normalize_columns_fuzzy(df)

    if "stock_id" not in df.columns or "stock_name" not in df.columns:
        raise RuntimeError(f"TWSE schema unexpected; columns={df.columns.tolist()}")

    df["market"] = "TWSE"
    if "industry" not in df.columns:
        df["industry"] = None

    keep = ["stock_id", "stock_name", "market", "industry"]
    return df[keep].dropna(subset=["stock_id"]).reset_index(drop=True)


def load_tpex() -> pd.DataFrame:
    data = fetch_json(TPEX_URL)
    df = pd.DataFrame(data)
    debug_df("TPEx raw", df)

    if df.empty:
        return df

    df = normalize_columns_fuzzy(df)

    if "stock_id" not in df.columns or "stock_name" not in df.columns:
        raise RuntimeError(f"TPEx schema unexpected; columns={df.columns.tolist()}")

    df["market"] = "TPEx"
    if "industry" not in df.columns:
        df["industry"] = None

    keep = ["stock_id", "stock_name", "market", "industry"]
    return df[keep].dropna(subset=["stock_id"]).reset_index(drop=True)


def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def ensure_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS stock_list (
      stock_id TEXT PRIMARY KEY,
      stock_name TEXT,
      market TEXT,
      industry TEXT,
      updated_at TEXT
    );
    """)
    conn.commit()


def clean_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert pandas types to SQLite-friendly Python primitives.
    """
    df = df.copy()
    # normalize id as str
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df["stock_name"] = df["stock_name"].astype(str).str.strip()

    # market/industry may be missing or NaN
    for c in ["market", "industry"]:
        if c in df.columns:
            df[c] = df[c].astype(str).where(df[c].notna(), None)
            # If industry became 'nan' string, fix
            df[c] = df[c].replace({"nan": None, "NaN": None, "None": None, "": None})
        else:
            df[c] = None

    # Replace pandas NA/NaN with None
    df = df.where(pd.notnull(df), None)
    return df


def upsert_stock_list(conn: sqlite3.Connection, df: pd.DataFrame):
    now = dt.datetime.now().isoformat(timespec="seconds")
    df = df.copy()
    df["updated_at"] = now
    df = clean_for_sqlite(df)

    sql = """
    INSERT INTO stock_list (stock_id, stock_name, market, industry, updated_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(stock_id) DO UPDATE SET
        stock_name = excluded.stock_name,
        market = excluded.market,
        industry = excluded.industry,
        updated_at = excluded.updated_at;
    """

    rows = list(df[["stock_id", "stock_name", "market", "industry", "updated_at"]].itertuples(index=False, name=None))
    conn.executemany(sql, rows)
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="tw_stock.db", help="SQLite db path")
    args = ap.parse_args()

    print(f"[INFO] Using DB: {args.db}")

    print("[INFO] Fetching TWSE stock list...")
    twse_df = load_twse()
    print(f"[INFO] TWSE normalized count: {len(twse_df)}")

    print("[INFO] Fetching TPEx stock list...")
    tpex_df = load_tpex()
    print(f"[INFO] TPEx normalized count: {len(tpex_df)}")

    stock_df = pd.concat([twse_df, tpex_df], ignore_index=True)
    stock_df = stock_df.drop_duplicates(subset=["stock_id"]).reset_index(drop=True)
    print(f"[INFO] Total unique stock count: {len(stock_df)}")

    conn = db_connect(args.db)
    try:
        ensure_schema(conn)
        upsert_stock_list(conn, stock_df)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM stock_list")
        cnt = cur.fetchone()[0]
        print(f"[OK] stock_list updated. row_count={cnt}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
