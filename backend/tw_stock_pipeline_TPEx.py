#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
tw_stock_pipeline_TPEx.py

TPEx (上櫃) historical pipeline with TWSE-like schema and "mode run / resume / gap".

Universe:
- Reads stock_ids from SQLite stock_list where market='TPEx'

Fetch (TPEx OpenAPI, JSON):
- Prices:  /tpex_mainboard_quotes
- Foreign: /tpex_3insti_daily_trading
- Margin:  /tpex_mainboard_margin_balance

Store:
- Upsert into SQLite daily_facts (PK: date, stock_id)
"""

from __future__ import annotations

import os
import sys
import math
import time
import json
import random
import sqlite3
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
import pandas as pd


# ----------------------------
# Config
# ----------------------------

@dataclass(frozen=True)
class PipelineConfig:
    db_path: str = "tw_stock.db"
    market: str = "TPEx"

    http_timeout_sec: int = 30
    sleep_sec_between_calls: float = 0.25
    retries: int = 6

    lot_size: int = 1000
    use_avg_price_for_cost: bool = True

    chunk_stocks: int = 50
    checkpoint_path: str = "tpex_pipeline_checkpoint.json"


# ----------------------------
# Utilities
# ----------------------------

def ensure_dir(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def is_nan(x: Any) -> bool:
    try:
        return x is None or (isinstance(x, float) and math.isnan(x))
    except Exception:
        return False

def parse_ymd(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

def daterange(start: dt.date, end: dt.date) -> List[dt.date]:
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += dt.timedelta(days=1)
    return out

def iter_dates(start: str, end: str) -> List[str]:
    s = parse_ymd(start)
    e = parse_ymd(end)
    return [d.isoformat() for d in daterange(s, e)]

def sqlite_scalar(v: Any) -> Any:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    try:
        import numpy as np
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
    except Exception:
        pass
    return v

def chunks(lst: List[str], n: int) -> List[List[str]]:
    if n <= 0:
        return [lst]
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def to_num_series(s: pd.Series) -> pd.Series:
    # robust numeric parse: remove commas/spaces
    return pd.to_numeric(s.astype(str).str.replace(",", "").str.strip(), errors="coerce")


# ----------------------------
# SQLite schema & helpers
# ----------------------------

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS stock_list (
  stock_id TEXT PRIMARY KEY,
  stock_name TEXT,
  market TEXT,
  industry TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS daily_facts (
  date TEXT NOT NULL,
  stock_id TEXT NOT NULL,

  open REAL,
  high REAL,
  low REAL,
  close REAL,
  volume REAL,
  avg_price REAL,

  foreign_buy REAL,
  foreign_sell REAL,
  foreign_net REAL,

  margin_balance REAL,
  short_balance REAL,

  -- derived
  foreign_net_lot REAL,
  short_margin_ratio REAL,
  foreign_consecutive_buy INTEGER,
  foreign_hold_est REAL,
  foreign_cost_est REAL,

  updated_at TEXT,

  PRIMARY KEY (date, stock_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_facts_stock_date ON daily_facts(stock_id, date);
"""

def db_connect(db_path: str) -> sqlite3.Connection:
    ensure_dir(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def db_init(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()

def upsert_df(conn: sqlite3.Connection, table: str, df: pd.DataFrame, pk_cols: List[str]) -> None:
    if df is None or df.empty:
        return

    cols = list(df.columns)
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join(cols)
    update_cols = [c for c in cols if c not in pk_cols]

    if update_cols:
        set_clause = ",".join([f"{c}=excluded.{c}" for c in update_cols])
        sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT({",".join(pk_cols)}) DO UPDATE SET
          {set_clause};
        """
    else:
        sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT({",".join(pk_cols)}) DO NOTHING;
        """

    rows = [tuple(sqlite_scalar(v) for v in row) for row in df.itertuples(index=False, name=None)]
    conn.executemany(sql, rows)
    conn.commit()

def load_universe_stock_ids(conn: sqlite3.Connection, market: str) -> List[str]:
    cur = conn.execute(
        "SELECT stock_id FROM stock_list WHERE market = ? ORDER BY stock_id",
        (market,),
    )
    return [str(r[0]).strip() for r in cur.fetchall() if str(r[0]).strip()]


# ----------------------------
# DataSource interface
# ----------------------------

class DataSource:
    def fetch_prices(self, start_date: str, end_date: str, stock_ids: List[str]) -> pd.DataFrame:
        raise NotImplementedError

    def fetch_foreign_trading(self, start_date: str, end_date: str, stock_ids: List[str]) -> pd.DataFrame:
        raise NotImplementedError

    def fetch_margin(self, start_date: str, end_date: str, stock_ids: List[str]) -> pd.DataFrame:
        raise NotImplementedError


# ----------------------------
# TPEx OpenAPI DataSource
# ----------------------------

class TPExOpenAPIDataSource(DataSource):
    OPENAPI_BASE = "https://www.tpex.org.tw/openapi/v1"

    # Verified/assumed paths (based on your schema snippets)
    PATH_QUOTES = "/tpex_mainboard_quotes"
    PATH_FOREIGN = "/tpex_3insti_daily_trading"
    PATH_MARGIN = "/tpex_mainboard_margin_balance"



    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 ...",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43.php?l=zh-tw",
        })

        # (path, date_iso) -> DataFrame
        self._openapi_daily_cache: dict[tuple[str, str], pd.DataFrame] = {}

    def _get_json_with_retries(self, path: str, params: Dict[str, Any]) -> Any:
        url = self.OPENAPI_BASE + path
        last_err: Optional[Exception] = None

        for i in range(self.cfg.retries):
            try:
                r = self.session.get(url, params=params, timeout=self.cfg.http_timeout_sec)
                text = r.text or ""
                if r.status_code >= 400:
                    preview = text[:220].replace("\n", " ").replace("\r", " ")
                    raise requests.HTTPError(f"{r.status_code} {preview}", response=r)

                # OpenAPI sometimes returns "[]" or "" — treat empty as empty list
                if not text.strip():
                    return []

                return r.json()

            except Exception as e:
                last_err = e
                sleep_s = min(8.0, (0.8 * (2 ** i)) + random.random() * 0.35)
                print(f"[WARN] OpenAPI retry {i+1}/{self.cfg.retries} path={path} params={params} err={e} sleep={sleep_s:.2f}s")
                time.sleep(sleep_s)

        raise RuntimeError(f"OpenAPI GET failed after retries. path={path} params={params} err={last_err}")

    def _fetch_openapi_daily_table(self, path: str, date_iso: str) -> pd.DataFrame:
        """
        單日抓表：OpenAPI 已用 date 參數限制日期，因此不應再用 payload['Date'] 過濾。
        只做：欄位去空白 + 統一補上 date_iso。
        """
        key = (path, date_iso)
        if key in self._openapi_daily_cache:
            return self._openapi_daily_cache[key].copy()
        
        candidate_params = [
            {"date": date_iso},   # 你的 debug 已證實這個就會中
            {"Date": date_iso},
            {"d": date_iso},
            {"dt": date_iso},
            {"qdate": date_iso},
        ]

        last_err = None
        for p in candidate_params:
            try:
                payload = self._get_openapi_json(path, params=p)

                # 有些 API 會包在 dict['data']
                if isinstance(payload, dict) and "data" in payload:
                    payload = payload["data"]

                if isinstance(payload, list):
                    df = pd.DataFrame(payload)
                elif isinstance(payload, dict):
                    df = pd.DataFrame([payload])
                else:
                    df = pd.DataFrame()

                if df.empty:
                    continue

                # 欄位名去前後空白（你 tpex_3insti_daily_trading 的 SELL 欄位真的有前導空白）
                df = df.rename(columns=lambda c: str(c).strip())

                # 單日端點：直接以查詢日期當作 date（不要 parse payload['Date']）
                df["date"] = date_iso

                # 存 cache
                self._openapi_daily_cache[key] = df.copy()

                return df

            except Exception as e:
                last_err = e
                continue




        print(f"[WARN] OpenAPI daily table empty/failed path={path} date={date_iso} err={last_err}")
        return pd.DataFrame()



    def _get_openapi_json(self, path: str, params: dict) -> object:
        """
        Robust OpenAPI fetch:
        - Accept JSON or CSV (TPEx occasionally returns non-JSON)
        - On non-JSON, parse CSV into list[dict]
        """
        base = "https://www.tpex.org.tw/openapi/v1"
        url = base + path

        last_err = None
        for i in range(self.cfg.retries):
            try:
                r = self.session.get(url, params=params, timeout=self.cfg.http_timeout_sec)
                ct = (r.headers.get("content-type") or "").lower()
                text = r.text or ""

                if r.status_code >= 400:
                    preview = text[:200].replace("\n", " ")
                    raise requests.HTTPError(f"{r.status_code} ct={ct} preview={preview}", response=r)

                if not text.strip():
                    raise ValueError(f"Empty body ct={ct}")

                # 1) If content-type says JSON OR the body looks like JSON, parse JSON
                t0 = text.lstrip()
                looks_json = t0.startswith("{") or t0.startswith("[")
                if "application/json" in ct or looks_json:
                    try:
                        return r.json()
                    except Exception as e:
                        # fall through to CSV attempt
                        last_err = e

                # 2) CSV fallback: TPEx sometimes returns CSV/plain text
                # Typical CSV header starts with "Date," or "資料日期,"
                head = text[:200].replace("\r", "")
                if "," in head and ("date" in head.lower() or "資料日期" in head):
                    df = pd.read_csv(io.StringIO(text))
                    # normalize column names (strip)
                    df = df.rename(columns=lambda c: str(c).strip())
                    return df.to_dict(orient="records")

                # 3) Not JSON / not CSV: surface preview for debugging
                preview = text[:200].replace("\n", " ")
                raise ValueError(f"Non-JSON/Non-CSV ct={ct} preview={preview}")

            except Exception as e:
                last_err = e
                sleep_s = min(8.0, (0.8 * (2 ** i)) + random.random() * 0.35)
                print(f"[WARN] OpenAPI retry {i+1}/{self.cfg.retries} url={url} params={params} err={e} sleep={sleep_s:.2f}s")
                time.sleep(sleep_s)

        raise RuntimeError(f"OpenAPI failed after retries url={url} params={params} err={last_err}")
    # -------- prices --------

    def fetch_prices(self, start_date: str, end_date: str, stock_ids: List[str]) -> pd.DataFrame:
        want = set(str(s).strip() for s in stock_ids)
        out = []
        for d in iter_dates(start_date, end_date):
            df = self._fetch_openapi_daily_table("/tpex_mainboard_quotes", d)
            if df.empty:
                time.sleep(self.cfg.sleep_sec_between_calls)
                continue

            code_col = "SecuritiesCompanyCode"
            df["stock_id"] = df[code_col].astype(str).str.strip()
            df = df[df["stock_id"].isin(want)]
            if df.empty:
                time.sleep(self.cfg.sleep_sec_between_calls)
                continue

            def to_num(s: pd.Series) -> pd.Series:
                return pd.to_numeric(s.astype(str).str.replace(",", "").str.strip(), errors="coerce")

            close = to_num(df["Close"]) if "Close" in df.columns else pd.NA
            open_ = to_num(df["Open"]) if "Open" in df.columns else pd.NA
            high  = to_num(df["High"]) if "High" in df.columns else pd.NA
            low   = to_num(df["Low"]) if "Low" in df.columns else pd.NA
            vol   = to_num(df["TradingShares"]) if "TradingShares" in df.columns else pd.NA
            amt   = to_num(df["TransactionAmount"]) if "TransactionAmount" in df.columns else pd.NA
            avgp  = (amt / vol).where(vol.notna() & (vol != 0), pd.NA)

            out.append(pd.DataFrame({
                "date": d,
                "stock_id": df["stock_id"],
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": vol,
                "avg_price": avgp,
            }))

            time.sleep(self.cfg.sleep_sec_between_calls)

        return pd.concat(out, ignore_index=True) if out else pd.DataFrame(
            columns=["date","stock_id","open","high","low","close","volume","avg_price"]
        )


    # -------- foreign --------

    def fetch_foreign_trading(self, start_date: str, end_date: str, stock_ids: List[str]) -> pd.DataFrame:
        want = set(str(s).strip() for s in stock_ids)
        out = []

        # canonical keys (strip 後)
        BUY_COL  = "Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Total Buy"
        SELL_COL = "Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Total Sell"
        NET_COL  = "Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference"

        for d in iter_dates(start_date, end_date):
            df = self._fetch_openapi_daily_table(self.PATH_FOREIGN, d)
            if df.empty:
                time.sleep(self.cfg.sleep_sec_between_calls)
                continue

            if "SecuritiesCompanyCode" not in df.columns:
                time.sleep(self.cfg.sleep_sec_between_calls)
                continue

            df["stock_id"] = df["SecuritiesCompanyCode"].astype(str).str.strip()
            df = df[df["stock_id"].isin(want)]
            if df.empty:
                time.sleep(self.cfg.sleep_sec_between_calls)
                continue

            fb = to_num_series(df[BUY_COL])  if BUY_COL  in df.columns else pd.Series([pd.NA] * len(df))
            fs = to_num_series(df[SELL_COL]) if SELL_COL in df.columns else pd.Series([pd.NA] * len(df))
            fn = to_num_series(df[NET_COL])  if NET_COL  in df.columns else (fb - fs)

            out.append(pd.DataFrame({
                "date": df["date"].astype(str),
                "stock_id": df["stock_id"],
                "foreign_buy": fb,
                "foreign_sell": fs,
                "foreign_net": fn,
            }))

            time.sleep(self.cfg.sleep_sec_between_calls)

        return pd.concat(out, ignore_index=True) if out else pd.DataFrame(
            columns=["date", "stock_id", "foreign_buy", "foreign_sell", "foreign_net"]
        )

    # -------- margin --------

    def fetch_margin(self, start_date: str, end_date: str, stock_ids: List[str]) -> pd.DataFrame:
        want = set(str(s).strip() for s in stock_ids)
        out = []

        for d in iter_dates(start_date, end_date):
            df = self._fetch_openapi_daily_table(self.PATH_MARGIN, d)
            if df.empty:
                time.sleep(self.cfg.sleep_sec_between_calls)
                continue

            if "SecuritiesCompanyCode" not in df.columns:
                time.sleep(self.cfg.sleep_sec_between_calls)
                continue

            df["stock_id"] = df["SecuritiesCompanyCode"].astype(str).str.strip()
            df = df[df["stock_id"].isin(want)]
            if df.empty:
                time.sleep(self.cfg.sleep_sec_between_calls)
                continue

            mp = to_num_series(df["MarginPurchaseBalance"]) if "MarginPurchaseBalance" in df.columns else pd.Series([pd.NA] * len(df))
            ss = to_num_series(df["ShortSaleBalance"])      if "ShortSaleBalance"      in df.columns else pd.Series([pd.NA] * len(df))

            # (張) -> shares (你的 schema 註解是 shares)
            margin_shares = mp * self.cfg.lot_size
            short_shares  = ss * self.cfg.lot_size

            out.append(pd.DataFrame({
                "date": df["date"].astype(str),
                "stock_id": df["stock_id"],
                "margin_balance": margin_shares,
                "short_balance": short_shares,
            }))

            time.sleep(self.cfg.sleep_sec_between_calls)

        return pd.concat(out, ignore_index=True) if out else pd.DataFrame(
            columns=["date", "stock_id", "margin_balance", "short_balance"]
        )


# ----------------------------
# Feature engineering (same as TWSE pipeline)
# ----------------------------

def compute_features(cfg: PipelineConfig, facts: pd.DataFrame) -> pd.DataFrame:
    if facts is None or facts.empty:
        return facts

    df = facts.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)

    if "foreign_net" in df.columns:
        df["foreign_net_lot"] = pd.to_numeric(df["foreign_net"], errors="coerce") / cfg.lot_size

    if "short_balance" in df.columns and "margin_balance" in df.columns:
        short_ = pd.to_numeric(df["short_balance"], errors="coerce")
        margin_ = pd.to_numeric(df["margin_balance"], errors="coerce").replace(0, pd.NA)
        df["short_margin_ratio"] = short_ / margin_

    if "foreign_net" in df.columns:
        df = df.sort_values(["stock_id", "date"])
        cons: List[int] = []
        for _, g in df.groupby("stock_id", sort=False):
            streak = 0
            nets = pd.to_numeric(g["foreign_net"], errors="coerce").fillna(0.0).tolist()
            for x in nets:
                if x > 0:
                    streak += 1
                else:
                    streak = 0
                cons.append(streak)
        df["foreign_consecutive_buy"] = cons

    price_col = "avg_price" if (cfg.use_avg_price_for_cost and "avg_price" in df.columns) else "close"
    if "foreign_net" in df.columns and price_col in df.columns:
        df = df.sort_values(["stock_id", "date"])
        hold_list: List[float] = []
        cost_list: List[Optional[float]] = []

        for _, g in df.groupby("stock_id", sort=False):
            hold = 0.0
            cost: Optional[float] = None

            nets = pd.to_numeric(g["foreign_net"], errors="coerce").fillna(0.0).tolist()
            pxs = pd.to_numeric(g[price_col], errors="coerce").tolist()

            for net, px in zip(nets, pxs):
                px_is_valid = (px is not None) and (not is_nan(px))
                if net > 0 and px_is_valid:
                    if cost is None or hold <= 0:
                        hold = float(net)
                        cost = float(px)
                    else:
                        new_hold = hold + float(net)
                        cost = (hold * cost + float(net) * float(px)) / new_hold
                        hold = new_hold
                elif net < 0:
                    hold = max(0.0, hold + float(net))
                    if hold == 0.0:
                        cost = None

                hold_list.append(hold)
                cost_list.append(cost)

        df["foreign_hold_est"] = hold_list
        df["foreign_cost_est"] = cost_list

    return df


# ----------------------------
# Pipeline assembly
# ----------------------------

def build_daily_facts(prices: pd.DataFrame, foreign: pd.DataFrame, margin: pd.DataFrame) -> pd.DataFrame:
    for d in [prices, foreign, margin]:
        if d is not None and not d.empty:
            d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.date.astype(str)
            d["stock_id"] = d["stock_id"].astype(str).str.strip()

    df: Optional[pd.DataFrame] = None
    for part in [prices, foreign, margin]:
        if part is None or part.empty:
            continue
        df = part if df is None else df.merge(part, on=["date", "stock_id"], how="outer")

    return pd.DataFrame() if df is None else df

def allowed_daily_facts_columns() -> List[str]:
    return [
        "date", "stock_id",
        "open", "high", "low", "close", "volume", "avg_price",
        "foreign_buy", "foreign_sell", "foreign_net",
        "margin_balance", "short_balance",
        "foreign_net_lot", "short_margin_ratio",
        "foreign_consecutive_buy", "foreign_hold_est", "foreign_cost_est",
        "updated_at",
    ]


# ----------------------------
# Modes
# ----------------------------

def save_checkpoint(cfg: PipelineConfig, payload: Dict[str, Any]) -> None:
    with open(cfg.checkpoint_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def load_checkpoint(cfg: PipelineConfig) -> Optional[Dict[str, Any]]:
    if not os.path.exists(cfg.checkpoint_path):
        return None
    try:
        with open(cfg.checkpoint_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def run_once(cfg: PipelineConfig, ds: DataSource, conn: sqlite3.Connection,
             stock_ids: List[str], start: str, end: str) -> None:
    prices = ds.fetch_prices(start, end, stock_ids=stock_ids)
    foreign = ds.fetch_foreign_trading(start, end, stock_ids=stock_ids)
    margin = ds.fetch_margin(start, end, stock_ids=stock_ids)

    facts = build_daily_facts(prices, foreign, margin)
    facts = compute_features(cfg, facts)
    if facts is None or facts.empty:
        return

    now = dt.datetime.now().isoformat(timespec="seconds")
    facts = facts.copy()
    facts["updated_at"] = now

    cols = allowed_daily_facts_columns()
    facts2 = facts[[c for c in cols if c in facts.columns]].copy()

    # Force numerics
    for c in [
        "open", "high", "low", "close", "volume", "avg_price",
        "foreign_buy", "foreign_sell", "foreign_net",
        "margin_balance", "short_balance",
        "foreign_net_lot", "short_margin_ratio",
        "foreign_hold_est", "foreign_cost_est",
    ]:
        if c in facts2.columns:
            facts2[c] = pd.to_numeric(facts2[c], errors="coerce")

    upsert_df(conn, "daily_facts", facts2, pk_cols=["date", "stock_id"])

def mode_run(cfg: PipelineConfig, ds: DataSource, start: str, end: str) -> None:
    conn = db_connect(cfg.db_path)
    try:
        db_init(conn)
        universe = load_universe_stock_ids(conn, cfg.market)
        print(f"[INFO] Universe market={cfg.market} | stocks={len(universe)}")

        batches = chunks(universe, cfg.chunk_stocks)
        for bi, batch in enumerate(batches, start=1):
            lo = (bi - 1) * cfg.chunk_stocks + 1
            hi = min(bi * cfg.chunk_stocks, len(universe))
            print(f"[RUN] batch {bi}/{len(batches)} | stocks {lo}-{hi}")

            run_once(cfg, ds, conn, batch, start, end)

            save_checkpoint(cfg, {
                "mode": "run",
                "market": cfg.market,
                "db": cfg.db_path,
                "start": start,
                "end": end,
                "chunk_stocks": cfg.chunk_stocks,
                "last_batch_done": bi,
                "total_batches": len(batches),
                "ts": dt.datetime.now().isoformat(timespec="seconds"),
            })
    finally:
        conn.close()

def mode_resume(cfg: PipelineConfig, ds: DataSource, start: str, end: str) -> None:
    ck = load_checkpoint(cfg)
    if not ck:
        print("[RESUME] No checkpoint found. Falling back to --mode run.")
        mode_run(cfg, ds, start, end)
        return

    last_done = int(ck.get("last_batch_done", 0))
    print(f"[RESUME] checkpoint found. last_batch_done={last_done}")

    conn = db_connect(cfg.db_path)
    try:
        db_init(conn)
        universe = load_universe_stock_ids(conn, cfg.market)
        print(f"[INFO] Universe market={cfg.market} | stocks={len(universe)}")

        batches = chunks(universe, cfg.chunk_stocks)
        for bi, batch in enumerate(batches, start=1):
            if bi <= last_done:
                continue
            lo = (bi - 1) * cfg.chunk_stocks + 1
            hi = min(bi * cfg.chunk_stocks, len(universe))
            print(f"[RESUME] batch {bi}/{len(batches)} | stocks {lo}-{hi}")

            run_once(cfg, ds, conn, batch, start, end)

            save_checkpoint(cfg, {
                "mode": "resume",
                "market": cfg.market,
                "db": cfg.db_path,
                "start": start,
                "end": end,
                "chunk_stocks": cfg.chunk_stocks,
                "last_batch_done": bi,
                "total_batches": len(batches),
                "ts": dt.datetime.now().isoformat(timespec="seconds"),
            })
    finally:
        conn.close()

def mode_gap(cfg: PipelineConfig, ds: DataSource, start: str, end: str) -> None:
    conn = db_connect(cfg.db_path)
    try:
        db_init(conn)
        universe = load_universe_stock_ids(conn, cfg.market)
        print(f"[INFO] Universe market={cfg.market} | stocks={len(universe)}")

        cur = conn.execute(
            """
            SELECT DISTINCT df.stock_id
            FROM daily_facts df
            JOIN stock_list sl ON sl.stock_id = df.stock_id
            WHERE sl.market = ?
              AND df.date >= ?
              AND df.date <= ?
            """,
            (cfg.market, start, end),
        )
        have = set(str(r[0]).strip() for r in cur.fetchall())
        missing = [sid for sid in universe if sid not in have]

        print(f"[GAP] stocks with 0 rows in range: {len(missing)}")
        if not missing:
            return

        batches = chunks(missing, cfg.chunk_stocks)
        for bi, batch in enumerate(batches, start=1):
            lo = (bi - 1) * cfg.chunk_stocks + 1
            hi = min(bi * cfg.chunk_stocks, len(missing))
            print(f"[GAP] batch {bi}/{len(batches)} | stocks {lo}-{hi}")
            run_once(cfg, ds, conn, batch, start, end)
    finally:
        conn.close()


# ----------------------------
# CLI
# ----------------------------

def parse_args(argv: List[str]) -> Dict[str, Any]:
    args: Dict[str, Any] = {
        "db": "tw_stock.db",
        "start": None,
        "end": None,
        "mode": "run",
        "chunk_stocks": 50,
        "sleep": 0.25,
        "timeout": 30,
        "retries": 6,
        "checkpoint": "tpex_pipeline_checkpoint.json",
    }

    it = iter(argv)
    for tok in it:
        if tok == "--db":
            args["db"] = next(it)
        elif tok == "--start":
            args["start"] = next(it)
        elif tok == "--end":
            args["end"] = next(it)
        elif tok == "--mode":
            args["mode"] = next(it)
        elif tok == "--chunk-stocks":
            args["chunk_stocks"] = int(next(it))
        elif tok == "--sleep":
            args["sleep"] = float(next(it))
        elif tok == "--timeout":
            args["timeout"] = int(next(it))
        elif tok == "--retries":
            args["retries"] = int(next(it))
        elif tok == "--checkpoint":
            args["checkpoint"] = next(it)
        elif tok in ("-h", "--help"):
            print(
                "Usage:\n"
                "  python3 tw_stock_pipeline_TPEx.py --db tw_stock.db --start YYYY-MM-DD --end YYYY-MM-DD "
                "--mode run|resume|gap --chunk-stocks 200\n"
            )
            raise SystemExit(0)

    if not args["start"] or not args["end"]:
        raise SystemExit("Error: --start and --end are required (YYYY-MM-DD).")

    return args

def main(argv: List[str]) -> int:
    a = parse_args(argv)

    cfg = PipelineConfig(
        db_path=a["db"],
        market="TPEx",
        chunk_stocks=a["chunk_stocks"],
        sleep_sec_between_calls=a["sleep"],
        http_timeout_sec=a["timeout"],
        retries=a["retries"],
        checkpoint_path=a["checkpoint"],
    )

    start = a["start"]
    end = a["end"]
    mode = a["mode"].lower()

    ds = TPExOpenAPIDataSource(cfg)

    if mode == "run":
        mode_run(cfg, ds, start, end)
    elif mode == "resume":
        mode_resume(cfg, ds, start, end)
    elif mode == "gap":
        mode_gap(cfg, ds, start, end)
    else:
        raise SystemExit(f"Unknown --mode {mode}. Use run|resume|gap.")

    print(f"[OK] Updated DB={cfg.db_path} market={cfg.market} range={start}~{end} mode={mode}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
