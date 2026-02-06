#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
tw_stock_pipeline2.py (TWSE legacy, chunk upsert, resumable + gap backfill)

Modes:
- --mode run  : normal pipeline (foreign+margin once, prices per stock)
- --mode gaps : scan DB for missing days per stock in [start,end], then backfill selected stocks

Checkpoint/Resume:
- pipeline_runs, pipeline_run_progress tables in SQLite
- --resume will continue from unfinished stocks for the same (db,start,end,market,mode=run)

Important limitation:
- TWSE legacy endpoints (twse.com.tw) cover TWSE-listed instruments.
  TPEx requires different endpoints for prices; thus --market TPEx is not supported here.
"""

from __future__ import annotations

import os
import sys
import time
import math
import random
import sqlite3
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple, Set

import pandas as pd
import requests


# ----------------------------
# Config
# ----------------------------

@dataclass(frozen=True)
class PipelineConfig:
    db_path: str = "tw_stock.db"
    http_timeout_sec: int = 30
    sleep_sec_between_calls: float = 0.6

    # Retry settings for transient server issues (429/5xx/504)
    max_attempts: int = 6
    base_backoff_sec: float = 1.2

    lot_size: int = 1000
    use_avg_price_for_cost: bool = True


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

def to_py(v: Any) -> Any:
    """Convert numpy/pandas scalars to plain Python types for sqlite binding."""
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    if hasattr(v, "to_pydatetime"):
        return v.to_pydatetime()
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    return v

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def iso_now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


# ----------------------------
# SQLite storage
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

-- Checkpoint tables
CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL,            -- run / gaps
  db_path TEXT NOT NULL,
  market TEXT NOT NULL,
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  status TEXT NOT NULL,          -- running / done / aborted
  note TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_run_progress (
  run_id TEXT NOT NULL,
  stock_id TEXT NOT NULL,
  status TEXT NOT NULL,          -- done / skipped / error
  updated_at TEXT NOT NULL,
  message TEXT,
  PRIMARY KEY (run_id, stock_id),
  FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_run_progress_run ON pipeline_run_progress(run_id);
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
        ON CONFLICT({",".join(pk_cols)}) DO UPDATE SET {set_clause};
        """
    else:
        sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT({",".join(pk_cols)}) DO NOTHING;
        """
    rows = [tuple(to_py(x) if not is_nan(x) else None for x in row)
            for row in df.itertuples(index=False, name=None)]
    conn.executemany(sql, rows)
    conn.commit()


# ----------------------------
# Run / Progress helpers
# ----------------------------

def compute_run_id(mode: str, db_path: str, market: str, start_date: str, end_date: str) -> str:
    key = f"{mode}|{os.path.abspath(db_path)}|{market.upper()}|{start_date}|{end_date}"
    return sha1(key)

def ensure_run(conn: sqlite3.Connection, run_id: str, mode: str, db_path: str,
               market: str, start_date: str, end_date: str) -> None:
    now = iso_now()
    row = conn.execute("SELECT run_id, status FROM pipeline_runs WHERE run_id = ?", (run_id,)).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO pipeline_runs(run_id, mode, db_path, market, start_date, end_date, created_at, updated_at, status, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running', NULL)
            """,
            (run_id, mode, os.path.abspath(db_path), market.upper(), start_date, end_date, now, now),
        )
    else:
        conn.execute("UPDATE pipeline_runs SET updated_at=?, status='running' WHERE run_id=?", (now, run_id))
    conn.commit()

def mark_run_done(conn: sqlite3.Connection, run_id: str, status: str = "done", note: Optional[str] = None) -> None:
    now = iso_now()
    conn.execute(
        "UPDATE pipeline_runs SET updated_at=?, status=?, note=? WHERE run_id=?",
        (now, status, note, run_id),
    )
    conn.commit()

def mark_stock_progress(conn: sqlite3.Connection, run_id: str, stock_id: str,
                        status: str, message: Optional[str] = None) -> None:
    now = iso_now()
    conn.execute(
        """
        INSERT INTO pipeline_run_progress(run_id, stock_id, status, updated_at, message)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(run_id, stock_id) DO UPDATE SET
          status=excluded.status,
          updated_at=excluded.updated_at,
          message=excluded.message
        """,
        (run_id, stock_id, status, now, message),
    )
    conn.commit()

def load_done_stocks(conn: sqlite3.Connection, run_id: str) -> Set[str]:
    df = pd.read_sql_query(
        "SELECT stock_id FROM pipeline_run_progress WHERE run_id=? AND status='done'",
        conn,
        params=(run_id,),
    )
    if df.empty:
        return set()
    return set(df["stock_id"].astype(str).str.strip().tolist())


# ----------------------------
# Read universe from DB
# ----------------------------

def load_universe_stock_ids(conn: sqlite3.Connection, market: str) -> List[str]:
    market = (market or "TWSE").upper()
    if market == "ALL":
        sql = "SELECT stock_id FROM stock_list WHERE stock_id IS NOT NULL"
        df = pd.read_sql_query(sql, conn)
    else:
        sql = "SELECT stock_id FROM stock_list WHERE market = ? AND stock_id IS NOT NULL"
        df = pd.read_sql_query(sql, conn, params=(market,))

    if df.empty:
        return []
    ids = df["stock_id"].astype(str).str.strip().tolist()

    out = []
    for x in ids:
        if x.isdigit() and 4 <= len(x) <= 6:
            out.append(x)
    return sorted(set(out))


# ----------------------------
# TWSE legacy datasource (soft fail + retries)
# ----------------------------

class TwseLegacy:
    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.session = requests.Session()

    @staticmethod
    def _to_float(x: Any) -> Optional[float]:
        if x is None:
            return None
        s = str(x).strip()
        if s in ("", "--", "NaN", "nan", "None"):
            return None
        try:
            return float(s.replace(",", ""))
        except Exception:
            return None

    @staticmethod
    def _parse_roc_date(roc: Any) -> str:
        s = str(roc).strip()
        if "/" not in s:
            return s[:10]
        y, mm, dd = s.split("/")
        y = int(y) + 1911
        return f"{y:04d}-{int(mm):02d}-{int(dd):02d}"

    @staticmethod
    def _iter_month_starts(start_date: str, end_date: str) -> List[str]:
        s = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
        e = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
        cur = dt.date(s.year, s.month, 1)
        out = []
        while cur <= e:
            out.append(cur.strftime("%Y%m%d"))
            cur = dt.date(cur.year + (cur.month // 12), (cur.month % 12) + 1, 1)
        return out

    def _get_json_soft(self, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        for attempt in range(1, self.cfg.max_attempts + 1):
            try:
                r = self.session.get(url, params=params, timeout=self.cfg.http_timeout_sec)
                if r.status_code in (429, 500, 502, 503, 504):
                    if attempt == self.cfg.max_attempts:
                        return None
                    sleep_s = self.cfg.base_backoff_sec * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
                    time.sleep(sleep_s)
                    continue
                if r.status_code >= 400:
                    return None
                return r.json()
            except (requests.Timeout, requests.ConnectionError):
                if attempt == self.cfg.max_attempts:
                    return None
                sleep_s = self.cfg.base_backoff_sec * (2 ** (attempt - 1)) + random.uniform(0, 0.8)
                time.sleep(sleep_s)
        return None

    def fetch_prices_one_stock(self, stock_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        base = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        month_starts = self._iter_month_starts(start_date, end_date)

        out: List[Dict[str, Any]] = []
        for m in month_starts:
            data = self._get_json_soft(base, {"response": "json", "date": m, "stockNo": stock_id})
            time.sleep(self.cfg.sleep_sec_between_calls)

            if not data or data.get("stat") != "OK":
                continue

            for r in data.get("data", []) or []:
                date_iso = self._parse_roc_date(r[0])
                if not (start_date <= date_iso <= end_date):
                    continue

                volume = self._to_float(r[1])
                amount = self._to_float(r[2])
                open_ = self._to_float(r[3])
                high = self._to_float(r[4])
                low = self._to_float(r[5])
                close = self._to_float(r[6])
                avg_price = (amount / volume) if (amount is not None and volume not in (None, 0)) else None

                out.append({
                    "date": date_iso,
                    "stock_id": stock_id,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "avg_price": avg_price,
                })

        return pd.DataFrame(out)

    def fetch_foreign_trading_all(self, start_date: str, end_date: str) -> pd.DataFrame:
        base = "https://www.twse.com.tw/fund/T86"

        def idx_map(fields: List[Any]) -> Dict[str, int]:
            return {str(f).strip(): i for i, f in enumerate(fields)}

        s = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
        e = dt.datetime.strptime(end_date, "%Y-%m-%d").date()

        out: List[Dict[str, Any]] = []
        cur = s
        while cur <= e:
            ymd = cur.strftime("%Y%m%d")
            data = self._get_json_soft(base, {"response": "json", "date": ymd, "selectType": "ALL"})
            time.sleep(self.cfg.sleep_sec_between_calls)

            if not data or data.get("stat") != "OK":
                cur += dt.timedelta(days=1)
                continue

            fields = data.get("fields") or []
            rows = data.get("data") or []
            if not fields or not rows:
                cur += dt.timedelta(days=1)
                continue

            m = idx_map(fields)

            sid_key = "證券代號"
            a_buy = "外陸資買進股數(不含外資自營商)"
            a_sell = "外陸資賣出股數(不含外資自營商)"
            a_net = "外陸資買賣超股數(不含外資自營商)"
            b_buy = "外資自營商買進股數"
            b_sell = "外資自營商賣出股數"
            b_net = "外資自營商買賣超股數"

            need = [sid_key, a_buy, a_sell, a_net, b_buy, b_sell, b_net]
            miss = [k for k in need if k not in m]
            if miss:
                raise RuntimeError(f"T86 schema changed. missing={miss}")

            for r in rows:
                sid = str(r[m[sid_key]]).strip()

                A_buy = self._to_float(r[m[a_buy]]) or 0.0
                A_sell = self._to_float(r[m[a_sell]]) or 0.0
                A_net = self._to_float(r[m[a_net]])
                if A_net is None:
                    A_net = A_buy - A_sell

                B_buy = self._to_float(r[m[b_buy]]) or 0.0
                B_sell = self._to_float(r[m[b_sell]]) or 0.0
                B_net = self._to_float(r[m[b_net]])
                if B_net is None:
                    B_net = B_buy - B_sell

                out.append({
                    "date": cur.isoformat(),
                    "stock_id": sid,
                    "foreign_buy": A_buy + B_buy,
                    "foreign_sell": A_sell + B_sell,
                    "foreign_net": A_net + B_net,
                })

            cur += dt.timedelta(days=1)

        return pd.DataFrame(out)

    def fetch_margin_all(self, start_date: str, end_date: str) -> pd.DataFrame:
        base = "https://www.twse.com.tw/exchangeReport/MI_MARGN"

        def idx_map(fields: List[Any]) -> Dict[str, int]:
            return {str(f).strip(): i for i, f in enumerate(fields)}

        s = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
        e = dt.datetime.strptime(end_date, "%Y-%m-%d").date()

        out: List[Dict[str, Any]] = []
        cur = s
        while cur <= e:
            ymd = cur.strftime("%Y%m%d")
            data = self._get_json_soft(base, {"response": "json", "date": ymd, "selectType": "ALL"})
            time.sleep(self.cfg.sleep_sec_between_calls)

            if not data or data.get("stat") != "OK":
                cur += dt.timedelta(days=1)
                continue

            fields = data.get("fields") or []
            rows = data.get("data") or []
            if not fields or not rows:
                cur += dt.timedelta(days=1)
                continue

            m = idx_map(fields)

            sid_key = "股票代號" if "股票代號" in m else ("證券代號" if "證券代號" in m else None)
            mar_key = "融資今日餘額"
            sht_key = "融券今日餘額"
            if sid_key is None or mar_key not in m or sht_key not in m:
                raise RuntimeError("MI_MARGN schema changed.")

            for r in rows:
                sid = str(r[m[sid_key]]).strip()
                out.append({
                    "date": cur.isoformat(),
                    "stock_id": sid,
                    "margin_balance": self._to_float(r[m[mar_key]]),
                    "short_balance": self._to_float(r[m[sht_key]]),
                })

            cur += dt.timedelta(days=1)

        return pd.DataFrame(out)


# ----------------------------
# Feature engineering (per stock)
# ----------------------------

def compute_features_one_stock(cfg: PipelineConfig, facts: pd.DataFrame) -> pd.DataFrame:
    if facts is None or facts.empty:
        return facts

    df = facts.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    df["stock_id"] = df["stock_id"].astype(str).str.strip()

    if "foreign_net" in df.columns:
        df["foreign_net_lot"] = pd.to_numeric(df["foreign_net"], errors="coerce") / cfg.lot_size

    if "short_balance" in df.columns and "margin_balance" in df.columns:
        short_ = pd.to_numeric(df["short_balance"], errors="coerce")
        margin_ = pd.to_numeric(df["margin_balance"], errors="coerce").replace(0, pd.NA)
        df["short_margin_ratio"] = short_ / margin_

    if "foreign_net" in df.columns:
        df = df.sort_values(["date"])
        streak = 0
        cons: List[int] = []
        nets = pd.to_numeric(df["foreign_net"], errors="coerce").fillna(0.0).tolist()
        for x in nets:
            if x > 0:
                streak += 1
            else:
                streak = 0
            cons.append(streak)
        df["foreign_consecutive_buy"] = cons

    price_col = "avg_price" if (cfg.use_avg_price_for_cost and "avg_price" in df.columns) else "close"
    if "foreign_net" in df.columns and price_col in df.columns:
        df = df.sort_values(["date"])
        hold = 0.0
        cost: Optional[float] = None
        holds: List[float] = []
        costs: List[Optional[float]] = []

        nets = pd.to_numeric(df["foreign_net"], errors="coerce").fillna(0.0).tolist()
        pxs = pd.to_numeric(df[price_col], errors="coerce").tolist()

        for net, px in zip(nets, pxs):
            px_ok = (px is not None) and (not is_nan(px))

            if net > 0 and px_ok:
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

            holds.append(hold)
            costs.append(cost)

        df["foreign_hold_est"] = holds
        df["foreign_cost_est"] = costs

    return df


# ----------------------------
# Gap detection helpers
# ----------------------------

def load_trading_calendar_from_db(conn: sqlite3.Connection, start_date: str, end_date: str) -> List[str]:
    """
    Build a trading day list from DB itself:
    - Use distinct dates that exist in daily_facts within range (any stock).
    This avoids requiring an external TWSE calendar.
    If DB is empty for that period, fallback to simple weekdays (best-effort).
    """
    df = pd.read_sql_query(
        """
        SELECT DISTINCT date
        FROM daily_facts
        WHERE date >= ? AND date <= ?
        ORDER BY date
        """,
        conn,
        params=(start_date, end_date),
    )
    if not df.empty:
        return df["date"].astype(str).tolist()

    # fallback: weekdays
    s = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
    e = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    out = []
    cur = s
    while cur <= e:
        if cur.weekday() < 5:
            out.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return out

def find_missing_stocks(conn: sqlite3.Connection, stock_ids: List[str],
                        trading_days: List[str], start_date: str, end_date: str,
                        min_missing: int = 1, limit: int = 200) -> pd.DataFrame:
    """
    Return a DataFrame: stock_id, present_days, expected_days, missing_days
    (sorted by missing_days desc)
    """
    expected = len(trading_days)
    if expected == 0 or not stock_ids:
        return pd.DataFrame(columns=["stock_id", "present_days", "expected_days", "missing_days"])

    # Count present days per stock in DB within range.
    # NOTE: It's fine even if some dates missing from trading_days; trading_days are the baseline.
    placeholders = ",".join(["?"] * len(stock_ids))
    df = pd.read_sql_query(
        f"""
        SELECT stock_id, COUNT(DISTINCT date) AS present_days
        FROM daily_facts
        WHERE date >= ? AND date <= ?
          AND stock_id IN ({placeholders})
        GROUP BY stock_id
        """,
        conn,
        params=[start_date, end_date] + stock_ids,
    )

    present_map = {str(r["stock_id"]): int(r["present_days"]) for _, r in df.iterrows()} if not df.empty else {}

    rows = []
    for sid in stock_ids:
        p = present_map.get(sid, 0)
        miss = max(0, expected - p)
        if miss >= min_missing:
            rows.append({"stock_id": sid, "present_days": p, "expected_days": expected, "missing_days": miss})

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values(["missing_days", "stock_id"], ascending=[False, True]).head(limit).reset_index(drop=True)
    return out


# ----------------------------
# Pipeline (run / gaps)
# ----------------------------

def run_mode_run(cfg: PipelineConfig, start_date: str, end_date: str, market: str,
                 resume: bool) -> None:
    market = (market or "TWSE").upper()
    if market in ("TPEX", "ALL"):
        raise SystemExit(
            "[ERROR] TWSE legacy mode supports only --market TWSE.\n"
            "        TPEx/ALL require different endpoints for prices."
        )

    conn = db_connect(cfg.db_path)
    try:
        db_init(conn)

        stock_ids = load_universe_stock_ids(conn, market=market)
        if not stock_ids:
            raise SystemExit(f"[ERROR] No stocks found in stock_list for market={market}.")

        run_id = compute_run_id("run", cfg.db_path, market, start_date, end_date)
        ensure_run(conn, run_id, "run", cfg.db_path, market, start_date, end_date)

        done = load_done_stocks(conn, run_id) if resume else set()
        todo = [sid for sid in stock_ids if sid not in done]

        print(f"[INFO] mode=run | market={market} | universe={len(stock_ids)} | remaining={len(todo)} | resume={resume} | run_id={run_id[:10]}...")

        ds = TwseLegacy(cfg)

        # Fetch foreign & margin once
        print(f"[INFO] Fetch foreign (T86) all-market: {start_date} ~ {end_date}")
        foreign_df = ds.fetch_foreign_trading_all(start_date, end_date)
        print(f"[INFO] foreign rows={0 if foreign_df is None else len(foreign_df)}")

        print(f"[INFO] Fetch margin (MI_MARGN) all-market: {start_date} ~ {end_date}")
        margin_df = ds.fetch_margin_all(start_date, end_date)
        print(f"[INFO] margin rows={0 if margin_df is None else len(margin_df)}")

        for d in (foreign_df, margin_df):
            if d is not None and not d.empty:
                d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.date.astype(str)
                d["stock_id"] = d["stock_id"].astype(str).str.strip()

        now = iso_now()
        total = len(todo)

        for i, sid in enumerate(todo, 1):
            if i == 1 or i % 50 == 0:
                print(f"[PRICE] processed {i}/{total} stocks ...")

            try:
                prices = ds.fetch_prices_one_stock(sid, start_date, end_date)
                if prices is None or prices.empty:
                    mark_stock_progress(conn, run_id, sid, "skipped", "no price rows")
                    continue

                prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date.astype(str)
                prices["stock_id"] = prices["stock_id"].astype(str).str.strip()

                f1 = foreign_df[foreign_df["stock_id"] == sid].copy() if foreign_df is not None and not foreign_df.empty else pd.DataFrame()
                m1 = margin_df[margin_df["stock_id"] == sid].copy() if margin_df is not None and not margin_df.empty else pd.DataFrame()

                facts = prices.copy()
                if not f1.empty:
                    facts = facts.merge(f1, on=["date", "stock_id"], how="outer")
                if not m1.empty:
                    facts = facts.merge(m1, on=["date", "stock_id"], how="outer")

                facts = facts[(facts["date"] >= start_date) & (facts["date"] <= end_date)].copy()
                facts = compute_features_one_stock(cfg, facts)
                facts["updated_at"] = now

                allowed = [
                    "date","stock_id",
                    "open","high","low","close","volume","avg_price",
                    "foreign_buy","foreign_sell","foreign_net",
                    "margin_balance","short_balance",
                    "foreign_net_lot","short_margin_ratio",
                    "foreign_consecutive_buy","foreign_hold_est","foreign_cost_est",
                    "updated_at"
                ]
                facts2 = facts[[c for c in allowed if c in facts.columns]].copy()
                upsert_df(conn, "daily_facts", facts2, pk_cols=["date", "stock_id"])

                mark_stock_progress(conn, run_id, sid, "done", f"rows={len(facts2)}")

            except Exception as e:
                # do not crash entire run
                mark_stock_progress(conn, run_id, sid, "error", f"{type(e).__name__}: {e}")
                continue

        mark_run_done(conn, run_id, "done", note="completed")
        print(f"[OK] mode=run done | DB={cfg.db_path} | range={start_date}~{end_date}")

    finally:
        conn.close()


def run_mode_gaps(cfg: PipelineConfig, start_date: str, end_date: str, market: str,
                  min_missing: int, max_stocks: int) -> None:
    market = (market or "TWSE").upper()
    if market in ("TPEX", "ALL"):
        raise SystemExit("[ERROR] gap mode supports only --market TWSE in this legacy script.")

    conn = db_connect(cfg.db_path)
    try:
        db_init(conn)

        stock_ids = load_universe_stock_ids(conn, market=market)
        if not stock_ids:
            raise SystemExit(f"[ERROR] No stocks found in stock_list for market={market}.")

        # Trading calendar baseline
        trading_days = load_trading_calendar_from_db(conn, start_date, end_date)
        print(f"[INFO] mode=gaps | market={market} | trading_days_baseline={len(trading_days)}")

        miss_df = find_missing_stocks(
            conn, stock_ids, trading_days, start_date, end_date,
            min_missing=min_missing, limit=max_stocks
        )

        if miss_df.empty:
            print("[OK] No missing stocks found (based on DB trading calendar baseline).")
            return

        run_id = compute_run_id("gaps", cfg.db_path, market, start_date, end_date)
        ensure_run(conn, run_id, "gaps", cfg.db_path, market, start_date, end_date)

        print(f"[INFO] Stocks to backfill={len(miss_df)} | min_missing={min_missing} | run_id={run_id[:10]}...")
        print(miss_df.head(20).to_string(index=False))

        ds = TwseLegacy(cfg)

        # We assume foreign/margin are already mostly present; but to be safe,
        # fetch them again for the range and merge as in run-mode.
        foreign_df = ds.fetch_foreign_trading_all(start_date, end_date)
        margin_df = ds.fetch_margin_all(start_date, end_date)
        for d in (foreign_df, margin_df):
            if d is not None and not d.empty:
                d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.date.astype(str)
                d["stock_id"] = d["stock_id"].astype(str).str.strip()

        now = iso_now()

        for idx, row in miss_df.iterrows():
            sid = str(row["stock_id"]).strip()
            try:
                prices = ds.fetch_prices_one_stock(sid, start_date, end_date)
                if prices is None or prices.empty:
                    mark_stock_progress(conn, run_id, sid, "skipped", "no price rows")
                    continue

                prices["date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date.astype(str)
                prices["stock_id"] = prices["stock_id"].astype(str).str.strip()

                f1 = foreign_df[foreign_df["stock_id"] == sid].copy() if foreign_df is not None and not foreign_df.empty else pd.DataFrame()
                m1 = margin_df[margin_df["stock_id"] == sid].copy() if margin_df is not None and not margin_df.empty else pd.DataFrame()

                facts = prices.copy()
                if not f1.empty:
                    facts = facts.merge(f1, on=["date", "stock_id"], how="outer")
                if not m1.empty:
                    facts = facts.merge(m1, on=["date", "stock_id"], how="outer")

                facts = facts[(facts["date"] >= start_date) & (facts["date"] <= end_date)].copy()
                facts = compute_features_one_stock(cfg, facts)
                facts["updated_at"] = now

                allowed = [
                    "date","stock_id",
                    "open","high","low","close","volume","avg_price",
                    "foreign_buy","foreign_sell","foreign_net",
                    "margin_balance","short_balance",
                    "foreign_net_lot","short_margin_ratio",
                    "foreign_consecutive_buy","foreign_hold_est","foreign_cost_est",
                    "updated_at"
                ]
                facts2 = facts[[c for c in allowed if c in facts.columns]].copy()
                upsert_df(conn, "daily_facts", facts2, pk_cols=["date", "stock_id"])

                mark_stock_progress(conn, run_id, sid, "done", f"rows={len(facts2)} missing_days={row['missing_days']}")

            except Exception as e:
                mark_stock_progress(conn, run_id, sid, "error", f"{type(e).__name__}: {e}")
                continue

        mark_run_done(conn, run_id, "done", note=f"gap backfill completed; min_missing={min_missing}")
        print("[OK] mode=gaps done.")

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
        "market": "TWSE",
        "mode": "run",          # run / gaps
        "resume": False,
        "min_missing": 5,       # gaps mode
        "max_stocks": 200,      # gaps mode
    }

    it = iter(argv[1:])
    for tok in it:
        if tok == "--db":
            args["db"] = next(it, "")
        elif tok == "--start":
            args["start"] = next(it, "")
        elif tok == "--end":
            args["end"] = next(it, "")
        elif tok == "--market":
            args["market"] = next(it, "TWSE")
        elif tok == "--mode":
            args["mode"] = next(it, "run")
        elif tok == "--resume":
            args["resume"] = True
        elif tok == "--min-missing":
            args["min_missing"] = int(next(it, "5"))
        elif tok == "--max-stocks":
            args["max_stocks"] = int(next(it, "200"))
        else:
            # ignore unknown token
            pass

    if not args["start"] or not args["end"]:
        raise SystemExit(
            "Usage:\n"
            "  Run (with resume):\n"
            "    python3 tw_stock_pipeline2.py --db tw_stock.db --start 2025-01-01 --end 2025-01-31 --market TWSE --mode run --resume\n"
            "\n"
            "  Gap backfill (top N stocks with >= min_missing days):\n"
            "    python3 tw_stock_pipeline2.py --db tw_stock.db --start 2025-01-01 --end 2025-01-31 --market TWSE --mode gaps --min-missing 5 --max-stocks 200\n"
        )

    args["mode"] = str(args["mode"]).lower()
    return args


def main(argv: List[str]) -> int:
    a = parse_args(argv)
    cfg = PipelineConfig(db_path=a["db"])

    if a["mode"] == "run":
        run_mode_run(cfg, start_date=a["start"], end_date=a["end"], market=a["market"], resume=a["resume"])
    elif a["mode"] == "gaps":
        run_mode_gaps(cfg, start_date=a["start"], end_date=a["end"], market=a["market"],
                      min_missing=a["min_missing"], max_stocks=a["max_stocks"])
    else:
        raise SystemExit(f"[ERROR] Unknown --mode {a['mode']}. Use run or gaps.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
