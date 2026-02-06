#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
plot_foreign_cost.py

Plot price + foreign cost line from SQLite DB (tw_stock.db).

Data source:
  table: daily_facts
  required columns:
    date, stock_id, close, foreign_cost_est

Usage:
  python plot_foreign_cost.py --stock 2330 --start 2025-12-01 --end 2025-12-15
"""

import argparse
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional


# ----------------------------
# Args
# ----------------------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="tw_stock.db", help="SQLite DB path")
    p.add_argument("--stock", required=True, help="Stock ID, e.g. 2330")
    p.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    p.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    p.add_argument("--show", action="store_true", help="Show plot window")
    p.add_argument("--out", default=None, help="Save figure to file (png)")
    return p.parse_args()


# ----------------------------
# Load data
# ----------------------------

def load_data(db_path: str, stock_id: str, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)

    sql = """
    SELECT
      date,
      stock_id,
      close,
      foreign_cost_est
    FROM daily_facts
    WHERE stock_id = ?
    """

    params = [stock_id]

    if start:
        sql += " AND date >= ?"
        params.append(start)
    if end:
        sql += " AND date <= ?"
        params.append(end)

    sql += " ORDER BY date"

    df = pd.read_sql(sql, conn, params=params)
    conn.close()

    if df.empty:
        raise RuntimeError("No data found for given conditions")

    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["foreign_cost_est"] = pd.to_numeric(df["foreign_cost_est"], errors="coerce")

    return df


# ----------------------------
# Plot
# ----------------------------

def plot_price_vs_cost(df: pd.DataFrame, stock_id: str, out: Optional[str], show: bool):
    plt.figure(figsize=(12, 6))

    # Price
    plt.plot(
        df["date"],
        df["close"],
        label="Close Price",
        linewidth=2
    )

    # Foreign cost line
    plt.plot(
        df["date"],
        df["foreign_cost_est"],
        label="Foreign Avg Cost",
        linewidth=2,
        linestyle="--"
    )

    plt.title(f"{stock_id} | Price vs Foreign Cost")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()

    if out:
        plt.savefig(out, dpi=150)
        print(f"[OK] Saved figure to {out}")

    if show:
        plt.show()

    plt.close()


# ----------------------------
# Main
# ----------------------------

def main():
    args = parse_args()

    df = load_data(
        db_path=args.db,
        stock_id=args.stock,
        start=args.start,
        end=args.end
    )

    plot_price_vs_cost(
        df=df,
        stock_id=args.stock,
        out=args.out,
        show=args.show
    )


if __name__ == "__main__":
    main()
