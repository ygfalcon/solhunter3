#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import datetime


def main() -> None:
    ap = argparse.ArgumentParser(description="Summarize recent trades from memory.db")
    ap.add_argument("--db", default="memory.db", help="Path to SQLite memory DB (default: memory.db)")
    ap.add_argument("--limit", type=int, default=20, help="Number of rows to show (default: 20)")
    args = ap.parse_args()

    path = args.db
    if not os.path.isabs(path):
        path = os.path.join(os.getcwd(), path)
    if not os.path.exists(path):
        print(f"No memory DB found at {path}")
        return

    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT id, token, direction, amount, price, reason, created_at
            FROM trades
            ORDER BY id DESC
            LIMIT ?
            """,
            (args.limit,),
        )
        rows = cur.fetchall()
        if not rows:
            print("No trades recorded yet.")
            return
        for r in rows:
            ts = r["created_at"] or ""
            try:
                ts = str(ts)
            except Exception:
                ts = ""
            print(
                f"#{r['id']:>5} | {ts:19} | {r['token']} | {r['direction']:<4} | amt={float(r['amount']):.6f} @ {float(r['price']):.6f} | agent={r['reason'] or ''}"
            )
    finally:
        con.close()


if __name__ == "__main__":
    main()

