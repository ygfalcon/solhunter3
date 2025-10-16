#!/usr/bin/env python3
"""Ensure tests/data/prices_long.json contains recent entries."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

DATA_PATH = Path("tests/data/prices_long.json")
# Allow overriding the max age via env for flexibility
MAX_AGE_MONTHS = int(os.environ.get("PRICES_MAX_AGE_MONTHS", "6"))


def main() -> None:
    if not DATA_PATH.exists():
        print(f"Missing {DATA_PATH}. Please refresh the test data.")
        sys.exit(1)

    try:
        with DATA_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as exc:  # pragma: no cover - parsing failures
        print(f"Failed to load {DATA_PATH}: {exc}")
        sys.exit(1)

    if not data:
        print(f"{DATA_PATH} contains no entries. Please refresh the test data.")
        sys.exit(1)

    try:
        latest = max(datetime.strptime(item["date"], "%Y-%m-%d") for item in data)
    except Exception as exc:
        print(f"Error reading dates from {DATA_PATH}: {exc}")
        sys.exit(1)

    cutoff = datetime.today() - timedelta(days=MAX_AGE_MONTHS * 30)
    if latest < cutoff:
        print(
            f"{DATA_PATH} is stale. Most recent entry {latest.date()} is older than"
            f" {MAX_AGE_MONTHS} months. Please refresh the dataset."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
