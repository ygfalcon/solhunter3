#!/usr/bin/env python3
"""Scan the repository for placeholder secrets."""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

ROOT = Path(__file__).resolve().parent.parent

PLACEHOLDER_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"REDACTED", re.IGNORECASE),
    re.compile(r"YOUR_[A-Z0-9_]+"),
    re.compile(r"EXAMPLE_[A-Z0-9_]+"),
    re.compile(r"EXAMPLE\\.COM", re.IGNORECASE),
    re.compile(r"XXXX", re.IGNORECASE),
)

# Allow legitimate references where the code intentionally matches placeholders.
ALLOWLIST: dict[str, tuple[str, ...]] = {
    "solhunter_zero/config.py": ("YOUR_KEY", "demo-helius-key"),
    "scripts/audit_placeholders.py": ("REDACTED", "XXXX", "demo-helius-key", "YOUR_SECRET"),
    "solhunter_zero/production/env.py": ("REDACTED", "redacted", "xxxx"),
    "tests/test_audit_placeholders.py": ("YOUR_SECRET",),
    "tests/test_production_env.py": ("REDACTED_VALUE", "YOUR_SECRET_KEY"),
}

SKIP_DIRS = {".git", "__pycache__", ".mypy_cache", ".pytest_cache", "node_modules", "artifacts", "build", "dist"}


@dataclass
class PlaceholderMatch:
    path: Path
    line: int
    pattern: str
    snippet: str

    def to_dict(self) -> dict[str, object]:
        return {
            "path": str(self.path),
            "line": self.line,
            "pattern": self.pattern,
            "snippet": self.snippet,
        }


def _iter_files(root: Path) -> Iterator[Path]:
    for path in root.rglob("*"):
        if path.is_dir():
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        yield path


def _is_allowed(path: Path, text: str) -> bool:
    try:
        rel = path.relative_to(ROOT)
    except ValueError:
        rel = path
    allow = ALLOWLIST.get(str(rel))
    if not allow:
        return False
    return any(token in text for token in allow)


def scan_for_placeholders(paths: Sequence[Path]) -> list[PlaceholderMatch]:
    matches: list[PlaceholderMatch] = []
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for idx, line in enumerate(text.splitlines(), start=1):
            for pattern in PLACEHOLDER_PATTERNS:
                if not pattern.search(line):
                    continue
                if _is_allowed(path, line):
                    continue
                try:
                    rel_path = path.relative_to(ROOT)
                except ValueError:
                    rel_path = path
                matches.append(
                    PlaceholderMatch(
                        path=rel_path,
                        line=idx,
                        pattern=pattern.pattern,
                        snippet=line.strip(),
                    )
                )
    return matches


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit placeholder secrets")
    parser.add_argument("paths", nargs="*", type=Path, help="Paths to scan", default=[ROOT])
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    targets: list[Path] = []
    for p in args.paths:
        if not p.is_absolute():
            p = ROOT / p
        if p.is_dir():
            targets.extend(_iter_files(p))
        else:
            targets.append(p)
    matches = scan_for_placeholders(targets)
    if args.__dict__.get("json"):
        print(json.dumps([m.to_dict() for m in matches], indent=2))
    else:
        if matches:
            print("Placeholder secrets detected:")
            for match in matches:
                print(f"  {match.path}:{match.line}: {match.pattern} :: {match.snippet}")
        else:
            print("No placeholder secrets detected.")
    return 1 if matches else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
