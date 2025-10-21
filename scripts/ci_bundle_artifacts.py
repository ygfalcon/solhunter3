#!/usr/bin/env python3
"""Bundle CI artifacts and synthesize ending-value reports."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect CI artifacts, copy logs/metrics into a single directory, "
            "and emit an ending-value summary derived from junit reports."
        )
    )
    parser.add_argument(
        "--job-name",
        required=True,
        help="Identifier for the CI job (included in the summary metadata).",
    )
    parser.add_argument(
        "--output",
        default="ci_artifacts",
        help="Destination directory for the bundled artifacts.",
    )
    parser.add_argument(
        "--junit-path",
        action="append",
        dest="junit_paths",
        default=[],
        help="Path to a junit XML report to bundle (can be repeated).",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help=(
            "Additional files or directories to copy into the artifact bundle. "
            "Supports glob patterns relative to the repository root."
        ),
    )
    parser.add_argument(
        "--metadata",
        action="append",
        default=[],
        help="Optional extra metadata in the form key=value for the summary file.",
    )
    return parser.parse_args()


def _expand_patterns(patterns: Iterable[str]) -> Iterator[Path]:
    seen: set[Path] = set()
    cwd = Path.cwd()
    for pattern in patterns:
        if not pattern:
            continue
        if any(ch in pattern for ch in "*?[]"):
            matches = cwd.glob(pattern)
        else:
            matches = (Path(pattern),)
        for match in matches:
            try:
                resolved = match.resolve()
            except FileNotFoundError:
                continue
            if resolved in seen:
                continue
            if not resolved.exists():
                continue
            seen.add(resolved)
            yield resolved


def _copy_item(src: Path, dest_root: Path) -> str:
    try:
        rel = src.relative_to(Path.cwd())
    except ValueError:
        rel = src.name
    target = dest_root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(src, target)
    else:
        shutil.copy2(src, target)
    return str(Path(rel))


def _parse_junit_report(path: Path) -> dict[str, int | str]:
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    try:
        tree = ET.parse(path)
    except Exception as exc:  # noqa: BLE001
        return {
            "path": str(path),
            "status": "unreadable",
            "error": str(exc),
            **totals,
            "passed": 0,
        }
    root = tree.getroot()
    suites = []
    if root.tag == "testsuite":
        suites = [root]
    else:
        suites = list(root.iter("testsuite"))
    for suite in suites:
        totals["tests"] += int(suite.attrib.get("tests", 0))
        totals["failures"] += int(suite.attrib.get("failures", 0))
        totals["errors"] += int(suite.attrib.get("errors", 0))
        totals["skipped"] += int(suite.attrib.get("skipped", 0))
    passed = totals["tests"] - totals["failures"] - totals["errors"] - totals["skipped"]
    status = "passed" if totals["failures"] == 0 and totals["errors"] == 0 else "failed"
    return {
        "path": str(path),
        "status": status,
        **totals,
        "passed": passed,
    }


def _parse_metadata(pairs: Iterable[str]) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for item in pairs:
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        metadata[key.strip()] = value.strip()
    return metadata


def _build_manifest(root: Path) -> list[dict[str, object]]:
    manifest: list[dict[str, object]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        stat = path.stat()
        manifest.append(
            {
                "path": str(path.relative_to(root)),
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
    return manifest


def main() -> int:
    args = _parse_args()
    output_root = Path(args.output)
    output_root.mkdir(parents=True, exist_ok=True)
    payload_root = output_root / "payload"
    payload_root.mkdir(parents=True, exist_ok=True)

    copied: list[str] = []

    for junit_path in args.junit_paths:
        path = Path(junit_path)
        if not path.exists():
            continue
        dest_dir = payload_root / "test-results"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_name = f"{len(list(dest_dir.iterdir())) + 1:02d}_{path.name}"
        dest = dest_dir / dest_name
        shutil.copy2(path, dest)
        copied.append(str(dest.relative_to(output_root)))

    for item in _expand_patterns(args.include):
        if item.resolve().is_relative_to(output_root.resolve()):  # type: ignore[attr-defined]
            continue
        rel = _copy_item(item, payload_root)
        copied.append(str(Path("payload") / rel))

    summaries = []
    for junit_path in args.junit_paths:
        path = Path(junit_path)
        if not path.exists():
            continue
        summaries.append(_parse_junit_report(path))

    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0, "passed": 0}
    for summary in summaries:
        for key in totals:
            totals[key] += int(summary.get(key, 0))
    overall_status = "passed"
    if totals["failures"] or totals["errors"]:
        overall_status = "failed"
    elif not summaries:
        overall_status = "unknown"

    summary_payload = {
        "job": args.job_name,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "overall_status": overall_status,
        "totals": totals,
        "reports": summaries,
        "copied": copied,
        "metadata": _parse_metadata(args.metadata),
        "hostname": os.uname().nodename if hasattr(os, "uname") else "unknown",
    }

    ending_values_path = output_root / "ending_values.json"
    ending_values_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

    manifest = _build_manifest(output_root)
    (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(json.dumps(summary_payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
