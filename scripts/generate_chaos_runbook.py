#!/usr/bin/env python3
"""Aggregate chaos remediation metadata into a Markdown runbook."""
from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path
from typing import Any, Iterable, Sequence


def _coerce_steps(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    steps: list[str] = []
    for item in value:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            steps.append(text)
    return steps


def _coerce_list(items: Any) -> list[str]:
    if not items:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _load_records(directory: Path) -> list[dict[str, Any]]:
    if not directory.exists():
        return []

    records: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        test_id = raw.get("test")
        markers = _coerce_list(raw.get("markers"))
        for record in raw.get("records", []):
            entry = dict(record)
            entry.setdefault("component", "Uncategorised")
            entry.setdefault("failure", "Unspecified failure mode")
            entry["remediation"] = _coerce_steps(entry.get("remediation"))
            entry["artifact"] = path.as_posix()
            entry["test"] = test_id
            entry["markers"] = markers
            records.append(entry)
    return records


def _render_component(component: str) -> str:
    component = component.strip()
    if not component:
        return "Uncategorised"
    return component


def _render_section(records: Iterable[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for record in records:
        failure = record.get("failure", "Unspecified failure mode")
        test_id = record.get("test") or "unknown test"
        lines.append(f"* **When** {failure} (from `{test_id}`):")

        detection = record.get("detection")
        if detection:
            lines.append(f"  * **Detection:** {detection}")

        impact = record.get("impact")
        if impact:
            lines.append(f"  * **Impact:** {impact}")

        steps = record.get("remediation") or []
        if steps:
            lines.append("  * **Remediation:**")
            for idx, step in enumerate(steps, start=1):
                lines.append(f"    {idx}. {step}")

        verification = record.get("verification")
        if verification:
            lines.append(f"  * **Verification:** {verification}")

        severity = record.get("severity")
        if severity:
            lines.append(f"  * **Severity:** {severity}")

        notes = record.get("notes")
        if notes:
            lines.append(f"  * **Notes:** {notes}")

        tags = _coerce_list(record.get("tags"))
        if tags:
            lines.append(f"  * **Tags:** {', '.join(tags)}")

        links = _coerce_list(record.get("links"))
        if links:
            lines.append(f"  * **Links:** {', '.join(links)}")

        artifact = record.get("artifact")
        if artifact:
            lines.append(f"  * **Source artifact:** `{artifact}`")

        markers = _coerce_list(record.get("markers"))
        if markers:
            lines.append(f"  * **Pytest markers:** {', '.join(markers)}")

        lines.append("")
    return lines


def render_markdown(records: Sequence[dict[str, Any]]) -> str:
    header = [
        "# Chaos Remediation Guide (Generated)",
        "",
        "This file is generated from pytest chaos/health fixtures. Do not edit manually.",
        "",
    ]

    if not records:
        header.append("_No chaos remediation records were found. Run the chaos test suite to populate this file._")
        header.append("")
        return "\n".join(header)

    sorted_records = sorted(
        records,
        key=lambda item: (
            _render_component(str(item.get("component", ""))).lower(),
            str(item.get("failure", "")).lower(),
            str(item.get("test", "")),
        ),
    )

    lines = list(header)
    for component, group in itertools.groupby(
        sorted_records, key=lambda item: _render_component(str(item.get("component", "")))
    ):
        lines.append(f"## {component}")
        lines.append("")
        lines.extend(_render_section(group))
    return "\n".join(lines).rstrip() + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default="artifacts/chaos",
        type=Path,
        help="Directory containing JSON remediation artifacts",
    )
    parser.add_argument(
        "--output",
        default="docs/runbook_generated.md",
        type=Path,
        help="Markdown file to write",
    )
    args = parser.parse_args(argv)

    records = _load_records(args.input)
    markdown = render_markdown(records)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(markdown, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
