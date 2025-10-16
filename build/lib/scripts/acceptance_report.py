"""Emit a readiness checklist for the Home-Run acceptance criteria."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List
import json
import sys

ROOT = Path(__file__).resolve().parent.parent


@dataclass
class CheckResult:
    name: str
    ok: bool
    details: str

    def as_dict(self) -> dict:
        return {"name": self.name, "ok": self.ok, "details": self.details}


MANDATORY_PATHS = {
    "config_doc": ROOT / "docs" / "config.md",
    "feature_flags_doc": ROOT / "docs" / "feature_flags.md",
    "slos_doc": ROOT / "docs" / "SLOs.md",
    "acceptance_doc": ROOT / "docs" / "acceptance_checklist.md",
    "data_contracts": ROOT / "docs" / "data_contracts.md",
    "ui_topic_map": ROOT / "docs" / "ui_topic_map.md",
    "runbook": ROOT / "docs" / "runbook.md",
    "preflight": ROOT / "preflight.json",
    "env_doctor": ROOT / "env_doctor.sh",
    "bus_smoke": ROOT / "scripts" / "bus_smoke.py",
    "env_doctor_py": ROOT / "scripts" / "env_doctor.py",
    "dockerfile": ROOT / "Dockerfile",
    "compose": ROOT / "docker-compose.yml",
}


def _check_paths() -> Iterable[CheckResult]:
    for name, path in MANDATORY_PATHS.items():
        yield CheckResult(
            name=f"file:{name}",
            ok=path.exists(),
            details=str(path.relative_to(ROOT)),
        )


def _check_proto_files() -> CheckResult:
    proto_dir = ROOT / "proto"
    ok = any(proto_dir.glob("*.proto"))
    return CheckResult("proto", ok, "proto/*.proto")


def _check_tests() -> Iterable[CheckResult]:
    critical_tests = {
        "agents": "tests/test_agents.py",
        "exit_engine": "tests/test_exit_agent.py",
        "swarm": "tests/test_swarm_pipeline.py",
        "full_system": "tests/test_full_system_integration.py",
    }
    for name, rel_path in critical_tests.items():
        path = ROOT / rel_path
        yield CheckResult(
            name=f"test:{name}",
            ok=path.exists(),
            details=rel_path,
        )


def run_checks() -> List[CheckResult]:
    results: List[CheckResult] = []
    results.extend(_check_paths())
    results.append(_check_proto_files())
    results.extend(_check_tests())
    return results


def main(argv: List[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    as_json = "--json" in argv
    results = run_checks()
    if as_json:
        payload = {"checks": [item.as_dict() for item in results]}
        print(json.dumps(payload, indent=2))
    else:
        width = max(len(item.name) for item in results) + 2
        for item in results:
            status = "PASS" if item.ok else "FAIL"
            print(f"{status:<4} {item.name.ljust(width)} {item.details}")
        failures = [item for item in results if not item.ok]
        if failures:
            print(f"\n{len(failures)} check(s) failed. Run with --json for machine output.")
            return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
