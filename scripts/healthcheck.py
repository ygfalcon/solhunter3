#!/usr/bin/env python3
"""Runtime health check wrapper around :mod:`scripts.preflight`.

This utility reuses the individual checks from :mod:`scripts.preflight` but
formats the output as a concise pass/fail table.  It exits with a non-zero
status if any *critical* check fails.  ``main`` accepts an optional iterable of
checks allowing callers to run a filtered subset of the default checks.
"""
from __future__ import annotations

from typing import Iterable, List, Tuple, Callable

from scripts import preflight
from solhunter_zero.health_runtime import check_rl_daemon_health

CheckFunc = Tuple[str, Callable[[], preflight.Check]]


def _default_checks() -> List[CheckFunc]:
    checks = list(preflight.CHECKS)
    names = {name for name, _ in checks}
    if "RL daemon" not in names:
        checks.append(("RL daemon", lambda: check_rl_daemon_health(require_health_file=True)))
    return checks


def _run_checks(checks: Iterable[CheckFunc]) -> List[Tuple[str, bool, str]]:
    results: List[Tuple[str, bool, str]] = []
    for name, func in checks:
        try:
            ok, msg = func()
        except Exception as exc:  # pragma: no cover - defensive
            ok, msg = False, str(exc)
        results.append((name, ok, msg))
    return results


def _print_table(results: List[Tuple[str, bool, str]]) -> None:
    if not results:
        return
    width = max(len(name) for name, _, _ in results)
    for name, ok, msg in results:
        status = "PASS" if ok else "FAIL"
        print(f"{name:<{width}}  {status}  {msg}")


def main(
    checks: Iterable[CheckFunc] | None = None,
    *,
    critical: Iterable[str] | None = None,
) -> int:
    """Execute ``checks`` and return ``0`` when all critical checks pass.

    Parameters
    ----------
    checks:
        Iterable of ``(name, callable)`` pairs.  When ``None`` the default
        checks from :mod:`scripts.preflight` are used.
    critical:
        Iterable of check names considered critical.  When ``None`` all provided
        checks are treated as critical.
    """
    selected = list(_default_checks() if checks is None else checks)
    results = _run_checks(selected)
    _print_table(results)
    crit = set(name for name, _ in selected) if critical is None else set(critical)
    failed = [name for name, ok, _ in results if not ok and name in crit]
    return 1 if failed else 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
