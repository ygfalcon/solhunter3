"""Shared runner delegating to :mod:`investor_demo`.

This module exposes :func:`run` which accepts a dataset source and a
report directory.  The dataset source may be one of:

* ``None`` - use the demo's default bundled dataset
* A preset name bundled with :mod:`solhunter_zero.investor_demo`
* A path to a JSON price file on disk
* An HTTP(S) URL returning JSON price data

The function translates the input into the appropriate command line
arguments and forwards them to :func:`solhunter_zero.investor_demo.main` so
that both the interactive demo and the paper trading CLI can share the same
lightweight implementation.
"""

from __future__ import annotations

from pathlib import Path
import os
import tempfile
from urllib.request import urlopen

import solhunter_zero.investor_demo as investor_demo


def run(
    dataset: str | Path | None,
    reports: Path,
    *,
    learn: bool = False,
    rl_demo: bool = False,
) -> None:
    """Invoke :func:`investor_demo.main` with the given dataset.

    Parameters
    ----------
    dataset:
        Source of price data.  ``None`` uses the default demo dataset.  A
        string beginning with ``http://`` or ``https://`` is treated as a URL
        and downloaded to a temporary file.  If the string refers to an
        existing path on disk the file is used directly.  Otherwise the value
        is forwarded as a preset name.
    reports:
        Directory where :mod:`investor_demo` will write its output.
    learn:
        When ``True`` the tiny learning loop in :mod:`investor_demo` is
        executed.
    rl_demo:
        When ``True`` the lightweight reinforcement learning demo is run and
        metrics are written.
    """

    reports.mkdir(parents=True, exist_ok=True)

    # When running inside the test suite, importing ``tests.stubs`` installs
    # lightweight stand-ins for heavy optional dependencies used by
    # :mod:`investor_demo`.  The environment variable is set by tests that invoke
    # the CLI scripts in a subprocess.
    if os.getenv("SOLHUNTER_PATCH_INVESTOR_DEMO"):
        try:  # pragma: no cover - exercised in subprocess tests
            import tests.stubs  # noqa: F401
        except Exception:
            pass

    forwarded = ["--reports", str(reports)]
    if rl_demo:
        forwarded.append("--rl-demo")
    if learn:
        forwarded.append("--learn")

    if dataset:
        ds = str(dataset)
        if ds.startswith("http://") or ds.startswith("https://"):
            with urlopen(ds, timeout=10) as resp:
                data = resp.read().decode("utf-8")
            with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json") as fh:
                fh.write(data)
                tmp_path = fh.name
            forwarded.extend(["--data", tmp_path])
        else:
            path = Path(ds)
            if path.exists():
                forwarded.extend(["--data", str(path)])
            else:
                forwarded.extend(["--preset", ds])

    investor_demo.main(forwarded)


__all__ = ["run"]

