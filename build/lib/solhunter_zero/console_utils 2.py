from __future__ import annotations

"""Utilities for rich console output."""

from rich.console import Console

__all__ = ["console", "console_print", "console_warning", "console_error"]

console = Console()


def console_print(*args, **kwargs) -> None:
    """Proxy to :meth:`rich.console.Console.print` for consistency."""
    console.print(*args, **kwargs)


def console_warning(message: str, *args, **kwargs) -> None:
    """Print *message* as a yellow warning."""
    console.print(f"[yellow]{message}[/]", *args, **kwargs)


def console_error(message: str, *args, **kwargs) -> None:
    """Print *message* as a red error."""
    console.print(f"[red]{message}[/]", *args, **kwargs)
