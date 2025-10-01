#!/usr/bin/env bash
# macOS launcher wrapper for the paper trading CLI.
cd "$(dirname "$0")"
exec ./paper.py "$@"
