#!/usr/bin/env bash
# macOS launcher wrapper for the investor demo.

cd "$(dirname "$0")"
exec ./demo.py "$@"
