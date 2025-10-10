#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

"$SCRIPT_DIR/env_doctor.sh"
"$SCRIPT_DIR/bus_smoke.sh"
"$SCRIPT_DIR/preflight_smoke.sh"
