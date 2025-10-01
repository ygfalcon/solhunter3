#!/usr/bin/env bash
# Utility function for log rotation.
#
# Usage:
#   source scripts/rotate_logs.sh
#   rotate_logs [logfile] [max_logs]
#
# Rotates the specified logfile (default: startup.log), keeping up to
# max_logs (default: 5) previous log files.
# Each rotation renames the existing logfile with a timestamp suffix and
# removes older logs beyond the retention limit.

rotate_logs() {
  local logfile="${1:-startup.log}"
  local max_logs="${2:-5}"
  local timestamp="$(date +'%Y%m%d-%H%M%S')"
  if [ -f "$logfile" ]; then
    mv "$logfile" "${logfile%.log}-$timestamp.log"
  fi
  ls -1t ${logfile%.log}-*.log 2>/dev/null | tail -n +$((max_logs+1)) | xargs -r rm -- || true
}
