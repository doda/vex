#!/bin/bash
set -euo pipefail

GARAGE_DIR="${GARAGE_DIR:-/tmp/vex-garage}"
GARAGE_PID_FILE="${GARAGE_PID_FILE:-$GARAGE_DIR/garage.pid}"

if [ -f "$GARAGE_PID_FILE" ]; then
  GARAGE_PID="$(cat "$GARAGE_PID_FILE")"
  if kill -0 "$GARAGE_PID" 2>/dev/null; then
    echo "Stopping Garage (PID: $GARAGE_PID)..."
    kill "$GARAGE_PID"
    rm -f "$GARAGE_PID_FILE"
    echo "Garage stopped"
  else
    echo "Garage process not running"
    rm -f "$GARAGE_PID_FILE"
  fi
else
  echo "No Garage PID file found"
fi
