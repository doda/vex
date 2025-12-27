#!/bin/bash
set -e

MINIO_DIR="${MINIO_DIR:-/tmp/vex-minio}"
PID_FILE="$MINIO_DIR/minio.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping MinIO (PID: $PID)..."
        kill "$PID"
        rm -f "$PID_FILE"
        echo "MinIO stopped"
    else
        echo "MinIO process not running"
        rm -f "$PID_FILE"
    fi
else
    echo "No MinIO PID file found"
fi
