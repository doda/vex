#!/bin/bash
set -e

MINIO_DIR="${MINIO_DIR:-/tmp/vex-minio}"
MINIO_BIN="$MINIO_DIR/minio"
MINIO_DATA="$MINIO_DIR/data"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
MINIO_ACCESS_KEY="${MINIO_ROOT_USER:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-vex}"

mkdir -p "$MINIO_DIR" "$MINIO_DATA"

if [ ! -f "$MINIO_BIN" ]; then
    echo "Downloading MinIO..."
    curl -sSL https://dl.min.io/server/minio/release/linux-amd64/minio -o "$MINIO_BIN"
    chmod +x "$MINIO_BIN"
fi

export MINIO_ROOT_USER="$MINIO_ACCESS_KEY"
export MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY"

echo "Starting MinIO on port $MINIO_PORT..."
"$MINIO_BIN" server "$MINIO_DATA" \
    --address ":$MINIO_PORT" \
    --console-address ":$MINIO_CONSOLE_PORT" &

MINIO_PID=$!
echo "$MINIO_PID" > "$MINIO_DIR/minio.pid"

echo "Waiting for MinIO to start..."
for i in {1..30}; do
    if curl -s "http://localhost:$MINIO_PORT/minio/health/live" > /dev/null 2>&1; then
        echo "MinIO started successfully (PID: $MINIO_PID)"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "MinIO failed to start" >&2
        exit 1
    fi
    sleep 1
done

MC_BIN="$MINIO_DIR/mc"
if [ ! -f "$MC_BIN" ]; then
    echo "Downloading MinIO client..."
    curl -sSL https://dl.min.io/client/mc/release/linux-amd64/mc -o "$MC_BIN"
    chmod +x "$MC_BIN"
fi

echo "Configuring MinIO client..."
"$MC_BIN" alias set local "http://localhost:$MINIO_PORT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api S3v4 > /dev/null 2>&1 || true

echo "Creating bucket '$MINIO_BUCKET'..."
"$MC_BIN" mb "local/$MINIO_BUCKET" --ignore-existing > /dev/null 2>&1 || true

echo ""
echo "MinIO is ready!"
echo "  Endpoint: http://localhost:$MINIO_PORT"
echo "  Console:  http://localhost:$MINIO_CONSOLE_PORT"
echo "  Access:   $MINIO_ACCESS_KEY"
echo "  Secret:   $MINIO_SECRET_KEY"
echo "  Bucket:   $MINIO_BUCKET"
echo "  PID file: $MINIO_DIR/minio.pid"
