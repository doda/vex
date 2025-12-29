#!/bin/bash
set -euo pipefail

GARAGE_DIR="${GARAGE_DIR:-/tmp/vex-garage}"
GARAGE_BIN="${GARAGE_BIN:-$GARAGE_DIR/garage}"
GARAGE_CONFIG="${GARAGE_CONFIG:-$GARAGE_DIR/garage.toml}"
GARAGE_LOG="${GARAGE_LOG:-$GARAGE_DIR/garage.log}"
GARAGE_PID_FILE="${GARAGE_PID_FILE:-$GARAGE_DIR/garage.pid}"
GARAGE_META="${GARAGE_META:-$GARAGE_DIR/meta}"
GARAGE_DATA="${GARAGE_DATA:-$GARAGE_DIR/data}"
GARAGE_PORT="${GARAGE_PORT:-3900}"
GARAGE_RPC_PORT="${GARAGE_RPC_PORT:-3901}"
GARAGE_ADMIN_PORT="${GARAGE_ADMIN_PORT:-3903}"
GARAGE_BUCKET="${GARAGE_BUCKET:-vex}"
GARAGE_REGION="${GARAGE_REGION:-garage}"
GARAGE_KEY_NAME="${GARAGE_KEY_NAME:-vex-test-key}"

mkdir -p "$GARAGE_DIR" "$GARAGE_META" "$GARAGE_DATA"

OS="$(uname -s)"
ARCH="$(uname -m)"
if [ "$OS" != "Linux" ]; then
  echo "Garage setup requires Linux binaries. OS=$OS is not supported by this script." >&2
  echo "Use a Linux sandbox (Modal) or provide GARAGE_BIN pointing to a compatible binary." >&2
  exit 1
fi

PLATFORM=""
case "$ARCH" in
  x86_64)
    PLATFORM="x86_64-unknown-linux-musl"
    ;;
  aarch64)
    PLATFORM="aarch64-unknown-linux-musl"
    ;;
  *)
    echo "Unsupported architecture: $ARCH" >&2
    exit 1
    ;;
esac

if [ ! -x "$GARAGE_BIN" ]; then
  echo "Downloading Garage binary for $PLATFORM..."
  GARAGE_URL="$(python3 - "$PLATFORM" <<'PY'
import json
import urllib.request
import sys

platform = sys.argv[1]
data = json.load(urllib.request.urlopen("https://garagehq.deuxfleurs.fr/_releases.json"))
release = next((item for item in data if item.get("type") == "tag" and item.get("name") == "Release"), None)
if not release or not release.get("builds"):
    print("missing release data", file=sys.stderr)
    sys.exit(1)
latest = release["builds"][0]["builds"]
for build in latest:
    if build.get("platform") == platform:
        print(build.get("url"))
        sys.exit(0)
print("no build found for platform " + platform, file=sys.stderr)
sys.exit(1)
PY
)"
  curl -fsSL "$GARAGE_URL" -o "$GARAGE_BIN"
  chmod +x "$GARAGE_BIN"
fi

if [ ! -f "$GARAGE_CONFIG" ]; then
  RPC_SECRET="$(openssl rand -hex 32)"
  ADMIN_TOKEN="$(openssl rand -base64 32)"
  METRICS_TOKEN="$(openssl rand -base64 32)"
  cat > "$GARAGE_CONFIG" <<EOF
metadata_dir = "$GARAGE_META"
data_dir = "$GARAGE_DATA"
db_engine = "sqlite"

replication_factor = 1

rpc_bind_addr = "[::]:$GARAGE_RPC_PORT"
rpc_public_addr = "127.0.0.1:$GARAGE_RPC_PORT"
rpc_secret = "$RPC_SECRET"

[s3_api]
s3_region = "$GARAGE_REGION"
api_bind_addr = "[::]:$GARAGE_PORT"
root_domain = ".s3.garage.localhost"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage.localhost"
index = "index.html"

[admin]
api_bind_addr = "[::]:$GARAGE_ADMIN_PORT"
admin_token = "$ADMIN_TOKEN"
metrics_token = "$METRICS_TOKEN"
EOF
fi

if [ -f "$GARAGE_PID_FILE" ]; then
  GARAGE_PID="$(cat "$GARAGE_PID_FILE")"
  if kill -0 "$GARAGE_PID" 2>/dev/null; then
    echo "Garage already running (PID: $GARAGE_PID)"
  else
    rm -f "$GARAGE_PID_FILE"
  fi
fi

if [ ! -f "$GARAGE_PID_FILE" ]; then
  echo "Starting Garage..."
  RUST_LOG="${RUST_LOG:-garage=info}" "$GARAGE_BIN" -c "$GARAGE_CONFIG" server > "$GARAGE_LOG" 2>&1 &
  GARAGE_PID=$!
  echo "$GARAGE_PID" > "$GARAGE_PID_FILE"
fi

echo "Waiting for Garage to become ready..."
for i in {1..30}; do
  if "$GARAGE_BIN" -c "$GARAGE_CONFIG" status >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "Garage failed to start. Check logs at $GARAGE_LOG" >&2
    exit 1
  fi
  sleep 1
done

LAYOUT_VERSION="$("$GARAGE_BIN" -c "$GARAGE_CONFIG" layout show | awk -F':' '/Current cluster layout version/ {gsub(/ /,"",$2); print $2}')"
if [ -z "$LAYOUT_VERSION" ] || [ "$LAYOUT_VERSION" = "0" ]; then
  NODE_ID=""
  for i in {1..10}; do
    STATUS_OUTPUT="$("$GARAGE_BIN" -c "$GARAGE_CONFIG" status 2>/dev/null || true)"
    NODE_ID="$(echo "$STATUS_OUTPUT" | awk '{t=tolower($1)} t ~ /^[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]*$/ {print $1; exit}')"
    if [ -n "$NODE_ID" ]; then
      break
    fi
    sleep 1
  done
  if [ -z "$NODE_ID" ]; then
    echo "$STATUS_OUTPUT" > "$GARAGE_DIR/status.txt"
    echo "Failed to detect Garage node ID (saved status to $GARAGE_DIR/status.txt)" >&2
    if [ -n "$STATUS_OUTPUT" ]; then
      echo "Garage status output:" >&2
      echo "$STATUS_OUTPUT" >&2
    fi
    exit 1
  fi
  "$GARAGE_BIN" -c "$GARAGE_CONFIG" layout assign -z dc1 -c 1G "$NODE_ID" >/dev/null
  "$GARAGE_BIN" -c "$GARAGE_CONFIG" layout apply --version 1 >/dev/null
fi

if ! "$GARAGE_BIN" -c "$GARAGE_CONFIG" bucket info "$GARAGE_BUCKET" >/dev/null 2>&1; then
  "$GARAGE_BIN" -c "$GARAGE_CONFIG" bucket create "$GARAGE_BUCKET" >/dev/null
fi

KEY_NAME="$GARAGE_KEY_NAME"
KEY_OUTPUT="$("$GARAGE_BIN" -c "$GARAGE_CONFIG" key create "$KEY_NAME" 2>/dev/null || true)"
if [ -z "$KEY_OUTPUT" ]; then
  KEY_NAME="${GARAGE_KEY_NAME}-$(date +%s)"
  KEY_OUTPUT="$("$GARAGE_BIN" -c "$GARAGE_CONFIG" key create "$KEY_NAME" 2>/dev/null || true)"
fi
if [ -z "$KEY_OUTPUT" ]; then
  echo "Failed to create Garage API key (name: $GARAGE_KEY_NAME)" >&2
  exit 1
fi

ACCESS_KEY="$(echo "$KEY_OUTPUT" | awk -F': ' '/Key ID:/ {print $2}' | sed 's/^ *//; s/ *$//')"
SECRET_KEY="$(echo "$KEY_OUTPUT" | awk -F': ' '/Secret key:/ {print $2}' | sed 's/^ *//; s/ *$//')"
if [ -z "$ACCESS_KEY" ] || [ -z "$SECRET_KEY" ]; then
  echo "Failed to parse Garage API key output" >&2
  exit 1
fi

if ! "$GARAGE_BIN" -c "$GARAGE_CONFIG" bucket allow --read --write --owner "$GARAGE_BUCKET" --key "$ACCESS_KEY" >/dev/null 2>&1; then
  "$GARAGE_BIN" -c "$GARAGE_CONFIG" bucket allow --read --write --owner "$GARAGE_BUCKET" --key "$KEY_NAME" >/dev/null
fi

if ! "$GARAGE_BIN" -c "$GARAGE_CONFIG" bucket info "$GARAGE_BUCKET" 2>/dev/null | grep -q "$ACCESS_KEY"; then
  echo "Garage bucket does not list access key $ACCESS_KEY as allowed." >&2
  "$GARAGE_BIN" -c "$GARAGE_CONFIG" bucket info "$GARAGE_BUCKET" >&2 || true
  exit 1
fi

cat > "$GARAGE_DIR/creds.env" <<EOF
export VEX_TEST_S3_ENDPOINT="http://localhost:$GARAGE_PORT"
export VEX_TEST_S3_ACCESS_KEY="$ACCESS_KEY"
export VEX_TEST_S3_SECRET_KEY="$SECRET_KEY"
export VEX_TEST_S3_BUCKET="$GARAGE_BUCKET"
export VEX_TEST_S3_REGION="$GARAGE_REGION"
export VEX_TEST_S3_TYPE="s3"
EOF

echo "Garage is ready!"
echo "  Endpoint: http://localhost:$GARAGE_PORT"
echo "  Bucket:   $GARAGE_BUCKET"
echo "  Config:   $GARAGE_CONFIG"
echo "  Logs:     $GARAGE_LOG"
echo "  Creds:    $GARAGE_DIR/creds.env"
echo ""
echo "To use in tests:"
echo "  source $GARAGE_DIR/creds.env"
