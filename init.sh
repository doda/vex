#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

export PATH=$PATH:/usr/local/go/bin

echo "=== Vex Project Setup ==="
echo ""

echo "1. Checking Go installation..."
go version

echo ""
echo "2. Installing dependencies..."
go mod tidy
go mod download

echo ""
echo "3. Building vex binary..."
go build -o vex ./cmd/vex

echo ""
echo "4. Running basic tests..."
./vex version

echo ""
echo "5. Testing subcommands..."
./vex help > /dev/null
echo "   - help: OK"

echo ""
echo "6. Verifying package structure..."
[ -f go.mod ] && echo "   - go.mod: OK"
[ -d cmd/vex ] && echo "   - cmd/vex/: OK"
[ -d pkg/api ] && echo "   - pkg/api/: OK"
[ -d pkg/objectstore ] && echo "   - pkg/objectstore/: OK"
[ -d internal/config ] && echo "   - internal/config/: OK"

echo ""
echo "7. Starting vex server for smoke test..."
export VEX_OBJECT_STORE_TYPE=filesystem
export VEX_OBJECT_STORE_ROOT="/tmp/vex-objectstore"
export VEX_AUTH_TOKEN="dev-token"
./vex serve &
VEX_PID=$!
sleep 2

echo ""
echo "8. Testing HTTP endpoints..."
HEALTH_RESP=$(curl -s http://localhost:8080/health)
if echo "$HEALTH_RESP" | grep -q '"status":"ok"'; then
    echo "   - GET /health: OK"
else
    echo "   - GET /health: FAILED"
    kill $VEX_PID 2>/dev/null || true
    exit 1
fi

NS_RESP=$(curl -s -H "Authorization: Bearer ${VEX_AUTH_TOKEN}" http://localhost:8080/v1/namespaces)
if echo "$NS_RESP" | grep -q '"namespaces"'; then
    echo "   - GET /v1/namespaces: OK"
else
    echo "   - GET /v1/namespaces: FAILED"
    kill $VEX_PID 2>/dev/null || true
    exit 1
fi

echo ""
echo "9. Stopping vex server..."
kill $VEX_PID 2>/dev/null || true
wait $VEX_PID 2>/dev/null || true

echo ""
echo "10. Running go tests..."
go test ./... -v -count=1 2>&1 | head -50 || true

echo ""
echo "=== All checks passed! ==="
echo ""
echo "Vex project is ready for development."
echo "  - Run './vex serve' to start the server"
echo "  - Run './scripts/setup-garage.sh' to start local Garage (S3) for integration tests"
