#!/bin/bash
set -e

# setup_and_run.sh
# This script sets up and starts the backend and frontend services locally on macOS.
# It is based on the build steps defined in backend/go-app/Dockerfile and frontend/Dockerfile.

# Get the absolute path of the project root
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BACKEND_DIR="$ROOT_DIR/backend/go-app"
FRONTEND_DIR="$ROOT_DIR/frontend"

# --- Pre-flight Checks ---

echo ">>> Checking environment..."

if [ "$(uname)" != "Darwin" ]; then
    echo "Error: This script is designed for macOS (Darwin). Detected: $(uname)"
    exit 1
fi

ARCH=$(uname -m)
if [ "$ARCH" != "arm64" ]; then
    echo "Warning: Detected architecture '$ARCH'. This project seems configured for 'arm64' (Apple Silicon)."
    echo "If you encounter linking errors, ensure you have the correct LanceDB libraries in backend/go-app/lib/darwin_amd64."
fi

if ! command -v go &> /dev/null; then
    echo "Error: 'go' is not installed."
    exit 1
fi

if ! command -v npm &> /dev/null; then
    echo "Error: 'npm' is not installed."
    exit 1
fi

if [ ! -f "$ROOT_DIR/config.yaml" ]; then
    echo "Error: config.yaml not found in project root ($ROOT_DIR/config.yaml)."
    echo "Please create it before running this script."
    exit 1
fi

# --- Backend Setup ---

echo ">>> Setting up Backend..."
cd "$BACKEND_DIR"

# Configure CGO to use the local LanceDB library
# This matches the Dockerfile's intent of linking against liblancedb_go,
# but uses the local 'lib/darwin_arm64' instead of downloading linux libs.
LIB_PATH="$BACKEND_DIR/lib/darwin_${ARCH}"
# Fallback to arm64 if the directory exists and we are on something else (or if specific mapping is needed)
if [ ! -d "$LIB_PATH" ] && [ -d "$BACKEND_DIR/lib/darwin_arm64" ]; then
    echo "Using darwin_arm64 library path as fallback..."
    LIB_PATH="$BACKEND_DIR/lib/darwin_arm64"
fi

export CGO_ENABLED=1
export CGO_CFLAGS="-I$BACKEND_DIR/include"
export CGO_LDFLAGS="-L$LIB_PATH -Wl,-rpath,$LIB_PATH -llancedb_go"

echo "  CGO_CFLAGS: $CGO_CFLAGS"
echo "  CGO_LDFLAGS: $CGO_LDFLAGS"

echo "  Downloading Go modules..."
go mod download

echo "  Building Backend binary..."
go build -o backend_server main.go

echo "  Starting Backend..."
./backend_server &
BACKEND_PID=$!
echo "  Backend started with PID $BACKEND_PID"

# --- Frontend Setup ---

echo ">>> Setting up Frontend..."
cd "$FRONTEND_DIR"

echo "  Installing Frontend dependencies..."
npm install

echo "  Building Frontend..."
npm run build

echo "  Starting Frontend (Preview Mode)..."
# Serving on port 3000 to avoid permission issues with port 80
npm run preview -- --port 3000 &
FRONTEND_PID=$!
echo "  Frontend started with PID $FRONTEND_PID at http://localhost:3000"

# --- Cleanup Trap ---

cleanup() {
    echo ""
    echo ">>> Stopping services..."
    kill $BACKEND_PID 2>/dev/null || true
    kill $FRONTEND_PID 2>/dev/null || true
    echo ">>> Stopped."
}

trap cleanup SIGINT SIGTERM

echo ""
echo ">>> Setup Complete!"
echo "Backend API: http://localhost:8080"
echo "Frontend UI: http://localhost:3000"
echo "Press CTRL+C to stop."

wait
