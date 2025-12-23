#!/bin/bash
set -e

# setup_and_run.sh
# This script builds Docker images for the backend and frontend,
# and starts them as containers.

# Get the absolute path of the project root
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG_FILE="$ROOT_DIR/config.yaml"

# --- Pre-flight Checks ---

echo ">>> Checking environment..."

if ! command -v docker &> /dev/null; then
    echo "Error: 'docker' is not installed."
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: config.yaml not found in project root ($CONFIG_FILE)."
    echo "Please create it before running this script."
    exit 1
fi

# --- Build Images ---

echo ">>> Building Backend Docker Image..."
# Context is backend/go-app as per Dockerfile comments
docker build -t unbound-backend "$ROOT_DIR/backend/go-app"

echo ">>> Building Frontend Docker Image..."
# Context is frontend
docker build -t unbound-frontend "$ROOT_DIR/frontend"

# --- Run Containers ---

echo ">>> Creating Docker Network..."
# Create network if it doesn't exist
docker network create unbound-net 2>/dev/null || true

echo ">>> Starting Backend Container..."
# Mount config.yaml to /app/config.yaml
# Run in detached mode first to get ID
BACKEND_ID=$(docker run -d --rm \
    --network unbound-net \
    --network-alias backend \
    -p 8080:8080 \
    -v "$CONFIG_FILE:/app/config.yaml" \
    --name unbound-backend-instance \
    unbound-backend)

echo "  Backend started with ID: ${BACKEND_ID:0:12}"

echo ">>> Starting Frontend Container..."
# Map host port 3000 to container port 80
FRONTEND_ID=$(docker run -d --rm \
    --network unbound-net \
    -p 3000:80 \
    --name unbound-frontend-instance \
    unbound-frontend)

echo "  Frontend started with ID: ${FRONTEND_ID:0:12}"

# --- Cleanup Trap ---

cleanup() {
    echo ""
    echo ">>> Stopping containers..."
    docker stop "$BACKEND_ID" >/dev/null 2>&1 || true
    docker stop "$FRONTEND_ID" >/dev/null 2>&1 || true
    echo ">>> Stopped."
}

trap cleanup SIGINT SIGTERM

echo ""
echo ">>> Setup Complete!"
echo "Backend API: http://localhost:8080"
echo "Frontend UI: http://localhost:3000"
echo "Press CTRL+C to stop."

# Wait for both containers to exit (or script interruption)
# We wait on the logs to keep the script running and show output
# This is a bit tricky with two containers. 
# Simple approach: wait endlessly.
sleep infinity