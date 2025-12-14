#!/bin/bash
PROJECT_ROOT=$(pwd)
BACKEND_DIR="$PROJECT_ROOT/backend"
export PYTHONPATH=$BACKEND_DIR

echo "Starting Worker..."
cd "$BACKEND_DIR" || exit
./venv/bin/python worker.py
