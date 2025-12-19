#!/bin/bash
set -e

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Initializing environment for worker_downloader in $SCRIPT_DIR..."

VENV_DIR="venv"

# Check if python3 is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 could not be found."
    exit 1
fi

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
else
    echo "Virtual environment already exists."
fi

# Activate
source "$VENV_DIR/bin/activate"

echo "Upgrading pip..."
pip install --upgrade pip

if [ -f "requirements.txt" ]; then
    echo "Installing base dependencies from requirements.txt..."
    pip install -r requirements.txt
else
    echo "Warning: requirements.txt not found."
fi

# Install implicit dependencies for backend app modules
# These are required because get_yt_metadata.py imports from backend.app
echo "Installing implicit dependencies (SQLAlchemy, Pydantic, PostgreSQL driver)..."
pip install sqlalchemy pydantic-settings psycopg2-binary

echo "Environment setup complete."
echo "To activate the environment, run: source $SCRIPT_DIR/venv/bin/activate"
