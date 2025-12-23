#!/bin/bash

# Get the absolute path of the directory containing this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$DIR/.."

# Ensure go is in PATH
export PATH=$PATH:/usr/local/go/bin

# Define paths for include and lib
INCLUDE_PATH="$PROJECT_ROOT/include"
LIB_PATH="$PROJECT_ROOT/lib/darwin_arm64"

# Check if paths exist
if [ ! -d "$INCLUDE_PATH" ]; then
    echo "Error: Include path not found at $INCLUDE_PATH"
    exit 1
fi

if [ ! -d "$LIB_PATH" ]; then
    echo "Error: Lib path not found at $LIB_PATH"
    exit 1
fi

# Set CGO flags
# Note: We explicitly add -llancedb_go to link against the library
export CGO_CFLAGS="-I$INCLUDE_PATH"
export CGO_LDFLAGS="-L$LIB_PATH -Wl,-rpath,$LIB_PATH -llancedb_go"

echo "Running tests with:"
echo "CGO_CFLAGS=$CGO_CFLAGS"
echo "CGO_LDFLAGS=$CGO_LDFLAGS"

# Run the test
# -v: verbose output
# -count=1: disable test caching
# We must run from the module root (backend/go-app)
cd "$PROJECT_ROOT" || exit 1
go test -v -count=1 ./database
