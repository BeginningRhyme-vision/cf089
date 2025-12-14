#!/bin/bash

# Configuration
PROJECT_ROOT=$(pwd)
BACKEND_DIR="$PROJECT_ROOT/backend"
FRONTEND_DIR="$PROJECT_ROOT/frontend"
BACKEND_LOG="$PROJECT_ROOT/backend.log"
FRONTEND_LOG="$PROJECT_ROOT/frontend.log"
BACKEND_PID_FILE="$PROJECT_ROOT/backend.pid"
FRONTEND_PID_FILE="$PROJECT_ROOT/frontend.pid"

# Function to check if a process is running
is_running() {
    if [ -f "$1" ]; then
        pid=$(cat "$1")
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0 # Running
        fi
    fi
    return 1 # Not running
}

# Start Backend
start_backend() {
    if is_running "$BACKEND_PID_FILE"; then
        echo "Backend is already running (PID: $(cat "$BACKEND_PID_FILE"))"
    else
        echo "Starting Backend..."
        cd "$BACKEND_DIR" || exit
        nohup ./venv/bin/python3 -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000 > "$BACKEND_LOG" 2>&1 &
        echo $! > "$BACKEND_PID_FILE"
        echo "Backend started (PID: $(cat "$BACKEND_PID_FILE"))"
        cd "$PROJECT_ROOT" || exit
    fi
}

# Start Frontend
start_frontend() {
    if is_running "$FRONTEND_PID_FILE"; then
        echo "Frontend is already running (PID: $(cat "$FRONTEND_PID_FILE"))"
    else
        echo "Starting Frontend..."
        cd "$FRONTEND_DIR" || exit
        # Use node directly to avoid some npm signal swallowing issues, or just npm
        nohup npm run dev -- --host > "$FRONTEND_LOG" 2>&1 &
        echo $! > "$FRONTEND_PID_FILE"
        echo "Frontend started (PID: $(cat "$FRONTEND_PID_FILE"))"
        cd "$PROJECT_ROOT" || exit
    fi
}

# Stop Backend
stop_backend() {
    if [ -f "$BACKEND_PID_FILE" ]; then
        pid=$(cat "$BACKEND_PID_FILE")
        echo "Stopping Backend (PID: $pid)..."
        # Kill the process group to ensure children (like reloader) are killed
        pkill -P "$pid" 2>/dev/null
        kill "$pid" 2>/dev/null
        rm "$BACKEND_PID_FILE"
        echo "Backend stopped."
    else
        echo "Backend PID file not found. attempting to kill by name..."
        pkill -f "uvicorn app.main:app"
        echo "Backend kill command sent."
    fi
}

# Stop Frontend
stop_frontend() {
    if [ -f "$FRONTEND_PID_FILE" ]; then
        pid=$(cat "$FRONTEND_PID_FILE")
        echo "Stopping Frontend (PID: $pid)..."
        pkill -P "$pid" 2>/dev/null
        kill "$pid" 2>/dev/null
        rm "$FRONTEND_PID_FILE"
        echo "Frontend stopped."
    else
        echo "Frontend PID file not found. attempting to kill by name..."
        pkill -f "vite"
        echo "Frontend kill command sent."
    fi
}

# Main Logic
case "$1" in
    start)
        start_backend
        start_frontend
        ;;
    stop)
        stop_backend
        stop_frontend
        ;;
    restart)
        stop_backend
        stop_frontend
        sleep 2
        start_backend
        start_frontend
        ;;
    backend)
        case "$2" in
            start) start_backend ;;
            stop) stop_backend ;;
            restart) stop_backend; sleep 1; start_backend ;;
            *) echo "Usage: $0 backend {start|stop|restart}" ;;
        esac
        ;;
    frontend)
        case "$2" in
            start) start_frontend ;;
            stop) stop_frontend ;;
            restart) stop_frontend; sleep 1; start_frontend ;;
            *) echo "Usage: $0 frontend {start|stop|restart}" ;;
        esac
        ;;
    logs)
        echo "Tailing logs (Ctrl+C to stop)..."
        tail -f "$BACKEND_LOG" "$FRONTEND_LOG"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|logs|backend [cmd]|frontend [cmd]}"
        exit 1
        ;;
esac
