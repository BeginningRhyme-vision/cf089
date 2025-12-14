#!/bin/bash

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$PROJECT_ROOT/backend"
VENV_PYTHON="$BACKEND_DIR/venv/bin/python"
WORKER_SCRIPT="$BACKEND_DIR/worker_transfer/worker.py"
SERVICE_NAME="unbound-future-worker"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

# Function: Run the worker directly (Foreground)
run_direct() {
    echo "Starting Worker in foreground..."
    export PYTHONPATH=$BACKEND_DIR
    cd "$BACKEND_DIR" || exit
    exec "$VENV_PYTHON" "$WORKER_SCRIPT"
}

# Function: Install as Systemd Service
install_service() {
    # Check if running on Linux/Systemd
    if [ ! -d "/etc/systemd/system" ]; then
        echo "Error: /etc/systemd/system directory not found. Service installation is intended for Ubuntu/systemd systems."
        exit 1
    fi

    CURRENT_USER=$(whoami)
    CURRENT_GROUP=$(id -gn)

    echo "Setting up systemd service for $SERVICE_NAME..."
    echo "Project Root: $PROJECT_ROOT"
    echo "Backend Dir:  $BACKEND_DIR"
    echo "User:         $CURRENT_USER"

    # 1. Stop Python Process (Service and any stray processes)
    echo "Stopping existing service if running..."
    sudo systemctl stop $SERVICE_NAME 2>/dev/null || true
    
    echo "Ensuring python worker processes are stopped..."
    pkill -f "worker_transfer/worker.py" || true

    # 2. Kill r2s3 service/process
    echo "Killing any running r2s3 processes..."
    sudo pkill r2s3 || true

    # 3. Recompile r2s3 if Go exists
    if command -v go &> /dev/null; then
        echo "Go is installed. Rebuilding r2s3..."
        if [ -d "$BACKEND_DIR/worker_transfer" ]; then
            pushd "$BACKEND_DIR/worker_transfer" > /dev/null
            rm -f r2s3
            if go build -o r2s3 .; then
                echo "r2s3 build successful."
            else
                echo "Error: r2s3 build failed."
                exit 1
            fi
            popd > /dev/null
        else
            echo "Error: Directory $BACKEND_DIR/worker_transfer not found."
            exit 1
        fi
    else
        echo "Go is not installed. Skipping rebuild of r2s3."
    fi

    # Install r2s3 binary to /usr/local/bin
    if [ -f "$BACKEND_DIR/worker_transfer/r2s3" ]; then
        echo "Installing r2s3 binary to /usr/local/bin..."
        sudo cp "$BACKEND_DIR/worker_transfer/r2s3" /usr/local/bin/
        sudo chmod +x /usr/local/bin/r2s3
    else
        echo "Warning: r2s3 binary not found in $BACKEND_DIR/worker_transfer/"
    fi

    # Generate Service File Content
    sudo bash -c "cat > $SERVICE_FILE" <<EOF
[Unit]
Description=Unbound Future Backend Worker
After=network.target

[Service]
Type=simple
User=$CURRENT_USER
Group=$CURRENT_GROUP
WorkingDirectory=$BACKEND_DIR
Environment="PYTHONPATH=$BACKEND_DIR"
ExecStart=$VENV_PYTHON $WORKER_SCRIPT
Restart=always
RestartSec=10
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=$SERVICE_NAME

[Install]
WantedBy=multi-user.target
EOF

    echo "Created service file at $SERVICE_FILE"

    # Reload systemd and start service
    echo "Reloading systemd daemon..."
    sudo systemctl daemon-reload
    
    echo "Enabling and starting $SERVICE_NAME..."
    sudo systemctl enable $SERVICE_NAME
    sudo systemctl restart $SERVICE_NAME
    
    echo "Setup complete! Service is running."
    echo "Check status with: $0 status"
}

# Function: Check Status
show_status() {
    if [ -d "/etc/systemd/system" ]; then
        sudo systemctl status $SERVICE_NAME --no-pager
    else
        echo "Not a systemd system. Cannot check service status."
    fi
}

# Function: Follow Logs
show_logs() {
    if [ -d "/etc/systemd/system" ]; then
        echo "Tailing logs for $SERVICE_NAME (Ctrl+C to stop)..."
        sudo journalctl -u $SERVICE_NAME -f
    else
        echo "Not a systemd system. Cannot check logs."
    fi
}

# Function: Stop Service
stop_service() {
     if [ -d "/etc/systemd/system" ]; then
        echo "Stopping $SERVICE_NAME..."
        sudo systemctl stop $SERVICE_NAME
        echo "Stopped."
    else
        echo "Not a systemd system."
    fi
}

# Main Dispatch
case "$1" in
    install)
        install_service
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    stop)
        stop_service
        ;;
    run|*)
        if [ "$1" == "help" ] || [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
            echo "Usage: $0 {run|install|status|logs|stop}"
            echo "  run     : Run the worker directly in the terminal (default if no arg provided)"
            echo "  install : Install and start as a systemd service (requires sudo)"
            echo "  status  : Check systemd service status"
            echo "  logs    : Follow systemd service logs"
            echo "  stop    : Stop the systemd service"
        else
            run_direct
        fi
        ;;
esac