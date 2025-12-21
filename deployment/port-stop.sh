#!/bin/bash

# Stop all port-forwards

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="${SCRIPT_DIR}/portforward-pids"

echo "Stopping all port-forwards..."

if [ ! -d "$PID_DIR" ]; then
    echo "No PID directory found. Nothing to stop."
    exit 0
fi

STOPPED=0
for pidfile in "$PID_DIR"/*.pid; do
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile")
        service_name=$(basename "$pidfile" .pid)

        if kill -0 "$pid" 2>/dev/null; then
            echo "  Stopping $service_name (PID: $pid)"
            kill "$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null
            STOPPED=$((STOPPED + 1))
        else
            echo "  Process $service_name (PID: $pid) already stopped"
        fi
        rm "$pidfile"
    fi
done

echo ""
echo "Stopped $STOPPED port-forward(s)"
echo "Done!"
