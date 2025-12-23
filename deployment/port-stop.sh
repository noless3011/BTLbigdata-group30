#!/bin/bash

# Script to stop port-forwarding processes created by port-forward.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="${SCRIPT_DIR}/portforward-pids"
LOG_DIR="${SCRIPT_DIR}/portforward-logs"

echo "========================================"
echo "Stopping Port-Forwarding Services"
echo "========================================"

if [ ! -d "$PID_DIR" ]; then
    echo "No PID directory found at $PID_DIR."
    echo "Nothing to stop."
    exit 0
fi

count=0

# Loop through all .pid files
for pidfile in "$PID_DIR"/*.pid; do
    if [ -f "$pidfile" ]; then
        # Read the PID from the file
        pid=$(cat "$pidfile")
        filename=$(basename "$pidfile" .pid)

        # Check if process exists
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping $filename (PID: $pid)..."
            kill "$pid"
            ((count++))
        else
            echo "Process for $filename (PID: $pid) is already dead."
        fi

        # Remove the PID file
        rm "$pidfile"
    fi
done

# Clean up log files (optional - uncomment if you want to delete logs on stop)
# rm -f "$LOG_DIR"/*.log

echo ""
if [ $count -eq 0 ]; then
    echo "No active port-forward processes were found in $PID_DIR."
else
    echo "Successfully stopped $count process(es)."
fi

# Fallback: Check for any stray kubectl port-forwards
echo ""
stray_count=$(pgrep -f "kubectl port-forward" | wc -l)
if [ "$stray_count" -gt 0 ]; then
    echo "⚠️  Warning: Found $stray_count stray 'kubectl port-forward' processes not tracked by this script."
    echo "To kill them all forcefully, run: pkill -f 'kubectl port-forward'"
fi

echo "Done."
