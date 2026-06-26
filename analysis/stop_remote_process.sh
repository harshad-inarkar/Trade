#!/bin/bash

# --- Configuration ---
REMOTE_HOST="oracle-server"

echo "🤖 Stop Process on $REMOTE_HOST..."
ssh "$REMOTE_HOST" << EOF
    
    # Kill old session to avoid 'address already in use' port 8000 blockages
    echo "🛑 Killing existing 'bot' tmux session if active..."
    tmux kill-session -t bot 2>/dev/null
    
    # Grace period for bound sockets to clear natively
    sleep 1

    # Check for and kill any process bound to port 8000
    echo "🔍 Checking for stuck processes on port 8000..."
    STUCK_PID=\$(sudo lsof -t -i:8000)
    if [ ! -z "\$STUCK_PID" ]; then
        echo "🛑 Killing zombie process (\$STUCK_PID) bound to port 8000..."
        sudo kill -9 \$STUCK_PID
    else
        echo "✅ Port 8000 is clear."
    fi

    echo "🔍 Checking for stuck processes on port 5000..."
    STUCK_PID=\$(sudo lsof -t -i:5000)
    if [ ! -z "\$STUCK_PID" ]; then
        echo "🛑 Killing zombie process (\$STUCK_PID) bound to port 5000..."
        sudo kill -9 \$STUCK_PID
    else
        echo "✅ Port 5000 is clear."
    fi

    sleep 1

EOF

echo "✅ Process Stopped!"