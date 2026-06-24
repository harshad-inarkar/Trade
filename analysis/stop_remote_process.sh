#!/bin/bash

# --- Configuration ---
REMOTE_USER="ubuntu"
REMOTE_HOST="80.225.247.216"


# Step 3: Automate Remote Orchestration via Tmux
echo "🤖 Automating Orchestration on server..."
ssh "$REMOTE_USER@$REMOTE_HOST" << EOF
    
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

    sleep 1

    echo "🎉 Process Stopped!"
EOF

echo "✨ Process Stopped!"