#!/bin/bash

# --- Configuration ---
REMOTE_HOST="oracle-server"
TARGET_DIR="~/trade_client"

# --- Source Files & Folders ---
SOURCE_TARGET=(
    "tradeapi"
    "orchest"
    "utils"
    "apps"
    "pyproject.toml"
)

# --- Argument Parsing ---
LOG_LEVEL="critical"
REFRESH_MASTER_SCRIPT="false"

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --log_level)
        LOG_LEVEL="$2"
        shift; shift
        ;;
        --refresh_master_script)
        REFRESH_MASTER_SCRIPT="true"
        shift
        ;;
        *)
        echo "Unknown argument: $key"
        exit 1
        ;;
    esac
done

echo "##############"
echo "🚀 Starting deployment to $REMOTE_HOST..."
echo "Using log_level=$LOG_LEVEL"
echo "With refresh_master_script=$REFRESH_MASTER_SCRIPT"

# Step 1: Create target directory remotely
ssh "$REMOTE_HOST" "mkdir -p $TARGET_DIR"

# Step 2: Sync files using rsync (Legacy macOS Compatible Mirror Mode)
echo "📦 Syncing source code..."
rsync -avz --delete -e "ssh" \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --exclude '*.log' \
    --exclude 'venv' \
    --exclude '.DS_Store' \
    "${SOURCE_TARGET[@]}" \
    "$REMOTE_HOST:$TARGET_DIR"

echo "✅ Rsync Complete!"

# Step 3: Automate Remote Orchestration via Tmux

echo "🤖 Automating Orchestration on server..."
ssh "$REMOTE_HOST" bash << EOF
    cd $TARGET_DIR

    # Activate virtual environment if present
    source ~/venv/bin/activate

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

    echo "🔍 Checking for stuck processes on port 5000..."
    STUCK_PID=\$(sudo lsof -t -i:5000)
    if [ ! -z "\$STUCK_PID" ]; then
        echo "🛑 Killing zombie process (\$STUCK_PID) bound to port 5000..."
        sudo kill -9 \$STUCK_PID
    else
        echo "✅ Port 5000 is clear."
    fi

    sleep 1

    # Spin up fresh orchestrator inside a detached background tmux window

    echo "🚀 Initializing orchestrator inside fresh 'bot' tmux session..."

    tmux new-session -d -s bot "export log_level='$LOG_LEVEL' && export refresh_master_script='$REFRESH_MASTER_SCRIPT' && python orchest/start_orchest.py -ml trade_app vol_app"

    echo "🎉 Server execution handed off safely!"
EOF

echo "✨ Deployment and automation workflow complete!"