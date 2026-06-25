#!/bin/bash

# 1. Define Directories
GITHUB_REPO_ROOT_DIR="$HOME/gitlibs/harshad-inarkar/Trade/analysis"
LOCAL_WORKING_ROOT_DIR="$HOME/Documents/projects/trade/analysis"


SOURCE_TARGET=(
    "tradeapi"
    "orchest"
    "utils"
    "apps"
    "push_src_git.sh"
    "pyproject.toml"
    "project_sanity_check.sh"
    "deploy_remote.sh"
    "stop_remote_process.sh"
    "search_replace.sh"
)

# 2. Ensure the destination exists
mkdir -p "$GITHUB_REPO_ROOT_DIR"


#3. Pull Rmote changes first
cd "$GITHUB_REPO_ROOT_DIR" || { echo "Error: Could not change to directory $GITHUB_REPO_ROOT_DIR"; exit 1; }

echo "Pull origin main..."
git pull origin main


echo "Copying directories and files..."
for item in "${SOURCE_TARGET[@]}"; do
    src_path="$LOCAL_WORKING_ROOT_DIR/$item"
    dest_path="$GITHUB_REPO_ROOT_DIR/$item"

    if [ -d "$src_path" ]; then
        rsync -av --delete "$src_path/" "$dest_path/"
        echo "Successfully copied directory $item"
    elif [ -f "$src_path" ]; then
        rsync -av "$src_path" "$dest_path"
        echo "Successfully copied file $item"
    else
        echo "Warning: Source $src_path does not exist. Skipping."
    fi
done

# 5. Change directory to GitHub repo root and push changes

echo "Committing and pushing changes to origin main..."
git add -A .
git status
git commit -m 'pushing latest changes'
git status
git push origin main
git status

echo "Done."