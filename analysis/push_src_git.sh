#!/bin/bash

# 1. Define Directories
GITHUB_REPO_ROOT_DIR="$HOME/gitlibs/harshad-inarkar/Trade/analysis"
LOCAL_WORKING_ROOT_DIR="$HOME/Documents/projects/trade/analysis"

# List of source directories to copy
SOURCE_DIRS=("tradeview" "tradeapi" "orchest" "utils" "web_scripts")

# 2. Ensure the destination exists
mkdir -p "$GITHUB_REPO_ROOT_DIR"


#3. Pull Rmote changes first
cd "$GITHUB_REPO_ROOT_DIR" || { echo "Error: Could not change to directory $GITHUB_REPO_ROOT_DIR"; exit 1; }

echo "Pull origin main..."
git pull origin main


# 4. Force copy source directories to GitHub repo root
echo "Copying directories..."
for dir in "${SOURCE_DIRS[@]}"; do
    src_path="$LOCAL_WORKING_ROOT_DIR/$dir"
    
    if [ -d "$src_path" ]; then
        # -r: recursive, -f: force, -v: verbose
        rsync -av --delete "$src_path/" "$GITHUB_REPO_ROOT_DIR/$dir/"
        echo "Successfully copied $dir"
    else
        echo "Warning: Source directory $src_path does not exist. Skipping."
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