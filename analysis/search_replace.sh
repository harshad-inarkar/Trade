#!/bin/bash

usage() {
    echo "Usage:"
    echo "  $0 -s \"search_txt\"                    # Search for text in .py files in current dir"
    echo "  $0 -s \"search_txt\" -r \"replace_txt\"        # Search and replace text in .py files"
    echo "  $0 -d \"/path/to/dir\" -s \"search_txt\"       # Specify directory to search (default: current dir)"
    echo "  $0 -e \"py, js, html\"                  # Specify file extensions to search (default: py)"
    echo "  $0 -d \"/path/to/dir\" -s \"search_txt\" -r \"replace_txt\"  # Search & replace in target dir"
    exit 1
}

SEARCH=""
REPLACE=""
SEARCH_DIR="."
EXTENSIONS="py" # Default extension

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s)
            shift
            if [[ -z "$1" ]]; then
                echo "Error: -s flag requires an argument."
                usage
            fi
            SEARCH="$1"
            ;;
        -r)
            shift
            if [[ -z "$1" ]]; then
                echo "Error: -r flag requires an argument."
                usage
            fi
            REPLACE="$1"
            ;;
        -d)
            shift
            if [[ -z "$1" ]]; then
                echo "Error: -d flag requires an argument."
                usage
            fi
            SEARCH_DIR="$1"
            ;;
        -e)
            shift
            if [[ -z "$1" ]]; then
                echo "Error: -e flag requires an argument."
                usage
            fi
            EXTENSIONS="$1"
            ;;
        *)
            echo "Unknown argument: $1"
            usage
            ;;
    esac
    shift
done

if [[ -z "$SEARCH" ]]; then
    echo "Error: -s (search) argument is required."
    usage
fi

if [[ ! -d "$SEARCH_DIR" ]]; then
    echo "Error: Target directory '$SEARCH_DIR' does not exist."
    exit 1
fi

# 1. Parse extensions into a format `find` can use
# Remove spaces, then split by comma
CLEAN_EXTS="${EXTENSIONS// /}"
IFS=',' read -r -a EXT_ARRAY <<< "$CLEAN_EXTS"

FIND_EXT_ARGS=()
for i in "${!EXT_ARRAY[@]}"; do
    ext="${EXT_ARRAY[$i]}"
    # Strip leading dots just in case the user typed ".py" instead of "py"
    ext="${ext#.}"
    
    if [[ $i -gt 0 ]]; then
        FIND_EXT_ARGS+=("-o")
    fi
    FIND_EXT_ARGS+=("-name" "*.$ext")
done

# 2. Array to safely hold filenames
FILES=()

# 3. Skip noisy directories and apply the dynamic extension filter
while IFS= read -r file; do
    [[ -n "$file" ]] && FILES+=("$file")
done < <(find "$SEARCH_DIR" -type d \( -name ".git" -o -name "venv" -o -name ".venv" -o -name "python*" -o -name "__pycache__" \) -prune -o -type f \( "${FIND_EXT_ARGS[@]}" \) -exec grep -Fl -- "$SEARCH" {} +)

if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "No files containing '$SEARCH' were found in $SEARCH_DIR for extensions: $EXTENSIONS."
    exit 0
fi

if [[ -z "$REPLACE" ]]; then
    echo "Found files containing '$SEARCH' in $SEARCH_DIR:"
    echo "---------------------------------------"
    for file in "${FILES[@]}"; do
        echo "  - $file"
    done
    echo "---------------------------------------"
    echo "Done! (No replacements performed)"
else
    echo "Replacing '$SEARCH' with '$REPLACE'."
    echo "---------------------------------------"
    echo "Modified files:"
    
    SEARCH_SED="${SEARCH//./\\.}"
    SEARCH_SED="${SEARCH_SED//|/\\|}"
    REPLACE_SED="${REPLACE//|/\\|}"
    
    for file in "${FILES[@]}"; do
        echo "  - $file"
        sed -i '' "s|${SEARCH_SED}|${REPLACE_SED}|g" "$file"
    done
    echo "---------------------------------------"
    echo "Done!"
fi