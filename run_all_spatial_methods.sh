#!/bin/bash
set -e

echo "=== Article 03: Spatial Methods Pipeline ==="

cd parser
source venv/bin/activate

PARSED_DIR="../data/article_3/parsed/timestamp"

# ---------------------------------------------------------
# Parse flags
# ---------------------------------------------------------
FORCE=false
PARSE=true
VIZ=true

for arg in "$@"; do
    case $arg in
        --force)
            FORCE=true
            ;;
        --parse)
            PARSE=true
            VIZ=false
            ;;
        --viz)
            PARSE=false
            VIZ=true
            ;;
        *)
            echo "Unknown flag: $arg"
            echo "Usage:"
            echo "  ./run_all_spatial_methods.sh          # parse + viz"
            echo "  ./run_all_spatial_methods.sh --force  # force re-parse + viz"
            echo "  ./run_all_spatial_methods.sh --parse  # only parse"
            echo "  ./run_all_spatial_methods.sh --viz    # only visualize"
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------
# Step 1: Parsing
# ---------------------------------------------------------
if $PARSE; then
    if $FORCE; then
        echo "--- Force flag detected: re-parsing logs ---"
        python3 parse_spatial_methods.py
    elif ls $PARSED_DIR/*.csv 1> /dev/null 2>&1; then
        echo "--- Parsed data already exists. Skipping parsing step. ---"
    else
        echo "--- Step 1: Parsing logs ---"
        python3 parse_spatial_methods.py
    fi

    echo "--- Parsing complete ---"
else
    echo "--- Skipping parsing step (viz-only mode) ---"
fi

# ---------------------------------------------------------
# Step 2: Visualization
# ---------------------------------------------------------
if $VIZ; then
    echo "--- Step 2: Generating visualizations ---"
    python3 visualize_spatial_methods.py
    echo "--- Visualization complete ---"
else
    echo "--- Skipping visualization step (parse mode) ---"
fi

echo "=== All tasks finished successfully ==="
