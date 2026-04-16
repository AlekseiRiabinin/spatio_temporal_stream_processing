#!/bin/bash
set -e

echo "=== Article 03: Parsing + Visualization Pipeline ==="

cd parser
source venv/bin/activate

PARSED_DIR="../data/article_3/parsed/timestamp"

FORCE=false
if [[ "$1" == "--force" ]]; then
    FORCE=true
fi

# ---------------------------------------------------------
# Step 1: Run parser only if needed or forced
# ---------------------------------------------------------
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

# ---------------------------------------------------------
# Step 2: Always run visualization
# ---------------------------------------------------------
echo "--- Step 2: Generating visualizations ---"
python3 visualize_spatial_methods.py
echo "--- Visualization complete ---"

echo "=== All tasks finished successfully ==="


# ./run_all_spatial_methods.sh --force
