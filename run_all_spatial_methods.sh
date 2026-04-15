#!/bin/bash
set -e

echo "=== Parsing Article 03 Spatial Methods Logs ==="

cd parser
source venv/bin/activate

python3 parser_spatial_methods.py

echo "=== Parsing complete ==="
