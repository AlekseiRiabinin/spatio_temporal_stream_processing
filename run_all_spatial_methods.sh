#!/bin/bash

OUTPUT="results_spatial_methods.txt"
> "$OUTPUT"

for f in logs/*.log; do
  echo "Processing $f" >> "$OUTPUT"
  ./parse_metrics_spatial_methods.sh "$f" >> "$OUTPUT"
  echo "----------------------" >> "$OUTPUT"
done
