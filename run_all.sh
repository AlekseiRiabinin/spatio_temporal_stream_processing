#!/bin/bash

OUTPUT="results.txt"
> "$OUTPUT"  # clear file

for f in logs/*.log; do
  echo "Processing $f" >> "$OUTPUT"
  ./parse_metrics.sh "$f" >> "$OUTPUT"
  echo "----------------------" >> "$OUTPUT"
done
