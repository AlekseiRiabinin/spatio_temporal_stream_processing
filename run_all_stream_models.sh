#!/bin/bash

OUTPUT="results_stream_models.txt"
> "$OUTPUT"  # clear file

for f in logs/*.log; do
  echo "Processing $f" >> "$OUTPUT"
  ./parse_metrics_stream_models.sh "$f" >> "$OUTPUT"
  echo "----------------------" >> "$OUTPUT"
done
