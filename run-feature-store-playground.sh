#!/bin/bash
set -e

echo "=== Submitting Feature Store Job ==="

docker compose -f docker/docker-compose.fs-stream.yml up feature_store_playground
