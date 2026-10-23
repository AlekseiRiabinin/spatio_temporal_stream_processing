#!/bin/bash
set -e

echo "=== Submitting Feature Store Data Generator ==="

docker compose -f docker/docker-compose.fs-stream.yml up feature_store_data_generator
