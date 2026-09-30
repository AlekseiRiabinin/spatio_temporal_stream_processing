#!/bin/bash
set -e

echo "=== Submitting Feature Store Job ==="

docker compose -f docker/docker-compose.city-rover.yml up feature_store_playground
