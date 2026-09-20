#!/bin/bash
set -e

echo "=== Submitting Flink Feature Transform Job ==="

docker compose -f docker/docker-compose.city-rover.yml up feature-transform-job
