#!/bin/bash
set -e

echo "=== Submitting Flink Feature Writer Job ==="

docker compose -f docker/docker-compose.city-rover.yml up feature-writer-job
