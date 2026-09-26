#!/bin/bash
set -e

echo "=== Submitting Latency Research Flink Job ==="

docker compose -f docker/docker-compose.city-rover.yml up feature-writer-job
