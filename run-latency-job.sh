#!/bin/bash
set -e

echo "=== Submitting Latency Research Flink Job ==="

docker compose -f docker/docker-compose.city-rover.yml up rover-flink-latency-job
