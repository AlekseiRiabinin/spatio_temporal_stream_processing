#!/bin/bash
set -e

echo "=== Submitting Trajectory Visualizer Spark Job ==="

docker compose -f docker/docker-compose.city-rover.yml up trajectory-visualizer-job
