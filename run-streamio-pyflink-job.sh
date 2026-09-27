#!/bin/bash
set -e

echo "=== Submitting Streamio PyFlink Job ==="

docker compose -f docker/docker-compose.city-rover.yml up streamio-pyflink-job
