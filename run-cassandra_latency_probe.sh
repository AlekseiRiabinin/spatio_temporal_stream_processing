#!/bin/bash
set -e

echo "=== Submitting Cassandra Latency Probe ==="

docker compose -f docker/docker-compose.fs-stream.yml up cassandra_latency_probe
