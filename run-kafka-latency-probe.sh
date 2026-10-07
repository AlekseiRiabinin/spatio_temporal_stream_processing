#!/bin/bash
set -e

echo "=== Submitting Kafka Latency Probe ==="

docker compose -f docker/docker-compose.fs-stream.yml up kafka_latency_probe
