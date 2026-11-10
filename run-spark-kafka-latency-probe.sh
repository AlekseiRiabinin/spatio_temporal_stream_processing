#!/bin/bash
set -e

echo "=== Submitting Spark Kafka Latency Probe ==="

docker compose -f docker/docker-compose.fs-stream.yml up spark_kafka_latency_probe
