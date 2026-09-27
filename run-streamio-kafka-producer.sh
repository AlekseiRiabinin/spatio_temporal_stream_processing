#!/bin/bash
set -e

echo "=== Submitting Streamio Kafka Producer ==="

docker compose -f docker/docker-compose.city-rover.yml up streamio-kafka-producer-test
