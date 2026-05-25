#!/bin/bash
set -e

COMPOSE="docker/docker-compose.adaptive-control.yml"

echo "=== Producer-only test (Article 04) ==="

# 1. Start Kafka only
echo "=== Kafka ==="
docker compose -f "$COMPOSE" up -d kafka-1

# 2. Wait for Kafka (simple safety delay)
echo "Waiting for Kafka to stabilize..."
sleep 15

# 3. Create topic
echo "=== Creating Kafka topic ==="
docker exec kafka-1 bash -c '
  kafka-topics.sh --create \
    --topic spatial-events \
    --partitions 4 \
    --replication-factor 1 \
    --if-not-exists \
    --bootstrap-server kafka-1:19092
'

# 4. Start ONLY producer
echo "=== Starting Geo Producer ==="
docker compose -f "$COMPOSE" up -d geo_producer_adaptive_control

echo "======================================"
echo "Producer test running"
echo "======================================"
