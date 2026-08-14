#!/bin/bash

set -e

# Compose file lives inside ./docker/
COMPOSE_FILE="docker/docker-compose.city-rover.yml"

echo "=== Starting CityRover System ==="
echo "Compose file: $COMPOSE_FILE"
echo ""

# ------------------------------------------------------------
# 1. Start Kafka first (required for rover-simulator)
# ------------------------------------------------------------
echo "1. Starting Kafka (Single-Node KRaft)..."
docker compose -f "$COMPOSE_FILE" up -d kafka-1

echo ""
echo "2. Waiting for Kafka to become ready..."

for i in {1..20}; do
    if docker exec kafka-1 bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list" >/dev/null 2>&1; then
        echo "Kafka is ready."
        break
    fi
    echo "Kafka not ready yet... retrying ($i/20)"
    sleep 2
done

# ------------------------------------------------------------
# 3. Create required Kafka topics
# ------------------------------------------------------------
echo ""
echo "3. Creating Kafka topics..."

docker exec kafka-1 bash -c '
    topics=(
        "rover-telemetry-raw:4"
        "rover-telemetry-enriched:4"
        "rover-analytics:4"
        "cityrover-spark-metrics:1"
    )

    for topic in "${topics[@]}"; do
        IFS=":" read -r name partitions <<< "$topic"

        if /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic "$name" >/dev/null 2>&1; then
            echo "Topic exists: $name"
        else
            echo "Creating topic: $name"
            /opt/kafka/bin/kafka-topics.sh --create \
                --topic "$name" \
                --partitions "$partitions" \
                --replication-factor 1 \
                --bootstrap-server kafka-1:19092
        fi
    done
'

echo ""
echo "Kafka topics created successfully."
docker exec kafka-1 bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list"

# ------------------------------------------------------------
# 4. Start graph-engine + rover-simulator
# ------------------------------------------------------------
echo ""
echo "4. Starting graph-engine and rover-simulator..."
docker compose -f "$COMPOSE_FILE" up -d graph-engine rover-simulator

echo ""
echo "=== CityRover System is running ==="
echo "Services:"
echo "  - kafka-1"
echo "  - graph-engine"
echo "  - rover-simulator"
echo ""
echo "Graph data volume: graph-data/"
