#!/bin/bash

set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"

echo "=== Starting CityRover System ==="
echo "Compose file: $COMPOSE_FILE"
echo ""

# ------------------------------------------------------------
# 1. Start Kafka
# ------------------------------------------------------------

echo "1. Starting Kafka..."

docker compose -f "$COMPOSE_FILE" up -d kafka-1

echo ""
echo "Waiting for Kafka..."

for i in {1..20}; do

    if docker exec kafka-1 bash -c \
        "/opt/kafka/bin/kafka-topics.sh \
         --bootstrap-server kafka-1:19092 \
         --list" \
        >/dev/null 2>&1; then

        echo "Kafka is ready."
        break
    fi

    echo "Kafka not ready yet... retrying ($i/20)"
    sleep 2

    if [ "$i" -eq 20 ]; then
        echo "ERROR: Kafka did not become ready."
        exit 1
    fi
done


# ------------------------------------------------------------
# 2. Create Kafka topics
# ------------------------------------------------------------

echo ""
echo "2. Creating Kafka topics..."

docker exec kafka-1 bash -c '

    topics=(
        "rover-telemetry-raw:4"
        "rover-telemetry-enriched:4"
        "rover-analytics:4"
        "cityrover-spark-metrics:1"
    )

    for topic in "${topics[@]}"; do

        IFS=":" read -r name partitions <<< "$topic"

        if /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka-1:19092 \
            --describe \
            --topic "$name" \
            >/dev/null 2>&1; then

            echo "Topic exists: $name"

        else

            echo "Creating topic: $name"

            /opt/kafka/bin/kafka-topics.sh \
                --create \
                --topic "$name" \
                --partitions "$partitions" \
                --replication-factor 1 \
                --bootstrap-server kafka-1:19092

        fi
    done
'

echo "Kafka topics initialized."


# ------------------------------------------------------------
# 3. Start MinIO + initialize buckets
# ------------------------------------------------------------

echo ""
echo "3. Starting MinIO..."

docker compose -f "$COMPOSE_FILE" up -d minio

echo ""
echo "Initializing MinIO..."

docker compose -f "$COMPOSE_FILE" up minio-setup

echo "MinIO initialized."


# ------------------------------------------------------------
# 4. Start graph engine
# ------------------------------------------------------------

echo ""
echo "4. Starting graph-engine..."

docker compose -f "$COMPOSE_FILE" up -d graph-engine


# ------------------------------------------------------------
# 5. Start rover simulator
# ------------------------------------------------------------

echo ""
echo "5. Starting rover-simulator..."

docker compose -f "$COMPOSE_FILE" up -d rover-simulator


# ------------------------------------------------------------
# Done
# ------------------------------------------------------------

echo ""
echo "=== CityRover System is running ==="
echo ""
echo "Services:"
echo "  - kafka-1"
echo "  - minio"
echo "  - graph-engine"
echo "  - rover-simulator"
echo ""
echo "MinIO:"
echo "  http://localhost:9002"
echo ""
echo "Kafka:"
echo "  kafka-1:19092"
