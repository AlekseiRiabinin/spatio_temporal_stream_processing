#!/bin/bash

set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"

echo "=== Starting CityRover System ==="
echo "Compose file: $COMPOSE_FILE"
echo ""

# ============================================================
# Helper: wait for container readiness
# ============================================================

wait_for_container() {
    local container="$1"
    local command="$2"
    local description="$3"
    local retries="${4:-30}"
    local sleep_seconds="${5:-2}"

    echo ""
    echo "Waiting for $description..."

    for i in $(seq 1 "$retries"); do
        if docker exec "$container" bash -c "$command" >/dev/null 2>&1; then
            echo "$description is ready."
            return 0
        fi

        echo "$description not ready yet... retrying ($i/$retries)"
        sleep "$sleep_seconds"
    done

    echo "ERROR: $description did not become ready."
    docker logs --tail 100 "$container"
    exit 1
}

# ============================================================
# 1. Start Kafka
# ============================================================

echo "1. Starting Kafka..."
docker compose -f "$COMPOSE_FILE" up -d kafka-1

wait_for_container \
    "kafka-1" \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list" \
    "Kafka" \
    20 \
    2

# ============================================================
# 2. Create Kafka topics
# ============================================================

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
            --topic "$name" >/dev/null 2>&1; then
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

# ============================================================
# 3. Start MinIO
# ============================================================

echo ""
echo "3. Starting MinIO..."
docker compose -f "$COMPOSE_FILE" up -d minio

echo ""
echo "Initializing MinIO..."
docker compose -f "$COMPOSE_FILE" up minio-setup

echo "MinIO initialized."

# ============================================================
# 4. Start PostgreSQL (Hive Metastore DB)
# ============================================================

echo ""
echo "4. Starting PostgreSQL..."
docker compose -f "$COMPOSE_FILE" up -d hive-postgres

wait_for_container \
    "hive-postgres" \
    "pg_isready -U postgres" \
    "PostgreSQL" \
    30 \
    2

# ============================================================
# 5. Ensure Hive Metastore database exists
# ============================================================

echo ""
echo "Checking Hive Metastore database..."

if docker exec hive-postgres \
    psql -U postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname='hive_metastore'" | grep -q 1; then
    echo "Hive Metastore database already exists."
else
    echo "Creating Hive Metastore database..."
    docker exec hive-postgres \
        psql -U postgres \
        -c "CREATE DATABASE hive_metastore;"
    echo "Hive Metastore database created."
fi

# ============================================================
# 6. Start Hive Metastore
# ============================================================

echo ""
echo "6. Starting Hive Metastore..."
docker compose -f "$COMPOSE_FILE" up -d hive-metastore

wait_for_container \
    "hive-metastore" \
    "nc -z localhost 9083" \
    "Hive Metastore" \
    30 \
    3

# ============================================================
# 7. Start Trino
# ============================================================

echo ""
echo "7. Starting Trino..."
docker compose -f "$COMPOSE_FILE" up -d trino

wait_for_container \
    "trino" \
    "curl -sf http://localhost:8080/v1/info" \
    "Trino" \
    30 \
    3

# ============================================================
# 8. Start Graph Engine
# ============================================================

echo ""
echo "8. Starting graph-engine..."
docker compose -f "$COMPOSE_FILE" up -d graph-engine

# ============================================================
# 9. Start Rover Simulator
# ============================================================

echo ""
echo "9. Starting rover-simulator..."
docker compose -f "$COMPOSE_FILE" up -d rover-simulator

# ============================================================
# Done
# ============================================================

echo ""
echo "============================================================"
echo "=== CityRover System is running ============================"
echo "============================================================"
echo ""
echo "Services:"
echo "  - kafka-1"
echo "  - minio"
echo "  - hive-postgres"
echo "  - hive-metastore"
echo "  - trino"
echo "  - graph-engine"
echo "  - rover-simulator"
echo ""
echo "MinIO Console: http://localhost:9001"
echo "MinIO S3 API: http://localhost:9002"
echo "Hive Metastore: thrift://localhost:9096"
echo "Trino UI: http://localhost:8090"
echo "Kafka: kafka-1:19092"
echo ""
