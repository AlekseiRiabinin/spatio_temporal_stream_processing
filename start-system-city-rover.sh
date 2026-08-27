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
        if docker exec "$container" sh -c "$command" >/dev/null 2>&1; then
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
# 8. Start Flink JobManager
# ============================================================

echo ""
echo "8. Starting Flink JobManager..."
docker compose -f "$COMPOSE_FILE" up -d flink-jobmanager

wait_for_container \
    "flink-jobmanager" \
    "curl -sf http://localhost:8081" \
    "Flink JobManager (REST API)" \
    30 \
    3

# ============================================================
# 9. Start Flink TaskManager
# ============================================================

echo ""
echo "9. Starting Flink TaskManager..."
docker compose -f "$COMPOSE_FILE" up -d flink-taskmanager

wait_for_container \
    "flink-taskmanager" \
    "curl -sf http://flink-jobmanager:8081/v1/taskmanagers" \
    "Flink TaskManager registration" \
    30 \
    3

# ============================================================
# 10. Start Prometheus
# ============================================================

echo ""
echo "10. Starting Prometheus..."
docker compose -f "$COMPOSE_FILE" up -d prometheus

wait_for_container \
    "prometheus" \
    "wget -qO- http://localhost:9090/-/ready" \
    "Prometheus" \
    20 \
    2

# ============================================================
# 11. Start Grafana
# ============================================================

echo ""
echo "11. Starting Grafana..."
docker compose -f "$COMPOSE_FILE" up -d grafana

wait_for_container \
    "grafana" \
    "curl -sf http://localhost:3000/api/health" \
    "Grafana" \
    20 \
    2

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
echo "  - flink-jobmanager"
echo "  - flink-taskmanager"
echo "  - prometheus"
echo "  - grafana"
echo ""
echo "Endpoints:"
echo "  MinIO Console:        http://localhost:9001"
echo "  MinIO S3 API:         http://localhost:9002"
echo "  Hive Metastore:       thrift://localhost:9096"
echo "  Trino UI:             http://localhost:8090"
echo "  Kafka Broker:         kafka-1:19092"
echo "  Flink Dashboard:      http://localhost:8081"
echo "  Flink JM Metrics:     http://localhost:9090/metrics"
echo "  Flink TM Metrics:     http://localhost:9091/metrics"
echo "  Prometheus UI:        http://localhost:9095"
echo "  Grafana UI:           http://localhost:3000"
echo ""
echo "CityRover system startup complete."
echo ""
