#!/bin/bash

set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"
NETWORK="city-rover-net"

echo "=== Starting CityRover System ==="
echo "Compose file: $COMPOSE_FILE"
echo "Docker network: $NETWORK"
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

    echo ""
    echo "ERROR: $description did not become ready."
    echo ""
    echo "--- Last 100 log lines from $container ---"
    docker logs --tail 100 "$container" || true
    echo ""
    exit 1
}

wait_for_http() {
    local url="$1"
    local description="$2"
    local retries="${3:-60}"
    local sleep_seconds="${4:-2}"

    echo ""
    echo "Waiting for $description..."

    for i in $(seq 1 "$retries"); do
        if curl -sf "$url" >/dev/null 2>&1; then
            echo "$description is ready."
            return 0
        fi

        echo "$description not ready yet... retrying ($i/$retries)"
        sleep "$sleep_seconds"
    done

    echo "ERROR: $description did not become ready."
    return 1
}


# ============================================================
# Helper: ensure external Docker network exists
# ============================================================

ensure_network() {

    echo "Checking Docker network: $NETWORK"

    if docker network inspect "$NETWORK" >/dev/null 2>&1; then
        echo "Docker network already exists: $NETWORK"
    else
        echo "Creating Docker network: $NETWORK"
        docker network create "$NETWORK"
        echo "Docker network created."
    fi
}

# ============================================================
# Helper: verify container is attached to city-rover-net
# ============================================================

verify_network() {
    local container="$1"

    if ! docker inspect "$container" \
        --format '{{json .NetworkSettings.Networks}}' \
        | grep -q "\"$NETWORK\""; then

        echo ""
        echo "ERROR: Container '$container' is not attached to '$NETWORK'."
        echo ""
        docker inspect "$container" \
            --format '{{range $name, $network := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' \
            || true
        echo ""
        exit 1
    fi
}


# ============================================================
# 0. Check Docker
# ============================================================

echo "0. Checking Docker..."

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not running."
    exit 1
fi

echo "Docker is running."

# ============================================================
# 0.1 Ensure CityRover Docker network
# ============================================================

echo ""
ensure_network

# ============================================================
# 0.2 Validate Compose configuration
# ============================================================

echo ""
echo "Validating Docker Compose configuration..."

docker compose \
    -f "$COMPOSE_FILE" \
    config >/dev/null

echo "Compose configuration is valid."

# ============================================================
# 1. Start Kafka
# ============================================================

echo ""
echo "1. Starting Kafka..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d kafka-1

verify_network "kafka-1"

wait_for_container \
    "kafka-1" \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list" \
    "Kafka" \
    30 \
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
# 3. Start Schema Registry
# ============================================================

echo ""
echo "3. Starting Schema Registry..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d schema-registry

verify_network "schema-registry"

wait_for_http \
    "http://localhost:8084/subjects" \
    "Schema Registry" \
    60 \
    2

# ============================================================
# 4. Start Kafka Connect
# ============================================================

echo ""
echo "4. Starting Kafka Connect..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d kafka-connect

verify_network "kafka-connect"

wait_for_http \
    "http://localhost:8083/connectors" \
    "Kafka Connect" \
    60 \
    2

# ============================================================
# 5. Start MinIO
# ============================================================

echo ""
echo "5. Starting MinIO..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d minio

verify_network "minio"

echo ""
echo "Initializing MinIO..."

docker compose \
    -f "$COMPOSE_FILE" \
    up minio-setup

echo "MinIO initialized."

# ============================================================
# 6. Start PostgreSQL (Hive Metastore DB)
# ============================================================

echo ""
echo "6. Starting PostgreSQL..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d hive-postgres

verify_network "hive-postgres"

wait_for_container \
    "hive-postgres" \
    "pg_isready -U postgres" \
    "PostgreSQL" \
    30 \
    2

# ============================================================
# 7. Ensure Hive Metastore database exists
# ============================================================

echo ""
echo "Checking Hive Metastore database..."

if docker exec hive-postgres \
    psql -U postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname='hive_metastore'" \
    | grep -q 1; then

    echo "Hive Metastore database already exists."

else

    echo "Creating Hive Metastore database..."

    docker exec hive-postgres \
        psql -U postgres \
        -c "CREATE DATABASE hive_metastore;"

    echo "Hive Metastore database created."

fi

# ============================================================
# 8. Start Hive Metastore
# ============================================================

echo ""
echo "8. Starting Hive Metastore..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d hive-metastore

verify_network "hive-metastore"

wait_for_container \
    "hive-metastore" \
    "nc -z localhost 9083" \
    "Hive Metastore" \
    30 \
    3

# ============================================================
# 9. Start Trino
# ============================================================

echo ""
echo "9. Starting Trino..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d trino

verify_network "trino"

wait_for_container \
    "trino" \
    "curl -sf http://localhost:8080/v1/info" \
    "Trino" \
    30 \
    3

# ============================================================
# 10. Start Flink JobManager
# ============================================================

echo ""
echo "10. Starting Flink JobManager..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d flink-jobmanager

verify_network "flink-jobmanager"

wait_for_container \
    "flink-jobmanager" \
    "curl -sf http://localhost:8081" \
    "Flink JobManager (REST API)" \
    30 \
    3

# ============================================================
# 11. Start Flink TaskManager
# ============================================================

echo ""
echo "11. Starting Flink TaskManager..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d flink-taskmanager

verify_network "flink-taskmanager"

wait_for_container \
    "flink-taskmanager" \
    "curl -sf http://flink-jobmanager:8081/v1/taskmanagers" \
    "Flink TaskManager registration" \
    30 \
    3

# ============================================================
# 12. Start Prometheus
# ============================================================

echo ""
echo "12. Starting Prometheus..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d prometheus

verify_network "prometheus"

wait_for_container \
    "prometheus" \
    "wget -qO- http://localhost:9090/-/ready" \
    "Prometheus" \
    20 \
    2

# ============================================================
# 13. Start Grafana
# ============================================================

echo ""
echo "13. Starting Grafana..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d grafana

verify_network "grafana"

wait_for_container \
    "grafana" \
    "curl -sf http://localhost:3000/api/health" \
    "Grafana" \
    20 \
    2

# ============================================================
# Final network verification
# ============================================================

echo ""
echo "Checking CityRover Docker network..."

echo ""
echo "Containers attached to $NETWORK:"
docker network inspect "$NETWORK" \
    --format '{{range $id, $container := .Containers}}  - {{$container.Name}}{{"\n"}}{{end}}'

# ============================================================
# Final status
# ============================================================

echo ""
echo "============================================================"
echo "=== CityRover System is running ============================"
echo "============================================================"
echo ""

echo "Services:"
echo "  - kafka-1"
echo "  - schema-registry"
echo "  - kafka-connect"
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
echo "  Kafka:                 localhost:19092"
echo "  Schema Registry:       http://localhost:8084"
echo "  Kafka Connect:         http://localhost:8083"
echo "  MinIO Console:         http://localhost:9001"
echo "  MinIO S3 API:          http://localhost:9002"
echo "  Hive Metastore:        thrift://localhost:9096"
echo "  Trino UI:              http://localhost:8090"
echo "  Flink Dashboard:       http://localhost:8081"
echo "  Flink JM Metrics:      http://localhost:9090/metrics"
echo "  Flink TM Metrics:      http://localhost:9091/metrics"
echo "  Prometheus UI:         http://localhost:9095"
echo "  Grafana UI:            http://localhost:3000"

echo ""
echo "Docker network:"
echo "  $NETWORK"

echo ""
echo "CityRover system startup complete."
echo ""
