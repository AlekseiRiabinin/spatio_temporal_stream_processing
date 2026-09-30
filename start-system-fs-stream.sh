#!/bin/bash
set -e

COMPOSE_FILE="docker/docker-compose.fs-stream.yml"
NETWORK="city-rover-net"

# ------------------------------------------------------------
# FS cluster container names & endpoints
# ------------------------------------------------------------
FS_JM_CONTAINER="flink-jobmanager-fs"
FS_TM_CONTAINER="flink-taskmanager-fs"
FS_JM_HOST="flink-jobmanager-fs"
FS_JM_REST_INTERNAL="http://flink-jobmanager-fs:8081"
FS_JM_REST_EXTERNAL="http://localhost:8082"
FS_TM_METRICS_EXTERNAL="http://localhost:9093/metrics"
FS_JM_METRICS_EXTERNAL="http://localhost:9092/metrics"

echo "=== Starting FS-Stream System ==="
echo "Compose file: $COMPOSE_FILE"
echo "Docker network: $NETWORK"
echo ""

# ============================================================
# Helpers
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
    echo "--- Last 100 log lines from $container ---"
    docker logs --tail 100 "$container" || true
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

ensure_network() {
    echo "Checking Docker network: $NETWORK"
    if docker network inspect "$NETWORK" >/dev/null 2>&1; then
        echo "Docker network already exists: $NETWORK"
    else
        echo "Creating Docker network: $NETWORK"
        docker network create "$NETWORK"
    fi
}

verify_network() {
    local container="$1"
    if ! docker inspect "$container" \
        --format '{{json .NetworkSettings.Networks}}' \
        | grep -q "\"$NETWORK\""; then
        echo ""
        echo "ERROR: Container '$container' is not attached to '$NETWORK'."
        docker inspect "$container" \
            --format '{{range $name, $network := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' || true
        exit 1
    fi
}

# ============================================================
# 0. Docker check + network
# ============================================================
echo "0. Checking Docker..."
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not running."
    exit 1
fi
echo "Docker is running."

echo ""
ensure_network

echo ""
echo "Validating Docker Compose configuration..."
docker compose -f "$COMPOSE_FILE" config >/dev/null
echo "Compose configuration is valid."

# ============================================================
# 1. Kafka
# ============================================================
echo ""
echo "1. Starting Kafka..."
docker compose -f "$COMPOSE_FILE" up -d kafka-1
verify_network "kafka-1"

wait_for_container \
    "kafka-1" \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list" \
    "Kafka" \
    30 \
    2

# ============================================================
# 2. Kafka topics (FS-specific only)
# ============================================================
echo ""
echo "2. Creating Kafka topics..."
docker exec kafka-1 bash -c '
    topics=(
        "stream_fs.test.intellinx_antifraud_dbo_fin_transactions:4"
        "stream_fs.test.intellinx_antifraud_dbo_nofin_transactions:4"
        "stream_fs.test.intellinx_antifraud_dbo_incoming_payments:4"
    )
    for topic in "${topics[@]}"; do
        IFS=":" read -r name partitions <<< "$topic"
        if /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka-1:19092 \
            --describe --topic "$name" >/dev/null 2>&1; then
            echo "Topic exists: $name"
        else
            echo "Creating topic: $name"
            /opt/kafka/bin/kafka-topics.sh \
                --create --topic "$name" \
                --partitions "$partitions" \
                --replication-factor 1 \
                --bootstrap-server kafka-1:19092
        fi
    done
'
echo "Kafka topics initialized."

# ============================================================
# 3. Cassandra
# ============================================================
echo ""
echo "3. Starting Cassandra..."
docker compose -f "$COMPOSE_FILE" up -d cassandra
verify_network "cassandra"

wait_for_container \
    "cassandra" \
    "cqlsh cassandra 9042 -e 'DESCRIBE KEYSPACES'" \
    "Cassandra" \
    20 \
    10
echo "Cassandra is ready."

# ============================================================
# 4. FS Flink JobManager
# ============================================================
echo ""
echo "4. Starting FS Flink JobManager..."
docker compose -f "$COMPOSE_FILE" up -d "$FS_JM_CONTAINER"
verify_network "$FS_JM_CONTAINER"

wait_for_container \
    "$FS_JM_CONTAINER" \
    "curl -sf http://localhost:8081" \
    "FS Flink JobManager (REST API)" \
    30 \
    3

# ============================================================
# 5. FS Flink TaskManager
# ============================================================
echo ""
echo "5. Starting FS Flink TaskManager..."
docker compose -f "$COMPOSE_FILE" up -d "$FS_TM_CONTAINER"
verify_network "$FS_TM_CONTAINER"

wait_for_container \
    "$FS_TM_CONTAINER" \
    "curl -sf ${FS_JM_REST_INTERNAL}/v1/taskmanagers" \
    "FS Flink TaskManager registration" \
    30 \
    3

# ============================================================
# 6. Verify Kafka connector JAR inside FS Flink image
# ============================================================
echo ""
echo "6. Verifying Kafka connector JAR in FS Flink image..."
KAFKA_JAR_COUNT=$(docker exec "$FS_JM_CONTAINER" sh -c 'ls /opt/flink/lib/ | grep -ci kafka' || true)
if [ "$KAFKA_JAR_COUNT" -gt 0 ]; then
    echo "Kafka connector JAR(s) found:"
    docker exec "$FS_JM_CONTAINER" sh -c 'ls /opt/flink/lib/ | grep -i kafka'
else
    echo "WARNING: No Kafka connector JAR found in /opt/flink/lib/"
    echo "The PyFlink job will need KAFKA_CONNECTOR_JAR to point to a valid JAR,"
    echo "or the JAR must be mounted into the FS Flink image."
fi

# ============================================================
# 7. Prometheus (optional — only if defined in fs-stream.yml)
# ============================================================
if docker compose -f "$COMPOSE_FILE" config --services | grep -q '^prometheus$'; then
    echo ""
    echo "7. Starting Prometheus..."
    docker compose -f "$COMPOSE_FILE" up -d prometheus
    verify_network "prometheus"
    wait_for_container \
        "prometheus" \
        "wget -qO- http://localhost:9090/-/ready" \
        "Prometheus" \
        20 \
        2
else
    echo ""
    echo "7. Prometheus not defined in $COMPOSE_FILE — skipping."
fi

# ============================================================
# 8. Grafana (optional)
# ============================================================
if docker compose -f "$COMPOSE_FILE" config --services | grep -q '^grafana$'; then
    echo ""
    echo "8. Starting Grafana..."
    docker compose -f "$COMPOSE_FILE" up -d grafana
    verify_network "grafana"
    wait_for_container \
        "grafana" \
        "curl -sf http://localhost:3000/api/health" \
        "Grafana" \
        20 \
        2
else
    echo ""
    echo "8. Grafana not defined in $COMPOSE_FILE — skipping."
fi

# ============================================================
# Final network verification
# ============================================================
echo ""
echo "Checking Docker network: $NETWORK"
echo ""
echo "Containers attached to $NETWORK:"
docker network inspect "$NETWORK" \
    --format '{{range $id, $container := .Containers}}  - {{$container.Name}}{{"\n"}}{{end}}'

# ============================================================
# Final status
# ============================================================
echo ""
echo "============================================================"
echo "=== FS-Stream System is running ============================"
echo "============================================================"
echo ""
echo "Services:"
echo "  - kafka-1"
echo "  - cassandra"
echo "  - $FS_JM_CONTAINER"
echo "  - $FS_TM_CONTAINER"
echo ""
echo "Endpoints:"
echo "  Kafka:                 localhost:19092"
echo "  Cassandra CQL:         localhost:9042"
echo "  Flink FS Dashboard:    $FS_JM_REST_EXTERNAL"
echo "  Flink FS JM Metrics:   $FS_JM_METRICS_EXTERNAL"
echo "  Flink FS TM Metrics:   $FS_TM_METRICS_EXTERNAL"
echo ""
echo "Docker network:"
echo "  $NETWORK"
echo ""
echo "FS-Stream system startup complete."
echo ""
