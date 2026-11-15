#!/bin/bash

set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"
NETWORK="city-rover-net"

echo "=== Starting CityRover System (ZIO pipeline) ==="
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
        "rover-telemetry-protobuf:4"
        "rover-analytics:4"
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
# 3. Start Kafka Connect
# ============================================================

echo ""
echo "3. Starting Kafka Connect..."

# ------------------------------------------------------------
# CityRover JSON → Protobuf Kafka Connect plugin
# ------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

CITYROVER_PLUGIN_DIR="$PROJECT_ROOT/custom-plugins/cityrover-json-protobuf"
CITYROVER_JAR="$CITYROVER_PLUGIN_DIR/target/scala-3.3.8/cityrover-json-protobuf-connect-assembly-0.1.0.jar"

CONNECT_PLUGINS_DIR="$PROJECT_ROOT/connect-plugins"
DESTINATION_JAR="$CONNECT_PLUGINS_DIR/cityrover-json-protobuf-connect-assembly-0.1.0.jar"

echo "Checking CityRover JSON → Protobuf Kafka Connect plugin..."

if [ ! -f "$CITYROVER_JAR" ]; then
    echo "CityRover connector JAR not found:"
    echo "  $CITYROVER_JAR"
    echo ""
    echo "Building connector..."

    (
        cd "$CITYROVER_PLUGIN_DIR" || exit 1
        sbt clean assembly
    )

    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to build CityRover connector."
        exit 1
    fi
fi

mkdir -p "$CONNECT_PLUGINS_DIR"

if [ -f "$DESTINATION_JAR" ]; then
    echo "CityRover connector JAR already exists:"
    echo "  $DESTINATION_JAR"
    echo "Skipping copy."
else
    echo "Copying CityRover connector JAR..."
    echo "  From: $CITYROVER_JAR"
    echo "  To:   $DESTINATION_JAR"
    cp "$CITYROVER_JAR" "$DESTINATION_JAR"
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to copy CityRover connector JAR."
        exit 1
    fi
    echo "CityRover connector JAR copied successfully."
fi

docker compose \
    -f "$COMPOSE_FILE" \
    up -d kafka-connect

verify_network "kafka-connect"

wait_for_http \
    "http://localhost:8083/connectors" \
    "Kafka Connect" \
    60 \
    2

# ------------------------------------------------------------
# Verify CityRover connector discovery
# ------------------------------------------------------------

echo ""
echo "Checking Kafka Connect plugin discovery..."

CONNECTOR_CLASS="cityrover.connect.JsonToProtobufConnector"

if curl -fsS "http://localhost:8083/connector-plugins" \
    | jq -e --arg class "$CONNECTOR_CLASS" \
    '.[] | select(.class == $class)' >/dev/null; then

    echo "CityRover JSON → Protobuf connector detected:"
    echo "  $CONNECTOR_CLASS"

else
    echo "ERROR: CityRover JSON → Protobuf connector was not detected."
    echo ""
    echo "Available connector plugins:"
    curl -fsS "http://localhost:8083/connector-plugins" | jq
    exit 1
fi

# ============================================================
# 4. Start MinIO
# ============================================================

echo ""
echo "4. Starting MinIO..."

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
# 5. Start Cassandra
# ============================================================

echo ""
echo "5. Starting Cassandra..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d cassandra

verify_network "cassandra"

echo "Waiting for Cassandra to become ready..."
wait_for_container \
    "cassandra" \
    "cqlsh cassandra 9042 -e 'DESCRIBE KEYSPACES'" \
    "Cassandra" \
    20 \
    10

echo "Cassandra is ready."

# ============================================================
# 6. Start graph-engine
# ============================================================

echo ""
echo "6. Starting graph-engine..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d graph-engine

verify_network "cityrover-graph-engine"

echo "Waiting for graph-engine to complete..."
if docker wait cityrover-graph-engine >/dev/null 2>&1; then
    echo "graph-engine completed."
else
    echo "ERROR: graph-engine did not complete successfully."
    docker logs --tail 100 cityrover-graph-engine || true
    exit 1
fi

# ============================================================
# 7. Start Prometheus
# ============================================================

echo ""
echo "7. Starting Prometheus..."

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
# 8. Start Grafana
# ============================================================

echo ""
echo "8. Starting Grafana..."

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
echo "=== CityRover ZIO system is running ======================="
echo "============================================================"
echo ""

echo "Services:"
echo "  - kafka-1"
echo "  - kafka-connect"
echo "  - minio"
echo "  - cassandra"
echo "  - graph-engine"
echo "  - rover-simulator"
echo "  - prometheus"
echo "  - grafana"
echo "  - feature-transform-zio"
echo "  - feature-writer-zio"

echo ""
echo "Endpoints:"
echo "  Kafka:                          localhost:19092"
echo "  Kafka Connect:                  http://localhost:8083"
echo "  MinIO Console:                  http://localhost:9001"
echo "  MinIO S3 API:                   http://localhost:9002"
echo "  Cassandra CQL:                  localhost:9042"
echo "  Prometheus UI:                  http://localhost:9095"
echo "  Grafana UI:                     http://localhost:3000"
echo "  feature-transform-zio /metrics: http://localhost:9084/metrics"
echo "  feature-writer-zio /metrics:    http://localhost:9085/metrics"

echo ""
echo "Docker network:"
echo "  $NETWORK"

echo ""
echo "CityRover ZIO system startup complete."
echo ""
