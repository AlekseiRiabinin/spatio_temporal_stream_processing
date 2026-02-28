#!/bin/bash
set -e

COMPOSE="docker/docker-compose.architecture.yml"

echo "=== Starting Spatial Stream Processing System ==="

# ------------------------------------------------------------
# Utility: check if service exists in compose
# ------------------------------------------------------------
service_exists() {
    docker compose -f "$COMPOSE" config --services | grep -q "^$1$"
}

# ------------------------------------------------------------
# Utility: check if service is running
# ------------------------------------------------------------
is_running() {
    docker compose -f "$COMPOSE" ps "$1" | grep -q "Up"
}

# ------------------------------------------------------------
# Utility: wait for container health
# ------------------------------------------------------------
wait_for_health() {
    local service=$1
    local attempts=20

    echo "Waiting for $service to become healthy..."

    for i in $(seq 1 $attempts); do
        status=$(docker inspect -f '{{.State.Health.Status}}' "$service" 2>/dev/null || echo "unknown")

        if [ "$status" = "healthy" ]; then
            echo "$service is healthy"
            return 0
        fi

        echo "  → $service status: $status ($i/$attempts)"
        sleep 3
    done

    echo "ERROR: $service failed to become healthy"
    docker compose -f "$COMPOSE" logs "$service" --tail=50
    exit 1
}

# ------------------------------------------------------------
# Utility: create required Kafka topics (idempotent)
# ------------------------------------------------------------
create_kafka_topics() {
    echo "=== Creating Kafka topics ==="

    # Give Kafka a moment to fully start
    sleep 5

    docker exec kafka-1 bash -c '
        set -e

        # Define topics as: name:partitions
        topics=(
            "spatial-events:20"
        )

        for topic in "${topics[@]}"; do
            IFS=":" read -r name partitions <<< "$topic"

            echo "Checking topic: $name"

            if kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic "$name" >/dev/null 2>&1; then
                echo "  → Topic $name already exists"
            else
                echo "  → Creating topic: $name"
                kafka-topics.sh --create \
                    --topic "$name" \
                    --partitions "$partitions" \
                    --replication-factor 1 \
                    --bootstrap-server kafka-1:19092
                echo "  → Topic $name created"
            fi
        done

        echo "✅ Kafka topics ready"
    '
}

# ------------------------------------------------------------
# 1. Start PostGIS (CRE)
# ------------------------------------------------------------
start_postgis() {
    echo "=== Starting PostGIS (CRE) ==="
    docker compose -f "$COMPOSE" up -d --no-deps postgis-cre
    wait_for_health postgis-cre
}

# ------------------------------------------------------------
# 2. Start Prometheus
# ------------------------------------------------------------
start_prometheus() {
    if ! service_exists prometheus; then
        echo "Prometheus service not defined — skipping"
        return
    fi

    echo "=== Starting Prometheus ==="
    docker compose -f "$COMPOSE" up -d --no-deps prometheus

    # Prometheus has no healthcheck by default
    sleep 3
    echo "Prometheus started"
}

# ------------------------------------------------------------
# 3. Start Kafka brokers (Bitnami KRaft)
# ------------------------------------------------------------
start_kafka() {
    echo "=== Starting Kafka brokers ==="
    docker compose -f "$COMPOSE" up -d kafka-1
}

# ------------------------------------------------------------
# 4. Start Flink cluster
# ------------------------------------------------------------
start_flink() {
    echo "=== Starting Flink JobManager ==="
    docker compose -f "$COMPOSE" up -d jobmanager

    echo "=== Starting Flink TaskManager ==="
    docker compose -f "$COMPOSE" up -d taskmanager

    sleep 5
}

# ------------------------------------------------------------
# 5. Start Flink job (Article 01 Architecture)
# ------------------------------------------------------------
start_flink_job() {
    echo "=== Starting GeoFlink Architecture Job ==="
    docker compose -f "$COMPOSE" up -d geoflink_architecture_job
}

# ------------------------------------------------------------
# 6. Start Geo Producer
# ------------------------------------------------------------
start_geo_producer() {
    echo "=== Starting Geo Producer ==="
    docker compose -f "$COMPOSE" up -d geo_producer_architecture
}

# ------------------------------------------------------------
# 7. Start Kafdrop (Kafka UI)
# ------------------------------------------------------------
start_kafdrop() {
    if service_exists kafdrop; then
        echo "=== Starting Kafdrop (Kafka UI) ==="
        docker compose -f "$COMPOSE" up -d kafdrop
    else
        echo "Kafdrop service not defined — skipping"
    fi
}

# ------------------------------------------------------------
# MAIN EXECUTION ORDER
# ------------------------------------------------------------
start_postgis
start_prometheus
start_kafka
create_kafka_topics
start_flink
start_flink_job
start_geo_producer
# start_kafdrop

echo "=== System startup complete ==="

echo ""
echo "============================================================"
echo "  ACCESS POINTS (Local Browser)"
echo "============================================================"

echo "Prometheus:"
echo "  URL: http://localhost:9091"
echo ""

echo "Flink Dashboard:"
echo "  URL: http://localhost:8081"
echo ""

echo "PostGIS:"
echo "  Host: localhost"
echo "  Port: 5435"
echo ""

echo "Kafka Broker (Bitnami KRaft):"
echo "  Broker 1: PLAINTEXT://localhost:19092"
echo ""

echo "Geo Producer:"
echo "  Streams events into Kafka topic 'spatial-events'"
echo ""

echo "Kafdrop (Kafka UI):"
echo "  URL: http://localhost:9003"
echo ""

echo "Flink Metrics Endpoint:"
echo "  URL: http://localhost:9000/metrics"
echo ""

echo "============================================================"
echo "  SYSTEM READY"
echo "============================================================"
