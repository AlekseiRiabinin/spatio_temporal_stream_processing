#!/bin/bash
set -e

COMPOSE="docker/docker-compose.yml"

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
# 1. Start PostGIS (PostgreSQL 18+)
# ------------------------------------------------------------
start_postgis() {
    echo "=== Starting PostGIS (CRE) ==="
    docker compose -f "$COMPOSE" up -d --no-deps postgis-cre
    wait_for_health postgis-cre
}

# ------------------------------------------------------------
# 2. Start Redis
# ------------------------------------------------------------
start_redis() {
    if ! service_exists redis-cre; then
        echo "Redis service not defined — skipping"
        return
    fi

    echo "=== Starting Redis (CRE) ==="
    docker compose -f "$COMPOSE" up -d --no-deps redis-cre

    for i in {1..10}; do
        if docker exec redis-cre redis-cli ping >/dev/null 2>&1; then
            echo "Redis is ready"
            return
        fi
        echo "Waiting for Redis... ($i/10)"
        sleep 2
    done

    echo "ERROR: Redis failed to start"
    docker compose -f "$COMPOSE" logs redis-cre --tail=20
    exit 1
}

# ------------------------------------------------------------
# 3. Start Tegola
# ------------------------------------------------------------
start_tegola() {
    if ! service_exists tegola-cre; then
        echo "Tegola service not defined — skipping"
        return
    fi

    echo "=== Starting Tegola (CRE) ==="
    docker compose -f "$COMPOSE" up -d --no-deps tegola-cre

    for i in {1..10}; do
        if curl -s http://localhost:8085 >/dev/null 2>&1; then
            echo "Tegola is ready"
            return
        fi
        echo "Waiting for Tegola... ($i/10)"
        sleep 3
    done

    echo "ERROR: Tegola failed to start"
    docker compose -f "$COMPOSE" logs tegola-cre --tail=20
    exit 1
}

# ------------------------------------------------------------
# 4. Start Kafka brokers
# ------------------------------------------------------------
start_kafka() {
    echo "=== Starting Kafka brokers ==="
    docker compose -f "$COMPOSE" up -d kafka-1 kafka-2
}

# ------------------------------------------------------------
# 5. Start Flink cluster
# ------------------------------------------------------------
start_flink() {
    echo "=== Starting Flink JobManager ==="
    docker compose -f "$COMPOSE" up -d jobmanager

    echo "=== Starting Flink TaskManager ==="
    docker compose -f "$COMPOSE" up -d taskmanager

    sleep 5
}

# ------------------------------------------------------------
# 6. Start Flink job
# ------------------------------------------------------------
start_flink_job() {
    echo "=== Starting GeoFlink Architecture Job ==="
    docker compose -f "$COMPOSE" up -d geoflink_architecture_job
}

# ------------------------------------------------------------
# 7. Start Geo Producer
# ------------------------------------------------------------
start_geo_producer() {
    echo "=== Starting Geo Producer ==="
    docker compose -f "$COMPOSE" up -d geo_producer_architecture
}

# ------------------------------------------------------------
# 8. Start pgAdmin
# ------------------------------------------------------------
start_pgadmin() {
    if ! service_exists pgadmin-cre; then
        echo "pgAdmin service not defined — skipping"
        return
    fi

    echo "=== Starting pgAdmin ==="
    docker compose -f "$COMPOSE" up -d pgadmin-cre
}

# ------------------------------------------------------------
# MAIN EXECUTION ORDER
# ------------------------------------------------------------
start_postgis
# start_redis
# start_tegola
start_kafka
start_flink
start_flink_job
start_geo_producer
# start_pgadmin

echo "=== System startup complete ==="

echo ""
echo "============================================================"
echo "  ACCESS POINTS (Local Browser)"
echo "============================================================"

echo "PostGIS (no UI, use pgAdmin or psql):"
echo "  Host: localhost"
echo "  Port: 5435"
echo "  Database: cre_db"
echo "  User: cre_user"
echo ""

echo "pgAdmin:"
echo "  URL: http://localhost:5051"
echo "  Login email: admin@localhost.localdomain"
echo "  Password: admin"
echo ""

echo "Tegola Vector Tile Server:"
echo "  URL: http://localhost:8085"
echo "  Tiles: http://localhost:8085/maps/{z}/{x}/{y}.pbf"
echo ""

echo "Kafka Brokers:"
echo "  Broker 1: PLAINTEXT://localhost:19092"
echo "  Broker 2: PLAINTEXT://localhost:19094"
echo ""

echo "Flink Dashboard:"
echo "  URL: http://localhost:8081"
echo ""

echo "Geo Producer (no UI):"
echo "  Streams events into Kafka topic 'spatial-events'"
echo ""

echo "============================================================"
echo "  SYSTEM READY"
echo "============================================================"
