#!/bin/bash

COMPOSE_FILE="docker/docker-compose.yml"

log() {
    echo "[ $(date '+%Y-%m-%d %H:%M:%S') ] $1"
}

# ------------------------------------------------------------
# 1. Check if a service is running
# ------------------------------------------------------------
is_up() {
    docker compose -f "$COMPOSE_FILE" ps "$1" | grep -q "Up"
}

# ------------------------------------------------------------
# 2. Wait for Kafka cluster to form
# ------------------------------------------------------------
check_kafka() {
    log "Waiting for Kafka brokers to respond..."

    for i in {1..15}; do
        if docker exec kafka-1 kafka-broker-api-versions.sh --bootstrap-server kafka-1:19092 >/dev/null 2>&1 && \
           docker exec kafka-2 kafka-broker-api-versions.sh --bootstrap-server kafka-2:19094 >/dev/null 2>&1; then
            log "Kafka brokers are responding."

            # Check KRaft quorum
            if docker exec kafka-1 kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092 describe --status 2>/dev/null | grep -q "LeaderId"; then
                log "Kafka KRaft quorum is healthy."
                return 0
            fi
        fi

        log "Kafka not ready yet... retrying ($i/15)"
        sleep 5
    done

    log "ERROR: Kafka cluster did not form."
    exit 1
}

# ------------------------------------------------------------
# 3. Create required Kafka topics
# ------------------------------------------------------------
create_topics() {
    log "Creating Kafka topics..."

    docker exec kafka-1 bash -c '
        kafka-topics.sh --create \
            --topic spatial-events \
            --partitions 4 \
            --replication-factor 2 \
            --bootstrap-server kafka-1:19092 \
            --if-not-exists
    '

    log "Kafka topics created."
}

# ------------------------------------------------------------
# 4. Start selected Geo Producer
# ------------------------------------------------------------
start_geo_producer() {
    local producer_service=$1

    log "Starting Geo Producer: $producer_service"
    docker compose -f "$COMPOSE_FILE" up -d "$producer_service"

    if is_up "$producer_service"; then
        log "Geo Producer '$producer_service' is running."
    else
        log "ERROR: Geo Producer '$producer_service' failed to start."
        docker compose -f "$COMPOSE_FILE" logs "$producer_service" --tail=50
        exit 1
    fi
}

# ------------------------------------------------------------
# 5. Start PostGIS and Redis
# ------------------------------------------------------------
start_storage() {
    log "Starting PostGIS and Redis..."
    docker compose -f "$COMPOSE_FILE" up -d postgis-cre redis-cre

    log "Waiting for PostGIS..."
    sleep 10

    if docker exec postgis-cre pg_isready -U cre_user >/dev/null 2>&1; then
        log "PostGIS is ready."
    else
        log "WARNING: PostGIS not responding yet."
    fi
}

# ------------------------------------------------------------
# 6. Start Flink cluster
# ------------------------------------------------------------
start_flink() {
    log "Starting Flink JobManager and TaskManager..."
    docker compose -f "$COMPOSE_FILE" up -d jobmanager taskmanager

    log "Waiting for Flink JobManager..."
    for i in {1..20}; do
        if curl -s http://localhost:8081 | grep -q "Flink Web Dashboard"; then
            log "Flink JobManager is ready."
            break
        fi
        sleep 3
    done
}

# ------------------------------------------------------------
# 7. Start Flink job
# ------------------------------------------------------------
start_flink_job() {
    log "Starting Flink job container..."
    docker compose -f "$COMPOSE_FILE" up -d geoflink_architecture_job
    sleep 10
    log "Flink job container started."
}

# ------------------------------------------------------------
# 8. Start visualization layer
# ------------------------------------------------------------
start_visualization() {
    log "Starting Tegola and pgAdmin..."
    docker compose -f "$COMPOSE_FILE" up -d tegola-cre pgadmin-cre
}

# ------------------------------------------------------------
# MAIN EXECUTION
# ------------------------------------------------------------
log "=== Starting Spatio-Temporal Architecture ==="

log "1. Starting Kafka brokers..."
docker compose -f "$COMPOSE_FILE" up -d kafka-1 kafka-2
check_kafka
create_topics

log "2. Starting Geo Producer..."
# Change this to any producer you want to run:
start_geo_producer "geo_producer_architecture"

log "3. Starting storage layer..."
start_storage

log "4. Starting Flink..."
start_flink
start_flink_job

log "5. Starting visualization..."
start_visualization

log "=== System started successfully ==="
docker compose -f "$COMPOSE_FILE" ps
