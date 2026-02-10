#!/bin/bash

COMPOSE_FILE="docker/docker-compose.yml"

log() {
    echo "[ $(date '+%Y-%m-%d %H:%M:%S') ] $1"
}

# Check if service is running
is_up() {
    docker compose -f "$COMPOSE_FILE" ps "$1" | grep -q "Up"
}

# Wait for Kafka
check_kafka() {
    log "Waiting for Kafka brokers..."

    for i in {1..15}; do
        if docker exec kafka-1 kafka-broker-api-versions.sh --bootstrap-server kafka-1:19092 >/dev/null 2>&1; then
            log "Kafka-1 is responding."
            return 0
        fi
        log "Kafka not ready yet ($i/15)..."
        sleep 3
    done

    log "ERROR: Kafka did not start."
    exit 1
}

# Create topic
create_topic() {
    log "Creating topic spatial-events..."

    docker exec kafka-1 bash -c '
        kafka-topics.sh --create \
            --topic spatial-events \
            --partitions 4 \
            --replication-factor 2 \
            --bootstrap-server kafka-1:19092 \
            --if-not-exists
    '

    log "Topic created."
}

# Start producer
start_producer() {
    log "Starting geo_producer_architecture..."
    docker compose -f "$COMPOSE_FILE" up -d geo_producer_architecture

    if is_up geo_producer_architecture; then
        log "Producer is running."
    else
        log "ERROR: Producer failed to start."
        docker compose -f "$COMPOSE_FILE" logs geo_producer_architecture --tail=50
        exit 1
    fi
}

# Consume messages
consume_messages() {
    log "Consuming messages from Kafka (press Ctrl+C to stop)..."

    docker exec -it kafka-1 kafka-console-consumer.sh \
        --bootstrap-server kafka-1:19092 \
        --topic spatial-events \
        --from-beginning
}

# MAIN
log "=== Testing geo_producer_architecture ==="

log "1. Starting Kafka..."
docker compose -f "$COMPOSE_FILE" up -d kafka-1 kafka-2
check_kafka

log "2. Creating topic..."
create_topic

log "3. Starting producer..."
start_producer

log "4. Reading messages..."
consume_messages
