#!/bin/bash
set -e

COMPOSE="docker/docker-compose.stream-models.yml"

MODEL=${1:-dataflow}
WINDOW=${2:-adaptive}

echo "=== Starting Stream Models System ==="
echo "Model: $MODEL"
echo "Window Strategy: $WINDOW"

# ------------------------------------------------------------
# Utility: check if service exists
# ------------------------------------------------------------
service_exists() {
    docker compose -f "$COMPOSE" config --services | grep -q "^$1$"
}

# ------------------------------------------------------------
# Utility: wait for service
# ------------------------------------------------------------
wait_for_jobmanager() {
    echo "Waiting for Flink JobManager..."

    for i in {1..20}; do
        if curl -s http://localhost:8081 >/dev/null; then
            echo "Flink JobManager ready"
            return
        fi
        sleep 3
    done

    echo "ERROR: JobManager not ready"
    exit 1
}

# ------------------------------------------------------------
# Kafka topics
# ------------------------------------------------------------
create_kafka_topics() {

    echo "=== Creating Kafka topics ==="
    sleep 5

    docker exec kafka-1 bash -c '
        topics=("spatial-events:4")

        for topic in "${topics[@]}"; do
            IFS=":" read -r name partitions <<< "$topic"

            if kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic "$name" >/dev/null 2>&1; then
                echo "Topic exists: $name"
            else
                echo "Creating topic: $name"
                kafka-topics.sh --create \
                    --topic "$name" \
                    --partitions "$partitions" \
                    --replication-factor 1 \
                    --bootstrap-server kafka-1:19092
            fi
        done
    '
}

# ------------------------------------------------------------
# Infrastructure
# ------------------------------------------------------------
start_kafka() {
    echo "=== Kafka ==="
    docker compose -f "$COMPOSE" up -d kafka-1
}

start_flink() {
    echo "=== Flink Cluster ==="
    docker compose -f "$COMPOSE" up -d jobmanager
    docker compose -f "$COMPOSE" up -d taskmanager
    wait_for_jobmanager
}

start_prometheus() {
    if service_exists prometheus; then
        echo "=== Prometheus ==="
        docker compose -f "$COMPOSE" up -d prometheus
    fi
}

start_kafdrop() {
    if service_exists kafdrop; then
        echo "=== Kafdrop ==="
        docker compose -f "$COMPOSE" up -d kafdrop
    fi
}

start_producer() {
    echo "=== Geo Producer ==="
    docker compose -f "$COMPOSE" up -d geo_producer_architecture
}

# ------------------------------------------------------------
# Start Flink Job
# ------------------------------------------------------------
start_job() {

    echo "=== Stream Model Job ==="
    echo "STREAM_MODEL=$MODEL"
    echo "WINDOW_STRATEGY=$WINDOW"

    STREAM_MODEL=$MODEL \
    WINDOW_STRATEGY=$WINDOW \
    docker compose -f "$COMPOSE" up -d geoflink_stream_models_job
}

# ------------------------------------------------------------
# Matrix Experiments
# ------------------------------------------------------------
run_matrix() {

    MODELS=("dataflow" "microbatch" "actor" "log")
    WINDOWS=("session" "dynamic" "adaptive" "multitrigger")

    for m in "${MODELS[@]}"; do
        for w in "${WINDOWS[@]}"; do

            echo ""
            echo "===================================="
            echo "Running: $m + $w"
            echo "===================================="

            STREAM_MODEL=$m \
            WINDOW_STRATEGY=$w \
            docker compose -f "$COMPOSE" up -d geoflink_stream_models_job

            sleep 20

            docker compose -f "$COMPOSE" stop geoflink_stream_models_job

        done
    done
}

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

if [ "$MODEL" = "matrix" ]; then

    start_kafka
    create_kafka_topics
    start_flink
    start_prometheus
    start_kafdrop
    start_producer

    run_matrix

else

    start_kafka
    create_kafka_topics
    start_flink
    start_prometheus
    start_kafdrop
    start_job
    start_producer

fi

echo ""
echo "======================================"
echo "System Ready"
echo "======================================"

echo "Flink UI: http://localhost:8081"
echo "Prometheus: http://localhost:9091"
echo "Kafdrop: http://localhost:9003"


# Example:
# ./start-system-stream-models.sh dataflow adaptive
