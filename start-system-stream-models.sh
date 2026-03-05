#!/bin/bash
set -e

COMPOSE="docker/docker-compose.stream-models.yml"

MODEL=${1:-dataflow}
WINDOW=${2:-adaptive}

echo "=== Starting Stream Models System ==="
echo "Stream Model:    $MODEL"
echo "Window Strategy: $WINDOW"

# ------------------------------------------------------------
# Set experiment parameters early (before any docker compose)
# ------------------------------------------------------------
set_params() {
    MODEL=$1
    WINDOW=$2

    case "$WINDOW" in
        session)
            EVENT_RATE=1
            WINDOW_SIZE=3
            COUNT_THRESHOLD=100
            PROCESSING_INTERVAL_MS=5000
            DENSITY_FACTOR=1.0
            ;;
        dynamic)
            EVENT_RATE=50
            WINDOW_SIZE=5
            COUNT_THRESHOLD=100
            PROCESSING_INTERVAL_MS=5000
            DENSITY_FACTOR=1.0
            ;;
        adaptive)
            EVENT_RATE=50
            WINDOW_SIZE=5
            COUNT_THRESHOLD=100
            PROCESSING_INTERVAL_MS=5000
            DENSITY_FACTOR=1.0
            ;;
        multitrigger)
            EVENT_RATE=50
            WINDOW_SIZE=10
            COUNT_THRESHOLD=100
            PROCESSING_INTERVAL_MS=5000
            DENSITY_FACTOR=1.0
            ;;
    esac
}

# Load parameters for single-run mode
if [ "$MODEL" != "matrix" ]; then
    set_params "$MODEL" "$WINDOW"
fi

# Export BEFORE any docker compose command
export STREAM_MODEL="$MODEL"
export WINDOW_STRATEGY="$WINDOW"
export EVENT_RATE="$EVENT_RATE"
export WINDOW_SIZE="$WINDOW_SIZE"
export COUNT_THRESHOLD="$COUNT_THRESHOLD"
export PROCESSING_INTERVAL_MS="$PROCESSING_INTERVAL_MS"
export DENSITY_FACTOR="$DENSITY_FACTOR"

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
    docker compose -f "$COMPOSE" up -d geo_producer_stream_models
}


# ------------------------------------------------------------
# Start Flink Job
# ------------------------------------------------------------
start_job() {

    echo "=== Stream Model Job ==="
    echo "STREAM_MODEL=$MODEL"
    echo "WINDOW_STRATEGY=$WINDOW"
    echo "EVENT_RATE=$EVENT_RATE"
    echo "WINDOW_SIZE=$WINDOW_SIZE"
    echo "COUNT_THRESHOLD=$COUNT_THRESHOLD"
    echo "PROCESSING_INTERVAL_MS=$PROCESSING_INTERVAL_MS"
    echo "DENSITY_FACTOR=$DENSITY_FACTOR"

    export STREAM_MODEL WINDOW_STRATEGY EVENT_RATE WINDOW_SIZE COUNT_THRESHOLD PROCESSING_INTERVAL_MS DENSITY_FACTOR

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

            set_params "$m" "$w"

            export STREAM_MODEL="$m" WINDOW_STRATEGY="$w" \
                   EVENT_RATE WINDOW_SIZE COUNT_THRESHOLD PROCESSING_INTERVAL_MS DENSITY_FACTOR

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
    # start_kafdrop
    start_producer

    run_matrix

else

    set_params "$MODEL" "$WINDOW"

    start_kafka
    create_kafka_topics
    start_flink
    start_prometheus
    # start_kafdrop
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
# ./start-system-stream-models.sh dataflow session
